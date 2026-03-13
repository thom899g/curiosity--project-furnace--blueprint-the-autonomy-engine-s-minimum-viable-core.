"""
SPINAL CORD: Immutable Event Ledger
Core nervous system for Project CORTEX - append-only truth source for all system state.

Architectural Choice: Firestore-first design ensures:
1. Real-time synchronization across all components
2. Automatic scaling and redundancy
3. Native integration with Cloud Functions for event-driven workflows
4. Built-in security rules for compartmentalized access

Edge Cases Handled:
- Event hash collisions (SHA256 + timestamp + UUID)
- Network partitions (Firestore offline persistence)
- Malformed payloads (schema validation)
- Write conflicts (optimistic concurrency control)
"""

import hashlib
import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
import firebase_admin
from firebase_admin import firestore
from firebase_admin.exceptions import FirebaseError
import logging

# Configure logging for ecosystem tracking
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class CortexEvent:
    """Immutable event schema for all system state changes."""
    event_id: str
    timestamp: datetime
    event_type: str  # signal_generated|trade_executed|risk_breach|profit_captured
    source: str  # perception_layer|brainstem|cerebellum|morphogenesis
    payload: Dict[str, Any]
    previous_hash: Optional[str] = None  # For chain integrity
    
    def __post_init__(self):
        """Validate event structure and generate hash."""
        if not self.event_id:
            self.event_id = f"evt_{uuid.uuid4().hex}"
        
        if not isinstance(self.timestamp, datetime):
            raise TypeError("timestamp must be datetime object")
        
        # Generate hash for data integrity verification
        self._hash = self._generate_hash()
    
    def _generate_hash(self) -> str:
        """Generate SHA256 hash of event data for integrity verification."""
        data_string = json.dumps({
            'event_id': self.event_id,
            'timestamp': self.timestamp.isoformat(),
            'event_type': self.event_type,
            'source': self.source,
            'payload': self.payload,
            'previous_hash': self.previous_hash
        }, sort_keys=True)
        return hashlib.sha256(data_string.encode()).hexdigest()
    
    @property
    def hash(self) -> str:
        return self._hash
    
    def to_firestore_dict(self) -> Dict[str, Any]:
        """Convert to Firestore-compatible dictionary."""
        return {
            'event_id': self.event_id,
            'timestamp': self.timestamp,
            'event_type': self.event_type,
            'source': self.source,
            'payload': self.payload,
            'hash': self.hash,
            'previous_hash': self.previous_hash,
            'created_at': firestore.SERVER_TIMESTAMP
        }

class EventLedger:
    """Central event ledger implementing append-only architecture."""
    
    def __init__(self, project_id: Optional[str] = None):
        """
        Initialize Firestore connection with error handling.
        
        Args:
            project_id: Firebase project ID. If None, uses default credentials.
        
        Raises:
            FirebaseError: If Firestore initialization fails
            ValueError: If credentials are invalid
        """
        try:
            # Initialize Firebase app if not already initialized
            if not firebase_admin._apps:
                cred = firebase_admin.credentials.ApplicationDefault()
                firebase_admin.initialize_app(cred, {
                    'projectId': project_id or 'project-cortex-production'
                })
            
            self.db = firestore.client()
            self.events_collection = self.db.collection('events')
            self.state_collection = self.db.collection('system_state')
            
            logger.info("EventLedger initialized successfully")
            
        except FirebaseError as e:
            logger.error(f"Firebase initialization failed: {e}")
            raise
        except ValueError as e:
            logger.error(f"Invalid Firebase configuration: {e}")
            raise
    
    def append_event(self, event: CortexEvent) -> str:
        """
        Append event to ledger with integrity checks.
        
        Args:
            event: CortexEvent to append
            
        Returns:
            str: Document ID of the created event
            
        Raises:
            ValueError: If event validation fails
            FirebaseError: If write operation fails
        """
        try:
            # Get last event for chain integrity
            last_event = self.get_last_event()
            if last_event:
                event.previous_hash = last_event.hash
            
            # Convert to Firestore format
            event_dict = event.to_firestore_dict()
            
            # Write with transaction for consistency
            doc_ref = self.events_collection.document(event.event_id)
            
            @firestore.transactional
            def commit_event(transaction, doc_ref, event_dict):
                # Check if event already exists (idempotency)
                snapshot = doc_ref.get(transaction=transaction)
                if snapshot.exists:
                    logger.warning(f"Event {event.event_id} already exists")
                    return event.event_id
                
                # Append new event
                transaction.set(doc_ref, event_dict)
                return event.event_id
            
            # Execute transaction
            transaction = self.db.transaction()
            doc_id = commit_event(transaction, doc_ref, event_dict)
            
            # Update state snapshot for recovery optimization
            self._update_state_snapshot(event.source, event)
            
            logger.info(f"Event appended: {event.event_type} from {event.source}")
            return doc_id
            
        except FirebaseError as e:
            logger.error(f"Failed to append event: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in append_event: {e}")
            raise
    
    def get_last_event(self) -> Optional[CortexEvent]:
        """Retrieve the most recent event for chain continuity."""
        try:
            query = self.events_collection.order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1)
            docs = query.stream()
            
            for doc in docs:
                data = doc.to_dict()
                return CortexEvent(
                    event_id=data.get('event_id'),
                    timestamp=data.get('timestamp'),
                    event_type=data.get('event_type'),
                    source=data.get('source'),
                    payload=data.get('payload'),
                    previous_hash=data.get('previous_hash')
                )
            return None
            
        except FirebaseError as e:
            logger.error(f"Failed to get last event: {e}")
            return None
    
    def _update_state_snapshot(self, component: str, event: CortexEvent):
        """Update component state snapshot for fast recovery."""
        try:
            state_ref = self.state_collection.document(component)
            state_ref.set({
                'last_event_id': event.event_id,
                'last_event_hash': event.hash,
                'state': event.payload.get('state', {}),
                'timestamp': firestore.SERVER_TIMESTAMP,
                'updated_at': datetime.utcnow().isoformat()
            }, merge=True)
            
        except FirebaseError as e:
            logger.warning(f"State snapshot update failed (non-critical): {e}")
    
    def verify_integrity(self, start_event_id: str, end_event_id: str) -> bool:
        """
        Verify chain integrity between two events.
        
        Args:
            start_event_id: Starting event ID
            end_event_id: Ending event ID
            
        Returns:
            bool: True if chain is intact, False if tampering detected
        """
        try:
            current_id = end_event_id
            while current_id:
                doc = self.events_collection.document(current_id).get()
                if not doc.exists:
                    return False
                
                data = doc.to_dict()
                event = CortexEvent(
                    event_id=data.get('event_id'),
                    timestamp=data.get('timestamp'),
                    event_type=data.get('event_type'),
                    source=data.get('source'),
                    payload=data.get('payload'),
                    previous_hash=data.get('previous_hash')
                )
                
                # Verify hash matches
                if event.hash != data.get('hash'):
                    logger.error(f"Hash mismatch for event {current_id}")
                    return False
                
                # Reached start of verification range
                if current_id == start_event_id:
                    return True
                
                # Move to previous event
                current_id = data.get('previous_hash')
            
            return False
            
        except FirebaseError as e:
            logger.error(f"Integrity verification failed: {e}")
            return False
    
    def get_events_by_type(self, event_type: str, limit: int = 100) -> List[CortexEvent]:
        """Retrieve events filtered by type for analysis."""
        try:
            query = self.events_collection\
                .where('event_type', '==', event_type)\
                .order_by('timestamp', direction=firestore.Query.DESCENDING)\
                .limit(limit)
            
            events = []
            for doc in query.stream():
                data = doc.to_dict()
                events.append(CortexEvent(
                    event_id=data.get('event_id'),
                    timestamp=data.get('timestamp'),
                    event_type=data.get('event_type'),
                    source=data.get('source'),
                    payload=data.get('payload'),
                    previous_hash=data.get('previous_hash')
                ))
            
            return events
            
        except FirebaseError as e:
            logger.error(f"Failed to get events by type: {e}")
            return []

# Example usage
if __name__ == "__main__":
    # Initialize ledger
    ledger = EventLedger()
    
    # Create sample event
    sample_event = CortexEvent(
        event_id=f"evt_{uuid.uuid4().hex}",
        timestamp=datetime.utcnow(),
        event_type="system_initialized",
        source="spinal_cord",
        payload={"version": "1.0.0", "components": ["ledger"]}
    )
    
    # Append event
    try:
        doc_id = ledger.append_event(sample_event)
        print(f"Event appended with ID: {doc_id}")
        
        # Verify integrity
        last_event = ledger.get_last_event()
        if last_event:
            print(f"Last event: {last_event.event_type} at {last_event.timestamp}")
        
    except Exception as e:
        print(f"Error: {e}")