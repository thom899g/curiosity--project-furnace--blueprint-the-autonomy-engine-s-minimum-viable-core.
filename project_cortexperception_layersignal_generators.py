"""
PERCEPTION LAYER: Adaptive Signal Generators (Ensemble)
Ensemble of specialized trading models with performance-weighted capital allocation.

Architectural Choice: Multi-generator consensus prevents single point of failure
and enables continuous model evolution. Each generator specializes in different
market regimes (momentum, mean reversion, volatility arbitrage).

Edge Cases Handled:
- Model divergence (consensus threshold configuration)
- Data staleness (TTL validation on market data)
- Overfitting detection (out-of-sample testing)
- Adversarial challenge protocol (random computation proofs)
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
import hashlib
import json
import logging
from enum import Enum
import firebase_admin
from firebase_admin import firestore

logger = logging.getLogger(__name__)

class ModelType(Enum):
    """Specialized generator types for different market regimes."""
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    VOLATILITY_ARB = "volatility_arbitrage"
    SENTIMENT = "sentiment_analysis"
    ON_CHAIN = "on_chain_metrics"

@dataclass
class PerformanceMetrics:
    """Comprehensive performance tracking for each generator."""
    accuracy: float = 0.0
    sharpe_ratio: float = 0.0
    total_pnl: float = 0.0
    win_rate: float = 0.0
    max_drawdown: float = 0.0
    trade_count: int = 0
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'accuracy': self.accuracy,
            'sharpe_ratio': self.sharpe_ratio,
            'total_pnl': self.total_pnl,
            'win_rate': self.win_rate,
            'max_drawdown': self.max_drawdown