"""
IsolationForest-based anomaly classifier.

Maintains a per-equipment sliding window of recent sensor readings to compute
contextual features (z-score, window std) before scoring with the model.

Classification thresholds:
  score < -0.50  → CRITICAL
  score < -0.30  → WARNING
  else           → NORMAL

IsolationForest score_samples returns negative values for anomalies;
more negative = more anomalous.
"""
import logging
import os
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional

import joblib
import numpy as np

from config import MODEL_PATH

logger = logging.getLogger(__name__)

THRESHOLD_CRITICAL = -0.50
THRESHOLD_WARNING  = -0.30
WINDOW_SIZE        = 50   # readings per equipment kept in memory


class AnomalyClassifier:
    def __init__(self, model_path: str = MODEL_PATH):
        if not os.path.exists(model_path):
            raise FileNotFoundError(
                f"Model not found at {model_path}. "
                "Run `python -m detection.trainer` first."
            )
        self.model   = joblib.load(model_path)
        self.windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
        logger.info("IsolationForest loaded from %s", model_path)

    def classify(self, event: dict) -> tuple[str, float]:
        """
        Classify a sensor event.
        Returns (severity, abs_anomaly_score).
        """
        eq_id   = event["equipment_id"]
        value   = float(event["value"])
        ts      = event.get("timestamp", datetime.now(timezone.utc).isoformat())

        features = self._extract_features(eq_id, value, ts)
        self.windows[eq_id].append(value)

        score = float(self.model.score_samples([features])[0])

        if score < THRESHOLD_CRITICAL:
            return "CRITICAL", abs(score)
        if score < THRESHOLD_WARNING:
            return "WARNING", abs(score)
        return "NORMAL", abs(score)

    def _extract_features(self, eq_id: str, value: float, timestamp: str) -> list[float]:
        """
        Feature vector: [value, z_score, window_std, hour_sin, hour_cos]
        - value:      raw sensor reading
        - z_score:    deviation from window mean, normalised by window std
        - window_std: rolling standard deviation of recent readings
        - hour_sin/cos: time-of-day encoding (handles cyclical nature)
        """
        window = list(self.windows[eq_id])
        if len(window) >= 2:
            mean = np.mean(window)
            std  = np.std(window) or 1.0
            z    = (value - mean) / std
        else:
            z   = 0.0
            std = 1.0

        try:
            dt    = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            hour  = dt.hour
        except Exception:
            hour  = 0

        h_sin = np.sin(2 * np.pi * hour / 24)
        h_cos = np.cos(2 * np.pi * hour / 24)

        return [value, z, std, h_sin, h_cos]
