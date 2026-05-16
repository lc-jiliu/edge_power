"""
Train the IsolationForest model on synthetic baseline data.
Run once at container build time via: python -m detection.trainer

Generates ~5000 normal samples across all equipment types and saves the
trained model to models/isolation_forest.pkl.
"""
import os
import random
import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from datetime import datetime

MODEL_DIR  = os.getenv("MODEL_PATH", "models/isolation_forest.pkl")
RANDOM_STATE = 42

# Equipment baselines: (mean, std) per sensor type
BASELINES = {
    "temperature": (80.0, 6.0),
    "pressure":    (115.0, 10.0),
    "power":       (440.0, 25.0),
    "vibration":   (0.05, 0.012),
}


def _synthetic_sample(sensor_type: str, hour: int) -> list[float]:
    """Return a feature vector: [value, z_score_proxy, std_proxy, hour_sin, hour_cos]."""
    mean, std = BASELINES.get(sensor_type, (50.0, 5.0))
    value = mean + random.gauss(0, std)
    z     = random.gauss(0, 0.5)          # z-score proxy for normal data
    std_w = std * random.uniform(0.8, 1.2) # window std proxy
    h_sin = np.sin(2 * np.pi * hour / 24)
    h_cos = np.cos(2 * np.pi * hour / 24)
    return [value, z, std_w, h_sin, h_cos]


def train() -> None:
    os.makedirs(os.path.dirname(MODEL_DIR), exist_ok=True)

    print("Training IsolationForest on synthetic baseline data...")
    samples = []
    sensor_types = ["temperature", "pressure", "power", "vibration"]
    for _ in range(1000):          # 1000 samples per sensor type = 4000 total
        for sensor_type in sensor_types:
            hour = random.randint(0, 23)
            samples.append(_synthetic_sample(sensor_type, hour))

    X = np.array(samples)
    model = IsolationForest(
        n_estimators=200,
        contamination=0.02,   # expect ~2% anomaly rate in production
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X)

    joblib.dump(model, MODEL_DIR)
    print(f"Model saved to {MODEL_DIR} ({len(samples)} training samples)")


if __name__ == "__main__":
    train()
