"""
late_delivery_classifier
========================
Trains a late-delivery risk classifier on warehouse data and writes
predictions back to mart_late_delivery_risk.

Model pipeline:
  - Features: shipping_mode, order_region, customer_segment, product_category
              (one-hot encoded) + days_for_shipment_scheduled, order_item_quantity
              (scaled)
  - Estimator: RandomForestClassifier — interpretable, handles class imbalance
               well with class_weight='balanced', no hyperparameter tuning needed
               for a portfolio-grade baseline
  - Output: predicted_risk_score (float probability of late), predicted_is_late
            (binary threshold 0.50), model_version, prediction_ts

Idempotency:
  predict() re-scores ALL rows in mart_late_delivery_risk every run.  The
  DELETE+INSERT pattern in the mart SQL means rows may change each run, so a
  full re-score is the correct approach.

Usage:
  clf = LateDeliveryClassifier()
  clf.train(engine)
  clf.save_model("/opt/airflow/models/late_delivery_clf.pkl")

  # Later (or from the Airflow DAG):
  clf = LateDeliveryClassifier.load_model("/opt/airflow/models/late_delivery_clf.pkl")
  clf.predict(engine)
"""
from __future__ import annotations

import hashlib
import logging
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import Engine, text

log = logging.getLogger(__name__)

MODEL_VERSION_PREFIX = "rf_v1"

# Categorical features one-hot encoded; numeric features standard-scaled
CATEGORICAL_FEATURES = [
    "shipping_mode",
    "order_region",
    "customer_segment",
    "product_category",
]
NUMERIC_FEATURES = [
    "days_for_shipment_scheduled",
    "order_item_quantity",
]
ALL_FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES
TARGET = "actual_is_late"


class LateDeliveryClassifier:
    """Thin wrapper around an sklearn Pipeline for late-delivery prediction."""

    def __init__(self) -> None:
        self.pipeline: Any = None
        self.model_version: str = ""
        self.trained_at: datetime | None = None

    # ── Training ──────────────────────────────────────────────────────────────

    def train(self, engine: Engine) -> dict[str, Any]:
        """Train on rows with a known actual_is_late outcome.

        Requires warehouse data to be populated — mart rows without actual_is_late
        are excluded from training (they are live orders, not resolved deliveries).

        Returns a dict of metrics: n_train, accuracy, precision, recall, f1.
        """
        from sklearn.compose import ColumnTransformer
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import (
            accuracy_score,
            f1_score,
            precision_score,
            recall_score,
        )
        from sklearn.model_selection import train_test_split
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler

        log.info("Loading training data from mart_late_delivery_risk …")
        df = self._load_training_data(engine)

        if df.empty:
            raise RuntimeError(
                "No training rows found in mart_late_delivery_risk with actual_is_late set. "
                "Run the full pipeline first so historical orders populate the mart."
            )

        log.info("Training rows: %d  (late=%d, on-time=%d)",
                 len(df), df[TARGET].sum(), (df[TARGET] == 0).sum())

        X = df[ALL_FEATURES]
        y = df[TARGET].astype(int)

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.20, random_state=42, stratify=y
        )

        preprocessor = ColumnTransformer(
            transformers=[
                (
                    "cat",
                    OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                    CATEGORICAL_FEATURES,
                ),
                ("num", StandardScaler(), NUMERIC_FEATURES),
            ]
        )

        self.pipeline = Pipeline(
            steps=[
                ("prep", preprocessor),
                (
                    "clf",
                    RandomForestClassifier(
                        n_estimators=200,
                        max_depth=12,
                        min_samples_leaf=5,
                        class_weight="balanced",   # handles late/on-time imbalance
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        )

        self.pipeline.fit(X_train, y_train)

        y_pred = self.pipeline.predict(X_test)
        metrics = {
            "n_train":  len(X_train),
            "n_test":   len(X_test),
            "accuracy": round(float(accuracy_score(y_test, y_pred)), 4),
            "precision": round(float(precision_score(y_test, y_pred, zero_division=0)), 4),
            "recall":    round(float(recall_score(y_test, y_pred, zero_division=0)), 4),
            "f1":        round(float(f1_score(y_test, y_pred, zero_division=0)), 4),
        }

        # Model version = short hash of training set fingerprint so we can
        # detect when the model needs retraining without re-running a full fit.
        fingerprint = f"{len(df)}_{df[TARGET].sum()}_{df['order_id'].max()}"
        version_hash = hashlib.md5(fingerprint.encode()).hexdigest()[:8]
        self.model_version = f"{MODEL_VERSION_PREFIX}_{version_hash}"
        self.trained_at    = datetime.now(timezone.utc)

        log.info(
            "Training complete | version=%s | acc=%.4f | prec=%.4f | rec=%.4f | f1=%.4f",
            self.model_version, metrics["accuracy"], metrics["precision"],
            metrics["recall"], metrics["f1"],
        )
        return metrics

    # ── Prediction ────────────────────────────────────────────────────────────

    def predict(self, engine: Engine) -> int:
        """Score all rows in mart_late_delivery_risk.

        Updates predicted_risk_score, predicted_is_late, model_version,
        and prediction_ts on every row.

        Returns: number of rows scored.
        """
        if self.pipeline is None:
            raise RuntimeError("Model not trained. Call train() or load_model() first.")

        log.info("Loading mart rows for prediction …")
        df = self._load_prediction_data(engine)

        if df.empty:
            log.warning("No rows to score in mart_late_delivery_risk.")
            return 0

        X = df[ALL_FEATURES].copy()
        # Fill nulls so the pipeline doesn't blow up on unseen data
        for col in CATEGORICAL_FEATURES:
            X[col] = X[col].fillna("Unknown")
        for col in NUMERIC_FEATURES:
            X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0)

        proba  = self.pipeline.predict_proba(X)[:, 1]   # P(late)
        labels = (proba >= 0.50).astype(int)

        df["predicted_risk_score"] = np.round(proba, 6)
        df["predicted_is_late"]    = labels
        df["model_version"]        = self.model_version
        df["prediction_ts"]        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        rows_updated = self._write_predictions(engine, df)
        log.info("Predictions written | rows=%d | model=%s", rows_updated, self.model_version)
        return rows_updated

    # ── Persistence ───────────────────────────────────────────────────────────

    def save_model(self, path: str | Path) -> None:
        """Persist the trained pipeline to disk with pickle."""
        if self.pipeline is None:
            raise RuntimeError("Nothing to save — model is not trained.")
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            pickle.dump(
                {
                    "pipeline":      self.pipeline,
                    "model_version": self.model_version,
                    "trained_at":    self.trained_at,
                },
                f,
            )
        log.info("Model saved: %s (%s)", path, self.model_version)

    @classmethod
    def load_model(cls, path: str | Path) -> "LateDeliveryClassifier":
        """Load a pickled model and return a ready-to-predict classifier."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Model file not found: {path}")
        with path.open("rb") as f:
            state = pickle.load(f)
        instance = cls()
        instance.pipeline      = state["pipeline"]
        instance.model_version = state["model_version"]
        instance.trained_at    = state["trained_at"]
        log.info("Model loaded: %s (trained %s)", path, instance.model_version)
        return instance

    # ── Private helpers ───────────────────────────────────────────────────────

    def _load_training_data(self, engine: Engine) -> pd.DataFrame:
        """Pull resolved orders (actual_is_late IS NOT NULL) for training."""
        query = text("""
            select
                order_id,
                shipping_mode,
                order_region,
                customer_segment,
                coalesce(product_category, 'Unknown') as product_category,
                coalesce(days_for_shipment_scheduled, 0) as days_for_shipment_scheduled,
                coalesce(order_item_quantity, 1) as order_item_quantity,
                actual_is_late
            from marts.mart_late_delivery_risk
            where actual_is_late is not null
        """)
        with engine.connect() as conn:
            return pd.read_sql(query, conn)

    def _load_prediction_data(self, engine: Engine) -> pd.DataFrame:
        """Pull all mart rows to score (including already-predicted ones)."""
        query = text("""
            select
                order_id,
                shipping_mode,
                order_region,
                customer_segment,
                coalesce(product_category, 'Unknown') as product_category,
                coalesce(days_for_shipment_scheduled, 0) as days_for_shipment_scheduled,
                coalesce(order_item_quantity, 1) as order_item_quantity
            from marts.mart_late_delivery_risk
        """)
        with engine.connect() as conn:
            return pd.read_sql(query, conn)

    def _write_predictions(self, engine: Engine, df: pd.DataFrame) -> int:
        """Bulk UPDATE mart_late_delivery_risk with prediction columns."""
        if df.empty:
            return 0

        rows = df[
            ["order_id", "predicted_risk_score", "predicted_is_late",
             "model_version", "prediction_ts"]
        ].to_dict(orient="records")

        stmt = text("""
            update marts.mart_late_delivery_risk
            set
                predicted_risk_score = :predicted_risk_score,
                predicted_is_late    = :predicted_is_late,
                model_version        = :model_version,
                prediction_ts        = :prediction_ts
            where order_id = :order_id
        """)

        with engine.begin() as conn:
            conn.execute(stmt, rows)

        return len(rows)
