"""Unit tests for LateDeliveryClassifier."""
from __future__ import annotations

import pickle
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.ml.late_delivery_classifier import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    LateDeliveryClassifier,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_order_df(n: int = 200, late_rate: float = 0.35) -> pd.DataFrame:
    """Minimal DataFrame matching the mart feature schema."""
    rng = np.random.default_rng(42)
    n_late = int(n * late_rate)
    return pd.DataFrame(
        {
            "order_id":                   range(1, n + 1),
            "shipping_mode":              rng.choice(
                ["Standard Class", "Second Class", "First Class", "Same Day"], n
            ),
            "order_region":               rng.choice(
                ["Western US", "Eastern Europe", "Southeast Asia", "Central US"], n
            ),
            "customer_segment":           rng.choice(
                ["Consumer", "Corporate", "Home Office"], n
            ),
            "product_category":           rng.choice(
                ["Electronics", "Apparel", "Furniture", "Food"], n
            ),
            "days_for_shipment_scheduled": rng.integers(1, 8, n),
            "order_item_quantity":         rng.integers(1, 10, n),
            "actual_is_late":             [1] * n_late + [0] * (n - n_late),
        }
    )


def _make_engine_mock(df: pd.DataFrame):
    """Return a mock Engine whose connect().execute() yields the given DataFrame."""
    mock_engine = MagicMock()
    mock_conn   = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__  = MagicMock(return_value=False)

    # pd.read_sql will call conn directly — patch it at module level
    return mock_engine


# ── Training tests ────────────────────────────────────────────────────────────

class TestTraining:
    def test_train_returns_metrics(self, tmp_path):
        """train() should return a dict with expected metric keys."""
        df = _make_order_df(300)
        clf = LateDeliveryClassifier()

        with patch("pandas.read_sql", return_value=df):
            metrics = clf.train(MagicMock())

        assert set(metrics.keys()) == {"n_train", "n_test", "accuracy", "precision", "recall", "f1"}
        assert 0.0 <= metrics["accuracy"] <= 1.0
        assert 0.0 <= metrics["f1"] <= 1.0

    def test_train_sets_model_version(self, tmp_path):
        """train() should populate model_version string."""
        df = _make_order_df(300)
        clf = LateDeliveryClassifier()

        with patch("pandas.read_sql", return_value=df):
            clf.train(MagicMock())

        assert clf.model_version.startswith("rf_v1_")
        assert len(clf.model_version) > len("rf_v1_")

    def test_train_raises_on_empty_data(self):
        """train() should raise RuntimeError when no training data is available."""
        clf = LateDeliveryClassifier()
        with patch("pandas.read_sql", return_value=pd.DataFrame()):
            with pytest.raises(RuntimeError, match="No training rows"):
                clf.train(MagicMock())

    def test_pipeline_is_set_after_training(self):
        """After train(), the sklearn Pipeline must be populated."""
        df = _make_order_df(200)
        clf = LateDeliveryClassifier()

        with patch("pandas.read_sql", return_value=df):
            clf.train(MagicMock())

        assert clf.pipeline is not None

    def test_train_different_seeds_produce_same_version_for_same_data(self):
        """Model version hash should be deterministic for the same dataset."""
        df = _make_order_df(200)
        clf1, clf2 = LateDeliveryClassifier(), LateDeliveryClassifier()

        with patch("pandas.read_sql", return_value=df):
            clf1.train(MagicMock())
        with patch("pandas.read_sql", return_value=df):
            clf2.train(MagicMock())

        assert clf1.model_version == clf2.model_version


# ── Prediction tests ──────────────────────────────────────────────────────────

class TestPrediction:
    def _trained_clf(self) -> LateDeliveryClassifier:
        df = _make_order_df(200)
        clf = LateDeliveryClassifier()
        with patch("pandas.read_sql", return_value=df):
            clf.train(MagicMock())
        return clf

    def test_predict_returns_row_count(self):
        """predict() should return the number of rows scored."""
        clf    = self._trained_clf()
        pred_df = _make_order_df(50).drop(columns=["actual_is_late"])

        mock_engine = MagicMock()
        with patch("pandas.read_sql", return_value=pred_df):
            with patch.object(clf, "_write_predictions", return_value=50):
                count = clf.predict(mock_engine)

        assert count == 50

    def test_predict_requires_trained_model(self):
        """predict() should raise RuntimeError on an untrained classifier."""
        clf = LateDeliveryClassifier()
        with pytest.raises(RuntimeError, match="not trained"):
            clf.predict(MagicMock())

    def test_predict_scores_are_probabilities(self):
        """Predicted risk scores must be in [0, 1]."""
        df  = _make_order_df(200)
        clf = LateDeliveryClassifier()
        with patch("pandas.read_sql", return_value=df):
            clf.train(MagicMock())

        # Build feature-only DataFrame
        X = df[ALL_FEATURES]
        proba = clf.pipeline.predict_proba(X)[:, 1]
        assert (proba >= 0).all() and (proba <= 1).all()

    def test_predict_handles_unknown_categories(self):
        """Classifier must not raise on categories unseen during training."""
        df  = _make_order_df(200)
        clf = LateDeliveryClassifier()
        with patch("pandas.read_sql", return_value=df):
            clf.train(MagicMock())

        # Add an unseen shipping_mode
        novel = df[ALL_FEATURES].copy().iloc[:5]
        novel["shipping_mode"] = "Teleportation"
        # Should not raise (OneHotEncoder handle_unknown='ignore')
        proba = clf.pipeline.predict_proba(novel)
        assert proba.shape == (5, 2)


# ── Persistence tests ─────────────────────────────────────────────────────────

class TestPersistence:
    def test_save_and_load_roundtrip(self, tmp_path):
        """A saved model must load and predict identically."""
        df  = _make_order_df(200)
        clf = LateDeliveryClassifier()
        with patch("pandas.read_sql", return_value=df):
            clf.train(MagicMock())

        model_file = tmp_path / "clf.pkl"
        clf.save_model(model_file)

        clf2 = LateDeliveryClassifier.load_model(model_file)
        assert clf2.model_version == clf.model_version
        assert clf2.pipeline is not None

        # Predictions from both pipelines must be identical
        X = df[ALL_FEATURES]
        np.testing.assert_array_almost_equal(
            clf.pipeline.predict_proba(X),
            clf2.pipeline.predict_proba(X),
        )

    def test_load_raises_on_missing_file(self, tmp_path):
        """load_model() must raise FileNotFoundError when pickle is absent."""
        with pytest.raises(FileNotFoundError):
            LateDeliveryClassifier.load_model(tmp_path / "nonexistent.pkl")

    def test_save_raises_on_untrained(self, tmp_path):
        """save_model() must raise RuntimeError before training."""
        clf = LateDeliveryClassifier()
        with pytest.raises(RuntimeError, match="not trained"):
            clf.save_model(tmp_path / "clf.pkl")

    def test_save_creates_parent_dirs(self, tmp_path):
        """save_model() must create parent directories if they don't exist."""
        df  = _make_order_df(200)
        clf = LateDeliveryClassifier()
        with patch("pandas.read_sql", return_value=df):
            clf.train(MagicMock())

        deep_path = tmp_path / "models" / "v1" / "clf.pkl"
        clf.save_model(deep_path)   # should not raise
        assert deep_path.exists()
