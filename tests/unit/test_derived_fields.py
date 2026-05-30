"""Unit tests for src/transformations/derived_fields.py"""
import pandas as pd
import pytest

from src.transformations.derived_fields import (
    compute_delivery_metrics,
    compute_profit_metrics,
    compute_supplier_score,
)


@pytest.fixture
def order_df():
    return pd.DataFrame({
        "days_for_shipping_real":      ["5", "3", "7", None],
        "days_for_shipment_scheduled": ["4", "3", "5", "3"],
        "sales_amount":                [100.0, 200.0, 0.0, 50.0],
        "order_profit":                [20.0,  -10.0, 0.0, 5.0],
    })


class TestDeliveryMetrics:
    def test_positive_delay_is_late(self, order_df):
        result = compute_delivery_metrics(order_df)
        assert result.loc[0, "delivery_delay_days"] == 1
        assert result.loc[0, "is_late_flag"] == 1

    def test_zero_delay_not_late(self, order_df):
        result = compute_delivery_metrics(order_df)
        assert result.loc[1, "delivery_delay_days"] == 0
        assert result.loc[1, "is_late_flag"] == 0

    def test_negative_delay_not_late(self, order_df):
        result = compute_delivery_metrics(order_df)
        assert result.loc[2, "delivery_delay_days"] == 2   # 7 - 5
        assert result.loc[2, "is_late_flag"] == 1

    def test_null_real_days_produces_null_delay(self, order_df):
        result = compute_delivery_metrics(order_df)
        assert pd.isna(result.loc[3, "delivery_delay_days"])

    def test_original_df_not_mutated(self, order_df):
        original_cols = set(order_df.columns)
        compute_delivery_metrics(order_df)
        assert set(order_df.columns) == original_cols


class TestProfitMetrics:
    def test_normal_margin(self, order_df):
        result = compute_profit_metrics(order_df)
        assert abs(result.loc[0, "profit_margin_pct"] - 0.2) < 1e-5

    def test_negative_margin(self, order_df):
        result = compute_profit_metrics(order_df)
        assert result.loc[1, "profit_margin_pct"] < 0

    def test_zero_sales_returns_null(self, order_df):
        result = compute_profit_metrics(order_df)
        assert pd.isna(result.loc[2, "profit_margin_pct"])


class TestSupplierScore:
    @pytest.fixture
    def supplier_df(self):
        # Row 0: on_time=1.0, margin=1.0, max volume → score=1.0 → platinum
        # Row 1: on_time=0.70, margin=0.20, mid volume → gold range
        # Row 2: on_time=0.55, margin=0.10, low volume → silver range
        # Row 3: on_time=0.40, margin=0.05, min volume → bronze
        return pd.DataFrame({
            "on_time_rate":      [1.00, 0.70, 0.55, 0.40],
            "avg_profit_margin": [1.00, 0.20, 0.10, 0.05],
            "total_orders":      [1000, 500,  200,  50],
        })

    def test_high_performer_is_platinum(self, supplier_df):
        # score = 1.0*0.5 + 1.0*0.3 + 1.0*0.2 = 1.0 → platinum
        result = compute_supplier_score(supplier_df)
        assert result.loc[0, "performance_tier"] == "platinum"

    def test_low_performer_is_bronze(self, supplier_df):
        result = compute_supplier_score(supplier_df)
        assert result.loc[3, "performance_tier"] == "bronze"

    def test_score_between_0_and_1(self, supplier_df):
        result = compute_supplier_score(supplier_df)
        assert (result["composite_score"].between(0, 1)).all()
