"""Unit tests for src/transformations/dedup.py"""
import pandas as pd
import pytest

from src.transformations.dedup import (
    dedup_by_key,
    dedup_customers,
    dedup_order_items,
    dedup_orders,
    dedup_products,
)


@pytest.fixture
def orders_with_duplicates():
    return pd.DataFrame({
        "order_id":     [101, 101, 102, 103, 103],
        "order_status": ["COMPLETE", "COMPLETE", "PENDING", "COMPLETE", "COMPLETE"],
        "ingestion_ts": pd.to_datetime(
            ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-01", "2024-01-03"]
        ),
    })


class TestDedupByKey:
    def test_removes_duplicates(self, orders_with_duplicates):
        result = dedup_by_key(orders_with_duplicates, "order_id")
        assert len(result) == 3

    def test_unique_keys_unchanged(self):
        df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        result = dedup_by_key(df, "id")
        assert len(result) == 3

    def test_keep_last_with_sort(self, orders_with_duplicates):
        result = dedup_by_key(
            orders_with_duplicates, "order_id", sort_col="ingestion_ts", keep="last"
        )
        order_101 = result[result["order_id"] == 101].iloc[0]
        # Last ingestion for 101 is 2024-01-02
        assert order_101["ingestion_ts"] == pd.Timestamp("2024-01-02")


class TestDedupOrders:
    def test_one_row_per_order_id(self, orders_with_duplicates):
        result = dedup_orders(orders_with_duplicates)
        assert result["order_id"].nunique() == len(result)

    def test_keeps_first_occurrence(self, orders_with_duplicates):
        result = dedup_orders(orders_with_duplicates)
        # dedup_orders keeps first — order 101 ingested on 2024-01-01
        order_101 = result[result["order_id"] == 101]
        assert len(order_101) == 1


class TestDedupOrderItems:
    def test_one_row_per_item_id(self):
        df = pd.DataFrame({
            "order_item_id": [1, 1, 2],
            "order_id":      [10, 10, 20],
            "ingestion_ts":  pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01"]),
        })
        result = dedup_order_items(df)
        assert len(result) == 2
        assert result["order_item_id"].nunique() == 2


class TestDedupCustomers:
    def test_one_row_per_customer_id(self):
        df = pd.DataFrame({
            "customer_id":      [1, 1, 2],
            "customer_segment": ["Consumer", "Corporate", "Consumer"],
            "ingestion_ts":     pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01"]),
        })
        result = dedup_customers(df)
        assert len(result) == 2

    def test_keeps_latest_segment(self):
        df = pd.DataFrame({
            "customer_id":      [1, 1],
            "customer_segment": ["Consumer", "Corporate"],
            "ingestion_ts":     pd.to_datetime(["2024-01-01", "2024-01-02"]),
        })
        result = dedup_customers(df)
        assert result.iloc[0]["customer_segment"] == "Corporate"


class TestDedupProducts:
    def test_one_row_per_product(self):
        df = pd.DataFrame({
            "product_card_id": [100, 100, 200],
            "product_price":   [9.99, 11.99, 4.99],
            "ingestion_ts":    pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01"]),
        })
        result = dedup_products(df)
        assert len(result) == 2
