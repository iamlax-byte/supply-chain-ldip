"""Unit tests for src/transformations/scd2.py — pure logic only (no DB)."""
import pandas as pd
import pytest

from src.transformations.scd2 import compute_row_hash


class TestComputeRowHash:
    """Tests for the row_hash helper — no DB required."""

    def test_same_values_produce_same_hash(self):
        df = pd.DataFrame({
            "segment": ["Consumer", "Consumer"],
            "city":    ["Seattle", "Seattle"],
        })
        hashes = compute_row_hash(df, ["segment", "city"])
        assert hashes.iloc[0] == hashes.iloc[1]

    def test_different_values_produce_different_hashes(self):
        df = pd.DataFrame({
            "segment": ["Consumer", "Corporate"],
            "city":    ["Seattle",  "Seattle"],
        })
        hashes = compute_row_hash(df, ["segment", "city"])
        assert hashes.iloc[0] != hashes.iloc[1]

    def test_null_handled_consistently(self):
        df = pd.DataFrame({
            "segment": [None,  None],
            "city":    ["NYC", "NYC"],
        })
        hashes = compute_row_hash(df, ["segment", "city"])
        assert hashes.iloc[0] == hashes.iloc[1]

    def test_null_differs_from_empty_string(self):
        df = pd.DataFrame({
            "segment": [None, ""],
            "city":    ["NYC", "NYC"],
        })
        hashes = compute_row_hash(df, ["segment", "city"])
        assert hashes.iloc[0] != hashes.iloc[1]

    def test_hash_is_32_hex_chars(self):
        df = pd.DataFrame({"a": ["x"]})
        h = compute_row_hash(df, ["a"]).iloc[0]
        assert len(h) == 32
        assert all(c in "0123456789abcdef" for c in h)

    def test_case_insensitive(self):
        """Tracked attribute comparison should be case-insensitive."""
        df = pd.DataFrame({"segment": ["Consumer", "consumer"]})
        hashes = compute_row_hash(df, ["segment"])
        assert hashes.iloc[0] == hashes.iloc[1]

    def test_whitespace_trimmed(self):
        df = pd.DataFrame({"city": ["Seattle", " Seattle "]})
        hashes = compute_row_hash(df, ["city"])
        assert hashes.iloc[0] == hashes.iloc[1]

    def test_column_order_matters(self):
        """Hash over [A, B] must differ from [B, A] to prevent false matches."""
        df = pd.DataFrame({"a": ["x"], "b": ["y"]})
        h_ab = compute_row_hash(df, ["a", "b"])
        h_ba = compute_row_hash(df, ["b", "a"])
        assert h_ab.iloc[0] != h_ba.iloc[0]


class TestSCD2MergeLogic:
    """Tests for the merge classification logic (pandas-only, no DB)."""

    def _classify(self, staging_df, current_df, natural_key="customer_id",
                  surrogate_key="customer_key", tracked_cols=None):
        """Re-implement the pandas classification logic from merge_scd2 for testing."""
        from src.transformations.scd2 import compute_row_hash
        tracked_cols = tracked_cols or ["segment"]

        staging_df = staging_df.copy()
        staging_df["row_hash"] = compute_row_hash(staging_df, tracked_cols)

        merged = staging_df.merge(
            current_df.rename(columns={"row_hash": "existing_hash"}),
            on=natural_key, how="left",
        )
        is_new     = merged[surrogate_key].isna()
        is_changed = (~is_new) & (merged["row_hash"] != merged["existing_hash"])
        is_same    = (~is_new) & (merged["row_hash"] == merged["existing_hash"])

        return merged, is_new, is_changed, is_same

    def test_new_customer_classified_as_new(self):
        staging = pd.DataFrame({"customer_id": [99], "segment": ["Consumer"]})
        current = pd.DataFrame({"customer_id": [], "customer_key": [], "row_hash": []})
        _, is_new, is_changed, _ = self._classify(staging, current)
        assert is_new.all()

    def test_unchanged_customer_classified_as_same(self):
        from src.transformations.scd2 import compute_row_hash
        staging = pd.DataFrame({"customer_id": [1], "segment": ["Consumer"]})
        row_hash = compute_row_hash(staging, ["segment"]).iloc[0]
        current = pd.DataFrame({
            "customer_id":  [1],
            "customer_key": [100],
            "row_hash":     [row_hash],
        })
        _, is_new, is_changed, is_same = self._classify(staging, current)
        assert is_same.all()
        assert not is_changed.any()

    def test_changed_customer_classified_as_changed(self):
        staging = pd.DataFrame({"customer_id": [1], "segment": ["Corporate"]})
        current = pd.DataFrame({
            "customer_id":  [1],
            "customer_key": [100],
            "row_hash":     ["old_hash_that_wont_match"],
        })
        _, is_new, is_changed, is_same = self._classify(staging, current)
        assert is_changed.all()
        assert not is_same.any()
