#!/usr/bin/env bash
# =============================================================================
# download_data.sh
# Download the DataCo Smart Supply Chain dataset from Kaggle.
#
# Prerequisites:
#   1. Create a free Kaggle account at https://www.kaggle.com
#   2. Go to: Account → Settings → API → "Create New API Token"
#      This downloads kaggle.json to your Downloads folder.
#   3. Move it into place:
#        mkdir -p ~/.kaggle
#        mv ~/Downloads/kaggle.json ~/.kaggle/
#        chmod 600 ~/.kaggle/kaggle.json
#   4. Install the Kaggle CLI:
#        pip install kaggle
#
# Usage:
#   bash scripts/download_data.sh
# =============================================================================

set -euo pipefail

DATASET="shashwatwork/dataco-smart-supply-chain-for-big-data-analysis"
OUTPUT_DIR="data/raw"
EXPECTED_FILE="DataCoSupplyChainDataset.csv"

# ── Preflight checks ──────────────────────────────────────────────────────────
if ! command -v kaggle &>/dev/null; then
  echo "ERROR: kaggle CLI not found."
  echo "       Install with: pip install kaggle"
  exit 1
fi

if [ ! -f "$HOME/.kaggle/kaggle.json" ]; then
  echo "ERROR: Kaggle credentials not found at ~/.kaggle/kaggle.json"
  echo ""
  echo "Setup steps:"
  echo "  1. Log in at https://www.kaggle.com"
  echo "  2. Go to Account → Settings → API → Create New API Token"
  echo "  3. Move the file: mkdir -p ~/.kaggle && mv ~/Downloads/kaggle.json ~/.kaggle/"
  echo "  4. Lock it down: chmod 600 ~/.kaggle/kaggle.json"
  exit 1
fi

# ── Download ─────────────────────────────────────────────────────────────────
mkdir -p "$OUTPUT_DIR"
echo "Downloading dataset: $DATASET"
kaggle datasets download \
  --dataset "$DATASET" \
  --path "$OUTPUT_DIR" \
  --unzip \
  --quiet

# ── Verify ───────────────────────────────────────────────────────────────────
if [ -f "$OUTPUT_DIR/$EXPECTED_FILE" ]; then
  SIZE=$(du -sh "$OUTPUT_DIR/$EXPECTED_FILE" | cut -f1)
  echo ""
  echo "✓ Dataset ready: $OUTPUT_DIR/$EXPECTED_FILE ($SIZE)"
  echo ""
  echo "Next step: python -m src.utils.init_db   (initialize schemas)"
  echo "           python -m src.ingestion.load_raw $OUTPUT_DIR/$EXPECTED_FILE"
else
  echo "WARNING: Expected file not found: $OUTPUT_DIR/$EXPECTED_FILE"
  echo "Files downloaded:"
  ls -lh "$OUTPUT_DIR/"
fi
