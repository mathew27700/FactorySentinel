#!/usr/bin/env bash
# Download the Azure Predictive Maintenance dataset into data/raw/ (git-ignored).
# Needs a Kaggle token in ~/.kaggle and `pip install kaggle` (see member guide, section 5).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/raw
if [ -f data/raw/PdM_telemetry.csv ]; then
  echo "Dataset already in data/raw/, skipping download."
else
  kaggle datasets download -d arnabbiswas1/microsoft-azure-predictive-maintenance \
    -p data/raw --unzip
fi
ls -lh data/raw
