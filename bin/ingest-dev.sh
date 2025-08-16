#!/bin/bash
PRW_ENV=dev
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
cd "$SCRIPT_DIR"

echo "To execute using local GH Actions, run:"
echo "  act -W .github/workflows/sources-epic.yml -P self-hosted=-self-hosted --rm --secret-file=.secrets.act"
echo "  act -W .github/workflows/ingest.yml -P self-hosted=-self-hosted --rm --secret-file=.secrets.act"
echo ""

# Export data sources
read -p "Caboodle connection (empty to skip export): " PRH_SOURCES_EPIC_INPUT_CONN
echo ""
if [ -n "$PRH_SOURCES_EPIC_INPUT_CONN" ]; then
    echo "Exporting sources"
    cd ../../prw-exporter/sources/epic
    set -a; source .env.$PRW_ENV
    pwd
    uv run python run_sql_scripts.py --caboodle "$PRH_SOURCES_EPIC_INPUT_CONN" --clarity "skip" --out "$PRH_SOURCES_EPIC_OUTPUT_PATH"
fi

# Run full ingest flow
cd "$SCRIPT_DIR/../ingest"
set -a; source .env.$PRW_ENV
pwd
uv run python main.py
