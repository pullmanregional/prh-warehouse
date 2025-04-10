#!/bin/bash
PRW_ENV=dev
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
cd "$SCRIPT_DIR"

# Export data sources
read -p "Caboodle connection (empty to skip export): " PRH_SOURCES_EPIC_INPUT_CONN
echo ""
if [ -n "$PRH_SOURCES_EPIC_INPUT_CONN" ]; then
    echo "Exporting sources"
    cd ../../prw-exporter/sources/epic/prefect
    echo $PRW_ENV
    pwd
    pipenv run prefect profile use dev
    PRW_ENV=$PRW_ENV PRH_SOURCES_EPIC_INPUT_CONN="$PRH_SOURCES_EPIC_INPUT_CONN" pipenv run python flow.py
fi

# Run full ingest flow
cd "$SCRIPT_DIR/../prefect"
pwd
pipenv run prefect profile use dev
PRW_ENV=$PRW_ENV pipenv run python prw_ingest.py
