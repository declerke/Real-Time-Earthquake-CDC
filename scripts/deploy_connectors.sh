#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTORS_DIR="$(cd "$(dirname "$0")/../connectors" && pwd)"

wait_for_connect() {
    local max_attempts=30
    local attempt=1
    echo "Waiting for Kafka Connect to be ready at ${CONNECT_URL} ..."
    until curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
        if (( attempt >= max_attempts )); then
            echo "ERROR: Kafka Connect did not become ready after ${max_attempts} attempts. Aborting."
            exit 1
        fi
        echo "  Attempt ${attempt}/${max_attempts} — sleeping 5s ..."
        sleep 5
        (( attempt++ ))
    done
    echo "Kafka Connect is ready."
}

deploy_connector() {
    local config_file="$1"
    local connector_name
    connector_name=$(python3 -c "import json,sys; d=json.load(open('$config_file')); print(d['name'])")

    echo ""
    echo "Deploying connector: ${connector_name}"
    echo "  Config file: ${config_file}"

    local existing_status
    existing_status=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${connector_name}")

    if [[ "$existing_status" == "200" ]]; then
        echo "  Connector already exists — updating config ..."
        local config_only
        config_only=$(python3 -c "import json,sys; d=json.load(open('$config_file')); print(json.dumps(d['config']))")
        curl -sf -X PUT \
            -H "Content-Type: application/json" \
            --data "${config_only}" \
            "${CONNECT_URL}/connectors/${connector_name}/config" | python3 -m json.tool
    else
        echo "  Creating new connector ..."
        curl -sf -X POST \
            -H "Content-Type: application/json" \
            --data @"${config_file}" \
            "${CONNECT_URL}/connectors" | python3 -m json.tool
    fi

    echo "  Checking connector status ..."
    sleep 3
    local status
    status=$(curl -sf "${CONNECT_URL}/connectors/${connector_name}/status")
    echo "${status}" | python3 -m json.tool
    local state
    state=$(echo "${status}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['connector']['state'])")
    if [[ "$state" == "RUNNING" ]]; then
        echo "  [OK] ${connector_name} is RUNNING."
    else
        echo "  [WARN] ${connector_name} is in state: ${state}. Check logs."
    fi
}

list_connectors() {
    echo ""
    echo "=== Deployed connectors ==="
    curl -sf "${CONNECT_URL}/connectors" | python3 -m json.tool
}

main() {
    wait_for_connect

    deploy_connector "${CONNECTORS_DIR}/debezium-source.json"
    deploy_connector "${CONNECTORS_DIR}/jdbc-sink.json"

    list_connectors
    echo ""
    echo "All connectors deployed successfully."
}

main "$@"