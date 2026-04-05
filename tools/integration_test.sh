#!/usr/bin/env bash
set -euo pipefail

# === Configuration ===
# Single BASE_URL mode (Python reimplementation, all modules on one port):
#   BASE_URL=http://localhost:8000
# Scala backend mode (three separate services):
#   DATA_URL=http://localhost:8891/easyminer-data
#   PREP_URL=http://localhost:8892/easyminer-preprocessing
#   MINE_URL=http://localhost:8893/easyminer-miner
BASE_URL="${BASE_URL:-}"
DATA_URL="${DATA_URL:-${BASE_URL:-http://localhost:8000}}"
PREP_URL="${PREP_URL:-${BASE_URL:-http://localhost:8000}}"
MINE_URL="${MINE_URL:-${BASE_URL:-http://localhost:8000}}"
API_KEY="${API_KEY:-test}"
CSV_FILE="${CSV_FILE:-tools/performance_test_data.csv}"
# Dataset profile: "perf" (12-col performance CSV) or "kdd" (42-col KDDCup99)
# Defaults to auto-detection from CSV filename.
DATASET_PROFILE="${DATASET_PROFILE:-auto}"
CHUNK_SIZE="${CHUNK_SIZE:-400000}"
POLL_INTERVAL="${POLL_INTERVAL:-2}"
POLL_TIMEOUT="${POLL_TIMEOUT:-1800}"
UPLOAD_TIMEOUT="${UPLOAD_TIMEOUT:-1800}"  # finalize + field-stats can take long for large files
MIN_CONFIDENCE="${MIN_CONFIDENCE:-0.3}"
MIN_SUPPORT="${MIN_SUPPORT:-0.005}"
EVAL_MIN_SUPPORT="${EVAL_MIN_SUPPORT:-0.05}"
ENABLE_CBA="${ENABLE_CBA:-false}"
EVAL_DIR="${EVAL_DIR:-}"
RESULTS_DIR="${RESULTS_DIR:-tools/results}"

# Eval dataset target variables (pre-discretized, all nominal)
declare -A EVAL_TARGETS=(
    [iris]="class"
    [mushroom]="class"
    [car]="class"
    [credit-a]="class"
    [heart-statlog]="class"
    [breast-w]="Class"
    [zoo]="class"
    [tic-tac-toe]="Class"
)

# Database connection for mining PMML (defaults match typical dev setup)
DB_SERVER="${DB_SERVER:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_NAME="${DB_NAME:-easyminer}"
DB_USER="${DB_USER:-easyminer}"
DB_PASSWORD="${DB_PASSWORD:-easyminer}"

DATA_API="${DATA_URL}/api/v1"
PREP_API="${PREP_URL}/api/v1"
MINE_API="${MINE_URL}/api/v1"

# === Flags ===
CLEANUP=false
DRY_RUN=false
VERBOSE=false

for arg in "$@"; do
    case "$arg" in
        --cleanup) CLEANUP=true ;;
        --dry-run) DRY_RUN=true ;;
        --verbose) VERBOSE=true ;;
        --cba) ENABLE_CBA=true ;;
        --eval-dir=*) EVAL_DIR="${arg#*=}" ;;
        --eval-dir) echo "Error: --eval-dir requires a path (use --eval-dir=<path>)"; exit 1 ;;
        --results-dir=*) RESULTS_DIR="${arg#*=}" ;;
        --help|-h)
            echo "Usage: $0 [--cleanup] [--dry-run] [--verbose]"
            echo ""
            echo "  --cleanup   Delete created resources at the end"
            echo "  --dry-run   Show what would be done without executing"
            echo "  --verbose   Show full request/response bodies"
            echo "  --cba       Run additional CBA mining task after standard mining"
            echo "  --eval-dir=PATH  Run all CSVs in PATH sequentially (multi-dataset mode)"
            echo "  --results-dir=PATH  Where to save results (default: tools/results)"
            echo ""
            echo "Environment variables:"
            echo "  BASE_URL       single URL for all services (Python reimpl, default: http://localhost:8000)"
            echo "  DATA_URL       data service URL (Scala: http://localhost:8891/easyminer-data)"
            echo "  PREP_URL       preprocessing service URL (Scala: http://localhost:8892/easyminer-preprocessing)"
            echo "  MINE_URL       miner service URL (Scala: http://localhost:8893/easyminer-miner)"
            echo "  API_KEY        (default: test)"
            echo "  CSV_FILE       (default: tools/performance_test_data.csv)"
            echo "  CHUNK_SIZE     (default: 400000 bytes)"
            echo "  POLL_INTERVAL  (default: 2 seconds)"
            echo "  POLL_TIMEOUT   (default: 120 seconds)"
            echo "  MIN_CONFIDENCE (default: 0.5)"
            echo "  MIN_SUPPORT    (default: 0.01)"
            echo "  DB_SERVER      (default: 127.0.0.1)"
            echo "  DB_PORT        (default: 3306)"
            echo "  DB_NAME        (default: easyminer)"
            echo "  DB_USER        (default: root)"
            echo "  DB_PASSWORD    (default: root)"
            exit 0
            ;;
        *) echo "Unknown option: $arg"; exit 1 ;;
    esac
done

# === Colors ===
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# === State for cleanup ===
DATASOURCE_ID=""
DATASET_ID=""
UPLOAD_UUID=""
STEP=0
STEP_START=""
STEP_NAMES=()
STEP_STARTS=()

# === Helpers ===
step() {
    STEP=$((STEP + 1))
    STEP_START=$(date +%s)
    STEP_NAMES+=("$1")
    STEP_STARTS+=("$STEP_START")
    echo -e "\n${BLUE}[Step ${STEP}]${NC} ${BOLD}$1${NC}"
}

ok() {
    local elapsed=""
    if [[ -n "$STEP_START" ]]; then
        elapsed=" ${CYAN}($(( $(date +%s) - STEP_START ))s)${NC}"
    fi
    echo -e "  ${GREEN}OK${NC} $1${elapsed}"
}

fail() {
    echo -e "  ${RED}FAIL${NC} $1" >&2
    exit 1
}

warn() {
    echo -e "  ${YELLOW}WARN${NC} $1"
}

verbose() {
    if $VERBOSE; then
        echo -e "  ${CYAN}>>>${NC} $1"
    fi
}

command -v curl &>/dev/null || { echo "Error: curl not found" >&2; exit 1; }

# HTTP request wrapper
# Usage: http METHOD URL [extra args...]
# Sets $RESP_BODY, $RESP_STATUS
http() {
    local method="$1" url="$2"
    shift 2
    local tmpfile
    tmpfile=$(mktemp)

    RESP_STATUS=$(curl -s -o "$tmpfile" -w '%{http_code}' \
        -X "$method" "$url" \
        -H "Authorization: ApiKey ${API_KEY}" \
        "$@") || RESP_STATUS="000"
    RESP_BODY=$(cat "$tmpfile")

    rm -f "$tmpfile"
    verbose "HTTP $method $url → $RESP_STATUS"
    if $VERBOSE && [[ -n "$RESP_BODY" ]]; then
        verbose "$(echo "$RESP_BODY" | head -20)"
    fi
}

# Poll a data/preprocessing task until completion (status 200=running, 201=done)
# Usage: poll_task TASK_ID [description] [api_base]
# Returns 0 on success (201), 1 on failure
poll_task() {
    local task_id="$1"
    local desc="${2:-task}"
    local api_base="${3:-${DATA_API}}"
    local elapsed=0

    echo -e "  Polling ${desc} (${task_id})..."
    while (( elapsed < POLL_TIMEOUT )); do
        http GET "${api_base}/task-status/${task_id}"

        case "$RESP_STATUS" in
            201)
                ok "${desc} completed"
                return 0
                ;;
            200)
                printf "  . still running (%ds)\r" "$elapsed"
                ;;
            *)
                fail "${desc} failed with status ${RESP_STATUS}: ${RESP_BODY}"
                ;;
        esac

        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
    done

    fail "${desc} timed out after ${POLL_TIMEOUT}s"
}

# Poll a mining task until completion (status 204/206=running, 303=done)
# Usage: poll_mine_task TASK_ID [description]
poll_mine_task() {
    local task_id="$1"
    local desc="${2:-mining task}"
    local elapsed=0

    echo -e "  Polling ${desc} (${task_id})..."
    while (( elapsed < POLL_TIMEOUT )); do
        http GET "${MINE_API}/partial-result/${task_id}"

        case "$RESP_STATUS" in
            303)
                ok "${desc} completed"
                return 0
                ;;
            204|206)
                printf "  . still running (%ds)\r" "$elapsed"
                ;;
            *)
                fail "${desc} failed with status ${RESP_STATUS}: ${RESP_BODY}"
                ;;
        esac

        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
    done

    fail "${desc} timed out after ${POLL_TIMEOUT}s"
}

# Cleanup handler
cleanup_on_exit() {
    local exit_code=$?
    local now
    now=$(date +%s)

    if [[ $exit_code -ne 0 ]]; then
        echo -e "\n${RED}Script failed at step ${STEP}${NC}"
    fi

    # Print per-step timing summary
    if [[ ${#STEP_NAMES[@]} -gt 0 ]]; then
        echo -e "\n${BOLD}=== Step Timing Summary ===${NC}"
        local i
        for i in "${!STEP_NAMES[@]}"; do
            local start="${STEP_STARTS[$i]}"
            # Duration: use next step's start, or now for the last step
            local end
            if [[ $i -lt $(( ${#STEP_STARTS[@]} - 1 )) ]]; then
                end="${STEP_STARTS[$((i+1))]}"
            else
                end="$now"
            fi
            local dur=$(( end - start ))
            local marker
            if [[ $exit_code -ne 0 && $i -eq $(( ${#STEP_NAMES[@]} - 1 )) ]]; then
                marker="${RED}FAIL${NC}"
            else
                marker="${GREEN} OK ${NC}"
            fi
            printf "  $(echo -e "$marker") [%2d] %-45s %ds\n" "$((i+1))" "${STEP_NAMES[$i]}" "$dur"
        done
        if [[ -n "${TEST_START:-}" ]]; then
            echo -e "       ${BOLD}Total: $(( now - TEST_START ))s${NC}"
        fi
    fi

    if $CLEANUP; then
        echo -e "\n${YELLOW}Cleaning up...${NC}"
        if [[ -n "$DATASET_ID" ]]; then
            http DELETE "${PREP_API}/dataset/${DATASET_ID}" 2>/dev/null || true
            echo -e "  Deleted dataset ${DATASET_ID}"
        fi
        if [[ -n "$DATASOURCE_ID" ]]; then
            http DELETE "${DATA_API}/datasource/${DATASOURCE_ID}" 2>/dev/null || true
            echo -e "  Deleted datasource ${DATASOURCE_ID}"
        fi
    fi
}
trap cleanup_on_exit EXIT

# === Dry run ===
if $DRY_RUN; then
    echo -e "\n${YELLOW}=== DRY RUN ===${NC}"
    echo "Would execute the following steps:"
    echo "  1. Health check: GET ${MINE_API}/status"
    echo "  2. Start upload: POST ${DATA_API}/upload/start"
    echo "  3. Upload CSV in chunks (~$(($(wc -c < "$CSV_FILE") / CHUNK_SIZE + 1)) chunks)"
    echo "  4. List fields: GET ${DATA_API}/datasource/{id}/field"
    echo "  5. Create dataset: POST ${PREP_API}/dataset"
    echo "  6. Get dataset ID: GET ${PREP_API}/dataset"
    echo "  7. Create attributes (kdd: 9 attrs — 4 nominal pass-through + 4 numeric equifrequent + label; perf: 5 nominal pass-through)"
    echo "  8. List attributes: GET ${PREP_API}/dataset/{id}/attribute"
    echo "  9. Launch mining task: POST ${MINE_API}/mine"
    echo " 10. Fetch mining result: GET ${MINE_API}/complete-result/{task_id}"
    if $CLEANUP; then
        echo " 11. Cleanup: DELETE dataset and datasource"
    fi
    exit 0
fi

# === Resolve dataset profile ===
resolve_profile() {
    if [[ "$DATASET_PROFILE" == "auto" ]]; then
        local csv_basename
        csv_basename="$(basename "$CSV_FILE" .csv)"
        if [[ -v "EVAL_TARGETS[$csv_basename]" ]]; then
            DATASET_PROFILE="eval"
            EVAL_DATASET_NAME="$csv_basename"
            EVAL_TARGET="${EVAL_TARGETS[$csv_basename]}"
        elif [[ "$CSV_FILE" == *kdd* || "$CSV_FILE" == *KDD* || "$CSV_FILE" == *kddcup* || "$CSV_FILE" == *KDDCup* ]]; then
            DATASET_PROFILE="kdd"
        else
            DATASET_PROFILE="perf"
        fi
    elif [[ "$DATASET_PROFILE" == "eval" ]]; then
        local csv_basename
        csv_basename="$(basename "$CSV_FILE" .csv)"
        EVAL_DATASET_NAME="$csv_basename"
        EVAL_TARGET="${EVAL_TARGETS[$csv_basename]:-class}"
    fi
}
run_single_dataset() {

# CBA metrics (populated only when --cba is enabled)
CBA_NUM_RULES=""
CBA_ACCURACY=""
CBA_MINING_TIME_MS=""

resolve_profile
echo -e "${CYAN}Dataset profile:${NC} ${DATASET_PROFILE}"

# Unique dataset name to avoid collisions with previous runs
DATASET_NAME="${DATASET_PROFILE}_$(date +%s)"

echo -e "\n${BOLD}=== EasyMiner Integration Test ===${NC}"
echo -e "Data:   ${DATA_URL}"
echo -e "Prep:   ${PREP_URL}"
echo -e "Miner:  ${MINE_URL}"
echo -e "CSV:    ${CSV_FILE}"
echo ""

# # === Step 1: Health check ===
# step "Health check"
# http GET "${MINE_API}/status"
# [[ "$RESP_STATUS" == "200" ]] || fail "Server not ready (status: ${RESP_STATUS})"
# ok "Server is ready"

# === Step 2: Start upload ===
PHASE_UPLOAD_START=$(date +%s)
step "Start upload"

if [[ "$DATASET_PROFILE" == "kdd" ]]; then
    UPLOAD_BODY=$(cat <<'JSON'
{
  "name": "KDDCup99_full.csv",
  "mediaType": "csv",
  "dbType": "limited",
  "separator": ",",
  "encoding": "utf-8",
  "quotesChar": "\"",
  "escapeChar": "\\",
  "locale": "en",
  "nullValues": [],
  "dataTypes": [
    "numeric","nominal","nominal","nominal","numeric","numeric",
    "nominal","numeric","numeric","numeric","numeric","nominal",
    "numeric","numeric","numeric","numeric","numeric","numeric",
    "numeric","numeric","nominal","nominal","numeric","numeric",
    "numeric","numeric","numeric","numeric","numeric","numeric",
    "numeric","numeric","numeric","numeric","numeric","numeric",
    "numeric","numeric","numeric","numeric","numeric","nominal"
  ]
}
JSON
)
elif [[ "$DATASET_PROFILE" == "eval" ]]; then
    EVAL_HEADER=$(head -1 "$CSV_FILE")
    EVAL_COL_COUNT=$(echo "$EVAL_HEADER" | awk -F',' '{print NF}')
    EVAL_DATATYPES=$(python3 -c "import json; print(json.dumps(['nominal']*${EVAL_COL_COUNT}))")
    UPLOAD_BODY=$(python3 -c "
import json
body = {
    'name': '$(basename "$CSV_FILE")',
    'mediaType': 'csv',
    'dbType': 'limited',
    'separator': ',',
    'encoding': 'utf-8',
    'quotesChar': '\"',
    'escapeChar': '\\\\',
    'locale': 'en',
    'nullValues': [],
    'dataTypes': ['nominal']*${EVAL_COL_COUNT}
}
print(json.dumps(body))
")
else
    UPLOAD_BODY=$(cat <<'JSON'
{
  "name": "performance_test_data.csv",
  "mediaType": "csv",
  "dbType": "limited",
  "separator": ",",
  "encoding": "utf-8",
  "quotesChar": "\"",
  "escapeChar": "\\",
  "locale": "en",
  "nullValues": [],
  "dataTypes": ["nominal","nominal","nominal","nominal","nominal","nominal","nominal","nominal","nominal","nominal","nominal","nominal"]
}
JSON
)
fi
verbose "$UPLOAD_BODY"

http POST "${DATA_API}/upload/start" -H "Content-Type: application/json; charset=utf-8" -d "$UPLOAD_BODY"

[[ "$RESP_STATUS" == "200" ]] || fail "Upload start failed (status: ${RESP_STATUS}): ${RESP_BODY}"
UPLOAD_UUID="$RESP_BODY"
# Strip quotes if present
UPLOAD_UUID="${UPLOAD_UUID//\"/}"
# Strip whitespace
UPLOAD_UUID="$(echo -n "$UPLOAD_UUID" | tr -d '[:space:]')"
ok "Upload UUID: ${UPLOAD_UUID}"

# === Step 3: Upload CSV in chunks ===
step "Upload CSV in chunks"

HEADER=$(head -1 "$CSV_FILE")

# Split CSV data (without header) into chunks at line boundaries
CHUNK_DIR=$(mktemp -d)
tail -n +2 "$CSV_FILE" | split -C "$CHUNK_SIZE" - "${CHUNK_DIR}/chunk_"
CHUNK_FILES=("${CHUNK_DIR}"/chunk_*)
TOTAL_CHUNKS=${#CHUNK_FILES[@]}

CHUNK_NUM=0
for CHUNK_SRC in "${CHUNK_FILES[@]}"; do
    CHUNK_NUM=$((CHUNK_NUM + 1))
    CHUNK_TMP=$(mktemp)

    # Prepend header only for the first chunk
    if (( CHUNK_NUM == 1 )); then
        { echo "$HEADER"; cat "$CHUNK_SRC"; } > "$CHUNK_TMP"
    else
        cp "$CHUNK_SRC" "$CHUNK_TMP"
    fi

    CHUNK_BYTES=$(wc -c < "$CHUNK_TMP")
    printf "  Uploading chunk %d/%d (%d bytes)...\r" "$CHUNK_NUM" "$TOTAL_CHUNKS" "$CHUNK_BYTES"

    retries=0 max_retries=10 backoff_ms=100
    while true; do
        http POST "${DATA_API}/upload/${UPLOAD_UUID}" -H "Content-Type: text/plain" --data-binary "@${CHUNK_TMP}"

        if [[ "$RESP_STATUS" == "202" ]]; then
            break
        elif [[ "$RESP_STATUS" == "429" ]]; then
            retries=$((retries + 1))
            if (( retries > max_retries )); then
                rm -f "$CHUNK_TMP"
                fail "Chunk ${CHUNK_NUM} still locked after ${max_retries} retries"
            fi
            printf "  . chunk %d locked, retry %d in %dms...\r" "$CHUNK_NUM" "$retries" "$backoff_ms"
            sleep "$(awk "BEGIN{print $backoff_ms/1000}")"
            backoff_ms=$(( backoff_ms < 16000 ? backoff_ms * 2 : 16000 ))
        else
            rm -f "$CHUNK_TMP"
            fail "Chunk upload failed (status: ${RESP_STATUS}): ${RESP_BODY}"
        fi
    done

    rm -f "$CHUNK_TMP"
done
echo ""

rm -rf "$CHUNK_DIR"
ok "Uploaded ${CHUNK_NUM} chunks"

# Finalize upload (empty POST)
# The Scala backend may return 202 while still processing field stats; poll until 200.
echo -e "  Finalizing upload..."
finalize_start=$(date +%s)
while true; do
    http POST "${DATA_API}/upload/${UPLOAD_UUID}" -H "Content-Type: text/plain" -d ""

    if [[ "$RESP_STATUS" == "200" ]]; then
        break
    elif [[ "$RESP_STATUS" == "202" || "$RESP_STATUS" == "429" ]]; then
        elapsed_fin=$(( $(date +%s) - finalize_start ))
        if (( elapsed_fin >= UPLOAD_TIMEOUT )); then
            fail "Upload finalize timed out after ${UPLOAD_TIMEOUT}s"
        fi
        printf "  . still processing (%ds / %ds)\r" "$elapsed_fin" "$UPLOAD_TIMEOUT"
        sleep "$POLL_INTERVAL"
    else
        fail "Upload finalize failed (status: ${RESP_STATUS}): ${RESP_BODY}"
    fi
done
DATASOURCE_ID=$(echo "$RESP_BODY" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
UPLOAD_SIZE=$(echo "$RESP_BODY" | python3 -c "import sys,json; print(json.load(sys.stdin)['size'])")
ok "Datasource ID: ${DATASOURCE_ID}, rows: ${UPLOAD_SIZE}"

# === Step 4: List fields ===
step "List fields"

# Poll until all fields have uniqueValuesSize > 0 (field stats computed asynchronously)
fields_start=$(date +%s)
while true; do
    http GET "${DATA_API}/datasource/${DATASOURCE_ID}/field"
    [[ "$RESP_STATUS" == "200" ]] || fail "List fields failed (status: ${RESP_STATUS}): ${RESP_BODY}"

    not_ready=$(echo "$RESP_BODY" | python3 -c "
import sys, json
fields = json.load(sys.stdin)
not_ready = [f['name'] for f in fields if not f.get('uniqueValuesSize')]
print(len(not_ready))
")
    if [[ "$not_ready" == "0" ]]; then
        break
    fi

    elapsed_fs=$(( $(date +%s) - fields_start ))
    if (( elapsed_fs >= UPLOAD_TIMEOUT )); then
        fail "Field stats timed out after ${UPLOAD_TIMEOUT}s (${not_ready} fields still pending)"
    fi
    printf "  . waiting for field stats (%d fields pending, %ds)\r" "$not_ready" "$elapsed_fs"
    sleep "$POLL_INTERVAL"
done

# Extract field info as JSON array
FIELDS_JSON="$RESP_BODY"
FIELD_COUNT=$(echo "$FIELDS_JSON" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
ok "Found ${FIELD_COUNT} fields (all stats ready)"

# Get field IDs by name
field_id() {
    echo "$FIELDS_JSON" | python3 -c "
import sys, json
fields = json.load(sys.stdin)
for f in fields:
    if f['name'] == '$1':
        print(f['id'])
        break
"
}

if [[ "$DATASET_PROFILE" == "kdd" ]]; then
    FIELD_ID_PROTOCOL=$(field_id "protocol_type")
    FIELD_ID_SERVICE=$(field_id "service")
    FIELD_ID_FLAG=$(field_id "flag")
    FIELD_ID_LOGGED_IN=$(field_id "logged_in")
    FIELD_ID_LABEL=$(field_id "label")
    FIELD_ID_SRC_BYTES=$(field_id "src_bytes")
    FIELD_ID_DST_BYTES=$(field_id "dst_bytes")
    FIELD_ID_COUNT=$(field_id "count")
    FIELD_ID_DST_HOST_COUNT=$(field_id "dst_host_count")
    verbose "Field IDs: protocol_type=${FIELD_ID_PROTOCOL}, service=${FIELD_ID_SERVICE}, flag=${FIELD_ID_FLAG}, logged_in=${FIELD_ID_LOGGED_IN}, label=${FIELD_ID_LABEL}, src_bytes=${FIELD_ID_SRC_BYTES}, dst_bytes=${FIELD_ID_DST_BYTES}, count=${FIELD_ID_COUNT}, dst_host_count=${FIELD_ID_DST_HOST_COUNT}"
elif [[ "$DATASET_PROFILE" == "eval" ]]; then
    IFS=',' read -ra EVAL_COLUMNS <<< "$EVAL_HEADER"
    declare -A EVAL_FIELD_IDS=()
    for col in "${EVAL_COLUMNS[@]}"; do
        col=$(echo "$col" | tr -d '\r')
        EVAL_FIELD_IDS["$col"]=$(field_id "$col")
        verbose "Field ID: ${col} = ${EVAL_FIELD_IDS[$col]}"
    done
else
    FIELD_ID_CATEGORY=$(field_id "Category")
    FIELD_ID_WILLRETURN=$(field_id "WillReturn")
    FIELD_ID_MEMBERSHIP=$(field_id "Membership")
    FIELD_ID_SATISFACTION=$(field_id "Satisfaction")
    FIELD_ID_BASKET=$(field_id "Basket")
    verbose "Field IDs: Category=${FIELD_ID_CATEGORY}, WillReturn=${FIELD_ID_WILLRETURN}, Membership=${FIELD_ID_MEMBERSHIP}, Satisfaction=${FIELD_ID_SATISFACTION}, Basket=${FIELD_ID_BASKET}"
fi

PHASE_UPLOAD_S=$(( $(date +%s) - PHASE_UPLOAD_START ))

# === Step 5: Create dataset ===
PHASE_PREP_START=$(date +%s)
step "Create dataset"

http POST "${PREP_API}/dataset" -d "dataSource=${DATASOURCE_ID}&name=${DATASET_NAME}" -H "Content-Type: application/x-www-form-urlencoded"

[[ "$RESP_STATUS" == "202" ]] || fail "Create dataset failed (status: ${RESP_STATUS}): ${RESP_BODY}"
TASK_ID=$(echo "$RESP_BODY" | python3 -c "import sys,json; print(json.load(sys.stdin)['taskId'])")
poll_task "$TASK_ID" "create dataset" "$PREP_API"

# === Step 6: Get dataset ID ===
step "Get dataset ID"

http GET "${PREP_API}/dataset"
[[ "$RESP_STATUS" == "200" ]] || fail "List datasets failed (status: ${RESP_STATUS}): ${RESP_BODY}"

DATASET_ID=$(echo "$RESP_BODY" | python3 -c "
import sys, json
datasets = json.load(sys.stdin)
for d in datasets:
    if d['name'] == '${DATASET_NAME}':
        print(d['id'])
        break
")
[[ -n "$DATASET_ID" ]] || fail "Dataset '${DATASET_NAME}' not found"
ok "Dataset ID: ${DATASET_ID}"

# === Step 7: Create attributes ===
step "Create attributes"

create_attribute() {
    local xml="$1"
    local name="$2"

    verbose "Creating attribute: ${name}"
    verbose "$xml"

    http POST "${PREP_API}/dataset/${DATASET_ID}/attribute" -H "Content-Type: application/xml" -d "$xml"

    [[ "$RESP_STATUS" == "202" ]] || fail "Create attribute '${name}' failed (status: ${RESP_STATUS}): ${RESP_BODY}"
    local tid
    tid=$(echo "$RESP_BODY" | python3 -c "import sys,json; print(json.load(sys.stdin)['taskId'])")
    poll_task "$tid" "create attribute '${name}'" "$PREP_API"
}

if [[ "$DATASET_PROFILE" == "kdd" ]]; then
    # 7a: protocol_type (pass-through nominal, 3 values: tcp/udp/icmp)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="protocol_type">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_PROTOCOL}" column="column" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "protocol_type"

    # 7b: service (pass-through nominal, ~70 distinct values)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="service">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_SERVICE}" column="column" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "service"

    # 7c: flag (pass-through nominal, 11 distinct TCP state values)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="flag">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_FLAG}" column="column" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "flag"

    # 7d: logged_in (pass-through nominal, binary 0/1 — whether user was authenticated)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="logged_in">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_LOGGED_IN}" column="column" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "logged_in"

    # 7e: label (pass-through nominal — attack category target)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="label">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_LABEL}" column="column" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "label"

    # 7f: src_bytes discretized into 5 equifrequent intervals (bytes from client to server)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="src_bytes_eq5">
        <Discretize field="${FIELD_ID_SRC_BYTES}">
            <Extension name="algorithm" value="equifrequent-intervals"/>
            <Extension name="bins" value="5"/>
        </Discretize>
    </DerivedField>
</TransformationDictionary>
XML
)" "src_bytes_eq5"

    # 7g: dst_bytes discretized into 5 equifrequent intervals (bytes from server to client)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="dst_bytes_eq5">
        <Discretize field="${FIELD_ID_DST_BYTES}">
            <Extension name="algorithm" value="equifrequent-intervals"/>
            <Extension name="bins" value="5"/>
        </Discretize>
    </DerivedField>
</TransformationDictionary>
XML
)" "dst_bytes_eq5"

    # 7h: count discretized into 4 equifrequent intervals (connections to same host in last 2s)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="count_eq4">
        <Discretize field="${FIELD_ID_COUNT}">
            <Extension name="algorithm" value="equifrequent-intervals"/>
            <Extension name="bins" value="4"/>
        </Discretize>
    </DerivedField>
</TransformationDictionary>
XML
)" "count_eq4"

    # 7i: dst_host_count discretized into 4 equifrequent intervals (connections to same dest host)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="dst_host_count_eq4">
        <Discretize field="${FIELD_ID_DST_HOST_COUNT}">
            <Extension name="algorithm" value="equifrequent-intervals"/>
            <Extension name="bins" value="4"/>
        </Discretize>
    </DerivedField>
</TransformationDictionary>
XML
)" "dst_host_count_eq4"

if false; then  # disabled: InlineTable enumeration is broken in this backend version
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="label">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_LABEL}" column="column" />
            <InlineTable>
                <row><column>normal</column><field>Normal</field></row>
                <row><column>neptune</column><field>DoS</field></row>
                <row><column>back</column><field>DoS</field></row>
                <row><column>land</column><field>DoS</field></row>
                <row><column>pod</column><field>DoS</field></row>
                <row><column>smurf</column><field>DoS</field></row>
                <row><column>teardrop</column><field>DoS</field></row>
                <row><column>apache2</column><field>DoS</field></row>
                <row><column>udpstorm</column><field>DoS</field></row>
                <row><column>processtable</column><field>DoS</field></row>
                <row><column>mailbomb</column><field>DoS</field></row>
                <row><column>satan</column><field>Probe</field></row>
                <row><column>ipsweep</column><field>Probe</field></row>
                <row><column>nmap</column><field>Probe</field></row>
                <row><column>portsweep</column><field>Probe</field></row>
                <row><column>mscan</column><field>Probe</field></row>
                <row><column>saint</column><field>Probe</field></row>
                <row><column>ftp_write</column><field>R2L</field></row>
                <row><column>guess_passwd</column><field>R2L</field></row>
                <row><column>imap</column><field>R2L</field></row>
                <row><column>multihop</column><field>R2L</field></row>
                <row><column>phf</column><field>R2L</field></row>
                <row><column>spy</column><field>R2L</field></row>
                <row><column>warezclient</column><field>R2L</field></row>
                <row><column>warezmaster</column><field>R2L</field></row>
                <row><column>sendmail</column><field>R2L</field></row>
                <row><column>named</column><field>R2L</field></row>
                <row><column>snmpgetattack</column><field>R2L</field></row>
                <row><column>snmpguess</column><field>R2L</field></row>
                <row><column>xlock</column><field>R2L</field></row>
                <row><column>xsnoop</column><field>R2L</field></row>
                <row><column>httptunnel</column><field>R2L</field></row>
                <row><column>buffer_overflow</column><field>U2R</field></row>
                <row><column>loadmodule</column><field>U2R</field></row>
                <row><column>perl</column><field>U2R</field></row>
                <row><column>rootkit</column><field>U2R</field></row>
                <row><column>ps</column><field>U2R</field></row>
                <row><column>sqlattack</column><field>U2R</field></row>
                <row><column>xterm</column><field>U2R</field></row>
            </InlineTable>
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "label"
fi  # end disabled block

elif [[ "$DATASET_PROFILE" == "eval" ]]; then
    for col in "${EVAL_COLUMNS[@]}"; do
        col=$(echo "$col" | tr -d '\r')
        fid="${EVAL_FIELD_IDS[$col]}"
        create_attribute "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<TransformationDictionary xmlns=\"http://www.dmg.org/PMML-4_2\">
    <DerivedField name=\"${col}\">
        <MapValues outputColumn=\"field\">
            <FieldColumnPair field=\"${fid}\" column=\"column\" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>" "$col"
    done

else
    # 7a: Category (simple pass-through)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="Category">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_CATEGORY}" column="column" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "Category"

    # 7b: WillReturn (simple pass-through)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="WillReturn">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_WILLRETURN}" column="column" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "WillReturn"

    # 7c: Membership (pass-through — InlineTable enumeration is broken in Scala 2.11 REST backend)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="Membership">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_MEMBERSHIP}" column="column" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "Membership"

    # 7d: Satisfaction (pass-through)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="Satisfaction">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_SATISFACTION}" column="column" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "Satisfaction"

    # 7e: Basket (pass-through)
    create_attribute "$(cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
    <DerivedField name="Basket">
        <MapValues outputColumn="field">
            <FieldColumnPair field="${FIELD_ID_BASKET}" column="column" />
        </MapValues>
    </DerivedField>
</TransformationDictionary>
XML
)" "Basket"
fi

# === Step 8: List attributes ===
step "List attributes"

http GET "${PREP_API}/dataset/${DATASET_ID}/attribute"
[[ "$RESP_STATUS" == "200" ]] || fail "List attributes failed (status: ${RESP_STATUS}): ${RESP_BODY}"

ATTR_COUNT=$(echo "$RESP_BODY" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
if [[ "$DATASET_PROFILE" == "kdd" ]]; then
    EXPECTED_ATTRS=9
elif [[ "$DATASET_PROFILE" == "eval" ]]; then
    EXPECTED_ATTRS=${#EVAL_COLUMNS[@]}
else
    EXPECTED_ATTRS=5
fi
[[ "$ATTR_COUNT" -ge "$EXPECTED_ATTRS" ]] || fail "Expected at least ${EXPECTED_ATTRS} attributes, got ${ATTR_COUNT}"
ok "Found ${ATTR_COUNT} attributes"

# Extract attribute IDs by name — Scala miner requires integer IDs in FieldRef, not names
if [[ "$DATASET_PROFILE" == "eval" ]]; then
    # Use associative array for eval attribute IDs (handles special chars in column names)
    declare -A EVAL_ATTR_IDS=()
    while IFS='=' read -r name aid; do
        EVAL_ATTR_IDS["$name"]="$aid"
    done < <(echo "$RESP_BODY" | python3 -c "
import sys, json
for a in json.load(sys.stdin):
    print(f\"{a['name']}={a['id']}\")
")
    echo "  Attribute IDs: $(for col in "${EVAL_COLUMNS[@]}"; do col=$(echo "$col" | tr -d '\r'); printf "%s=%s " "$col" "${EVAL_ATTR_IDS[$col]:-?}"; done)"
else
    eval "$(echo "$RESP_BODY" | python3 -c "
import sys, json
attrs = {a['name']: a['id'] for a in json.load(sys.stdin)}
for name, aid in attrs.items():
    safe = name.replace(' ', '_').replace('-', '_')
    print(f'ATTR_ID_{safe}={aid}')
")"
fi

if [[ "$DATASET_PROFILE" == "kdd" ]]; then
    echo "  Attribute IDs: protocol_type=${ATTR_ID_protocol_type}, service=${ATTR_ID_service}, flag=${ATTR_ID_flag}, logged_in=${ATTR_ID_logged_in}, label=${ATTR_ID_label}, src_bytes_eq5=${ATTR_ID_src_bytes_eq5}, dst_bytes_eq5=${ATTR_ID_dst_bytes_eq5}, count_eq4=${ATTR_ID_count_eq4}, dst_host_count_eq4=${ATTR_ID_dst_host_count_eq4}"
elif [[ "$DATASET_PROFILE" == "eval" ]]; then
    : # already printed above
else
    echo "  Attribute IDs: Category=${ATTR_ID_Category}, WillReturn=${ATTR_ID_WillReturn}, Membership=${ATTR_ID_Membership}, Satisfaction=${ATTR_ID_Satisfaction}, Basket=${ATTR_ID_Basket}"
fi

if $VERBOSE; then
    echo "$RESP_BODY" | python3 -c "
import sys, json
for a in json.load(sys.stdin):
    print(f\"  - {a['name']} (id={a['id']}, unique_values={a['unique_values_size']})\")" || true
fi

PHASE_PREP_S=$(( $(date +%s) - PHASE_PREP_START ))

# === Step 9: Launch mining task ===
PHASE_MINE_START=$(date +%s)
step "Launch mining task"

# Scala miner uses a custom BBASetting/DBASetting PMML format.
# - BBASetting id=N: leaf attribute node (FieldRef = integer attribute ID)
# - DBASetting type="Literal": wraps one BBASetting
# - DBASetting type="Conjunction": joins multiple DBASettings with AND
# - AntecedentSetting / ConsequentSetting: reference DBASetting IDs by text content
# - InterestMeasureThreshold: bare <InterestMeasure> + <Threshold> (no extra nesting)
# - HypothesesCountMax: max rules, required

if [[ "$DATASET_PROFILE" == "kdd" ]]; then
    # Antecedent: protocol_type + service + flag + logged_in + src_bytes_eq5 + dst_bytes_eq5
    #             + count_eq4 + dst_host_count_eq4  →  Consequent: label
    # 8 antecedent attributes (4 nominal + 4 numeric discretized) stress both
    # the discretization pipeline and the mining search space on ~4.9M rows.
    MINE_PMML="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<PMML version=\"4.2\" xmlns=\"http://www.dmg.org/PMML-4_2\">
  <Header>
    <Extension name=\"dataset\" value=\"${DATASET_ID}\"/>
  </Header>
  <TaskSetting>
    <BBASetting id=\"1\"><FieldRef>${ATTR_ID_protocol_type}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"2\"><FieldRef>${ATTR_ID_service}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"3\"><FieldRef>${ATTR_ID_flag}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"4\"><FieldRef>${ATTR_ID_logged_in}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"5\"><FieldRef>${ATTR_ID_src_bytes_eq5}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"6\"><FieldRef>${ATTR_ID_dst_bytes_eq5}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"7\"><FieldRef>${ATTR_ID_count_eq4}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"8\"><FieldRef>${ATTR_ID_dst_host_count_eq4}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"9\"><FieldRef>${ATTR_ID_label}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <DBASetting id=\"101\" type=\"Literal\"><BASettingRef>1</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"102\" type=\"Literal\"><BASettingRef>2</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"103\" type=\"Literal\"><BASettingRef>3</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"104\" type=\"Literal\"><BASettingRef>4</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"105\" type=\"Literal\"><BASettingRef>5</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"106\" type=\"Literal\"><BASettingRef>6</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"107\" type=\"Literal\"><BASettingRef>7</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"108\" type=\"Literal\"><BASettingRef>8</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"109\" type=\"Literal\"><BASettingRef>9</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"200\" type=\"Conjunction\">
      <BASettingRef>101</BASettingRef>
      <BASettingRef>102</BASettingRef>
      <BASettingRef>103</BASettingRef>
      <BASettingRef>104</BASettingRef>
      <BASettingRef>105</BASettingRef>
      <BASettingRef>106</BASettingRef>
      <BASettingRef>107</BASettingRef>
      <BASettingRef>108</BASettingRef>
    </DBASetting>
    <AntecedentSetting>200</AntecedentSetting>
    <ConsequentSetting>109</ConsequentSetting>
    <InterestMeasureThreshold><InterestMeasure>SUPP</InterestMeasure><Threshold>${MIN_SUPPORT}</Threshold></InterestMeasureThreshold>
    <InterestMeasureThreshold><InterestMeasure>CONF</InterestMeasure><Threshold>${MIN_CONFIDENCE}</Threshold></InterestMeasureThreshold>
    <InterestMeasureThreshold><InterestMeasure>RULE_LENGTH</InterestMeasure><Threshold>6</Threshold></InterestMeasureThreshold>
    <HypothesesCountMax>5000</HypothesesCountMax>
  </TaskSetting>
</PMML>"

elif [[ "$DATASET_PROFILE" == "eval" ]]; then
    # Dynamically build mining PMML from eval dataset columns
    bba_xml="" dba_lit_xml="" conjunction_refs="" target_dba_id=""
    bba_id=1 dba_id=101

    for col in "${EVAL_COLUMNS[@]}"; do
        col=$(echo "$col" | tr -d '\r')
        attr_id="${EVAL_ATTR_IDS[$col]}"

        bba_xml+="    <BBASetting id=\"${bba_id}\"><FieldRef>${attr_id}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
"
        dba_lit_xml+="    <DBASetting id=\"${dba_id}\" type=\"Literal\"><BASettingRef>${bba_id}</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
"

        if [[ "$col" == "$EVAL_TARGET" ]]; then
            target_dba_id="$dba_id"
        else
            conjunction_refs+="      <BASettingRef>${dba_id}</BASettingRef>
"
        fi

        bba_id=$((bba_id + 1))
        dba_id=$((dba_id + 1))
    done

    antecedent_count=$(( ${#EVAL_COLUMNS[@]} - 1 ))
    rule_length=$(( antecedent_count + 1 < 6 ? antecedent_count + 1 : 6 ))

    [[ -n "$target_dba_id" ]] || fail "Target variable '${EVAL_TARGET}' not found in columns"

    MINE_PMML="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<PMML version=\"4.2\" xmlns=\"http://www.dmg.org/PMML-4_2\">
  <Header>
    <Extension name=\"dataset\" value=\"${DATASET_ID}\"/>
  </Header>
  <TaskSetting>
${bba_xml}${dba_lit_xml}    <DBASetting id=\"200\" type=\"Conjunction\">
${conjunction_refs}    </DBASetting>
    <AntecedentSetting>200</AntecedentSetting>
    <ConsequentSetting>${target_dba_id}</ConsequentSetting>
    <InterestMeasureThreshold><InterestMeasure>SUPP</InterestMeasure><Threshold>${EVAL_MIN_SUPPORT}</Threshold></InterestMeasureThreshold>
    <InterestMeasureThreshold><InterestMeasure>CONF</InterestMeasure><Threshold>${MIN_CONFIDENCE}</Threshold></InterestMeasureThreshold>
    <InterestMeasureThreshold><InterestMeasure>RULE_LENGTH</InterestMeasure><Threshold>${rule_length}</Threshold></InterestMeasureThreshold>
    <HypothesesCountMax>5000</HypothesesCountMax>
  </TaskSetting>
</PMML>"

else
    # Antecedent: Category + Membership + Satisfaction + Basket  →  Consequent: WillReturn
    MINE_PMML="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<PMML version=\"4.2\" xmlns=\"http://www.dmg.org/PMML-4_2\">
  <Header>
    <Extension name=\"dataset\" value=\"${DATASET_ID}\"/>
  </Header>
  <TaskSetting>
    <BBASetting id=\"1\"><FieldRef>${ATTR_ID_Category}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"2\"><FieldRef>${ATTR_ID_Membership}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"3\"><FieldRef>${ATTR_ID_Satisfaction}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"4\"><FieldRef>${ATTR_ID_Basket}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <BBASetting id=\"5\"><FieldRef>${ATTR_ID_WillReturn}</FieldRef><Coefficient><Type>All</Type></Coefficient></BBASetting>
    <DBASetting id=\"101\" type=\"Literal\"><BASettingRef>1</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"102\" type=\"Literal\"><BASettingRef>2</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"103\" type=\"Literal\"><BASettingRef>3</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"104\" type=\"Literal\"><BASettingRef>4</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"105\" type=\"Literal\"><BASettingRef>5</BASettingRef><LiteralSign>Positive</LiteralSign></DBASetting>
    <DBASetting id=\"200\" type=\"Conjunction\">
      <BASettingRef>101</BASettingRef>
      <BASettingRef>102</BASettingRef>
      <BASettingRef>103</BASettingRef>
      <BASettingRef>104</BASettingRef>
    </DBASetting>
    <AntecedentSetting>200</AntecedentSetting>
    <ConsequentSetting>105</ConsequentSetting>
    <InterestMeasureThreshold><InterestMeasure>SUPP</InterestMeasure><Threshold>${MIN_SUPPORT}</Threshold></InterestMeasureThreshold>
    <InterestMeasureThreshold><InterestMeasure>CONF</InterestMeasure><Threshold>${MIN_CONFIDENCE}</Threshold></InterestMeasureThreshold>
    <InterestMeasureThreshold><InterestMeasure>RULE_LENGTH</InterestMeasure><Threshold>5</Threshold></InterestMeasureThreshold>
    <HypothesesCountMax>1000</HypothesesCountMax>
  </TaskSetting>
</PMML>"
fi

verbose "$MINE_PMML"

http POST "${MINE_API}/mine" -H "Content-Type: application/xml" -d "$MINE_PMML"

[[ "$RESP_STATUS" == "202" ]] || fail "Mining start failed (status: ${RESP_STATUS}): ${RESP_BODY}"
# Scala backend returns XML with <task-id>; Python reimpl returns JSON with taskId
MINE_TASK_ID=$(echo "$RESP_BODY" | python3 -c "
import sys
from xml.etree import ElementTree as ET
raw = sys.stdin.read()
try:
    root = ET.fromstring(raw)
    el = root.find('.//{*}task-id') or root.find('.//task-id')
    if el is not None and el.text:
        print(el.text.strip())
        sys.exit(0)
except Exception:
    pass
import json
try:
    d = json.loads(raw)
    miner = d.get('miner', d)
    print(miner.get('task-id') or miner.get('taskId', ''))
except Exception:
    pass
" 2>/dev/null)
[[ -n "$MINE_TASK_ID" ]] || fail "Could not extract mining task ID from response: ${RESP_BODY}"
ok "Mining task started: ${MINE_TASK_ID}"

poll_mine_task "$MINE_TASK_ID" "mining"

# === Step 10: Fetch mining result ===
step "Fetch mining result"

http GET "${MINE_API}/complete-result/${MINE_TASK_ID}"
[[ "$RESP_STATUS" == "200" ]] || fail "Fetch result failed (status: ${RESP_STATUS}): ${RESP_BODY}"

echo "$RESP_BODY" | python3 -c "
import sys
from xml.etree import ElementTree as ET

raw = sys.stdin.read()

# Scala returns PMML XML directly; Python reimpl may return JSON-wrapped string
import json
try:
    decoded = json.loads(raw)
    if isinstance(decoded, str):
        raw = decoded
except Exception:
    pass

if raw.strip().startswith('<'):
    try:
        root = ET.fromstring(raw)
        # Extract timings from Header Extensions
        header = root.find('.//{http://www.dmg.org/PMML-4_0}Header') or root.find('.//Header')
        if header is not None:
            exts = {e.get('name'): e.get('value') for e in header.findall('.//{*}Extension') or header.findall('.//Extension')}
            pre  = exts.get('pre-mining-time', '?')
            mine = exts.get('mining-time', '?')
            post = exts.get('post-mining-time', '?')
            print(f'  Timing: data-load={pre}ms, mining={mine}ms, post={post}ms')
        ns = {'guha': 'http://keg.vse.cz/ns/GUHA0.1rev1'}
        model = root.find('.//guha:AssociationModel', ns)
        if model is not None:
            n_rules = model.attrib.get('numberOfRules', '?')
            n_txn   = model.attrib.get('numberOfTransactions', '?')
            print(f'  Transactions: {n_txn}, Rules found: {n_rules}')
            rules_el = model.find('guha:AssociationRules', ns)
            if rules_el is not None:
                rules = rules_el.findall('guha:AssociationRule', ns)
                for r in rules[:5]:
                    conf = r.attrib.get('confidence', '?')
                    supp = r.attrib.get('support', '?')
                    ant  = r.find('guha:Antecedent', ns)
                    con  = r.find('guha:Consequent', ns)
                    ant_t = ant.text.strip() if ant is not None and ant.text else r.attrib.get('antecedent', '?')
                    con_t = con.text.strip() if con is not None and con.text else r.attrib.get('consequent', '?')
                    print(f'    {ant_t} => {con_t}  (conf={conf}, supp={supp})')
                if len(rules) > 5:
                    print(f'    ... and {len(rules) - 5} more rules')
        else:
            print(f'  PMML result (no guha:AssociationModel found)')
    except Exception as e:
        print(f'  Could not parse result XML: {e}')
else:
    print(f'  Result (non-XML): {raw[:200]}')
" 2>/dev/null || echo "  (Could not parse result)"

ok "Mining result retrieved"

# Save result for eval profile
if [[ "$DATASET_PROFILE" == "eval" ]]; then
    mkdir -p "$RESULTS_DIR"
    echo "$RESP_BODY" > "${RESULTS_DIR}/${EVAL_DATASET_NAME}.xml"
    ok "Result saved to ${RESULTS_DIR}/${EVAL_DATASET_NAME}.xml"
fi

# === Step 10b: CBA mining task (optional) ===
if $ENABLE_CBA; then
    step "Launch CBA mining task"

    # Inject CBA InterestMeasureThreshold into existing MINE_PMML (before </TaskSetting>)
    CBA_PMML="${MINE_PMML//<\/TaskSetting>/    <InterestMeasureThreshold><InterestMeasure>CBA<\/InterestMeasure><Threshold>1<\/Threshold><\/InterestMeasureThreshold>
  <\/TaskSetting>}"

    verbose "$CBA_PMML"

    http POST "${MINE_API}/mine" -H "Content-Type: application/xml" -d "$CBA_PMML"

    [[ "$RESP_STATUS" == "202" ]] || fail "CBA mining start failed (status: ${RESP_STATUS}): ${RESP_BODY}"
    CBA_TASK_ID=$(echo "$RESP_BODY" | python3 -c "
import sys
from xml.etree import ElementTree as ET
raw = sys.stdin.read()
try:
    root = ET.fromstring(raw)
    el = root.find('.//{*}task-id') or root.find('.//task-id')
    if el is not None and el.text:
        print(el.text.strip())
        sys.exit(0)
except Exception:
    pass
import json
try:
    d = json.loads(raw)
    miner = d.get('miner', d)
    print(miner.get('task-id') or miner.get('taskId', ''))
except Exception:
    pass
" 2>/dev/null)
    [[ -n "$CBA_TASK_ID" ]] || fail "Could not extract CBA task ID from response: ${RESP_BODY}"
    ok "CBA mining task started: ${CBA_TASK_ID}"

    poll_mine_task "$CBA_TASK_ID" "CBA mining"

    step "Fetch CBA mining result"

    http GET "${MINE_API}/complete-result/${CBA_TASK_ID}"
    [[ "$RESP_STATUS" == "200" ]] || fail "Fetch CBA result failed (status: ${RESP_STATUS}): ${RESP_BODY}"

    echo "$RESP_BODY" | python3 -c "
import sys
from xml.etree import ElementTree as ET
import json

raw = sys.stdin.read()
try:
    decoded = json.loads(raw)
    if isinstance(decoded, str):
        raw = decoded
except Exception:
    pass

if raw.strip().startswith('<'):
    try:
        root = ET.fromstring(raw)
        header = root.find('.//{http://www.dmg.org/PMML-4_0}Header') or root.find('.//Header')
        if header is not None:
            exts = {e.get('name'): e.get('value') for e in header.findall('.//{*}Extension') or header.findall('.//Extension')}
            pre  = exts.get('pre-mining-time', '?')
            mine = exts.get('mining-time', '?')
            post = exts.get('post-mining-time', '?')
            print(f'  Timing: data-load={pre}ms, mining={mine}ms, post={post}ms')
            # CBA-specific extensions
            for k in ('cba_applied', 'cba_accuracy', 'cba_original_rules_count', 'cba_m1_rules_count', 'cba_m2_rules_count'):
                if k in exts:
                    print(f'  {k}: {exts[k]}')
        ns = {'guha': 'http://keg.vse.cz/ns/GUHA0.1rev1'}
        model = root.find('.//guha:AssociationModel', ns)
        if model is not None:
            n_rules = model.attrib.get('numberOfRules', '?')
            n_txn   = model.attrib.get('numberOfTransactions', '?')
            print(f'  Transactions: {n_txn}, Rules found: {n_rules}')
    except Exception as e:
        print(f'  Could not parse CBA result XML: {e}')
else:
    print(f'  Result (non-XML): {raw[:200]}')
" 2>/dev/null || echo "  (Could not parse CBA result)"

    ok "CBA mining result retrieved"

    # Save CBA result for eval profile
    if [[ "$DATASET_PROFILE" == "eval" ]]; then
        mkdir -p "$RESULTS_DIR"
        echo "$RESP_BODY" > "${RESULTS_DIR}/${EVAL_DATASET_NAME}_cba.xml"
        ok "CBA result saved to ${RESULTS_DIR}/${EVAL_DATASET_NAME}_cba.xml"
    fi

    # Extract CBA metrics into shell variables
    read CBA_NUM_RULES CBA_ACCURACY CBA_MINING_TIME_MS < <(echo "$RESP_BODY" | python3 -c "
import sys, json
from xml.etree import ElementTree as ET
raw = sys.stdin.read()
try:
    decoded = json.loads(raw)
    if isinstance(decoded, str): raw = decoded
except: pass
num_rules = ''; accuracy = ''; mining_time = ''
if raw.strip().startswith('<'):
    try:
        root = ET.fromstring(raw)
        header = root.find('.//{http://www.dmg.org/PMML-4_0}Header') or root.find('.//Header')
        if header is not None:
            exts = {e.get('name'): e.get('value') for e in header.findall('.//{*}Extension') or header.findall('.//Extension')}
            accuracy = exts.get('cba_accuracy', '')
            mining_time = exts.get('mining-time', '')
        ns = {'guha': 'http://keg.vse.cz/ns/GUHA0.1rev1'}
        model = root.find('.//guha:AssociationModel', ns)
        if model is not None:
            num_rules = model.attrib.get('numberOfRules', '')
    except: pass
print(num_rules, accuracy, mining_time)
" 2>/dev/null || echo "  ")
fi

PHASE_MINE_S=$(( $(date +%s) - PHASE_MINE_START ))

} # end run_single_dataset

# === Multi-dataset or single-dataset execution ===
if [[ -n "$EVAL_DIR" ]]; then
    # Multi-dataset mode
    mkdir -p "$RESULTS_DIR"
    SUMMARY_FILE="${RESULTS_DIR}/summary.csv"
    echo "dataset,status,total_time_s,upload_s,preprocessing_s,mining_s,num_rules,num_transactions,error,cba_num_rules,cba_accuracy,cba_mining_time_ms" > "$SUMMARY_FILE"

    CSV_FILES=("$EVAL_DIR"/*.csv)
    TOTAL_DATASETS=${#CSV_FILES[@]}
    CURRENT_DATASET=0
    PASS_COUNT=0
    FAIL_COUNT=0
    TEST_START=$(date +%s)

    # Override trap for multi-dataset mode (don't exit on single dataset failure)
    trap - EXIT

    for csv in "${CSV_FILES[@]}"; do
        CURRENT_DATASET=$((CURRENT_DATASET + 1))
        ds_name="$(basename "$csv" .csv)"
        echo -e "\n${BOLD}${BLUE}======================================${NC}"
        echo -e "${BOLD}${BLUE}  Dataset ${CURRENT_DATASET}/${TOTAL_DATASETS}: ${ds_name}${NC}"
        echo -e "${BOLD}${BLUE}======================================${NC}"

        # Reset per-dataset state
        CSV_FILE="$csv"
        DATASET_PROFILE="auto"
        DATASOURCE_ID=""
        DATASET_ID=""
        UPLOAD_UUID=""
        STEP=0
        STEP_START=""
        STEP_NAMES=()
        STEP_STARTS=()

        ds_start=$(date +%s)
        ds_status_file=$(mktemp)

        # Run in subshell so fail()/exit doesn't kill the loop
        (
            trap cleanup_on_exit EXIT
            run_single_dataset

            # Extract metrics from saved result
            num_rules="0" num_transactions="0"
            if [[ -f "${RESULTS_DIR}/${ds_name}.xml" ]]; then
                read num_rules num_transactions < <(python3 -c "
import sys, json
from xml.etree import ElementTree as ET
raw = open('${RESULTS_DIR}/${ds_name}.xml').read()
try:
    decoded = json.loads(raw)
    if isinstance(decoded, str): raw = decoded
except: pass
if raw.strip().startswith('<'):
    root = ET.fromstring(raw)
    ns = {'guha': 'http://keg.vse.cz/ns/GUHA0.1rev1'}
    model = root.find('.//guha:AssociationModel', ns)
    if model is not None:
        print(model.attrib.get('numberOfRules', '0'), model.attrib.get('numberOfTransactions', '0'))
        sys.exit(0)
print('0 0')
" 2>/dev/null || echo "0 0")
            fi
            echo "pass|${num_rules}|${num_transactions}|${PHASE_UPLOAD_S}|${PHASE_PREP_S}|${PHASE_MINE_S}||${CBA_NUM_RULES}|${CBA_ACCURACY}|${CBA_MINING_TIME_MS}" > "$ds_status_file"
        ) || echo "fail|0|0||||step ${STEP}|||" > "$ds_status_file"

        ds_elapsed=$(( $(date +%s) - ds_start ))
        IFS='|' read -r ds_status ds_rules ds_txn ds_upload ds_prep ds_mine ds_error ds_cba_rules ds_cba_accuracy ds_cba_mining_time < "$ds_status_file"
        rm -f "$ds_status_file"

        if [[ "$ds_status" == "pass" ]]; then
            PASS_COUNT=$((PASS_COUNT + 1))
            echo -e "${GREEN}${BOLD}  [PASS]${NC} ${ds_name} (${ds_elapsed}s, ${ds_rules} rules)"
        else
            FAIL_COUNT=$((FAIL_COUNT + 1))
            echo -e "${RED}${BOLD}  [FAIL]${NC} ${ds_name} (${ds_elapsed}s, error: ${ds_error})"
        fi

        echo "${ds_name},${ds_status},${ds_elapsed},${ds_upload},${ds_prep},${ds_mine},${ds_rules},${ds_txn},${ds_error},${ds_cba_rules},${ds_cba_accuracy},${ds_cba_mining_time}" >> "$SUMMARY_FILE"
    done

    # Overall summary
    TEST_END=$(date +%s)
    TEST_ELAPSED=$(( TEST_END - TEST_START ))
    TEST_MIN=$(( TEST_ELAPSED / 60 ))
    TEST_SEC=$(( TEST_ELAPSED % 60 ))

    echo -e "\n${BOLD}=== Evaluation Summary ===${NC}"
    echo -e "  Passed: ${GREEN}${PASS_COUNT}${NC}/${TOTAL_DATASETS}"
    echo -e "  Failed: ${RED}${FAIL_COUNT}${NC}/${TOTAL_DATASETS}"
    echo -e "  Total time: ${TEST_MIN}m ${TEST_SEC}s"
    echo -e "  Results: ${RESULTS_DIR}/summary.csv"

    if [[ $FAIL_COUNT -gt 0 ]]; then
        echo -e "\n  ${YELLOW}Failed datasets:${NC}"
        grep ',fail,' "$SUMMARY_FILE" | while IFS=',' read -r name status time _upl _prep _mine rules txn err; do
            echo -e "    ${RED}-${NC} ${name}: ${err}"
        done
    fi

    exit $(( FAIL_COUNT > 0 ? 1 : 0 ))
fi

# Single-dataset mode
TEST_START=$(date +%s)
run_single_dataset

# === Done ===
TEST_END=$(date +%s)
TEST_ELAPSED=$(( TEST_END - TEST_START ))
TEST_MIN=$(( TEST_ELAPSED / 60 ))
TEST_SEC=$(( TEST_ELAPSED % 60 ))

echo -e "\n${GREEN}${BOLD}=== Integration test completed successfully ===${NC}"
echo -e "  Datasource ID: ${DATASOURCE_ID}"
echo -e "  Dataset ID:    ${DATASET_ID}"
echo -e "  Mining task:   ${MINE_TASK_ID}"
echo -e "  Total time:    ${TEST_MIN}m ${TEST_SEC}s"

if ! $CLEANUP; then
    echo -e "\n  ${YELLOW}Tip:${NC} Run with --cleanup to delete created resources"
fi
