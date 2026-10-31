#!/usr/bin/env bash
# self_check.sh
#
# A non-mounting self-check for the IoTDB-FS environment.
# Validates binary execution, configuration, and IoTDB connectivity.

set -Eeuo pipefail

# Find the root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_PATH="${ROOT_DIR}/bin/iotdb_fs"
WEB_BIN_PATH="${ROOT_DIR}/bin/iotdb_fs_web"
CONF_FILE="${ROOT_DIR}/resources/config.properties"

# ANSI colors
RED='\e[1;31m'
GREEN='\e[1;32m'
YELLOW='\e[1;33m'
CYAN='\e[1;36m'
NC='\e[0m'

log_info() { printf "${GREEN}[check]${NC} %s\n" "$*"; }
log_warn() { printf "${YELLOW}[warn]${NC} %s\n" "$*"; }
log_error() { printf "${RED}[error]${NC} %s\n" "$*" >&2; }
log_step() { printf "\n${CYAN}>>> %s${NC}\n" "$*"; }

error_exit() { log_error "$*"; exit 1; }

echo "===================================================="
printf "       IoTDB-FS Runtime Environment Self-Check\n"
echo "===================================================="

# 1. Binary Check
log_step "Checking Binary Integrity"
if [[ ! -x "$BIN_PATH" ]]; then
    error_exit "Binary not found or not executable: $BIN_PATH"
fi
if [[ ! -x "$WEB_BIN_PATH" ]]; then
    error_exit "Web binary not found or not executable: $WEB_BIN_PATH"
fi
log_info "Binaries exist and are executable."

# Check if it can run (at least show help)
if ! "$BIN_PATH" --help > /dev/null 2>&1; then
    # We ignore the exit code, just check if it fails to load due to missing libraries
    LDD_OUT=$(ldd "$BIN_PATH" 2>&1 || true)
    if [[ "$LDD_OUT" =~ "not found" ]]; then
        log_error "Dependency check failed. Missing libraries:"
        echo "$LDD_OUT" | grep "not found"
        exit 1
    fi
fi
if ! "$WEB_BIN_PATH" --help > /dev/null 2>&1; then
    LDD_OUT=$(ldd "$WEB_BIN_PATH" 2>&1 || true)
    if [[ "$LDD_OUT" =~ "not found" ]]; then
        log_error "Web binary dependency check failed. Missing libraries:"
        echo "$LDD_OUT" | grep "not found"
        exit 1
    fi
fi
log_info "Binary load tests passed."

# 2. Configuration Check
log_step "Checking Configuration"
if [[ ! -f "$CONF_FILE" ]]; then
    error_exit "Configuration file not found: $CONF_FILE"
fi
log_info "Config file found: $CONF_FILE"

# Extract connection details
IOTDB_HOST=$(grep "^iotdb.host=" "$CONF_FILE" | cut -d'=' -f2- | sed 's/[[:space:]]*$//')
IOTDB_PORT=$(grep "^iotdb.port=" "$CONF_FILE" | cut -d'=' -f2- | sed 's/[[:space:]]*$//')
IOTDB_USER=$(grep "^iotdb.user=" "$CONF_FILE" | cut -d'=' -f2- | sed 's/[[:space:]]*$//')

log_info "Target IoTDB: ${IOTDB_HOST}:${IOTDB_PORT} (User: ${IOTDB_USER})"

if [[ "$IOTDB_HOST" == "localhost" || "$IOTDB_HOST" == "hostname" ]]; then
    log_warn "Static builds might have issues with hostname resolution. Using IP address is recommended."
fi

# 3. Connectivity Check (Socket level)
log_step "Checking Network Connectivity"
if command -v nc >/dev/null 2>&1; then
    if nc -z -w 3 "$IOTDB_HOST" "$IOTDB_PORT" > /dev/null 2>&1; then
        log_info "Network connection to ${IOTDB_HOST}:${IOTDB_PORT} established."
    else
        log_warn "Could not connect to ${IOTDB_HOST}:${IOTDB_PORT}. Is IoTDB running?"
    fi
else
    # Fallback to /dev/tcp
    if (echo > /dev/tcp/"$IOTDB_HOST"/"$IOTDB_PORT") > /dev/null 2>&1; then
         log_info "Network connection to ${IOTDB_HOST}:${IOTDB_PORT} established."
    else
         log_warn "Could not connect to ${IOTDB_HOST}:${IOTDB_PORT}. Is IoTDB running?"
    fi
fi

# 4. IoTDB Session Test (Attempt to use the binary to check connection if supported)
# Since we haven't added a --test-connection flag yet, we'll suggest using it.
log_step "System Verification"
log_info "1. FUSE support: $([ -c /dev/fuse ] && echo "Detected (/dev/fuse)" || echo "Not detected (mounting may fail)")"
log_info "2. Current User: $(whoami)"

echo ""
echo "===================================================="
log_info "Self-check completed."
echo "===================================================="
printf "If network connectivity was successful, you can proceed to mount:\n"
printf "  ./scripts/start.sh\n"
printf "Or launch the web explorer:\n"
printf "  ./bin/iotdb_fs_web --host 0.0.0.0 --port 8080\n"
echo "===================================================="
