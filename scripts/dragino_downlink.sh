#!/usr/bin/env bash
# ==============================================================================
# Dragino Downlink Helper for TTN (HTTP v3) – CSV batch + single-device mode
#
# What it does
# ------------
# * Queues a small sequence of Dragino downlinks on TTN via /down/replace:
#     1) Set TDC to 60s (fast phase to drain the queue quickly)
#     2) Set measurement profile (0–2 m depth profile)
#     3) (optional) 12V pulse (when an external PS-LB powers the probe)
#     4) Set final TDC in minutes (from CSV or CLI)
# * Works in two modes:
#     A) CSV mode (default): read devices from CSV (device_id,minutes,ext_power)
#     B) Single-device mode: use command-line flags (-d/-m/-x) for one device
#
# Key constraints / design choices
# --------------------------------
# * No jitter, no random sleeps.
# * No 'confirmed' and no 'priority' in the downlink JSON (lean payload).
# * Delay between devices is used only in CSV mode (DELAY_BETWEEN_DEV, default 1s).
# * Class A reminder: downlinks are delivered AFTER the next uplink (FIFO).
#
# Secrets / configuration
# -----------------------
# * REQUIRED .env file (default: ./scripts/.env). Override with:
#       ENV_FILE=/absolute/path/.env ./dragino_downlink.sh
# * .env MUST define:
#       TTN_APP_ID, TTN_REGION, TTN_API_KEY
#   (Do NOT commit this file; if a key leaks, rotate it in TTN Console.)
#
# Examples
# --------
# 1) Single device, 15 min final TDC, with 12V pulse (ext_power=0):
#       ./dragino_downlink.sh -d dds75-lb-13 -m 15 -x 0
# 2) Single device, 30 min final TDC, without 12V pulse (ext_power=1/default):
#       ./dragino_downlink.sh -d dds75-lb-13 -m 30
# 3) CSV batch (default file: devices.csv):
#       ./dragino_downlink.sh
#
# CSV format (no header required):
#   device_id,interval_min,ext_power
#   dds75-lb-01,15,0
#   dds75-lb-02,30,1
#   ...
# ==============================================================================

set -euo pipefail
# -e : exit on error
# -u : treat unset variables as errors
# -o pipefail : pipeline fails if any command within fails

# ------------------------------------------------------------------------------
# 1) Load REQUIRED .env (override path via ENV_FILE=...)
# ------------------------------------------------------------------------------
ENV_FILE="${ENV_FILE:-./scripts/.env}"
if [ -f "$ENV_FILE" ]; then
  # `set -a` auto-exports variables sourced from the .env into the environment
  set -a
  . "$ENV_FILE"
  set +a
else
  echo "Error: ENV file '$ENV_FILE' not found." >&2
  echo "Create it with TTN_APP_ID, TTN_REGION, TTN_API_KEY, e.g.:" >&2
  echo "  TTN_APP_ID=your-app" >&2
  echo "  TTN_REGION=eu1" >&2
  echo "  TTN_API_KEY=NNSXS... (Application key with downlink rights)" >&2
  echo "Tip: run with ENV_FILE=/path/.env ./dragino_downlink.sh" >&2
  echo "Security: never commit .env; if a key leaked, rotate it in TTN Console." >&2
  exit 1
fi

# ------------------------------------------------------------------------------
# 2) Essential config from environment
#    (these ?: checks make them mandatory and fail fast with a clear message)
# ------------------------------------------------------------------------------
APP="${TTN_APP_ID:?TTN_APP_ID is required}"
REGION="${TTN_REGION:?TTN_REGION is required}"     # e.g., eu1
KEY="${TTN_API_KEY:?TTN_API_KEY is required}"       # NNSXS...
API="https://${REGION}.cloud.thethings.network/api/v3"

# Optional overrides (from ENV or .env)
CSV="${CSV:-devices.csv}"                           # CSV: device_id,interval_min,ext_power
FPORT="${FPORT:-2}"                                 # FPort used by the device decoder
DELAY_BETWEEN_DEV="${DELAY_BETWEEN_DEV:-1.0}"       # used in CSV mode only

# ------------------------------------------------------------------------------
# 3) Dependency checks (hard requirements)
# ------------------------------------------------------------------------------
need(){ command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1" >&2; exit 1; }; }
need xxd
need base64
need curl
need jq
need awk

# ------------------------------------------------------------------------------
# 4) CLI parsing (optional single-device run)
#    -d DEVICE  : TTN device ID
#    -m MINUTES : final interval in minutes (integer)
#    -x EXT     : "0" => include 12V pulse, "1" (default) => no 12V pulse
# ------------------------------------------------------------------------------
DEVICE=""; MINUTES=""; EXT=""
usage(){
  cat <<USAGE
Usage:
  $0                  # CSV mode (reads ${CSV})
  $0 -d DEVICE -m MIN [-x 0|1]

Options:
  -d DEVICE   TTN device ID (single-device mode)
  -m MIN      Final measurement interval in minutes (integer)
  -x EXT      ext_power flag: "0" => add 12V pulse, "1" (default) => skip 12V
  -h          Show this help

Examples:
  $0 -d dds75-lb-13 -m 15 -x 0
  $0 -d dds75-lb-13 -m 30
  $0   # CSV mode, uses ${CSV}
USAGE
}
while getopts ":d:m:x:h" opt; do
  case "$opt" in
    d) DEVICE="$OPTARG" ;;
    m) MINUTES="$OPTARG" ;;
    x) EXT="$OPTARG" ;;
    h) usage; exit 0 ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage; exit 2 ;;
    :)  echo "Option -$OPTARG requires an argument" >&2; usage; exit 2 ;;
  esac
done
# default: skip 12V pulse unless explicitly -x 0
EXT="${EXT:-1}"

# ------------------------------------------------------------------------------
# 5) Helpers
#    a) to_b64: hex string -> raw bytes -> base64 (TTN expects base64 in frm_payload)
#    b) tdc_hex_from_minutes: minutes -> seconds -> 3-byte big-endian hex (6 hex chars)
# ------------------------------------------------------------------------------
to_b64(){ printf "%s" "$1" | xxd -r -p | base64; }

tdc_hex_from_minutes(){
  local min="$1"
  local sec=$(( min * 60 ))
  # Format as 3-byte big-endian hex: 000000 .. FFFFFF (uppercase)
  printf "%06X" "$sec"
}

# ------------------------------------------------------------------------------
# 6) Fixed Dragino commands (HEX payloads)
#    Adjust only if your firmware uses different opcodes/semantics.
# ------------------------------------------------------------------------------
HEX_PROBE="080002"      # Set probe profile: depth 0–2 m
HEX_TDC_60S="0100003C"  # TDC = 60 s (initial fast phase)
HEX_12V_8S="07031F40"   # 12 V pulse = 8000 ms (used when PS-LB feeds the probe)

# ------------------------------------------------------------------------------
# 7) Core: queue downlinks for ONE device (builds JSON and POSTs /down/replace)
#    Sequence: TDC=60s -> PROBE -> (optional 12V) -> TDC=final
#    Notes:
#      - We DO NOT set 'confirmed' or 'priority' (keep payload minimal).
#      - The queue is replaced atomically (single API call).
# ------------------------------------------------------------------------------
queue_for_device() {
  local DEV="$1"        # device id
  local MIN="$2"        # final interval (minutes)
  local EXTPOWER="$3"   # "0" => include 12V pulse; anything else => skip

  # Build final TDC payload: 01 + 3-byte big-endian seconds
  local TDC3; TDC3=$(tdc_hex_from_minutes "$MIN")
  local HEX_TDC_FINAL="01${TDC3}"

  # Convert HEX to base64 (what TTN expects in frm_payload)
  local P_TDC60 P_PROBE P_12V8 P_TDCF
  P_TDC60=$(to_b64 "$HEX_TDC_60S")
  P_PROBE=$(to_b64 "$HEX_PROBE")
  P_12V8=$(to_b64 "$HEX_12V_8S")
  P_TDCF=$(to_b64 "$HEX_TDC_FINAL")

  echo "== ${DEV} (final interval ${MIN} min, ext_power=${EXTPOWER}) =="

  # Compose "downlinks" array; order matters (FIFO processed across uplinks).
  local JSON
  if [[ "$EXTPOWER" == "0" ]]; then
    # include 12V pulse
    JSON=$(jq -n --arg p1 "$P_TDC60" --arg p2 "$P_PROBE" --arg p3 "$P_12V8" --arg p4 "$P_TDCF" --argjson fp "$FPORT" \
      '{downlinks:[
        {f_port:$fp, frm_payload:$p1},
        {f_port:$fp, frm_payload:$p2},
        {f_port:$fp, frm_payload:$p3},
        {f_port:$fp, frm_payload:$p4}
      ]}')
  else
    # skip 12V pulse
    JSON=$(jq -n --arg p1 "$P_TDC60" --arg p2 "$P_PROBE" --arg p4 "$P_TDCF" --argjson fp "$FPORT" \
      '{downlinks:[
        {f_port:$fp, frm_payload:$p1},
        {f_port:$fp, frm_payload:$p2},
        {f_port:$fp, frm_payload:$p4}
      ]}')
  fi

  # Single API call: replace the device queue
  local HTTP
  HTTP=$(curl -sS -o /tmp/resp.json -w "%{http_code}" \
    -X POST "$API/as/applications/$APP/devices/$DEV/down/replace" \
    -H "Authorization: Bearer $KEY" \
    -H "Content-Type: application/json" \
    -d "$JSON")

  echo "HTTP: $HTTP"
  cat /tmp/resp.json; echo
  if ! [[ "$HTTP" =~ ^2 ]]; then
    echo "Error: queuing downlinks for '$DEV' failed (HTTP $HTTP). See response above." >&2
    return 1
  fi
}

# ------------------------------------------------------------------------------
# 8A) Single-device mode (if -d is provided)
# ------------------------------------------------------------------------------
if [[ -n "$DEVICE" ]]; then
  # Validate required args for this mode
  [[ -n "$MINUTES" ]] || { echo "Missing -m MINUTES for single run." >&2; usage; exit 2; }
  [[ "$MINUTES" =~ ^[0-9]+$ ]] || { echo "-m must be an integer (minutes)" >&2; exit 2; }
  [[ "$EXT" =~ ^[01]$ ]] || { echo "-x must be 0 or 1" >&2; exit 2; }

  queue_for_device "$DEVICE" "$MINUTES" "$EXT"
  echo "Note (Class A): downlinks are delivered AFTER the next uplink (usually one per uplink)."
  exit 0
fi

# ------------------------------------------------------------------------------
# 8B) CSV mode (default)
# ------------------------------------------------------------------------------
if [ ! -f "$CSV" ]; then
  echo "Error: CSV '$CSV' not found. Expecting 'device_id,interval_min,ext_power' rows." >&2
  exit 1
fi

# Iterate devices from CSV; add a gentle fixed delay between devices.
# No random jitter, just a simple sleep.
while IFS=, read -r DEV MINUTES EXT; do
  # Skip empty lines and comments
  [[ -z "${DEV// }" || "$DEV" =~ ^# ]] && continue

  # Trim whitespace from all fields
  DEV="$(echo "$DEV" | awk '{$1=$1};1')"
  MINUTES="$(echo "$MINUTES" | awk '{$1=$1};1')"
  EXT="$(echo "$EXT" | awk '{$1=$1};1')"

  # Light input validation for CSV entries
  if ! [[ "$MINUTES" =~ ^[0-9]+$ ]]; then
    echo "Warning: invalid minutes '$MINUTES' for device '$DEV' — skipping." >&2
    continue
  fi
  if ! [[ "$EXT" =~ ^[01]$ ]]; then
    echo "Warning: invalid ext_power '$EXT' for device '$DEV' (use 0 or 1) — defaulting to 1." >&2
    EXT="1"
  fi

  queue_for_device "$DEV" "$MINUTES" "$EXT"

  # Fixed delay between devices (to be nice to the API and your gateways)
  sleep "$DELAY_BETWEEN_DEV"
done < "$CSV"

echo "Note (Class A): one downlink is delivered after each uplink."
echo "Keep TDC=60s until each device has drained its queue; then it switches to the final interval."
