#!/usr/bin/env bash
# Join this machine to the swarm as a worker: a LaunchAgent on macOS, a systemd --user unit on Linux.
# Idempotent: re-running rewrites the service and restarts it.
#
#   SWARM_HUB_TOKEN=... scripts/join-worker.sh --hub http://100.x.y.z:8900 [--node-id ID] [--slots N]
#                                              [--tags a,b] [--projects DIR] [--no-load] [--uninstall]
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HUB_URL="${SWARM_HUB_URL:-}"
NODE_ID="${SWARM_NODE_ID:-$(hostname -s | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_\n-' '-')}"
SLOTS="${SWARM_MAX_SLOTS:-2}"
TAGS="${SWARM_EXTRA_TAGS:-}"
PROJECTS="${SWARM_PROJECTS_DIR:-$HOME/Projects}"
TOKEN="${SWARM_HUB_TOKEN:-}"
LOAD=1
UNINSTALL=0

while (( $# )); do
  case "$1" in
    --hub) HUB_URL="$2"; shift 2 ;;
    --node-id) NODE_ID="$2"; shift 2 ;;
    --slots) SLOTS="$2"; shift 2 ;;
    --tags) TAGS="$2"; shift 2 ;;
    --projects) PROJECTS="$2"; shift 2 ;;
    --no-load) LOAD=0; shift ;;
    --uninstall) UNINSTALL=1; shift ;;
    -h|--help) sed -n '2,6p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

OS="$(uname -s)"
LABEL="com.swarm-hub.worker"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
UNIT="$HOME/.config/systemd/user/swarm-worker.service"

if (( UNINSTALL )); then
  if [[ "$OS" == "Darwin" ]]; then
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true; rm -f "$PLIST"
  else
    systemctl --user disable --now swarm-worker.service 2>/dev/null || true; rm -f "$UNIT"; systemctl --user daemon-reload || true
  fi
  echo "worker service removed"; exit 0
fi

[[ -n "$HUB_URL" ]] || { echo "--hub URL is required (e.g. http://100.64.0.1:8900)" >&2; exit 2; }
[[ "$SLOTS" =~ ^[0-9]+$ ]] || { echo "--slots must be a number" >&2; exit 2; }
NODE="$(command -v node || true)"
[[ -n "$NODE" ]] || { echo "node not found on PATH (need Node >= 24)" >&2; exit 1; }
(( $("$NODE" -p 'process.versions.node.split(".")[0]') >= 24 )) || { echo "Node >= 24 required" >&2; exit 1; }
command -v git >/dev/null || { echo "git is required" >&2; exit 1; }

auth=()
[[ -n "$TOKEN" ]] && auth=(-H "Authorization: Bearer $TOKEN")
curl -fsS -m 5 ${auth[@]+"${auth[@]}"} "$HUB_URL/api/fleet" >/dev/null \
  || { echo "cannot reach $HUB_URL/api/fleet (check Tailscale and SWARM_HUB_TOKEN)" >&2; exit 1; }

[[ -d "$REPO_DIR/node_modules/zod" ]] || (cd "$REPO_DIR" && npm ci --omit=dev --no-audit --no-fund >/dev/null)

PATH_VALUE="$(dirname "$NODE"):$HOME/.local/bin:$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
declare -a ENV_PAIRS=(
  "SWARM_HUB_URL=$HUB_URL" "SWARM_HUB_TOKEN=$TOKEN" "SWARM_NODE_ID=$NODE_ID" "SWARM_MAX_SLOTS=$SLOTS"
  "SWARM_EXTRA_TAGS=$TAGS" "SWARM_PROJECTS_DIR=$PROJECTS" "PATH=$PATH_VALUE" "HOME=$HOME"
)
ARGS=("$NODE" --experimental-strip-types --disable-warning=ExperimentalWarning "$REPO_DIR/src/worker.ts")

if [[ "$OS" == "Darwin" ]]; then
  LOG_DIR="$HOME/Library/Logs/swarm-hub"
  mkdir -p "$LOG_DIR" "$(dirname "$PLIST")"
  xml_escape() { sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' <<<"$1"; }
  {
    echo '<?xml version="1.0" encoding="UTF-8"?>'
    echo '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">'
    echo '<plist version="1.0"><dict>'
    echo "  <key>Label</key><string>$LABEL</string>"
    echo '  <key>ProgramArguments</key><array>'
    for a in "${ARGS[@]}"; do echo "    <string>$(xml_escape "$a")</string>"; done
    echo '  </array>'
    echo "  <key>WorkingDirectory</key><string>$(xml_escape "$REPO_DIR")</string>"
    echo '  <key>EnvironmentVariables</key><dict>'
    for pair in "${ENV_PAIRS[@]}"; do echo "    <key>${pair%%=*}</key><string>$(xml_escape "${pair#*=}")</string>"; done
    echo '  </dict>'
    echo '  <key>RunAtLoad</key><true/><key>KeepAlive</key><true/><key>ThrottleInterval</key><integer>5</integer>'
    echo "  <key>StandardOutPath</key><string>$LOG_DIR/worker.log</string>"
    echo "  <key>StandardErrorPath</key><string>$LOG_DIR/worker.log</string>"
    echo '</dict></plist>'
  } > "$PLIST"
  chmod 600 "$PLIST"
  plutil -lint "$PLIST" >/dev/null
  if (( LOAD )); then
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
  fi
  echo "worker $NODE_ID → $HUB_URL (LaunchAgent $LABEL, logs $LOG_DIR/worker.log)"
else
  mkdir -p "$(dirname "$UNIT")"
  {
    echo "[Unit]"; echo "Description=swarm-hub worker"; echo "After=network-online.target"
    echo "[Service]"
    for pair in "${ENV_PAIRS[@]}"; do printf 'Environment="%s"\n' "${pair//\"/\\\"}"; done
    printf 'ExecStart='; printf '%q ' "${ARGS[@]}"; echo
    echo "WorkingDirectory=$REPO_DIR"; echo "Restart=always"; echo "RestartSec=5"
    echo "[Install]"; echo "WantedBy=default.target"
  } > "$UNIT"
  chmod 600 "$UNIT"
  if (( LOAD )); then
    systemctl --user daemon-reload
    systemctl --user enable --now swarm-worker.service
    systemctl --user restart swarm-worker.service
  fi
  echo "worker $NODE_ID → $HUB_URL (systemd --user swarm-worker.service; journalctl --user -u swarm-worker -f)"
fi
