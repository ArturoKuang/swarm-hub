#!/usr/bin/env bash
# Install the swarm hub as a per-user LaunchAgent on the Mac mini (or any Mac).
# Idempotent: re-running rewrites the plist and restarts the agent with the same token.
#
#   scripts/install-hub-macmini.sh            # install + (re)start
#   scripts/install-hub-macmini.sh --no-load  # write files only
#   scripts/install-hub-macmini.sh --uninstall
set -euo pipefail

LABEL="com.swarm-hub.hub"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${SWARM_HUB_HOME:-$HOME/.swarm-hub}"
ENV_FILE="$STATE_DIR/env"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_DIR="$HOME/Library/Logs/swarm-hub"
LOAD=1

for arg in "$@"; do
  case "$arg" in
    --no-load) LOAD=0 ;;
    --uninstall)
      launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
      rm -f "$PLIST"
      echo "removed $LABEL (state in $STATE_DIR kept)"
      exit 0 ;;
    -h|--help) sed -n '2,8p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

[[ "$(uname -s)" == "Darwin" ]] || { echo "this installer is for macOS; on Linux run: npm run hub" >&2; exit 1; }
NODE="$(command -v node || true)"
[[ -n "$NODE" ]] || { echo "node not found on PATH (need Node >= 24)" >&2; exit 1; }
NODE_MAJOR="$("$NODE" -p 'process.versions.node.split(".")[0]')"
(( NODE_MAJOR >= 24 )) || { echo "Node >= 24 required, found $("$NODE" --version)" >&2; exit 1; }

mkdir -p "$STATE_DIR" "$LOG_DIR" "$(dirname "$PLIST")"
chmod 700 "$STATE_DIR"
[[ -d "$REPO_DIR/node_modules/zod" ]] || (cd "$REPO_DIR" && npm ci --omit=dev --no-audit --no-fund >/dev/null)

# Keep an existing token so joined workers keep working across reinstalls.
if [[ ! -f "$ENV_FILE" ]]; then
  umask 077
  cat > "$ENV_FILE" <<EOF
SWARM_HUB_TOKEN=$(openssl rand -hex 24)
SWARM_HUB_HOST=0.0.0.0
SWARM_HUB_PORT=8900
SWARM_HUB_DB=$STATE_DIR/hub.db
OMB_URL=http://127.0.0.1:8799
EOF
fi
set -a; source "$ENV_FILE"; set +a

xml_escape() { sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' <<<"$1"; }
env_entries=""
while IFS='=' read -r key value; do
  [[ -z "$key" || "$key" == \#* ]] && continue
  env_entries+="    <key>$(xml_escape "$key")</key><string>$(xml_escape "$value")</string>"$'\n'
done < "$ENV_FILE"

cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$NODE</string>
    <string>--experimental-strip-types</string>
    <string>--disable-warning=ExperimentalWarning</string>
    <string>$REPO_DIR/src/hub.ts</string>
  </array>
  <key>WorkingDirectory</key><string>$REPO_DIR</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>$(dirname "$NODE"):/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
$env_entries  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>5</integer>
  <key>StandardOutPath</key><string>$LOG_DIR/hub.log</string>
  <key>StandardErrorPath</key><string>$LOG_DIR/hub.log</string>
</dict>
</plist>
EOF
chmod 600 "$PLIST"
plutil -lint "$PLIST" >/dev/null

TS_IP="$(tailscale ip -4 2>/dev/null | grep -E '^[0-9.]+$' | head -1 || true)"
HUB_URL="http://${TS_IP:-$(hostname)}:${SWARM_HUB_PORT}"

if (( LOAD )); then
  launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$PLIST"
  for _ in {1..20}; do curl -fsS "http://127.0.0.1:${SWARM_HUB_PORT}/health" >/dev/null 2>&1 && break; sleep 0.5; done
  curl -fsS "http://127.0.0.1:${SWARM_HUB_PORT}/health" >/dev/null && echo "hub is up: $HUB_URL" || { echo "hub did not come up; see $LOG_DIR/hub.log" >&2; exit 1; }
else
  echo "wrote $PLIST (not loaded)"
fi

cat <<EOF

Join a worker from any tailnet machine:
  SWARM_HUB_TOKEN=<token from $ENV_FILE> scripts/join-worker.sh --hub $HUB_URL
Token: stored in $ENV_FILE (mode 600)
Logs:  $LOG_DIR/hub.log
EOF
