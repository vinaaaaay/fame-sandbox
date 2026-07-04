#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
SANDBOX_MCP_PORT="${SANDBOX_MCP_PORT:-8081}"
CONTAINER_PORT="${CONTAINER_PORT:-8080}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; }

# ── Preflight: Arch and OS ──────────────────────────────────────────────────
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    aarch64) ARCH="arm64"  ;;
    *)       err "Unsupported architecture: $ARCH"; exit 1 ;;
esac

if ! grep -qi ubuntu /etc/os-release 2>/dev/null; then
    warn "This script is tested on Ubuntu only. Proceed at your own risk."
fi

# ── 1. Install Docker (if missing) ──────────────────────────────────────────
if command -v docker &>/dev/null; then
    info "Docker already installed ($(docker --version))"
else
    info "Installing Docker..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq ca-certificates curl gnupg
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    UBUNTU_CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")
    echo "deb [arch=$ARCH signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $UBUNTU_CODENAME stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update -qq
    sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    sudo usermod -aG docker "$USER"
    info "Docker installed. You may need to log out and back in for group changes to take effect."
    # Start Docker if not running
    sudo systemctl enable --now docker 2>/dev/null || true
fi

# ── 2. Verify docker compose ────────────────────────────────────────────────
if ! docker compose version &>/dev/null; then
    err "docker compose plugin not found. Please install docker-compose-plugin."
    exit 1
fi
info "docker compose $(docker compose version --short)"

# ── 3. Check .env files ─────────────────────────────────────────────────────
if [ ! -f "$REPO_DIR/sandbox_mcp/.env" ]; then
    warn "sandbox_mcp/.env not found — creating from template"
    cat > "$REPO_DIR/sandbox_mcp/.env" <<-EOF
SANDBOX_MCP_AUTH_KEY="$(openssl rand -hex 16)"
EOF
fi

if [ ! -f "$REPO_DIR/sandbox_mcp/client/.env" ]; then
    cp "$REPO_DIR/sandbox_mcp/.env" "$REPO_DIR/sandbox_mcp/client/.env"
fi

info ".env files ready"

# ── 4. Build & start the sandbox container ──────────────────────────────────
info "Building sandbox Docker image (this may take a while)..."
docker compose -f "$REPO_DIR/docker-compose.yaml" build sandbox

info "Starting sandbox container..."
docker compose -f "$REPO_DIR/docker-compose.yaml" up -d sandbox
info "Sandbox container started. Waiting for health check..."

# Health check
HEALTH_URL="http://localhost:${CONTAINER_PORT}/v1/bash/exec"
PROBE_CMD="python3 -c 'print(\"ready\")'"
MAX_WAIT=120
START_TIME=$(date +%s)
while true; do
    NOW=$(date +%s)
    if [ $((NOW - START_TIME)) -ge $MAX_WAIT ]; then
        warn "Sandbox health check timed out after ${MAX_WAIT}s — continuing anyway"
        break
    fi
    if curl -sf -X POST "$HEALTH_URL" \
        -H "Content-Type: application/json" \
        -d "{\"command\": \"$PROBE_CMD\"}" \
        --max-time 10 > /dev/null 2>&1; then
        info "Sandbox container is healthy"
        break
    fi
    sleep 2
done

# ── 5. Create Python venv & install deps ────────────────────────────────────
info "Setting up Python virtual environment..."
if [ ! -d "$REPO_DIR/mcp_env" ]; then
    python3 -m venv "$REPO_DIR/mcp_env"
fi

source "$REPO_DIR/mcp_env/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "$REPO_DIR/requirements_mcp.txt"
deactivate
info "Python dependencies installed"

# ── 6. Install MCP server systemd service ───────────────────────────────────
SERVICE_NAME="sandbox-mcp"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

info "Configuring systemd service for MCP server..."
sudo tee "$SERVICE_FILE" > /dev/null <<-EOF
[Unit]
Description=Sandbox MCP Server
After=docker.service network-online.target
Requires=docker.service
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$REPO_DIR
Environment=SANDBOX_MCP_PORT=$SANDBOX_MCP_PORT
Environment=SANDBOX_MCP_AUTH_KEY=$(grep SANDBOX_MCP_AUTH_KEY "$REPO_DIR/sandbox_mcp/.env" | cut -d= -f2- | tr -d '"')
Environment=CONTAINER_URL=http://localhost:$CONTAINER_PORT
Environment=SANDBOX_COMPOSE_DIR=$REPO_DIR
ExecStart=$REPO_DIR/mcp_env/bin/python $REPO_DIR/sandbox_mcp/main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
info "systemd service configured and enabled"

sudo systemctl restart "$SERVICE_NAME"
info "MCP server service restarted"

# ── 7. Summary ──────────────────────────────────────────────────────────────
AUTH_KEY=$(grep SANDBOX_MCP_AUTH_KEY "$REPO_DIR/sandbox_mcp/.env" | cut -d= -f2- | tr -d '"')
echo ""
echo "==============================================="
echo -e "${GREEN}  Setup complete!${NC}"
echo "==============================================="
echo ""
echo "  Sandbox container  : http://localhost:${CONTAINER_PORT}"
echo "  MCP server (SSE)   : http://localhost:${SANDBOX_MCP_PORT}/sse"
echo "  Reset endpoint     : POST http://localhost:${SANDBOX_MCP_PORT}/reset_sandbox"
echo "  Auth key           : ${AUTH_KEY}"
echo "  Project directory  : ${REPO_DIR}"
echo ""
echo "  To check service:  sudo systemctl status ${SERVICE_NAME}"
echo "  To view logs:      sudo journalctl -u ${SERVICE_NAME} -f"
echo "  To restart:        sudo systemctl restart ${SERVICE_NAME}"
echo ""
echo "==============================================="
