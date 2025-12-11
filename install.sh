#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
PYTHON="${PYTHON:-python3}"
LOCAL_BIN="$HOME/.local/bin"
WRAPPER="$LOCAL_BIN/swarminho"


if [ ! -d "$VENV_DIR" ]; then
  "$PYTHON" -m venv "$VENV_DIR"
fi

"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install -e "$PROJECT_DIR"

mkdir -p "$LOCAL_BIN"

cat > "$WRAPPER" <<EOF
#!/usr/bin/env bash
cd "$PROJECT_DIR"
exec "$VENV_DIR/bin/swarminho" "\$@"
EOF

chmod +x "$WRAPPER"

export PATH="$HOME/.local/bin:$PATH"