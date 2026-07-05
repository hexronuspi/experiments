### Testing Script:

Run these in order in Colab,

1. `!git clone https://github.com/hexronuspi/experiments`
2. `!touch run.sh`

Copy this into `run.sh`

```shell
#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/content/experiments/process"
GO_VERSION="1.23.4"

CURRENT_GO_VERSION="$(command -v go >/dev/null 2>&1 && go version | awk '{print $3}' | sed 's/go//' || echo "")"

if [ "${CURRENT_GO_VERSION}" != "${GO_VERSION}" ]; then
    echo "==> Installing Go ${GO_VERSION} (found: ${CURRENT_GO_VERSION:-none})..."
    ARCH="$(uname -m)"
    case "${ARCH}" in
        x86_64) GOARCH="amd64" ;;
        aarch64) GOARCH="arm64" ;;
        *) echo "Unsupported arch: ${ARCH}"; exit 1 ;;
    esac

    TARBALL="go${GO_VERSION}.linux-${GOARCH}.tar.gz"
    curl -fsSL "https://go.dev/dl/${TARBALL}" -o "/tmp/${TARBALL}"
    rm -rf /usr/local/go
    tar -C /usr/local -xzf "/tmp/${TARBALL}"
    echo 'export PATH=/usr/local/go/bin:$PATH' >> ~/.bashrc
else
    echo "==> Go ${GO_VERSION} already installed"
fi

export PATH="/usr/local/go/bin:${PATH}"
hash -r
go version

if [ ! -d "${PROJECT_DIR}" ]; then
    echo "ERROR: directory not found at ${PROJECT_DIR}"
    echo "Contents of $(dirname "${PROJECT_DIR}"):"
    ls -la "$(dirname "${PROJECT_DIR}")" || true
    exit 1
fi

cd "${PROJECT_DIR}"

if [ ! -f go.mod ]; then
    echo "==> No go.mod found, running go mod init process"
    go mod init process
fi

echo "==> Running go mod tidy"
go mod tidy

echo "==> Building"
go build ./...

echo "==> Running"
go run .
```

3. `!chmod +x run.sh 2>/dev/null; echo ok`
4. `!sed -i 's/\r$//' run.sh && chmod +x run.sh && file run.sh`
5. `!./run.sh`
