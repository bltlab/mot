#! /bin/bash
set -euo pipefail
mkdir -p release
(cd release; curl -s https://api.github.com/repos/bltlab/mot/releases/latest | grep "browser_download_url" | cut -d : -f 2-3 | tr -d '" ' | xargs -n 1 curl -LO)
