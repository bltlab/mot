#! /bin/bash
set -euo pipefail
mkdir -p release
(cd release; gh release download -p "*.tgz")
