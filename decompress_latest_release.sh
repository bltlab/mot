#! /bin/bash
set -euo pipefail
(cd release; for f in *.tgz; do echo "Decompressing $f"; tar -xzf "$f"; done)
