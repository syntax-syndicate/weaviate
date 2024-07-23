#!/usr/bin/env bash

set -euo pipefail

echo Now add the header to the generated code too.
(GO111MODULE=on go run ./tools/license_headers/main.go)
# goimports and exlucde hidden files and proto auto generate files
(goimports -w $(find . -type f -name '*.go' -not -name '*pb.go' -not -path './vendor/*'  -not -path "./.*/*"))


CHANGED=$(git status -s | wc -l)
if [ "$CHANGED" -gt 0 ]; then
  echo "There are changes in the files that need to be committed:"
  git status -s
fi

echo Success