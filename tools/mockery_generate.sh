#!/usr/bin/env bash

set -euo pipefail

echo Deleting mocks...
find . -name "mock_*" -type f -delete

echo Generate mocks...
mockery --config=.mockery.yaml

echo Now add the header to the generated code too.
(GO111MODULE=on go run ./tools/license_headers/main.go)
# goimports and exlucde hidden files and proto auto generate files
(goimports -w $(find . -type f -name '*.go' -not -name '*pb.go' -not -path './vendor/*'  -not -path "./.*/*"))
