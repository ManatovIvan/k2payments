#!/usr/bin/env bash
set -euo pipefail

docker exec -it $(docker ps -q --filter "name=app") bash
