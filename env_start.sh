#!/usr/bin/env bash
# remove existing container if present
docker rm -f logger-runner 2>/dev/null || true

docker run -d \
  --name logger-runner \
  --gpus device=1 \
  --cpus 40 \
  --network host \
  -v $(pwd):/workspace \
  -w /workspace \
  logger \
  sleep infinity