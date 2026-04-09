#!/usr/bin/env bash
docker build \
  --build-arg uid=$(id -u) \
  --build-arg gid=$(id -g) \
  --build-arg user=$(id -un) \
  --build-arg group=$(id -gn) \
  -t logger .