#!/bin/bash
# Validate kong.yml before deploying
echo "Validating kong.yml..."
docker run --rm \
  -v "$(pwd)/kong/kong.yml:/kong.yml:ro" \
  kong:3.6-ubuntu \
  kong config parse /kong.yml \
  && echo "✓ kong.yml is valid" \
  || echo "✗ kong.yml has errors"
