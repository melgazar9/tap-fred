#!/usr/bin/env bash

set -e

echo "Running black..."
black .
echo "Running flake8..."
flake8