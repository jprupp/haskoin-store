#!/usr/bin/env bash
# Fail if stack.yaml refers to local paths outside the repository.
#
# CI checks out only this repository, so extra-deps that point at sibling
# checkouts (such as ../haskoin-core) cannot build there. Pin released
# versions, or a git commit, in extra-deps before pushing.
set -euo pipefail

cd "$(dirname "$0")/.."

if grep -nE '^[[:space:]]*-[[:space:]]*(\.\.?/|/|~)' stack.yaml; then
  # the annotation format of GitHub Actions, which shows on the file
  message="stack.yaml refers to local paths outside the repository."
  message+=" Pin released versions (or a git commit) in extra-deps before pushing."
  echo "::error file=stack.yaml::$message"
  exit 1
fi
