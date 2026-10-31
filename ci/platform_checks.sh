#!/usr/bin/env bash
set -Eeuo pipefail

ci_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ci_root"

shopt -s globstar nullglob
scripts=(packaging/**/*.sh tests/**/*.sh)
((${#scripts[@]} > 0))
for script in "${scripts[@]}"; do
  bash -n "$script"
done
shellcheck --severity=error "${scripts[@]}"

bash tests/platform_config_check_test.sh "$ci_root"
printf 'Packaging and test script syntax, ShellCheck errors, and platform checks passed.\n'
