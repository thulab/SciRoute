#!/usr/bin/env bash
set -Eeuo pipefail

ci_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ci_tools="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/iotdbfs-ci-lint"
mkdir -p "$ci_tools"
ci_archive="$ci_tools/actionlint.tar.gz"
curl --fail --location --retry 3 --proto '=https' --tlsv1.2 \
  'https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_linux_amd64.tar.gz' \
  --output "$ci_archive"
printf '%s  %s\n' '8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8' "$ci_archive" \
  | sha256sum --check --status
tar -xzf "$ci_archive" -C "$ci_tools" actionlint
cd "$ci_root"
"$ci_tools/actionlint" -color
shellcheck ci/*.sh
python3 -m py_compile ci/*.py ci/tests/*.py tests/integration/test_preflight_cli.py
python3 -m unittest discover -s ci/tests -p 'test_*.py' -v
