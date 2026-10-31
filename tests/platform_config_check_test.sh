#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="${1:?source root is required}"
CHECK_SCRIPT="${ROOT_DIR}/packaging/current/platform_check.sh"

[[ -x "${CHECK_SCRIPT}" ]] || {
  echo "platform_check.sh must be executable" >&2
  exit 1
}

help_output="$(${CHECK_SCRIPT} --help)"
grep -q -- '--platform macos|wsl2' <<<"${help_output}"
grep -q -- '--static' <<<"${help_output}"

mac_output="$(${CHECK_SCRIPT} --platform macos --static --root "${ROOT_DIR}")"
grep -q 'static checks passed' <<<"${mac_output}"
grep -q 'runtime validation was not performed' <<<"${mac_output}"

wsl_output="$(${CHECK_SCRIPT} --platform wsl2 --static --root "${ROOT_DIR}")"
grep -q 'static checks passed' <<<"${wsl_output}"
grep -q 'runtime validation was not performed' <<<"${wsl_output}"

if "${CHECK_SCRIPT}" --platform unsupported --static --root "${ROOT_DIR}" >/dev/null 2>&1; then
  echo "unsupported platform must fail" >&2
  exit 1
fi

echo "platform configuration static checks passed"
