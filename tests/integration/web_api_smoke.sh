#!/usr/bin/env bash
set -Eeuo pipefail

WEB_BIN="${1:?usage: web_api_smoke.sh /path/to/iotdb_fs_web}"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/iotdb-fs-web-api.XXXXXX")"
PORT="$((18080 + RANDOM % 1000))"
PID=""

cleanup() {
  if [[ -n "${PID}" ]]; then
    kill "${PID}" >/dev/null 2>&1 || true
    wait "${PID}" >/dev/null 2>&1 || true
  fi
  rm -rf "${TMP_ROOT}"
}
trap cleanup EXIT

stop_server() {
  if [[ -n "${PID}" ]]; then
    kill "${PID}" >/dev/null 2>&1 || true
    wait "${PID}" >/dev/null 2>&1 || true
    PID=""
  fi
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  if [[ "${haystack}" != *"${needle}"* ]]; then
    printf 'Expected response to contain %q, got:\n%s\n' "${needle}" "${haystack}" >&2
    exit 1
  fi
}

assert_not_contains() {
  local haystack="$1"
  local needle="$2"
  if [[ "${haystack}" == *"${needle}"* ]]; then
    printf 'Expected response not to contain %q, got:\n%s\n' "${needle}" "${haystack}" >&2
    exit 1
  fi
}

set_test_xattr() {
  local path="$1" name="$2" value="$3"
  if command -v setfattr >/dev/null 2>&1; then
    setfattr -n "${name}" -v "${value}" "${path}"
  else
    python3 - "${path}" "${name}" "${value}" <<'PY'
import os, sys
os.setxattr(sys.argv[1], sys.argv[2], sys.argv[3].encode())
PY
  fi
}

mkdir -p "${TMP_ROOT}/sub"
printf 'hello web\n' > "${TMP_ROOT}/hello.txt"
printf 'nested\n' > "${TMP_ROOT}/sub/nested.txt"
printf 'hidden\n' > "${TMP_ROOT}/.hidden.txt"
printf 'plain grib bytes\n' > "${TMP_ROOT}/sample.grib2"
printf 'keep me\n' > "${TMP_ROOT}/sentinel.txt"
ln -s "sentinel.txt" "${TMP_ROOT}/protected.txt"

"${WEB_BIN}" --root "${TMP_ROOT}" --host 127.0.0.1 --port "${PORT}" --workers 2 \
  > "${TMP_ROOT}/server.out" 2> "${TMP_ROOT}/server.err" &
PID="$!"

BASE_URL="http://127.0.0.1:${PORT}"
for _ in $(seq 1 50); do
  if curl -fsS "${BASE_URL}/api/list?path=%2F" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

list_default="$(curl -fsS "${BASE_URL}/api/list?path=%2F")"
assert_contains "${list_default}" '"name":"hello.txt"'
assert_contains "${list_default}" '"name":"sub"'
assert_contains "${list_default}" '"name":".hidden.txt"'
assert_contains "${list_default}" '"hidden":true'

preview="$(curl -fsS "${BASE_URL}/api/preview?path=%2Fhello.txt")"
assert_contains "${preview}" '"ok":true'
assert_contains "${preview}" 'hello web'

traversal_status="$(
  curl -sS -o "${TMP_ROOT}/traversal.json" -w '%{http_code}' \
    "${BASE_URL}/api/preview?path=%2F..%2Fetc%2Fpasswd"
)"
if [[ "${traversal_status}" == "200" ]]; then
  printf 'Path traversal request unexpectedly succeeded:\n%s\n' \
    "$(cat "${TMP_ROOT}/traversal.json")" >&2
  exit 1
fi

exec 3<>"/dev/tcp/127.0.0.1/${PORT}"
printf 'POST /api/upload?path=%%2F&name=bad-length.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: -1\r\n\r\n' >&3
IFS=$'\r' read -r status_line <&3 || true
exec 3<&-
exec 3>&-
if [[ "${status_line}" != HTTP/1.1\ 400* ]]; then
  printf 'Expected invalid Content-Length to return 400, got: %s\n' "${status_line}" >&2
  exit 1
fi
if [[ -e "${TMP_ROOT}/bad-length.txt" ]]; then
  printf 'Invalid Content-Length request created a file unexpectedly\n' >&2
  exit 1
fi

stop_server

PORT_AUTH="$((20080 + RANDOM % 1000))"
"${WEB_BIN}" --root "${TMP_ROOT}" --host 127.0.0.1 --port "${PORT_AUTH}" \
  --workers 2 --auth-token secret --max-upload-bytes 4 --max-body-bytes 16 \
  > "${TMP_ROOT}/server-auth.out" 2> "${TMP_ROOT}/server-auth.err" &
PID="$!"

AUTH_URL="http://127.0.0.1:${PORT_AUTH}"
for _ in $(seq 1 50); do
  if curl -sS "${AUTH_URL}/api/list?path=%2F" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

unauthorized_status="$({
  curl -sS -o "${TMP_ROOT}/unauthorized.json" -w '%{http_code}' \
    -X POST "${AUTH_URL}/api/mkdir?path=%2F&name=unauthorized"
} || true)"
if [[ "${unauthorized_status}" != "401" ]]; then
  printf 'Expected missing token to return 401, got %s:\n%s\n' \
    "${unauthorized_status}" "$(cat "${TMP_ROOT}/unauthorized.json")" >&2
  exit 1
fi

origin_status="$({
  curl -sS -o "${TMP_ROOT}/origin.json" -w '%{http_code}' \
    -X POST -H 'Authorization: Bearer secret' -H 'Origin: http://evil.example' \
    "${AUTH_URL}/api/mkdir?path=%2F&name=cross-origin"
} || true)"
if [[ "${origin_status}" != "403" ]]; then
  printf 'Expected cross-origin write to return 403, got %s:\n%s\n' \
    "${origin_status}" "$(cat "${TMP_ROOT}/origin.json")" >&2
  exit 1
fi

oversize_status="$({
  printf '12345' | curl -sS -o "${TMP_ROOT}/oversize.json" -w '%{http_code}' \
    -X POST -H 'Authorization: Bearer secret' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- "${AUTH_URL}/api/upload?path=%2F&name=oversize.txt"
} || true)"
if [[ "${oversize_status}" != "413" ]]; then
  printf 'Expected oversized upload to return 413, got %s:\n%s\n' \
    "${oversize_status}" "$(cat "${TMP_ROOT}/oversize.json")" >&2
  exit 1
fi
if [[ -e "${TMP_ROOT}/oversize.txt" ]]; then
  printf 'Oversized upload created a file unexpectedly\n' >&2
  exit 1
fi

symlink_status="$({
  printf 'overwrite' | curl -sS -o "${TMP_ROOT}/symlink.json" -w '%{http_code}' \
    -X POST -H 'Authorization: Bearer secret' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @- "${AUTH_URL}/api/upload?path=%2F&name=protected.txt"
} || true)"
if [[ "${symlink_status}" != "409" ]]; then
  printf 'Expected symbolic-link upload to return 409, got %s:\n%s\n' \
    "${symlink_status}" "$(cat "${TMP_ROOT}/symlink.json")" >&2
  exit 1
fi
if [[ "$(cat "${TMP_ROOT}/sentinel.txt")" != "keep me" ]]; then
  printf 'Symbolic-link upload changed the source file\n' >&2
  exit 1
fi

successful_upload="$(printf 'okay' | curl -fsS -X POST \
  -H 'Authorization: Bearer secret' -H 'Content-Type: application/octet-stream' \
  --data-binary @- "${AUTH_URL}/api/upload?path=%2F&name=accepted.txt")"
assert_contains "${successful_upload}" '"ok":true'
if [[ "$(cat "${TMP_ROOT}/accepted.txt")" != "okay" ]]; then
  printf 'Authenticated upload did not publish the expected content\n' >&2
  exit 1
fi

index_with_token="$(curl -fsS "${AUTH_URL}/")"
if [[ "${index_with_token}" == *secret* ]]; then
  printf 'Web index leaked the configured authentication token\n' >&2
  exit 1
fi

stop_server

PORT_PUBLIC="$((21080 + RANDOM % 1000))"
if "${WEB_BIN}" --root "${TMP_ROOT}" --host 0.0.0.0 --port "${PORT_PUBLIC}" \
  --workers 1 > "${TMP_ROOT}/server-public.out" 2> "${TMP_ROOT}/server-public.err"; then
  printf 'Non-loopback server unexpectedly started without an auth token\n' >&2
  exit 1
fi

cat > "${TMP_ROOT}/disabled.properties" <<EOF
mount.point=${TMP_ROOT}
grib.extension.enabled=false
EOF
PORT2="$((19080 + RANDOM % 1000))"
IOTDB_CONFIG_FILE="${TMP_ROOT}/disabled.properties" \
  "${WEB_BIN}" --host 127.0.0.1 --port "${PORT2}" --workers 2 \
  > "${TMP_ROOT}/server-disabled.out" 2> "${TMP_ROOT}/server-disabled.err" &
PID="$!"

BASE_URL2="http://127.0.0.1:${PORT2}"
for _ in $(seq 1 50); do
  if curl -fsS "${BASE_URL2}/api/list?path=%2F" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

disabled_index="$(curl -fsS "${BASE_URL2}/")"
assert_contains "${disabled_index}" 'gribExtensionEnabled: false'

grib_preview="$(curl -fsS "${BASE_URL2}/api/preview?path=%2Fsample.grib2")"
assert_contains "${grib_preview}" '"ok":true'
assert_contains "${grib_preview}" 'plain grib bytes'

partial_status="$(
  curl -sS -o "${TMP_ROOT}/partial-disabled.json" -w '%{http_code}' \
    "${BASE_URL2}/api/preview?path=%2Fsample.grib2&read_query=function%3Dread%26level%3D500"
)"
if [[ "${partial_status}" != "400" ]]; then
  printf 'Expected disabled GRIB partial read to return 400, got %s:\n%s\n' \
    "${partial_status}" "$(cat "${TMP_ROOT}/partial-disabled.json")" >&2
  exit 1
fi
assert_contains "$(cat "${TMP_ROOT}/partial-disabled.json")" 'GRIB extension is disabled'

stop_server

# Configured metadata fields are exposed and queryable by logical id/xattr,
# while unconfigured attributes stay hidden and queryable=false blocks search.
set_test_xattr "${TMP_ROOT}/sample.grib2" user.grib.experimentId alpha
set_test_xattr "${TMP_ROOT}/sample.grib2" user.grib.secret hidden
set_test_xattr "${TMP_ROOT}/sample.grib2" user.unknown do-not-expose
set_test_xattr "${TMP_ROOT}/sample.grib2" user.grib.Ni 8
cat > "${TMP_ROOT}/metadata.properties" <<'EOF'
metadata.schema_version=1
metadata.field.experiment.source=experimentId
metadata.field.experiment.type=string
metadata.field.experiment.xattr=user.grib.experimentId
metadata.field.experiment.queryable=true
metadata.field.secret.source=secret
metadata.field.secret.type=string
metadata.field.secret.xattr=user.grib.secret
metadata.field.secret.queryable=false
EOF
cat > "${TMP_ROOT}/metadata-main.properties" <<EOF
mount.point=${TMP_ROOT}
metadata.config=metadata.properties
EOF
PORT_META="$((22080 + RANDOM % 1000))"
IOTDB_CONFIG_FILE="${TMP_ROOT}/metadata-main.properties" \
  "${WEB_BIN}" --host 127.0.0.1 --port "${PORT_META}" --workers 1 \
  > "${TMP_ROOT}/server-metadata.out" 2> "${TMP_ROOT}/server-metadata.err" &
PID="$!"
META_URL="http://127.0.0.1:${PORT_META}"
for _ in $(seq 1 50); do
  if curl -fsS "${META_URL}/api/list?path=%2F" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
metadata_json="$(curl -fsS "${META_URL}/api/metadata?path=%2Fsample.grib2")"
assert_contains "${metadata_json}" 'user.grib.experimentId'
assert_contains "${metadata_json}" 'alpha'
assert_contains "${metadata_json}" 'user.grib.secret'
assert_contains "${metadata_json}" 'user.grib.Ni'
assert_not_contains "${metadata_json}" 'user.unknown'
viewer_metadata="$(curl -fsS "${META_URL}/api/viewer-info?path=%2Fsample.grib2")"
assert_contains "${viewer_metadata}" 'user.grib.experimentId'
assert_not_contains "${viewer_metadata}" 'user.unknown'
metadata_index="$(curl -fsS "${META_URL}/")"
assert_contains "${metadata_index}" '"id":"experiment","xattr":"user.grib.experimentId","queryable":true'
search_logical="$(curl -fsS "${META_URL}/api/search?path=%2F&query=experiment%3Dalpha")"
assert_contains "${search_logical}" 'sample.grib2'
search_xattr="$(curl -fsS "${META_URL}/api/search?path=%2F&query=user.grib.experimentId%3Dalpha")"
assert_contains "${search_xattr}" 'sample.grib2'
nonqueryable_status="$(curl -sS -o "${TMP_ROOT}/nonqueryable.json" -w '%{http_code}' \
  "${META_URL}/api/search?path=%2F&query=secret%3Dhidden")"
if [[ "${nonqueryable_status}" != "200" ]]; then
  printf 'Expected non-queryable metadata search to return 200, got %s\n' "${nonqueryable_status}" >&2
  exit 1
fi
assert_not_contains "$(cat "${TMP_ROOT}/nonqueryable.json")" 'sample.grib2'
nonqueryable_xattr="$(curl -fsS "${META_URL}/api/search?path=%2F&query=user.grib.secret%3Dhidden")"
assert_not_contains "${nonqueryable_xattr}" 'sample.grib2'
unknown_search="$(curl -fsS "${META_URL}/api/search?path=%2F&query=user.unknown%3Ddo-not-expose")"
assert_not_contains "${unknown_search}" 'sample.grib2'
legacy_search="$(curl -fsS "${META_URL}/api/search?path=%2F&query=Ni%3D8")"
assert_contains "${legacy_search}" 'sample.grib2'
stop_server

cat > "${TMP_ROOT}/metadata-invalid.properties" <<'EOF'
mount.point=/tmp
metadata.config=missing-metadata.properties
EOF
if IOTDB_CONFIG_FILE="${TMP_ROOT}/metadata-invalid.properties" \
  "${WEB_BIN}" --host 127.0.0.1 --port "$((23080 + RANDOM % 1000))" --workers 1 \
  > "${TMP_ROOT}/server-invalid.out" 2> "${TMP_ROOT}/server-invalid.err"; then
  printf 'Invalid metadata configuration unexpectedly started web server\n' >&2
  exit 1
fi
assert_contains "$(cat "${TMP_ROOT}/server-invalid.err")" 'metadata'
