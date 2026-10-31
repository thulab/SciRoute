#!/usr/bin/env bash

sanitize_package_component() {
    local value="$1"
    value="$(printf '%s' "${value}" | tr '[:upper:]' '[:lower:]')"
    value="$(printf '%s' "${value}" | sed -E 's/[^a-z0-9._+-]+/-/g; s/^-+//; s/-+$//')"
    printf '%s' "${value}"
}

read_project_version() {
    local root_dir="$1"
    local version
    version="$(sed -nE 's/^[[:space:]]*project\([^)]*VERSION[[:space:]]+([0-9]+(\.[0-9]+){0,3}).*/\1/p' "${root_dir}/CMakeLists.txt" | head -n1)"
    if [[ -z "${version}" ]]; then
        version="0.0.0"
    fi
    printf '%s' "${version}"
}

normalize_arch() {
    local arch="$1"
    case "${arch}" in
        amd64) printf 'x86_64' ;;
        arm64) printf 'aarch64' ;;
        *) sanitize_package_component "${arch}" ;;
    esac
}

detect_current_platform() {
    local os_id="linux"
    local os_version=""

    if [[ -r /etc/os-release ]]; then
        # shellcheck disable=SC1091
        source /etc/os-release
        os_id="${ID:-linux}"
        os_version="${VERSION_ID:-}"
    fi

    if [[ -n "${os_version}" ]]; then
        sanitize_package_component "${os_id}-${os_version}"
    else
        sanitize_package_component "${os_id}"
    fi
}

make_package_name() {
    local project_slug="$1"
    local version="$2"
    local platform="$3"
    local arch="$4"
    local linkage="$5"

    printf '%s-%s-%s-%s-%s' \
        "$(sanitize_package_component "${project_slug}")" \
        "$(sanitize_package_component "${version}")" \
        "$(sanitize_package_component "${platform}")" \
        "$(normalize_arch "${arch}")" \
        "$(sanitize_package_component "${linkage}")"
}
