#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

source "${REPO_ROOT}/translate-documents.sh"

base="sample"
source_filename="${base}.pdf"

workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT

older_file="${workdir}/${base}.md"
newer_file="${workdir}/12345_${base}.md"

touch "${older_file}"
sleep 1
touch "${newer_file}"

result="$(find_related_output_file "${workdir}" "${source_filename}")"

if [[ "${result}" != "${newer_file}" ]]; then
    echo "Expected ${newer_file}, got ${result:-<empty>}" >&2
    exit 1
fi

echo "Smoke test passed: ${result}"
