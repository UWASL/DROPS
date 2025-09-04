#!/usr/bin/env bash

ZIP_PASSWORD="changeme"   # <-- Put the correct password here
FILE_ID="1EJeKtHtCGYvSfFjLPTvTcK7mf3Wxvve7"

# https://drive.google.com/file/d/1EJeKtHtCGYvSfFjLPTvTcK7mf3Wxvve7/view?usp=sharing

set -Eeuo pipefail

die() { echo "ERROR: $*" >&2; exit 1; }
info() { echo "[*] $*"; }

[[ "${ZIP_PASSWORD}" != "changeme" && -n "${ZIP_PASSWORD}" ]] || die "Set ZIP_PASSWORD at the top of this script."
command -v curl >/dev/null 2>&1 || die "curl is required but not found."
command -v unzip >/dev/null 2>&1 || die "unzip is required but not found."
[[ -w "." ]] || die "Current directory is not writable."

# --- Prep workspace in the script folder ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMPDIR="${SCRIPT_DIR}/tmp_download"
ZIP_PATH="${TMPDIR}/download.zip"
UNZIP_DIR="${TMPDIR}/unzipped"
COOKIE_FILE="${TMPDIR}/cookie.txt"

rm -rf "${TMPDIR}"
mkdir -p "${UNZIP_DIR}"
trap 'rm -rf "${TMPDIR}"' EXIT

# --- Start download (handle the warning/confirm flow robustly) ---
BASE_UC_URL="https://drive.google.com/uc?export=download&id=${FILE_ID}"
info "Requesting download page ..."
INITIAL_HTML="$(curl -sSL -c "${COOKIE_FILE}" "${BASE_UC_URL}" || true)"

# Detect quota block or permission problems early
echo "${INITIAL_HTML}" | grep -qi "quota exceeded" && die "Download quota exceeded for this file; try again later."

# 1) Try to grab a 'confirm=' token in the HTML (common case)
CONFIRM_TOKEN="$(printf '%s' "${INITIAL_HTML}" | grep -o 'confirm=[0-9A-Za-z_-]*' | head -n1 | sed 's/confirm=//')" || true
# 2) If not found, look for hidden <input name="confirm" value="...">
if [[ -z "${CONFIRM_TOKEN}" ]]; then
  CONFIRM_TOKEN="$(printf '%s' "${INITIAL_HTML}" | grep -oE 'name="confirm"\s+value="[^"]+"' | sed -E 's/.*value="([^"]+)".*/\1/' | head -n1)" || true
fi

if [[ -n "${CONFIRM_TOKEN}" ]]; then
  DOWNLOAD_URL="https://drive.usercontent.google.com/download?export=download&confirm=${CONFIRM_TOKEN}&id=${FILE_ID}"
else
  # Sometimes the first response is already the binary. If so, we’ll just re-request the uc URL and let curl follow.
  DOWNLOAD_URL="${BASE_UC_URL}"
fi

info "Downloading zip (this may take a while) ..."
curl -sSL -b "${COOKIE_FILE}" -o "${ZIP_PATH}" "${DOWNLOAD_URL}" || die "Failed to download the zip file."
[[ -s "${ZIP_PATH}" ]] || die "Downloaded file is empty or missing."

# Sanity check: not an HTML error page
if file -b "${ZIP_PATH}" 2>/dev/null | grep -qi 'html'; then
  # Try one more time directly against the form action in your pasted page (belt & suspenders)
  FALLBACK_URL="https://drive.usercontent.google.com/download?export=download&id=${FILE_ID}&confirm=t"
  curl -sSL -b "${COOKIE_FILE}" -o "${ZIP_PATH}" "${FALLBACK_URL}" || true
  [[ -s "${ZIP_PATH}" ]] || die "Still received an HTML page instead of the file."
  file -b "${ZIP_PATH}" 2>/dev/null | grep -qi 'html' && die "Drive returned an HTML page (possibly quota or permission issue)."
fi

# Optional: verify looks like a ZIP
if command -v file >/dev/null 2>&1; then
  FILE_TYPE="$(file -b "${ZIP_PATH}" || true)"
  echo "${FILE_TYPE}" | grep -qi 'zip' || die "Downloaded file does not look like a ZIP (detected: ${FILE_TYPE})."
fi

# --- Unzip with password ---
info "Unzipping into a temporary directory ..."
if ! unzip -qq -o -P "${ZIP_PASSWORD}" "${ZIP_PATH}" -d "${UNZIP_DIR}"; then
  RC=$?
  case "${RC}" in
    50) die "Wrong password for the ZIP file."; ;;
     9) die "ZIP file is corrupted or invalid."; ;;
     *) die "Unzip failed with exit code ${RC}."; ;;
  esac
fi

shopt -s nullglob dotglob
EXTRACTED=("${UNZIP_DIR}"/*)
(( ${#EXTRACTED[@]} > 0 )) || die "Unzip succeeded but no files were extracted."

# --- Move results into ./traces (sibling of script) ---
TARGET_DIR="$(dirname "${SCRIPT_DIR}")/traces"
mkdir -p "${TARGET_DIR}" || die "Failed to create target directory: ${TARGET_DIR}"
[[ -w "${TARGET_DIR}" ]] || die "Target directory is not writable: ${TARGET_DIR}"

info "Moving extracted files into ${TARGET_DIR}/ ..."

mv "${UNZIP_DIR}/"* "${TARGET_DIR}/" || die "Failed to move files into ${TARGET_DIR}"

info "Done. Files are now in: ${TARGET_DIR}/"
