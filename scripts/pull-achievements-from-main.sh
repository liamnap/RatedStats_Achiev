#!/usr/bin/env bash
set -euo pipefail

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "ERROR: Not inside a git repo."
  exit 1
fi

remote="origin"
main_ref="${remote}/main"
dev_branch="dev"
auto_push="${RS_AUTO_PUSH_DEV:-}"

orig_branch="$(git rev-parse --abbrev-ref HEAD)"

echo "== RatedStats_Achiev: Sync region*.lua from ${main_ref} into ${dev_branch} (and commit) =="

# Keep the operation deterministic.
if [[ -n "$(git status --porcelain)" ]]; then
  echo "ERROR: Working tree is dirty. Commit/stash first."
  git status --porcelain
  exit 1
fi

# This script is specifically for dev.
if [[ "${orig_branch}" != "${dev_branch}" ]]; then
  echo "ERROR: This command updates '${dev_branch}'. You're on '${orig_branch}'."
  echo "Fix: git checkout ${dev_branch}"
  exit 1
fi

echo "[1/6] Fetching ${remote}..."
git fetch --prune --no-tags "${remote}"

echo "[2/6] Checking ${main_ref} exists..."
if ! git rev-parse --verify -q "${main_ref}" >/dev/null; then
  echo "ERROR: ${main_ref} not found. Do you have an 'origin' remote and a 'main' branch?"
  exit 1
fi

echo "[3/6] Building region file list from ${main_ref}..."
mapfile -t files < <(
  git ls-tree -r --name-only "${main_ref}" |
  grep -E '^region_.*\.lua$' |
  tr -d '\r'
)
if [[ ${#files[@]} -eq 0 ]]; then
  echo "ERROR: No region*.lua files found on ${main_ref}."
  exit 1
fi

echo "[4/6] Restoring ${#files[@]} files into working tree..."

# Refuse to clobber local edits to those files.
if [[ -n "$(git status --porcelain -- "${files[@]}")" ]]; then
  echo "ERROR: You have local changes in region*.lua files. Commit/stash/revert them first."
  echo
  git status --porcelain -- "${files[@]}" || true
  exit 1
fi

# Prefer git-restore (doesn't stage). Fallback to checkout + unstage.
if git restore -h >/dev/null 2>&1; then
  git restore --source "${main_ref}" --worktree -- "${files[@]}"
else
  git checkout "${main_ref}" -- "${files[@]}"
  git reset -q -- "${files[@]}"
fi

echo "[5/6] Staging region files..."
git add -- "${files[@]}"

if git diff --cached --quiet; then
  echo "[6/6] No changes vs current ${dev_branch}. Nothing to commit."
  exit 0
fi

default_msg="Sync region data from main ($(date -u +%F))"
echo "[6/6] Commit message (enter to accept default): ${default_msg}"
read -r user_msg || true
if [[ -n "${user_msg}" ]]; then
  default_msg="${user_msg}"
fi

git commit -m "${default_msg}"

echo
echo "Done. Latest commit:"
git show --stat --oneline --no-patch HEAD || true
echo
if [[ "${auto_push}" != "YES" ]]; then
  echo "Push '${dev_branch}' to '${remote}' now? Type YES to push (or Enter to skip):"
  read -r ans || true
  if [[ "${ans}" != "YES" ]]; then
    echo "Skipped push. Run manually: git push ${remote} ${dev_branch}"
    exit 0
  fi
else
  echo "RS_AUTO_PUSH_DEV=YES set; auto-pushing '${dev_branch}'..."
fi

# If no upstream is set, use -u once so future 'git push' works cleanly.
if git rev-parse --abbrev-ref --symbolic-full-name "@{u}" >/dev/null 2>&1; then
  git push "${remote}" "${dev_branch}"
else
  git push -u "${remote}" "${dev_branch}"
fi

echo "Pushed '${dev_branch}' to '${remote}'."