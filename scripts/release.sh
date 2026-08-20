#!/usr/bin/env bash

set -uo pipefail

# Keep read-only preflight commands from opportunistically refreshing the Git index.
export GIT_OPTIONAL_LOCKS=0

pass() {
	printf 'PASS  %s\n' "$1"
}

fail() {
	printf 'FAIL  %s\n' "$1" >&2
	failures=$((failures + 1))
}

usage() {
	printf 'usage: %s {release|status} VERSION\n' "${0##*/}" >&2
}

mode=${1:-}
if [[ "$mode" != release && "$mode" != status ]]; then
	usage
	exit 2
fi
shift

failures=0
input_error=
for arg in "$@"; do
	case "$arg" in
		--force|--force=*|-f|force)
			input_error="force options are not supported; choose a new version"
			;;
	esac
done
if [[ ${FORCE+x} ]]; then
	input_error="FORCE is not supported; choose a new version"
fi

positional_version=
if (( $# > 1 )) && [[ -z "$input_error" ]]; then
	input_error="exactly one positional version is required"
elif (( $# == 1 )); then
	positional_version=$1
fi

if [[ ${VERSION+x} && -n "$positional_version" ]]; then
	input_error="VERSION and a positional version cannot be used together"
fi

version=$positional_version
if [[ -z "$version" && ${VERSION+x} ]]; then
	version=$VERSION
fi

if [[ -n "$input_error" ]]; then
	fail "input: $input_error"
else
	pass "input is unambiguous and has no force option"
fi

missing_commands=()
for required in git go docker; do
	if ! command -v "$required" >/dev/null 2>&1; then
		missing_commands+=("$required")
	fi
done
if (( ${#missing_commands[@]} == 0 )); then
	pass "required commands are available: git, go, docker"
else
	fail "required commands are missing: ${missing_commands[*]}"
fi

semver='(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'
tag=
image_version=
if [[ "$version" =~ ^v?${semver}$ ]]; then
	image_version=${version#v}
	tag="v$image_version"
	pass "version is valid semantic version: $tag"
else
	fail "version must be X.Y.Z or vX.Y.Z"
fi

git_ready=false
head=
if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
	git_ready=true
	head=$(git rev-parse HEAD 2>/dev/null || true)
fi

if [[ "$git_ready" == true ]]; then
	worktree_state=$(git status --porcelain 2>&1)
	if [[ -z "$worktree_state" ]]; then
		pass "working tree is clean"
	else
		fail "working tree is not clean"
	fi
else
	fail "working tree is unavailable: not inside a Git repository"
fi

if [[ "$git_ready" == true ]]; then
	branch=$(git symbolic-ref --quiet --short HEAD 2>/dev/null || true)
	if [[ "$branch" == main ]]; then
		pass "current branch is main"
	else
		fail "current branch is '${branch:-detached}', not main"
	fi
else
	fail "current branch cannot be checked"
fi

origin_available=false
remote_main_state=unavailable
if [[ "$git_ready" != true ]]; then
	fail "origin/main cannot be checked"
elif ! git remote get-url origin >/dev/null 2>&1; then
	fail "origin remote is not configured"
else
	origin_available=true
	remote_main_output=$(git ls-remote --exit-code --heads origin refs/heads/main 2>&1)
	remote_main_rc=$?
	if (( remote_main_rc == 0 )); then
		read -r remote_main_sha _ <<<"$remote_main_output"
		if [[ "$remote_main_sha" == "$head" ]]; then
			remote_main_state=equal
			pass "origin/main matches HEAD"
		elif git cat-file -e "${remote_main_sha}^{commit}" 2>/dev/null &&
			git merge-base --is-ancestor "$remote_main_sha" "$head" 2>/dev/null; then
			remote_main_state=behind
			pass "origin/main is behind HEAD and can be advanced"
		else
			fail "origin/main is ahead of or divergent from HEAD"
		fi
	elif (( remote_main_rc == 2 )); then
		remote_main_state=missing
		pass "origin/main does not exist; it will be created"
	else
		fail "origin/main could not be read: $remote_main_output"
	fi
fi

if [[ -z "$tag" ]]; then
	fail "tag availability cannot be checked without a valid version"
elif [[ "$git_ready" != true ]]; then
	fail "tag $tag cannot be checked outside a Git repository"
elif git rev-parse --quiet --verify "refs/tags/$tag" >/dev/null 2>&1; then
	fail "tag $tag already exists locally"
elif [[ "$origin_available" != true ]]; then
	fail "tag $tag cannot be checked without origin"
else
	remote_tag_output=$(git ls-remote --exit-code --tags origin "refs/tags/$tag" 2>&1)
	remote_tag_rc=$?
	if (( remote_tag_rc == 0 )); then
		fail "tag $tag already exists on origin"
	elif (( remote_tag_rc == 2 )); then
		pass "tag $tag is unused locally and on origin"
	else
		fail "remote tag $tag could not be checked: $remote_tag_output"
	fi
fi

if [[ "$git_ready" != true || -z "$head" ]]; then
	fail "CI conclusion cannot be checked without HEAD"
elif ! command -v gh >/dev/null 2>&1; then
	pass "CI conclusion not checked: gh is unavailable; local gates remain authoritative"
else
	ci_conclusion=$(gh run list --commit "$head" --workflow CI --limit 1 \
		--json conclusion --jq '.[0].conclusion // ""' 2>/dev/null)
	ci_rc=$?
	if (( ci_rc != 0 )); then
		pass "CI conclusion not checked: gh could not read it; local gates remain authoritative"
	elif [[ -z "$ci_conclusion" ]]; then
		pass "CI has no conclusion for HEAD; local gates remain authoritative"
	elif [[ "$ci_conclusion" == success ]]; then
		pass "CI conclusion for HEAD is success"
	else
		fail "CI conclusion for HEAD is $ci_conclusion, not success"
	fi
fi

if (( failures > 0 )); then
	printf '%d release preflight check(s) failed\n' "$failures" >&2
	exit 1
fi

if [[ "$mode" == status ]]; then
	printf 'Release %s can proceed; no changes were made.\n' "$tag"
	exit 0
fi

run_gate() {
	printf 'GATE  %s\n' "$*"
	"$@"
}

run_gate make validate || exit 1
run_gate make test-e2e || exit 1
run_gate make docker-build VERSION="$image_version" || exit 1

printf 'TAG   creating annotated tag %s on HEAD\n' "$tag"
git tag -a "$tag" -m "Release $tag" HEAD || exit 1

if [[ "$remote_main_state" == missing || "$remote_main_state" == behind ]]; then
	printf 'PUSH  main to origin\n'
	git push origin main || exit 1
fi

printf 'PUSH  %s to origin\n' "$tag"
git push origin "$tag" || exit 1

printf '%s\n' \
	"Release $tag is tagged and pushed." \
	"The release workflow now builds linux/amd64 and linux/arm64 images." \
	"With registry secrets it pushes :$image_version, :latest, and :$head." \
	"Without registry secrets the tag remains, but no image is published."
