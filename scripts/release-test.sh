#!/usr/bin/env bash

set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
real_make=$(command -v make)
test_root=$(mktemp -d)
trap 'rm -rf "$test_root"' EXIT

fake_bin="$test_root/fake-bin"
mkdir -p "$fake_bin"

for command_name in go docker; do
	printf '#!/usr/bin/env bash\nexit 0\n' >"$fake_bin/$command_name"
	chmod +x "$fake_bin/$command_name"
done

cat >"$fake_bin/gh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$fake_bin/gh"

cat >"$fake_bin/make" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$RELEASE_TEST_GATE_LOG"
if [[ ${RELEASE_TEST_FAIL_GATE:-} == "${1:-}" ]]; then
	exit 1
fi
exit 0
EOF
chmod +x "$fake_bin/make"

fixture_number=0
new_fixture() {
	fixture_number=$((fixture_number + 1))
	fixture="$test_root/fixture-$fixture_number"
	repo="$fixture/repo"
	origin="$fixture/origin.git"
	gate_log="$fixture/gates.log"
	mkdir -p "$repo/scripts"
	git init --quiet --bare "$origin"
	git -C "$repo" init --quiet --initial-branch=main
	git -C "$repo" config user.name 'Release Test'
	git -C "$repo" config user.email 'release-test@example.invalid'
	cp "$project_root/Makefile" "$repo/Makefile"
	cp "$project_root/scripts/release.sh" "$repo/scripts/release.sh"
	chmod +x "$repo/scripts/release.sh"
	git -C "$repo" add Makefile scripts/release.sh
	git -C "$repo" commit --quiet -m initial
	git -C "$repo" remote add origin "$origin"
	: >"$gate_log"
}

push_main() {
	git -C "$repo" push --quiet --set-upstream origin main
}

snapshot_repo() {
	local snapshot_prefix=$1
	git -C "$repo" rev-parse HEAD >"$snapshot_prefix.head"
	git -C "$repo" show-ref >"$snapshot_prefix.refs" || true
	git --git-dir="$origin" show-ref >"$snapshot_prefix.remote-refs" || true
}

assert_unchanged() {
	local snapshot_prefix=$1
	local after_prefix="$snapshot_prefix.after"
	snapshot_repo "$after_prefix"
	cmp "$snapshot_prefix.head" "$after_prefix.head"
	cmp "$snapshot_prefix.refs" "$after_prefix.refs"
	cmp "$snapshot_prefix.remote-refs" "$after_prefix.remote-refs"
}

run_make() {
	(
		cd "$repo"
		PATH="$fake_bin:$PATH" RELEASE_TEST_GATE_LOG="$gate_log" \
			RELEASE_TEST_FAIL_GATE="${RELEASE_TEST_FAIL_GATE:-}" \
			"$real_make" --no-print-directory -s "$@"
	)
}

run_script() {
	(
		cd "$repo"
		PATH="$fake_bin:$PATH" RELEASE_TEST_GATE_LOG="$gate_log" \
			./scripts/release.sh "$@"
	)
}

expect_rejection() {
	local name=$1
	shift
	local snapshot="$fixture/$name-before"
	snapshot_repo "$snapshot"
	if run_make "$@" >"$fixture/$name.output" 2>&1; then
		printf 'FAIL: %s was accepted\n' "$name" >&2
		exit 1
	fi
	assert_unchanged "$snapshot"
	if [[ -s "$gate_log" ]]; then
		printf 'FAIL: %s ran a release gate\n' "$name" >&2
		exit 1
	fi
	printf 'PASS: %s rejected without mutation\n' "$name"
}

new_fixture
push_main
printf 'dirty\n' >"$repo/untracked.txt"
expect_rejection dirty-working-tree release 1.2.3

new_fixture
push_main
git -C "$repo" switch --quiet -c topic
expect_rejection non-main-branch release 1.2.3

new_fixture
push_main
git -C "$repo" tag -a v1.2.3 -m existing
expect_rejection existing-local-tag release 1.2.3

new_fixture
push_main
git -C "$repo" tag -a v1.2.3 -m existing
git -C "$repo" push --quiet origin v1.2.3
git -C "$repo" tag -d v1.2.3 >/dev/null
expect_rejection existing-remote-tag release 1.2.3

new_fixture
push_main
expect_rejection malformed-version release 01.2.3

new_fixture
push_main
expect_rejection ambiguous-version release 1.2.3 VERSION=1.2.3

new_fixture
push_main
expect_rejection force-variable release 1.2.3 FORCE=1

new_fixture
push_main
force_snapshot="$fixture/force-flag-before"
snapshot_repo "$force_snapshot"
if run_script release 1.2.3 --force >"$fixture/force-flag.output" 2>&1; then
	printf 'FAIL: force flag was accepted\n' >&2
	exit 1
fi
if ! grep -qi force "$fixture/force-flag.output"; then
	printf 'FAIL: force flag rejection did not name the reason\n' >&2
	exit 1
fi
assert_unchanged "$force_snapshot"
if [[ -s "$gate_log" ]]; then
	printf 'FAIL: force flag ran a release gate\n' >&2
	exit 1
fi
printf 'PASS: force flag rejected without mutation\n'

new_fixture
push_main
gate_failure_snapshot="$fixture/gate-failure-before"
snapshot_repo "$gate_failure_snapshot"
if RELEASE_TEST_FAIL_GATE=validate run_make release 1.2.3 \
	>"$fixture/gate-failure.output" 2>&1; then
	printf 'FAIL: failed validation gate was accepted\n' >&2
	exit 1
fi
assert_unchanged "$gate_failure_snapshot"
if [[ $(cat "$gate_log") != validate ]]; then
	printf 'FAIL: release continued after a failed gate\n' >&2
	exit 1
fi
printf 'PASS: failed gate stops before mutation\n'

new_fixture
push_main
status_snapshot="$fixture/status-before"
snapshot_repo "$status_snapshot"
status_output=$(run_make release-status v1.2.3)
assert_unchanged "$status_snapshot"
if [[ -s "$gate_log" ]]; then
	printf 'FAIL: release-status ran a release gate\n' >&2
	exit 1
fi
if [[ "$status_output" != *'no changes were made'* ]]; then
	printf 'FAIL: release-status did not report its read-only result\n' >&2
	exit 1
fi
printf 'PASS: release-status is read-only\n'

new_fixture
push_main
printf 'next\n' >"$repo/next.txt"
git -C "$repo" add next.txt
git -C "$repo" commit --quiet -m next
run_make release 1.2.3 >"$fixture/release-behind.output"
if [[ $(git -C "$repo" cat-file -t refs/tags/v1.2.3) != tag ]]; then
	printf 'FAIL: release did not create an annotated tag\n' >&2
	exit 1
fi
if [[ $(git --git-dir="$origin" rev-parse refs/heads/main) != $(git -C "$repo" rev-parse HEAD) ]]; then
	printf 'FAIL: release did not advance remote main\n' >&2
	exit 1
fi
git --git-dir="$origin" rev-parse refs/tags/v1.2.3 >/dev/null
expected_gates=$'validate\ntest-e2e\ndocker-build VERSION=1.2.3'
if [[ $(cat "$gate_log") != "$expected_gates" ]]; then
	printf 'FAIL: release gates were %q, want %q\n' "$(cat "$gate_log")" "$expected_gates" >&2
	exit 1
fi
printf 'PASS: release gates, tags, and advances remote main\n'

new_fixture
run_make release v1.2.4 >"$fixture/first-release.output"
if [[ $(git -C "$repo" cat-file -t refs/tags/v1.2.4) != tag ]]; then
	printf 'FAIL: first release did not create an annotated tag\n' >&2
	exit 1
fi
git --git-dir="$origin" rev-parse refs/heads/main >/dev/null
git --git-dir="$origin" rev-parse refs/tags/v1.2.4 >/dev/null
printf 'PASS: first release creates remote main and pushes its tag\n'
