# Contributing

## Design first

Behaviour is specified in [`docs/spec.md`](docs/spec.md). A change in behaviour updates the spec in the same pull request. When the code and the spec disagree, one of them is a bug.

## Local checks

These run in CI on every pull request:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo +nightly doc --workspace --no-deps --all-features
cargo hack check --workspace --feature-powerset --depth 3 --at-least-one-of aws-lc-rs,ring \
  --exclude-features test-util --no-dev-deps        # cargo install cargo-hack
cargo check -p centrifuge-client-core --target wasm32-unknown-unknown   # rustup target add wasm32-unknown-unknown
cargo deny check                                    # cargo install cargo-deny
typos                                               # cargo install typos-cli
```

The MSRV (`rust-version` in `Cargo.toml`) is checked in CI with `cargo check`.

### Test servers

Most tests need no server: they use the in-process fake server (`crates/centrifuge-fake-server`, docs/spec.md §11.2) over every transport and protocol. The real-server tests run against Centrifugo `v6.10.2` from `docker-compose.yml` (configs in `tests/centrifugo/`), and are skipped unless enabled:

```sh
docker compose up -d                     # insecure on :8010, secure (tokens required) on :8012
docker compose --profile pro up -d       # also Centrifugo PRO on :8014 (channel compaction; local only, not in CI)
CENTRIFUGO_TESTS=1 CENTRIFUGO_PRO_TESTS=1 cargo test -p centrifuge-client --test real_server
docker compose --profile pro down
```

The ports differ from other SDKs' stacks (8000/8002), so both can run at once.

### The TLA+ trace replay

`crates/centrifuge-client-core/src/machine/replay.rs` replays traces of the SDK TLA+ model into the core (docs/spec.md §11.1). The traces are committed under `crates/centrifuge-client-core/tests/tla/`, so the test needs nothing else. After a change to the model, regenerate them (needs Java and the model, which is in a private repository):

```sh
TLA_MODEL=../centrifugo-pro-source/tla/sdk cargo xtask tla-traces
```

A divergence names the step and the trace's actions. Either the core is wrong, or `tests/tla/Replay.tla` (the schedules this SDK implements) needs a change; never edit the traces.

### Benchmarks

```sh
cargo bench -p centrifuge-client-core     # codecs, stream reassembly, fossil delta
```

Pull requests touching the core get a comparison against their base in the "Benchmarks" workflow's summary.

### Validation harness

Seeded chaos, probes of known bug patterns from other SDKs, real-server chaos and process suspension live in the `sdk_validation` repository (`centrifuge-rust/`), built against a frozen copy of a commit here. Run it before releases and for changes to the connection or subscription logic.

## Layout

- `crates/centrifuge-client`: the SDK. Package `centrifuge-client`, imported as `centrifuge`.
- `crates/centrifuge-client-core`: the sans-IO state machine (docs/spec.md §9). It does no IO and has no runtime. It must keep building for `wasm32-unknown-unknown` (checked in CI), so don't add dependencies to it that break that.
- `crates/centrifuge-fake-server`: the in-process fake Centrifugo for tests (not published).
- `xtask`: maintainer tasks (`cargo xtask proto`, `cargo xtask tla-traces`).
- `docs/spec.md`: the behaviour; `docs/api.md`: the public API on one page; `docs/decisions-pending.md`: implementation choices awaiting review.

Both crates are released together with the same version.

## Commits and pull requests

- PRs are squash-merged. **The PR title becomes the commit message**, and it must follow [Conventional Commits](https://www.conventionalcommits.org) (checked in CI):
  - `feat: ...` → minor version bump before 1.0 (and a changelog "Added" entry)
  - `fix: ...` → patch bump ("Fixed")
  - `feat!: ...` or a `BREAKING CHANGE:` footer → breaking bump
  - `perf`, `refactor`, `docs`, `revert` → changelog entries; `test`, `ci`, `build`, `chore` → no entry
- Each bug fix comes with a test that fails without the fix (docs/spec.md §11.5).

## Releases

As in the other Centrifugal SDKs: bump the version, then tag. A release can include any number of merged PRs.

1. **Bump the version** in a normal PR (or commit) to `main`, titled `chore: bump to X.Y.Z`. Both crates share one version, set in two places of the root `Cargo.toml`:
   - `[workspace.package]`: `version = "X.Y.Z"`;
   - `[workspace.dependencies]`: `centrifuge-client-core = { version = "=X.Y.Z", ... }`.

   Then run `cargo check` to update `Cargo.lock`. Bump by semver: before 1.0, a breaking change bumps the minor version (`0.1.x` → `0.2.0`); anything else bumps the patch.
2. **Wait for CI** to pass on `main`.
3. **Tag it.** Either create the release on GitHub (Releases → Draft a new release → new tag `vX.Y.Z` on `main`, write the notes, publish), or push a tag, `git tag vX.Y.Z && git push origin vX.Y.Z`, and let the workflow create the release with generated notes (the PRs merged since the previous tag). Edit the notes afterwards if needed.
4. The tag runs `.github/workflows/release.yml`. It checks that the tag names the workspace version, points at a commit on `main` that passed CI, and that the bump matches the API change (`cargo-semver-checks`). Then it publishes the core and the client to crates.io through **trusted publishing** (OIDC, no long-lived token).

Versions already on crates.io are skipped, so rerunning the workflow after a partial failure finishes the release. If the semver check fails, the bump was too small: delete the tag and the release, bump again, and retag. Nobody runs `cargo publish` by hand, except for the first publish below.

### Release setup (one time, maintainers)

1. **First publish.** crates.io trusted publishing can't create a new crate, so publish `0.1.0` of both crates once by hand:
   - Create a crates.io API token with only the `publish-new` scope, limited to the crates `centrifuge-client-core` and `centrifuge-client`, expiring in a day.
   - From a clean checkout of the pushed `main` commit, publish the core first:
     `cargo publish -p centrifuge-client-core && cargo publish -p centrifuge-client`
   - Revoke the token.
   - Create the `v0.1.0` release on GitHub, with the `0.1.0` section of `CHANGELOG.md` as its notes. The release workflow finds both crates published and has nothing else to do.
2. **Trusted publisher.** On crates.io, for **both** `centrifuge-client-core` and `centrifuge-client`, go to Settings → Trusted Publishing and add GitHub: owner `centrifugal`, repository `centrifuge-rust`, workflow `release.yml`, environment `release`. After the first release through CI works, also turn on **Trusted Publishing Only** mode there, which rejects token publishes.
3. **GitHub environment.** Create an environment named `release` (Settings → Environments). Under deployment branches and tags, allow only tags matching `v*`. Optionally add required reviewers: they approve each publish.
4. **Tag protection.** Add a tag ruleset for `v*` (Settings → Rules → Rulesets) that only maintainers can bypass, so only they can create, move or delete release tags.
5. **Branch protection on `main`.** Require the `CI success` and `Conventional commit title` checks. Allow squash merges only.
6. **Codecov** (optional). Install the Codecov GitHub App for the repository. Uploads use OIDC, so no token is needed.
