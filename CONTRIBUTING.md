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

Releases are automated with [release-plz](https://release-plz.dev):

1. Every merge to `main` opens or updates a **release PR**. It bumps the version from the commit history, checks the bump with `cargo-semver-checks`, and updates `CHANGELOG.md`.
2. Review the release PR (version and changelog), then merge it.
3. The release workflow publishes to crates.io through **trusted publishing** (OIDC, no long-lived token), then creates the `vX.Y.Z` tag and the GitHub release.

Nobody runs `cargo publish` by hand, except for the first publish below.

### Release setup (one time, maintainers)

1. **First publish.** crates.io trusted publishing can't create a new crate, so publish the first version of both crates once by hand (`0.1.0`, or a `0.0.1` name reservation if it isn't published yet), core first:
   `cargo publish -p centrifuge-client-core && cargo publish -p centrifuge-client` with a short-lived crates.io token, then revoke the token.
2. **Trusted publisher.** On crates.io, for **both** `centrifuge-client-core` and `centrifuge-client`, go to Settings → Trusted Publishing and add GitHub: owner `centrifugal`, repository `centrifuge-rust`, workflow `release-plz.yml`, environment `release`.
3. **GitHub environment.** Create an environment named `release` (Settings → Environments). Optionally restrict it to the `main` branch and add required reviewers.
4. **GitHub App for release PRs.** PRs opened with the default `GITHUB_TOKEN` don't trigger CI.
   - Create (or reuse) an org GitHub App with *Contents: read & write* and *Pull requests: read & write*, and install it on this repository.
   - Store its client ID as the repository variable `RELEASE_PLZ_APP_CLIENT_ID`, and its private key as the secret `RELEASE_PLZ_APP_PRIVATE_KEY`.
5. **Branch protection on `main`.** Require the `CI success` and `Conventional commit title` checks. Allow squash merges only.
6. **Codecov** (optional). Install the Codecov GitHub App for the repository. Uploads use OIDC, so no token is needed.
