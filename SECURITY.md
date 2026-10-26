# Security Policy

## Supported versions

Security fixes are released for the latest minor version on crates.io.

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub: use the **"Report a vulnerability"** button in the repository's [Security](https://github.com/centrifugal/centrifuge-rust/security) tab. Reports are visible only to maintainers.

Please do **not** open a public issue for security problems.

Include as much detail as you can:

- a description of the vulnerability
- steps to reproduce
- affected versions and configuration
- potential impact

We will acknowledge the report and assess it promptly. Fixed vulnerabilities are announced in the GitHub release notes and submitted to the [RustSec advisory database](https://rustsec.org).

## How we detect vulnerabilities

- Dependencies are checked against RustSec advisories on every pull request and weekly (`cargo-deny`).
- Dependabot keeps dependencies and GitHub Actions current.
- GitHub workflows are audited with `zizmor`, and every action is pinned to a commit SHA.
- The crate forbids `unsafe` code.
