//! `cargo xtask tla-traces`: generates the TLA+ traces that
//! `crates/centrifuge-core/tests/tla_replay.rs` replays (docs/spec.md §11.1).
//!
//! It runs TLC in simulation mode on a copy of the SDK model (the model's
//! directory is only read), with `tests/tla/Replay.tla` narrowing the
//! schedules to the ones this SDK implements, and writes the traces as
//! diff-encoded JSON lines: `tests/tla/traces/<config>.jsonl`. They're
//! committed, so CI replays them without the (private) model.
//!
//! Environment:
//! - `TLA_MODEL`: the model directory (default `../centrifugo-pro-source/tla/sdk`)
//! - `TLA2TOOLS`: `tla2tools.jar` (default: `<model>/../tla2tools.jar`)
//! - `TLA_TRACES`: traces per config (default 40)
//! - `TLA_DEPTH`: steps per trace (default 150)

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Map, Value};

/// Configs of the model with only stream subscriptions (phase 1: no map or
/// shared poll subscriptions).
const CONFIGS: &[&str] = &[
    "MC_small",
    "MC_close",
    "MC_delta",
    "MC_emulation",
    "MC_emulation_errors",
    "MC_epochs",
    "MC_errors",
    "MC_get_state",
    "MC_invalidate",
    "MC_large",
    "MC_large_emulation",
    "MC_not_recoverable",
    "MC_refresh",
    "MC_replace",
    "MC_server_subs",
    "MC_server_subs_disconnect",
    "MC_tokens",
    "MC_two_channels",
];

/// Configs of wrong behaviours (bug variants of the model, several of them
/// centrifuge-js today) that phase 1 can express. TLC's counterexample of each
/// (under the replayed schedules) must not replay into the core.
const BUG_CONFIGS: &[&str] = &[
    "bug_accept_stale_reply",
    "bug_invalidate_empty_epoch",
    "bug_no_unsub_defer",
    "bug_no_unsub_wait",
    "bug_ss_store_unannounced",
    "bug_stale_flow_continuation",
    "bug_stale_flow_continuation_ws",
    "bug_unsub_push_any_state",
];

/// Fields of map and shared poll subscriptions, dropped from the traces.
const DROP: &[&str] = &[
    "m", "trk", "sig", "spe", "rp", "rtm", "spk", "spf", "spu", "syn", "sbuf", "pbuf", "fep",
    "foff", "frec", "kv", "log", "mres", "ksig", "keys", "vers", "sigs", "phase", "cur", "ents",
    "mpubs", "key", "val",
];

type Res<T> = Result<T, Box<dyn std::error::Error>>;

#[allow(clippy::too_many_lines, clippy::similar_names)] // one linear procedure
pub(crate) fn traces(root: &Path) -> Res<()> {
    let model = std::env::var_os("TLA_MODEL").map_or_else(
        || root.join("../centrifugo-pro-source/tla/sdk"),
        PathBuf::from,
    );
    let jar =
        std::env::var_os("TLA2TOOLS").map_or_else(|| model.join("../tla2tools.jar"), PathBuf::from);
    let per: usize = std::env::var("TLA_TRACES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40);
    let depth: usize = std::env::var("TLA_DEPTH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(150);
    let tests = root.join("crates/centrifuge-core/tests/tla");
    let out_dir = tests.join("traces");
    std::fs::create_dir_all(&out_dir)?;
    let work = std::env::temp_dir().join("centrifuge-xtask-tla");
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work)?;
    for f in ["SDK.tla", "SDKBase.tla"] {
        std::fs::copy(model.join(f), work.join(f))?;
    }
    std::fs::copy(tests.join("Replay.tla"), work.join("Replay.tla"))?;
    let commit = git_head(&model);
    for (i, cfg) in CONFIGS.iter().enumerate() {
        let text = std::fs::read_to_string(model.join("configs").join(format!("{cfg}.cfg")))?;
        let constants = constants(&text)?;
        let dir = work.join(cfg);
        std::fs::create_dir_all(&dir)?;
        std::fs::write(work.join(format!("{cfg}.cfg")), replay_cfg(&text))?;
        let status = Command::new("java")
            .current_dir(&work)
            .args(["-XX:+UseParallelGC", "-cp"])
            .arg(&jar)
            .args(["tlc2.TLC", "-simulate"])
            .arg(format!("file={},num={per}", dir.join("tr").display()))
            .args([
                "-depth",
                &depth.to_string(),
                "-seed",
                &(1000 + i).to_string(),
                "-deadlock",
                "-workers",
                "1",
            ])
            .args(["-config", &format!("{cfg}.cfg"), "-metadir"])
            .arg(work.join("meta").join(cfg))
            .arg("Replay.tla")
            .output()?;
        if !status.status.success() {
            return Err(format!(
                "TLC failed on {cfg}:\n{}",
                String::from_utf8_lossy(&status.stdout)
            )
            .into());
        }
        let mut seen = std::collections::HashSet::new();
        let mut lines = String::new();
        let mut files: Vec<_> = std::fs::read_dir(&dir)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .collect();
        files.sort();
        for f in files {
            let states = parse_trace(&std::fs::read_to_string(&f)?)?;
            if states.len() < 3 {
                continue;
            }
            let encoded = encode(&states);
            if !seen.insert(encoded.to_string()) {
                continue;
            }
            let line =
                serde_json::json!({ "config": cfg, "constants": constants, "states": encoded });
            writeln!(lines, "{line}")?;
        }
        let path = out_dir.join(format!("{cfg}.jsonl"));
        std::fs::write(&path, lines)?;
        println!("{cfg}: {} traces", seen.len());
    }
    let mut bugs = String::new();
    for cfg in BUG_CONFIGS {
        let text = std::fs::read_to_string(model.join("configs").join(format!("{cfg}.cfg")))?;
        std::fs::write(work.join(format!("{cfg}.cfg")), replay_cfg(&text))?;
        let out = Command::new("java")
            .current_dir(&work)
            .args(["-XX:+UseParallelGC", "-cp"])
            .arg(&jar)
            .args([
                "tlc2.TLC",
                "-deadlock",
                "-workers",
                "auto",
                "-config",
                &format!("{cfg}.cfg"),
                "-metadir",
            ])
            .arg(work.join("meta").join(cfg))
            .arg("Replay.tla")
            .output()?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let Some(at) = stdout.find("\nState 1:") else {
            println!("{cfg}: not reachable under the replayed schedules");
            continue;
        };
        let violated = stdout
            .lines()
            .find(|l| l.contains("is violated"))
            .unwrap_or("")
            .trim()
            .to_owned();
        let states = parse_states(&stdout[at..], "\nState ")?;
        let line = serde_json::json!({
            "config": cfg, "violated": violated, "constants": constants(&text)?, "states": encode(&states),
        });
        writeln!(bugs, "{line}")?;
        println!(
            "{cfg}: counterexample of {} steps ({violated})",
            states.len()
        );
    }
    std::fs::write(tests.join("bugs.jsonl"), bugs)?;
    std::fs::write(
        out_dir.join("README.md"),
        format!(
            "Generated by `cargo xtask tla-traces` from the SDK TLA+ model (`tla/sdk`, {commit}) with \
             `../Replay.tla`, {per} simulated traces of up to {depth} steps per config, deduplicated. \
             Replayed by `tests/tla_replay.rs`. Don't edit by hand.\n"
        ),
    )?;
    Ok(())
}

fn git_head(dir: &Path) -> String {
    Command::new("git")
        .current_dir(dir)
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown commit".into(),
            |o| format!("commit {}", String::from_utf8_lossy(&o.stdout).trim()),
        )
}

fn constants(cfg: &str) -> Res<Value> {
    let mut m = Map::new();
    let mut inside = false;
    for line in cfg.lines() {
        let t = line.trim();
        if t == "CONSTANTS" {
            inside = true;
            continue;
        }
        if inside {
            let Some((k, v)) = t.split_once(" = ") else {
                break;
            };
            m.insert(k.to_owned(), Parser::new(v).value()?);
        }
    }
    Ok(Value::Object(m))
}

/// The states of a TLC trace file (`STATE_n == /\ var = value ...`).
fn parse_trace(text: &str) -> Res<Vec<BTreeMap<String, Value>>> {
    parse_states(text, "STATE_")
}

/// The model's config for the replayed schedules: `ReplaySpec`, safety only.
fn replay_cfg(text: &str) -> String {
    let mut cfg = String::new();
    for line in text.lines() {
        if line.starts_with("PROPERTIES") {
            break;
        }
        if line.starts_with("SPECIFICATION") {
            cfg.push_str("SPECIFICATION ReplaySpec\n");
        } else {
            cfg.push_str(line);
            cfg.push('\n');
        }
    }
    cfg
}

/// States separated by `marker`: `STATE_n ==` in trace files, `State n: <...>`
/// in TLC's error output.
fn parse_states(text: &str, marker: &str) -> Res<Vec<BTreeMap<String, Value>>> {
    let mut states = Vec::new();
    for block in text.split(marker).skip(1) {
        let body = block
            .split_once("==")
            .or_else(|| block.split_once(">\n"))
            .map_or("", |(_, b)| b);
        // TLC's output goes on after the last state.
        let body = body.split("\n\n").next().unwrap_or(body);
        let mut vars = BTreeMap::new();
        for part in body
            .split("\n/\\ ")
            .map(|p| p.trim_start_matches("/\\ ").trim())
        {
            let Some((name, value)) = part.split_once(" = ") else {
                continue;
            };
            let value = value
                .trim_end_matches(|c: char| c.is_whitespace() || c == '=')
                .trim_end_matches("====")
                .trim();
            let name = name.trim();
            if name == "n" {
                continue; // the model's bound counters
            }
            let mut v = Parser::new(value).value()?;
            strip(&mut v);
            vars.insert(name.to_owned(), v);
        }
        states.push(vars);
    }
    Ok(states)
}

fn strip(v: &mut Value) {
    match v {
        Value::Object(m) => {
            m.retain(|k, _| !DROP.contains(&k.as_str()));
            m.values_mut().for_each(strip);
        }
        Value::Array(a) => a.iter_mut().for_each(strip),
        _ => {}
    }
}

/// The first state whole, then only the variables that changed.
fn encode(states: &[BTreeMap<String, Value>]) -> Value {
    let mut out = Vec::new();
    for (i, s) in states.iter().enumerate() {
        let mut m = Map::new();
        for (k, v) in s {
            if i == 0 || k == "act" || states[i - 1].get(k) != Some(v) {
                m.insert(k.clone(), v.clone());
            }
        }
        out.push(Value::Object(m));
    }
    Value::Array(out)
}

/// TLA+ values as TLC prints them, to JSON: records and functions become
/// objects, sequences and sets arrays.
struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            s: s.as_bytes(),
            i: 0,
        }
    }

    fn ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }

    fn eat(&mut self, t: &str) -> bool {
        self.ws();
        if self.s[self.i..].starts_with(t.as_bytes()) {
            self.i += t.len();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, t: &str) -> Res<()> {
        if self.eat(t) {
            Ok(())
        } else {
            Err(format!(
                "expected {t:?} at {}: {:?}",
                self.i,
                String::from_utf8_lossy(&self.s[self.i..(self.i + 40).min(self.s.len())])
            )
            .into())
        }
    }

    fn ident(&mut self) -> String {
        self.ws();
        let start = self.i;
        while self.i < self.s.len()
            && (self.s[self.i].is_ascii_alphanumeric() || self.s[self.i] == b'_')
        {
            self.i += 1;
        }
        String::from_utf8_lossy(&self.s[start..self.i]).into_owned()
    }

    fn value(&mut self) -> Res<Value> {
        self.ws();
        if self.eat("<<") {
            let mut items = Vec::new();
            if !self.eat(">>") {
                loop {
                    items.push(self.value()?);
                    if self.eat(">>") {
                        break;
                    }
                    self.expect(",")?;
                }
            }
            return Ok(Value::Array(items));
        }
        if self.eat("[") {
            let mut m = Map::new();
            if !self.eat("]") {
                loop {
                    let k = self.ident();
                    self.expect("|->")?;
                    m.insert(k, self.value()?);
                    if self.eat("]") {
                        break;
                    }
                    self.expect(",")?;
                }
            }
            return Ok(Value::Object(m));
        }
        if self.eat("{") {
            let mut items = Vec::new();
            if !self.eat("}") {
                loop {
                    items.push(self.value()?);
                    if self.eat("}") {
                        break;
                    }
                    self.expect(",")?;
                }
            }
            return Ok(Value::Array(items));
        }
        if self.eat("(") {
            let mut m = Map::new();
            loop {
                let k = match self.value()? {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                self.expect(":>")?;
                m.insert(k, self.value()?);
                if self.eat(")") {
                    break;
                }
                self.expect("@@")?;
            }
            return Ok(Value::Object(m));
        }
        if self.eat("\"") {
            let mut s = String::new();
            while self.i < self.s.len() && self.s[self.i] != b'"' {
                if self.s[self.i] == b'\\' {
                    self.i += 1;
                }
                s.push(char::from(self.s[self.i]));
                self.i += 1;
            }
            self.i += 1;
            return Ok(Value::String(s));
        }
        if self.eat("TRUE") {
            return Ok(Value::Bool(true));
        }
        if self.eat("FALSE") {
            return Ok(Value::Bool(false));
        }
        self.ws();
        let start = self.i;
        if self.i < self.s.len() && self.s[self.i] == b'-' {
            self.i += 1;
        }
        while self.i < self.s.len() && self.s[self.i].is_ascii_digit() {
            self.i += 1;
        }
        let text = std::str::from_utf8(&self.s[start..self.i])?;
        let n: i64 = text
            .parse()
            .map_err(|e| format!("bad value at {start}: {e} {text:?}"))?;
        Ok(Value::from(n))
    }
}
