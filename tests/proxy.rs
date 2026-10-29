//! Tests de punta a punta con procesos reales: cliente -> proxy -> servidor.

use mcp_idempotency_journal::{KEY_MISMATCH, OUTCOME_UNKNOWN};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::time::Duration;

struct Peer {
    child: Child,
    stdin: ChildStdin,
    rx: Receiver<Value>,
}

impl Peer {
    fn start(dir: &Path, proxy: bool, extra_env: &[(&str, &str)], proxy_args: &[&str]) -> Peer {
        let mut cmd = if proxy {
            let mut c = Command::new(env!("CARGO_BIN_EXE_mcp-journal-proxy"));
            c.arg("--journal").arg(dir.join("journal.log"));
            c.args(proxy_args);
            c.arg("--").arg(env!("CARGO_BIN_EXE_mock-mcp-server"));
            c
        } else {
            Command::new(env!("CARGO_BIN_EXE_mock-mcp-server"))
        };
        cmd.env("MOCK_LEDGER", dir.join("ledger.txt"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0);
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let out = child.stdout.take().unwrap();
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            for l in BufReader::new(out).lines().map_while(Result::ok) {
                if tx.send(serde_json::from_str(&l).unwrap()).is_err() {
                    return;
                }
            }
        });
        let mut p = Peer { child, stdin, rx };
        p.send(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}));
        assert_eq!(
            p.recv(&json!(1)).unwrap()["result"]["protocolVersion"],
            "2025-11-25"
        );
        p.send(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        p
    }

    fn send(&mut self, v: &Value) {
        writeln!(self.stdin, "{v}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn recv(&mut self, id: &Value) -> Option<Value> {
        loop {
            match self.rx.recv_timeout(Duration::from_secs(10)) {
                Ok(v) if v["id"] == *id => return Some(v),
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
    }

    fn call(&mut self, id: u64, op: &str, key: Option<&str>, amount: i64) -> Option<Value> {
        let mut params = json!({"name":"transfer","arguments":{"op_id":op,"amount":amount}});
        if let Some(k) = key {
            params["_meta"] = json!({"idempotencyKey": k});
        }
        self.send(&json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":params}));
        self.recv(&json!(id))
    }

    fn kill(mut self) {
        unsafe {
            libc::killpg(self.child.id() as i32, libc::SIGKILL);
        }
        let _ = self.child.wait();
    }

    fn close(mut self) {
        drop(self.stdin);
        let _ = self.child.wait();
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn effects(dir: &Path, op: &str) -> usize {
    std::fs::read_to_string(dir.join("ledger.txt"))
        .unwrap_or_default()
        .lines()
        .filter(|l| l.split(' ').next() == Some(op))
        .count()
}

fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// Issue #3394: el efecto se commitea, la respuesta del intento 1 se
/// pierde y la app reintenta una vez con un id JSON-RPC nuevo.
#[test]
fn issue_3394_baseline_without_proxy_duplicates() {
    let d = tmp();
    let mut p = Peer::start(d.path(), false, &[], &[]);
    let _lost = p.call(2, "op-a", Some("n1"), 10).unwrap(); // respuesta descartada
    let retry = p.call(3, "op-a", Some("n1"), 10).unwrap();
    assert!(retry["result"].is_object());
    p.close();
    assert_eq!(
        effects(d.path(), "op-a"),
        2,
        "baseline: 2 efectos para 1 operación"
    );
}

#[test]
fn issue_3394_with_proxy_single_effect_and_same_result() {
    let d = tmp();
    let mut p = Peer::start(d.path(), true, &[], &[]);
    let first = p.call(2, "op-a", Some("n1"), 10).unwrap(); // se "pierde"
    let retry = p.call(3, "op-a", Some("n1"), 10).unwrap();
    assert_eq!(retry["id"], 3, "la respuesta usa el id nuevo");
    assert_eq!(
        retry["result"], first["result"],
        "se repite el resultado guardado"
    );
    p.close();
    assert_eq!(effects(d.path(), "op-a"), 1);
}

#[test]
fn same_key_with_other_arguments_is_rejected() {
    let d = tmp();
    let mut p = Peer::start(d.path(), true, &[], &[]);
    p.call(2, "op-a", Some("n1"), 10).unwrap();
    let r = p.call(3, "op-a", Some("n1"), 99).unwrap();
    assert_eq!(r["error"]["code"], KEY_MISMATCH);
    p.close();
    assert_eq!(effects(d.path(), "op-a"), 1);
}

#[test]
fn calls_without_key_pass_through() {
    // Sin clave el proxy no inventa una: dos calls iguales pueden ser
    // repeticiones intencionales.
    let d = tmp();
    let mut p = Peer::start(d.path(), true, &[], &[]);
    p.call(2, "op-a", None, 10).unwrap();
    p.call(3, "op-a", None, 10).unwrap();
    p.close();
    assert_eq!(effects(d.path(), "op-a"), 2);
}

#[test]
fn journal_survives_proxy_kill() {
    let d = tmp();
    let mut p = Peer::start(d.path(), true, &[], &[]);
    let first = p.call(2, "op-a", Some("n1"), 10).unwrap();
    p.kill();
    let mut p2 = Peer::start(d.path(), true, &[], &[]);
    let retry = p2.call(7, "op-a", Some("n1"), 10).unwrap();
    assert_eq!(retry["result"], first["result"]);
    p2.close();
    assert_eq!(effects(d.path(), "op-a"), 1);
}

/// El servidor muere después de commitear: el proxy no sabe si hubo
/// efecto y no reintenta por su cuenta.
#[test]
fn server_crash_after_commit_reports_unknown_without_duplicate() {
    let d = tmp();
    let env = [("MOCK_CRASH_PCT", "100"), ("MOCK_SEED", "1")];
    let mut p = Peer::start(d.path(), true, &env, &[]);
    let r = p.call(2, "op-a", Some("n1"), 10).unwrap();
    assert_eq!(r["error"]["code"], OUTCOME_UNKNOWN);
    let r2 = p.call(3, "op-a", Some("n1"), 10).unwrap();
    assert_eq!(
        r2["error"]["code"], OUTCOME_UNKNOWN,
        "intención sin resultado"
    );
    p.close();
    assert_eq!(effects(d.path(), "op-a"), 1);
}

/// Con un servidor que deduplica por la misma clave, el proxy puede
/// reenviar y se llega a exactamente una vez con resultado.
#[test]
fn cooperative_server_gives_exactly_once() {
    let d = tmp();
    let crash = [
        ("MOCK_CRASH_PCT", "100"),
        ("MOCK_SEED", "1"),
        ("MOCK_HONOR_KEY", "1"),
    ];
    let mut p = Peer::start(d.path(), true, &crash, &["--server-dedups"]);
    let r = p.call(2, "op-a", Some("n1"), 10).unwrap();
    assert_eq!(r["error"]["code"], OUTCOME_UNKNOWN);
    p.close();
    let mut p2 = Peer::start(
        d.path(),
        true,
        &[("MOCK_HONOR_KEY", "1")],
        &["--server-dedups"],
    );
    let r2 = p2.call(3, "op-a", Some("n1"), 10).unwrap();
    assert_eq!(r2["result"]["content"][0]["text"], "deduped op-a");
    p2.close();
    assert_eq!(effects(d.path(), "op-a"), 1);
}

fn harness(mode: &str, dir: &Path, faults: bool) -> Value {
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_idem-harness"));
    let mut c = Command::new(exe);
    c.args([
        "--mode",
        mode,
        "--calls",
        "10000",
        "--seed",
        "1",
        "--workdir",
    ])
    .arg(dir);
    if faults {
        c.args(["--lost-pct", "10", "--crash-pct", "3", "--kill-pct", "1"]);
    }
    let out = c.output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

#[test]
fn ten_thousand_calls_with_faults() {
    let d = tmp();
    let base = harness("baseline", &d.path().join("b"), true);
    let prox = harness("proxy", &d.path().join("p"), true);
    let coop = harness("proxy-coop", &d.path().join("c"), true);
    eprintln!("baseline {base}\nproxy {prox}\nproxy-coop {coop}");
    assert!(
        base["duplicate_effects"].as_u64().unwrap() > 0,
        "baseline > 0"
    );
    assert_eq!(prox["duplicate_effects"], 0);
    assert_eq!(coop["duplicate_effects"], 0);
    for r in [&base, &prox, &coop] {
        assert_eq!(r["calls"], 10000);
        assert_eq!(r["success_without_effect"], 0);
        assert_eq!(r["client_gave_up"], 0);
        assert!(r["lost_injected"].as_u64().unwrap() > 0);
        assert!(r["kills_injected"].as_u64().unwrap() > 0);
    }
    // Exactamente una vez con servidor cooperativo.
    assert_eq!(coop["ledger_effects"], 10000);
    assert_eq!(coop["client_success"], 10000);
}
