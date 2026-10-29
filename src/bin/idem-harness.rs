//! Harness de inyección de fallas para tools/call mutantes con reintento.
//!
//! uso: idem-harness --mode baseline|proxy|proxy-coop --calls N --workdir DIR
//!        [--seed S] [--lost-pct P] [--crash-pct C] [--kill-pct K]
//!
//! Fallas (deterministas por hash de semilla, op y intento):
//! - lost: la respuesta llega pero el cliente la pierde (timeout) y
//!   reintenta con un id JSON-RPC nuevo, como en la issue #3394.
//! - crash: el servidor muere después de commitear el efecto y antes de
//!   responder.
//! - kill: SIGKILL al grupo de procesos (proxy + servidor, o el servidor
//!   en baseline) justo después de enviar la call.
//!
//! Al final cuenta las filas del ledger por op: duplicados = filas extra.

use mcp_idempotency_journal::OUTCOME_UNKNOWN;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    Baseline,
    Proxy,
    ProxyCoop,
}

struct Cfg {
    mode: Mode,
    calls: usize,
    seed: u64,
    lost_pct: u64,
    crash_pct: u64,
    kill_pct: u64,
    dir: PathBuf,
}

fn h100(seed: u64, parts: &[&str]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325 ^ seed.wrapping_mul(0x9e3779b97f4a7c15);
    for p in parts {
        for b in p.bytes().chain(std::iter::once(0)) {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    h % 100
}

enum Line {
    Msg(Value),
    Eof,
}

struct Peer {
    child: Child,
    stdin: ChildStdin,
    rx: Receiver<Line>,
}

fn bin(name: &str) -> PathBuf {
    let me = std::env::current_exe().unwrap();
    me.parent().unwrap().join(name)
}

fn spawn(cfg: &Cfg) -> Peer {
    let ledger = cfg.dir.join("ledger.txt");
    let mut cmd = match cfg.mode {
        Mode::Baseline => Command::new(bin("mock-mcp-server")),
        Mode::Proxy | Mode::ProxyCoop => {
            let mut c = Command::new(bin("mcp-journal-proxy"));
            c.arg("--journal")
                .arg(cfg.dir.join("journal.log"))
                .arg("--session")
                .arg("harness");
            if cfg.mode == Mode::ProxyCoop {
                c.arg("--server-dedups");
            }
            c.arg("--").arg(bin("mock-mcp-server"));
            c
        }
    };
    cmd.env("MOCK_LEDGER", &ledger)
        .env("MOCK_CRASH_PCT", cfg.crash_pct.to_string())
        .env("MOCK_SEED", cfg.seed.to_string())
        .env(
            "MOCK_HONOR_KEY",
            if cfg.mode == Mode::ProxyCoop {
                "1"
            } else {
                "0"
            },
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .process_group(0);
    let mut child = cmd.spawn().expect("spawn");
    let stdin = child.stdin.take().unwrap();
    let out = child.stdout.take().unwrap();
    let (tx, rx) = channel();
    std::thread::spawn(move || {
        for l in BufReader::new(out).lines() {
            let Ok(l) = l else { break };
            if let Ok(v) = serde_json::from_str(&l) {
                if tx.send(Line::Msg(v)).is_err() {
                    return;
                }
            }
        }
        let _ = tx.send(Line::Eof);
    });
    let mut p = Peer { child, stdin, rx };
    let init = json!({"jsonrpc":"2.0","id":"init","method":"initialize","params":{
        "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"idem-harness","version":"0.1.0"}}});
    p.send(&init);
    match p.wait_id(&json!("init")) {
        Some(_) => {}
        None => panic!("initialize sin respuesta"),
    }
    p.send(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
    p
}

impl Peer {
    fn send(&mut self, v: &Value) {
        let mut s = v.to_string();
        s.push('\n');
        let _ = self.stdin.write_all(s.as_bytes());
        let _ = self.stdin.flush();
    }

    /// Espera la respuesta con ese id. `None` si el proceso murió.
    fn wait_id(&mut self, id: &Value) -> Option<Value> {
        loop {
            match self.rx.recv_timeout(Duration::from_secs(20)) {
                Ok(Line::Msg(v)) if v["id"] == *id => return Some(v),
                Ok(Line::Msg(_)) => continue,
                Ok(Line::Eof) => return None,
                Err(RecvTimeoutError::Timeout) => {
                    panic!("sin respuesta en 20 s para id {id}: el sistema bajo prueba se colgó")
                }
                Err(RecvTimeoutError::Disconnected) => return None,
            }
        }
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
        // Espera a que muera todo el grupo (el servidor hijo del proxy).
        for _ in 0..200 {
            if unsafe { libc::killpg(self.child.id() as i32, 0) } != 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        unsafe {
            libc::killpg(self.child.id() as i32, libc::SIGKILL);
        }
    }
}

#[derive(Default, Debug)]
struct Stats {
    success: usize,
    unknown: usize,
    gave_up: usize,
    attempts: usize,
    lost_injected: usize,
    kills_injected: usize,
    restarts: usize,
    replayed: usize,
    lat_us: Vec<u64>,
}

fn pct(v: &[u64], p: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i]
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let get = |k: &str| a.iter().position(|x| x == k).map(|i| a[i + 1].clone());
    let cfg = Cfg {
        mode: match get("--mode").as_deref() {
            Some("baseline") => Mode::Baseline,
            Some("proxy") => Mode::Proxy,
            Some("proxy-coop") => Mode::ProxyCoop,
            _ => panic!("--mode baseline|proxy|proxy-coop"),
        },
        calls: get("--calls").map(|s| s.parse().unwrap()).unwrap_or(10_000),
        seed: get("--seed").map(|s| s.parse().unwrap()).unwrap_or(1),
        lost_pct: get("--lost-pct").map(|s| s.parse().unwrap()).unwrap_or(0),
        crash_pct: get("--crash-pct").map(|s| s.parse().unwrap()).unwrap_or(0),
        kill_pct: get("--kill-pct").map(|s| s.parse().unwrap()).unwrap_or(0),
        dir: PathBuf::from(get("--workdir").expect("--workdir")),
    };
    std::fs::create_dir_all(&cfg.dir).unwrap();
    for f in ["ledger.txt", "journal.log"] {
        let _ = std::fs::remove_file(cfg.dir.join(f));
    }
    let mut st = Stats::default();
    let mut peer = spawn(&cfg);
    let mut next_id = 0u64;
    let mut client_saw: HashMap<String, &'static str> = HashMap::new();
    let t_all = Instant::now();
    for i in 0..cfg.calls {
        let op = format!("op{i}");
        let nonce = format!("k{i}");
        let mut attempt = 0usize;
        let outcome = loop {
            attempt += 1;
            if attempt > 20 {
                break "gave_up";
            }
            st.attempts += 1;
            next_id += 1;
            let id = json!(next_id);
            let call = json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{
                "name":"transfer","arguments":{"op_id":op,"amount":1},
                "_meta":{"idempotencyKey":nonce}}});
            let ats = attempt.to_string();
            let t0 = Instant::now();
            peer.send(&call);
            if h100(cfg.seed, &["kill", &op, &ats]) < cfg.kill_pct {
                st.kills_injected += 1;
                peer.kill();
                st.restarts += 1;
                peer = spawn(&cfg);
                continue;
            }
            let Some(resp) = peer.wait_id(&id) else {
                // Murió el proceso con el que habla el cliente.
                st.restarts += 1;
                peer.kill();
                peer = spawn(&cfg);
                continue;
            };
            let dt = t0.elapsed().as_micros() as u64;
            if h100(cfg.seed, &["lost", &op, &ats]) < cfg.lost_pct {
                st.lost_injected += 1;
                continue;
            }
            if resp.get("result").is_some() {
                if attempt == 1 {
                    st.lat_us.push(dt);
                }
                if resp["result"]["content"][0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("deduped"))
                {
                    st.replayed += 1;
                }
                break "success";
            }
            if resp["error"]["code"] == OUTCOME_UNKNOWN {
                if cfg.mode == Mode::ProxyCoop {
                    continue;
                }
                break "unknown";
            }
            panic!("error inesperado: {resp}");
        };
        match outcome {
            "success" => st.success += 1,
            "unknown" => st.unknown += 1,
            _ => st.gave_up += 1,
        }
        client_saw.insert(op, outcome);
    }
    let elapsed = t_all.elapsed().as_secs_f64();
    peer.close();
    let ledger = std::fs::read_to_string(cfg.dir.join("ledger.txt")).unwrap_or_default();
    let mut effects: HashMap<String, usize> = HashMap::new();
    for l in ledger.lines() {
        if let Some(op) = l.split(' ').next() {
            *effects.entry(op.to_string()).or_default() += 1;
        }
    }
    let mut dup_effects = 0;
    let mut ops_dup = 0;
    let mut missing = 0;
    let mut success_without_effect = 0;
    for i in 0..cfg.calls {
        let op = format!("op{i}");
        let c = effects.get(&op).copied().unwrap_or(0);
        if c > 1 {
            dup_effects += c - 1;
            ops_dup += 1;
        }
        if c == 0 {
            missing += 1;
            if client_saw.get(&op) == Some(&"success") {
                success_without_effect += 1;
            }
        }
    }
    let mut lat = st.lat_us.clone();
    lat.sort();
    let summary = json!({
        "mode": format!("{:?}", cfg.mode),
        "calls": cfg.calls,
        "seed": cfg.seed,
        "lost_pct": cfg.lost_pct, "crash_pct": cfg.crash_pct, "kill_pct": cfg.kill_pct,
        "attempts": st.attempts,
        "lost_injected": st.lost_injected,
        "kills_injected": st.kills_injected,
        "restarts": st.restarts,
        "client_success": st.success,
        "client_unknown": st.unknown,
        "client_gave_up": st.gave_up,
        "ledger_effects": ledger.lines().count(),
        "duplicate_effects": dup_effects,
        "ops_with_duplicates": ops_dup,
        "ops_without_effect": missing,
        "success_without_effect": success_without_effect,
        "server_dedup_replies": st.replayed,
        "lat_first_attempt_n": lat.len(),
        "lat_p50_us": pct(&lat, 0.50),
        "lat_p99_us": pct(&lat, 0.99),
        "lat_p999_us": pct(&lat, 0.999),
        "elapsed_s": (elapsed * 1000.0).round() / 1000.0,
    });
    println!("{summary}");
}
