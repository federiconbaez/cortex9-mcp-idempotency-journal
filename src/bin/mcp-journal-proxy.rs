//! Proxy stdio MCP con diario de idempotencia.
//!
//! uso: mcp-journal-proxy --journal <archivo> [--session <id>] [--server-dedups] -- <servidor> [args...]
//!
//! - tools/call con `_meta.idempotencyKey`: si la clave tiene resultado
//!   guardado, se responde con ese resultado y no se reenvía; si solo tiene
//!   intención (el proxy o el servidor murieron en el medio), se responde
//!   OUTCOME_UNKNOWN, salvo con `--server-dedups`, donde se reenvía porque
//!   el servidor deduplica por la misma clave.
//! - Si el servidor muere, se relanza y se le repite el `initialize` del
//!   cliente; las calls en vuelo reciben OUTCOME_UNKNOWN.

use mcp_idempotency_journal::journal::{Entry, Journal};
use mcp_idempotency_journal::{call_key, KEY_MISMATCH, OUTCOME_UNKNOWN};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Sender};

enum Ev {
    Client(String),
    ClientEof,
    Server(u64, String),
    ServerEof(u64),
}

struct Server {
    child: Child,
    stdin: ChildStdin,
    gen: u64,
}

fn spawn(cmd: &[String], gen: u64, tx: &Sender<Ev>) -> Server {
    let mut child = Command::new(&cmd[0])
        .args(&cmd[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("no pude lanzar el servidor");
    let stdin = child.stdin.take().unwrap();
    let out = child.stdout.take().unwrap();
    let tx = tx.clone();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines() {
            match line {
                Ok(l) => {
                    if tx.send(Ev::Server(gen, l)).is_err() {
                        return;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = tx.send(Ev::ServerEof(gen));
    });
    Server { child, stdin, gen }
}

fn send(w: &mut impl Write, v: &Value) {
    let mut s = v.to_string();
    s.push('\n');
    let _ = w.write_all(s.as_bytes());
    let _ = w.flush();
}

struct InFlight {
    client_id: Value,
    key: String,
    args_hash: String,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut journal_path = None;
    let mut session = "default".to_string();
    let mut server_dedups = false;
    let mut i = 1;
    let mut cmd = vec![];
    while i < args.len() {
        match args[i].as_str() {
            "--journal" => {
                journal_path = Some(args[i + 1].clone());
                i += 1;
            }
            "--session" => {
                session = args[i + 1].clone();
                i += 1;
            }
            "--server-dedups" => server_dedups = true,
            "--" => {
                cmd = args[i + 1..].to_vec();
                break;
            }
            other => panic!("argumento desconocido {other}"),
        }
        i += 1;
    }
    assert!(
        !cmd.is_empty(),
        "falta el comando del servidor después de --"
    );
    let mut journal = Journal::open(std::path::Path::new(
        &journal_path.expect("falta --journal"),
    ))
    .expect("no pude abrir el diario");

    let (tx, rx) = channel();
    {
        let tx = tx.clone();
        std::thread::spawn(move || {
            for line in std::io::stdin().lock().lines() {
                match line {
                    Ok(l) => {
                        if tx.send(Ev::Client(l)).is_err() {
                            return;
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = tx.send(Ev::ClientEof);
        });
    }
    let mut out = std::io::stdout().lock();
    let mut gen = 1;
    let mut server = spawn(&cmd, gen, &tx);
    let mut init_msgs: Vec<Value> = vec![];
    let mut swallow: Vec<Value> = vec![];
    let mut inflight: HashMap<String, InFlight> = HashMap::new();
    let mut waiters: HashMap<String, Vec<Value>> = HashMap::new();
    let mut next = 0u64;

    let reply_err = |out: &mut std::io::StdoutLock, id: &Value, code: i64, msg: &str, key: &str| {
        send(
            out,
            &json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":msg,"data":{"idempotencyKey":key}}}),
        );
    };

    while let Ok(ev) = rx.recv() {
        match ev {
            Ev::ClientEof => break,
            Ev::Client(line) => {
                let Ok(msg) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                let m = msg["method"].as_str().unwrap_or_default();
                if m == "initialize" || m == "notifications/initialized" {
                    init_msgs.push(msg.clone());
                }
                let Some((key, h)) = call_key(&session, &msg) else {
                    send(&mut server.stdin, &msg);
                    continue;
                };
                let id = msg["id"].clone();
                let forward = match journal.map.get(&key).cloned() {
                    Some(Entry::Done {
                        args_hash,
                        response,
                    }) => {
                        if args_hash != h {
                            reply_err(
                                &mut out,
                                &id,
                                KEY_MISMATCH,
                                "idempotencyKey reutilizada con otros argumentos",
                                &key,
                            );
                        } else {
                            let mut r = response.clone();
                            r["id"] = id;
                            send(&mut out, &r);
                        }
                        false
                    }
                    Some(Entry::Intent { args_hash }) => {
                        if args_hash != h {
                            reply_err(
                                &mut out,
                                &id,
                                KEY_MISMATCH,
                                "idempotencyKey reutilizada con otros argumentos",
                                &key,
                            );
                            false
                        } else if inflight.values().any(|f| f.key == key) {
                            // Duplicado concurrente: espera el mismo resultado.
                            waiters.entry(key.clone()).or_default().push(id);
                            false
                        } else if server_dedups {
                            true
                        } else {
                            reply_err(
                                &mut out,
                                &id,
                                OUTCOME_UNKNOWN,
                                "la call pudo haberse ejecutado; resultado desconocido",
                                &key,
                            );
                            false
                        }
                    }
                    None => {
                        journal.intent(&key, &h).expect("fdatasync del diario");
                        true
                    }
                };
                if forward {
                    next += 1;
                    let pid = format!("jp-{next}");
                    let mut fwd = msg.clone();
                    fwd["id"] = Value::String(pid.clone());
                    inflight.insert(
                        pid,
                        InFlight {
                            client_id: msg["id"].clone(),
                            key,
                            args_hash: h,
                        },
                    );
                    send(&mut server.stdin, &fwd);
                }
            }
            Ev::Server(g, line) => {
                if g != server.gen {
                    continue;
                }
                let Ok(mut msg) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if let Some(pos) = swallow.iter().position(|s| *s == msg["id"]) {
                    swallow.remove(pos);
                    continue;
                }
                let pid = msg["id"].as_str().unwrap_or_default().to_string();
                if let Some(f) = inflight.remove(&pid) {
                    let mut stored = msg.clone();
                    stored.as_object_mut().unwrap().remove("id");
                    journal
                        .done(&f.key, &f.args_hash, &stored)
                        .expect("escritura del diario");
                    msg["id"] = f.client_id;
                    send(&mut out, &msg);
                    for w in waiters.remove(&f.key).unwrap_or_default() {
                        let mut r = stored.clone();
                        r["id"] = w;
                        send(&mut out, &r);
                    }
                } else {
                    send(&mut out, &msg);
                }
            }
            Ev::ServerEof(g) => {
                if g != server.gen {
                    continue;
                }
                let _ = server.child.wait();
                for (_, f) in inflight.drain() {
                    reply_err(
                        &mut out,
                        &f.client_id,
                        OUTCOME_UNKNOWN,
                        "el servidor murió con la call en vuelo",
                        &f.key,
                    );
                    for w in waiters.remove(&f.key).unwrap_or_default() {
                        reply_err(
                            &mut out,
                            &w,
                            OUTCOME_UNKNOWN,
                            "el servidor murió con la call en vuelo",
                            &f.key,
                        );
                    }
                }
                gen += 1;
                server = spawn(&cmd, gen, &tx);
                for (n, m) in init_msgs.iter().enumerate() {
                    let mut m = m.clone();
                    if m.get("id").is_some() {
                        let sid = json!(format!("jp-init-{gen}-{n}"));
                        m["id"] = sid.clone();
                        swallow.push(sid);
                    }
                    send(&mut server.stdin, &m);
                }
            }
        }
    }
    drop(server.stdin);
    let _ = server.child.wait();
}
