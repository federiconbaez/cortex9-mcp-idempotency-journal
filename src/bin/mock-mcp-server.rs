//! Servidor MCP de prueba con una tool mutante `transfer` que agrega una
//! fila al ledger por cada ejecución (no idempotente a propósito, como en
//! la reproducción de la issue #3394).
//!
//! Variables de entorno:
//! - MOCK_LEDGER: archivo ledger (obligatorio).
//! - MOCK_CRASH_PCT, MOCK_SEED: con esa probabilidad (determinista por
//!   hash de semilla e id JSON-RPC) el proceso muere DESPUÉS de commitear
//!   el efecto y ANTES de responder.
//! - MOCK_HONOR_KEY=1: deduplica por `_meta.idempotencyKey` guardando la
//!   clave en la misma fila del ledger (servidor cooperativo).

use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, Write};

fn hash01(seed: u64, s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325 ^ seed;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h % 100
}

fn main() {
    let ledger_path = std::env::var("MOCK_LEDGER").expect("MOCK_LEDGER");
    let crash_pct: u64 = std::env::var("MOCK_CRASH_PCT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let seed: u64 = std::env::var("MOCK_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let honor = std::env::var("MOCK_HONOR_KEY").is_ok_and(|v| v == "1");
    let mut seen: HashMap<String, String> = HashMap::new();
    if honor {
        if let Ok(s) = std::fs::read_to_string(&ledger_path) {
            for l in s.lines() {
                let mut it = l.split(' ');
                if let (Some(op), Some(k)) = (it.next(), it.next()) {
                    if k != "-" {
                        seen.insert(k.to_string(), op.to_string());
                    }
                }
            }
        }
    }
    let mut ledger = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ledger_path)
        .unwrap();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let id = msg["id"].clone();
        let resp = match msg["method"].as_str().unwrap_or_default() {
            "initialize" => json!({"jsonrpc":"2.0","id":id,"result":{
                "protocolVersion":"2025-11-25",
                "capabilities":{"tools":{}},
                "serverInfo":{"name":"mock-mcp-server","version":"0.1.0"}}}),
            "tools/list" => json!({"jsonrpc":"2.0","id":id,"result":{"tools":[{
                "name":"transfer",
                "description":"Mueve fondos (efecto externo no idempotente)",
                "inputSchema":{"type":"object","properties":{"op_id":{"type":"string"},"amount":{"type":"integer"}}},
                "annotations":{"readOnlyHint":false,"idempotentHint":false}}]}}),
            "tools/call" => {
                let op = msg["params"]["arguments"]["op_id"]
                    .as_str()
                    .unwrap_or("?")
                    .to_string();
                let key = msg["params"]["_meta"]["idempotencyKey"]
                    .as_str()
                    .map(String::from);
                let deduped = honor && key.as_ref().is_some_and(|k| seen.contains_key(k));
                if !deduped {
                    let k = key.clone().unwrap_or_else(|| "-".into());
                    ledger.write_all(format!("{op} {k}\n").as_bytes()).unwrap();
                    if honor {
                        if let Some(k) = key {
                            seen.insert(k, op.clone());
                        }
                    }
                    if crash_pct > 0 && hash01(seed, &id.to_string()) < crash_pct {
                        // Efecto commiteado, respuesta nunca enviada.
                        std::process::exit(137);
                    }
                }
                let text = if deduped {
                    format!("deduped {op}")
                } else {
                    format!("ok {op}")
                };
                json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":text}],"isError":false}})
            }
            _ if msg.get("id").is_none() => continue,
            _ => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"método desconocido"}})
            }
        };
        let mut s = resp.to_string();
        s.push('\n');
        if out.write_all(s.as_bytes()).is_err() || out.flush().is_err() {
            break;
        }
    }
}
