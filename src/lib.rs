//! mcp-idempotency-journal: proxy stdio para MCP que da semántica "a lo
//! sumo una vez" a los `tools/call` que traen una clave de idempotencia, y
//! "exactamente una vez" si el servidor también deduplica por clave.

pub mod journal;

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Código JSON-RPC para "la call pudo haberse ejecutado; resultado desconocido".
pub const OUTCOME_UNKNOWN: i64 = -32001;
/// Código JSON-RPC para "misma clave con argumentos distintos".
pub const KEY_MISMATCH: i64 = -32002;

/// JSON canónico: claves de objetos ordenadas, sin espacios.
pub fn canonical(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys
                .iter()
                .map(|k| format!("{}:{}", Value::String((*k).clone()), canonical(&m[*k])))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(canonical).collect::<Vec<_>>().join(",")
        ),
        other => other.to_string(),
    }
}

pub fn sha256_hex(s: &str) -> String {
    Sha256::digest(s.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Clave y hash de argumentos de un `tools/call`. Devuelve `None` si la
/// call no trae `_meta.idempotencyKey` (el proxy no inventa claves: dos
/// calls idénticas sin clave pueden ser repeticiones intencionales).
pub fn call_key(session: &str, msg: &Value) -> Option<(String, String)> {
    if msg["method"] != "tools/call" {
        return None;
    }
    let p = &msg["params"];
    let nonce = p["_meta"]["idempotencyKey"].as_str()?;
    let tool = p["name"].as_str().unwrap_or_default();
    let args_hash = sha256_hex(&canonical(&p["arguments"]));
    let key = sha256_hex(&format!("{session}\u{0}{tool}\u{0}{nonce}"));
    Some((key, args_hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_ignores_key_order() {
        assert_eq!(
            canonical(&json!({"b":1,"a":{"y":2,"x":[1,{"d":0,"c":1}]}})),
            canonical(&json!({"a":{"x":[1,{"c":1,"d":0}],"y":2},"b":1}))
        );
    }

    #[test]
    fn key_requires_nonce_and_depends_on_tool() {
        let a = json!({"method":"tools/call","params":{"name":"t","arguments":{"x":1},"_meta":{"idempotencyKey":"n1"}}});
        let b = json!({"method":"tools/call","params":{"name":"u","arguments":{"x":1},"_meta":{"idempotencyKey":"n1"}}});
        let none = json!({"method":"tools/call","params":{"name":"t","arguments":{"x":1}}});
        assert_ne!(call_key("s", &a).unwrap().0, call_key("s", &b).unwrap().0);
        assert!(call_key("s", &none).is_none());
        assert_ne!(call_key("s", &a).unwrap().0, call_key("s2", &a).unwrap().0);
    }
}
