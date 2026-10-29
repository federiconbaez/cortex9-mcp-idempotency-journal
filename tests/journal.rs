use mcp_idempotency_journal::journal::{Entry, Journal};
use serde_json::json;
use std::io::Write;

#[test]
fn intent_and_done_survive_reopen() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("j.log");
    {
        let mut j = Journal::open(&p).unwrap();
        j.intent("k1", "h1").unwrap();
        j.done("k1", "h1", &json!({"jsonrpc":"2.0","result":{"x":1}}))
            .unwrap();
        j.intent("k2", "h2").unwrap();
    }
    let j = Journal::open(&p).unwrap();
    assert_eq!(j.recovered_lines, 3);
    assert!(!j.torn_tail);
    assert!(matches!(&j.map["k1"], Entry::Done { response, .. } if response["result"]["x"] == 1));
    assert_eq!(
        j.map["k2"],
        Entry::Intent {
            args_hash: "h2".into()
        }
    );
}

#[test]
fn torn_tail_is_dropped_and_truncated() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("j.log");
    {
        let mut j = Journal::open(&p).unwrap();
        j.intent("k1", "h1").unwrap();
    }
    // Cola desgarrada: media línea sin crc válido.
    std::fs::OpenOptions::new()
        .append(true)
        .open(&p)
        .unwrap()
        .write_all(b"deadbeef {\"t\":\"done\",\"k\":\"k1\"")
        .unwrap();
    {
        let mut j = Journal::open(&p).unwrap();
        assert!(j.torn_tail);
        assert_eq!(j.recovered_lines, 1);
        assert!(matches!(j.map["k1"], Entry::Intent { .. }));
        j.intent("k3", "h3").unwrap();
    }
    let j = Journal::open(&p).unwrap();
    assert!(
        !j.torn_tail,
        "los appends después de truncar quedan legibles"
    );
    assert_eq!(j.recovered_lines, 2);
}

#[test]
fn corrupted_crc_stops_recovery() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("j.log");
    {
        let mut j = Journal::open(&p).unwrap();
        j.intent("k1", "h1").unwrap();
        j.intent("k2", "h2").unwrap();
    }
    let mut data = std::fs::read(&p).unwrap();
    data[12] ^= 0xff; // byte dentro del JSON de la primera línea
    std::fs::write(&p, data).unwrap();
    let j = Journal::open(&p).unwrap();
    assert!(j.torn_tail);
    assert_eq!(j.recovered_lines, 0);
}
