//! Diario append-only: una línea por evento, `crc32hex json\n`.
//! La recuperación se detiene en la primera línea rota (cola desgarrada).

use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    /// Se registró la intención y la call pudo haber llegado al servidor.
    Intent { args_hash: String },
    /// Resultado JSON-RPC guardado (campo `result` o `error`).
    Done { args_hash: String, response: Value },
}

pub struct Journal {
    file: File,
    pub map: HashMap<String, Entry>,
    pub recovered_lines: usize,
    pub torn_tail: bool,
}

fn frame(v: &Value) -> Vec<u8> {
    let s = v.to_string();
    format!("{:08x} {}\n", crc32fast::hash(s.as_bytes()), s).into_bytes()
}

impl Journal {
    pub fn open(path: &Path) -> std::io::Result<Journal> {
        let mut map = HashMap::new();
        let mut recovered_lines = 0;
        let mut torn_tail = false;
        if path.exists() {
            let r = BufReader::new(File::open(path)?);
            for line in r.split(b'\n') {
                let line = line?;
                let ok = (|| {
                    let s = std::str::from_utf8(&line).ok()?;
                    let (crc, body) = s.split_once(' ')?;
                    if u32::from_str_radix(crc, 16).ok()? != crc32fast::hash(body.as_bytes()) {
                        return None;
                    }
                    serde_json::from_str::<Value>(body).ok()
                })();
                let Some(v) = ok else {
                    torn_tail = true;
                    break;
                };
                recovered_lines += 1;
                let k = v["k"].as_str().unwrap_or_default().to_string();
                let h = v["h"].as_str().unwrap_or_default().to_string();
                match v["t"].as_str() {
                    Some("intent") => {
                        map.entry(k).or_insert(Entry::Intent { args_hash: h });
                    }
                    Some("done") => {
                        map.insert(
                            k,
                            Entry::Done {
                                args_hash: h,
                                response: v["r"].clone(),
                            },
                        );
                    }
                    _ => {}
                }
            }
            if torn_tail {
                // Se trunca la cola rota para que los appends nuevos queden legibles.
                let good: u64 = {
                    let data = std::fs::read(path)?;
                    let mut n = 0u64;
                    for (i, l) in data.split(|b| *b == b'\n').enumerate() {
                        if i >= recovered_lines {
                            break;
                        }
                        n += l.len() as u64 + 1;
                    }
                    n
                };
                OpenOptions::new().write(true).open(path)?.set_len(good)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Journal {
            file,
            map,
            recovered_lines,
            torn_tail,
        })
    }

    /// Registra la intención y la vuelve durable (fdatasync) antes de
    /// reenviar la call. El mismo fdatasync cubre los `done` anteriores.
    pub fn intent(&mut self, key: &str, args_hash: &str) -> std::io::Result<()> {
        self.file
            .write_all(&frame(&json!({"t":"intent","k":key,"h":args_hash})))?;
        self.sync()?;
        self.map.insert(
            key.to_string(),
            Entry::Intent {
                args_hash: args_hash.to_string(),
            },
        );
        Ok(())
    }

    /// Guarda el resultado. Sin fdatasync propio: si se pierde en un corte,
    /// la clave queda como `Intent` y el reintento recibe "resultado
    /// desconocido", nunca una segunda ejecución.
    pub fn done(&mut self, key: &str, args_hash: &str, response: &Value) -> std::io::Result<()> {
        self.file.write_all(&frame(
            &json!({"t":"done","k":key,"h":args_hash,"r":response}),
        ))?;
        self.map.insert(
            key.to_string(),
            Entry::Done {
                args_hash: args_hash.to_string(),
                response: response.clone(),
            },
        );
        Ok(())
    }

    pub fn sync(&self) -> std::io::Result<()> {
        if unsafe { libc::fdatasync(self.file.as_raw_fd()) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}
