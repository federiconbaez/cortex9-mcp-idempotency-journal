# mcp-idempotency-journal

[Español](#español) · [English](#english)

## Español

Proxy stdio para MCP que pone un **diario durable de idempotencia** delante
de un servidor arbitrario. Un `tools/call` que trae
`params._meta.idempotencyKey` se registra (intención + `fdatasync`) antes de
reenviarse; el resultado se guarda al volver. Un reintento con la misma
clave y los mismos argumentos recibe el resultado guardado con el id
JSON-RPC nuevo, sin volver a ejecutar la tool.

Motivación: modelcontextprotocol/modelcontextprotocol#3394 (leída para este
repo). La spec 2025-11-25 no define semántica de reintento para
`tools/call`, el reintento lleva un id nuevo y un servidor no puede
reconocerlo como duplicado.

### Garantías (y lo que NO garantiza)

- **A lo sumo una vez** por clave, con cualquier servidor. Si el proxy o el
  servidor mueren entre el reenvío y la respuesta, la clave queda con
  intención y sin resultado: el reintento recibe `-32001` (resultado
  desconocido) y **no** se re-ejecuta. Eso puede dejar un efecto sin
  confirmar al cliente; el proxy no lo puede resolver solo.
- **Exactamente una vez** solo si el servidor también deduplica por la
  misma clave (`--server-dedups`, servidor cooperativo). Sin cooperación de
  la tool no hay exactamente-una-vez de punta a punta.
- Misma clave con otros argumentos: `-32002`, nunca un replay.
- Sin clave, el proxy no inventa una: dos calls idénticas sin clave pueden
  ser repeticiones intencionales y se reenvían las dos.
- La clave se acota por `--session` y por nombre de tool. No hay
  retención/poda, ni ligadura a identidad de autorización, ni transporte
  HTTP: solo stdio.
- Los cortes se simulan con SIGKILL al grupo de procesos; no hay prueba de
  corte de luz real (el `fdatasync` está, pero no se verificó con
  dm-flakey ni similar).

### Mediciones (box de desarrollo, ext4 sobre virtio)

Reproducción de #3394 (`tests/proxy.rs`): un efecto commiteado, respuesta
perdida, un reintento con id nuevo. Sin proxy: **2 efectos**. Con proxy:
**1 efecto** y el reintento recibe el mismo resultado.

10.000 calls mutantes con reintento, semilla 1, 10 % respuestas perdidas,
3 % crash del servidor después del commit, 1 % SIGKILL del grupo:

```
target/release/idem-harness --mode <modo> --calls 10000 --seed 1 \
  --lost-pct 10 --crash-pct 3 --kill-pct 1 --workdir target/runs/x
```

| Modo | Efectos duplicados | Ops con duplicado | Cliente: éxito / desconocido |
|---|---|---|---|
| baseline (sin proxy) | **1340** | 1277 | 10000 / 0 |
| proxy | **0** | 0 | 9592 / 408 |
| proxy + servidor cooperativo | **0** | 0 | 10000 / 0 |

En modo proxy, 408 calls terminaron con resultado desconocido (el efecto sí
ocurrió una vez en el ledger): es el costo honesto de "a lo sumo una vez".

Overhead (10.000 calls sin fallas, `./scripts/overhead.sh`): p99 directo
41 µs, p99 con proxy 969 µs, **overhead p99 = 0,93 ms**. En 7 corridas en
la box el overhead p99 estuvo entre 0,93 y 1,66 ms. Lo domina el
`fdatasync` de la intención; en otro disco puede superar 2 ms.

## English

A stdio MCP proxy that puts a **durable idempotency journal** in front of an
arbitrary server. A `tools/call` carrying `params._meta.idempotencyKey` is
journaled (intent + `fdatasync`) before forwarding; the result is stored on
return. A retry with the same key and arguments gets the stored result under
the new JSON-RPC id without re-executing the tool.

Guarantees: **at most once** per key with any server (a crash between
forward and response yields `-32001` "outcome unknown", never a second
execution); **exactly once** only when the server also deduplicates on the
same key (`--server-dedups`). Key reuse with different arguments returns
`-32002`. Calls without a key pass through. No retention policy, no
auth-context binding, no HTTP transport; crashes are simulated with SIGKILL,
not power loss.

Measured on the dev box: issue #3394 reproduced (2 effects without the proxy,
1 with it). 10,000 mutating calls with injected faults: baseline 1,340
duplicate effects, proxy 0 (408 outcome-unknown replies), cooperative server
0 with 10,000 successes. p99 overhead 0.93 ms (0.93-1.66 ms over 7 runs),
dominated by `fdatasync`.

## Licencia / License

Apache-2.0.
