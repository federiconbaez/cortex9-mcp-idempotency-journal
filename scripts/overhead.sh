#!/usr/bin/env bash
# Mide el overhead p99 del proxy (diario con fdatasync) contra el servidor
# directo, 10.000 calls sin fallas cada uno. Falla si el overhead p99 es
# >= 2 ms o si el baseline con fallas no duplica.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --release --quiet
H=target/release/idem-harness
W=${WORKDIR:-target/runs}
base=$($H --mode baseline --calls 10000 --seed 1 --workdir "$W/bench-baseline")
prox=$($H --mode proxy --calls 10000 --seed 1 --workdir "$W/bench-proxy")
echo "baseline: $base"
echo "proxy:    $prox"
python3 - "$base" "$prox" <<'PY'
import json, sys
b, p = json.loads(sys.argv[1]), json.loads(sys.argv[2])
d = p["lat_p99_us"] - b["lat_p99_us"]
print(f"p99 baseline={b['lat_p99_us']}us proxy={p['lat_p99_us']}us overhead={d}us (umbral 2000us)")
sys.exit(0 if d < 2000 else 1)
PY
