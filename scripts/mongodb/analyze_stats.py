import json
import sys
from pathlib import Path

if len(sys.argv) != 3:
    raise SystemExit(
        "Usage: python analyze_stats.py before.json after.json"
    )

before = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
after = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))

fields = {
    "update_delta": ("operations", "update"),
    "query_delta": ("operations", "query"),
    "connections_created_delta": ("connections", "totalCreated"),
    "connections_rejected_delta": ("connections", "rejected"),
    "cache_bytes_delta": ("wiredTigerCache", "currentBytes"),
    "dirty_bytes_delta": ("wiredTigerCache", "dirtyBytes"),
}

result = {
    "before_timestamp": before["timestamp"],
    "after_timestamp": after["timestamp"],
}

for output_key, (section, key) in fields.items():
    result[output_key] = after[section][key] - before[section][key]

print(json.dumps(result, ensure_ascii=False, indent=2))
