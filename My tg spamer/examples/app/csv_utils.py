import csv
import io
import json

def parse_csv(data: bytes):
    decoded = data.decode("utf-8")
    reader = csv.DictReader(io.StringIO(decoded))
    pairs = []
    cols = reader.fieldnames or []
    for row in reader:
        target = row.get("username") or row.get("phone") or row.get("chat_link") or ""
        ctx = {k: v for k, v in row.items() if k not in ("username", "phone", "chat_link")}
        pairs.append((target, ctx))
    return pairs, cols