import base64, ujson, requests
from kafka import KafkaConsumer
import time

BOOTSTRAP = "kafka:9092"
INPUT_TOPICS = "trending.windows.1m,trending.windows.5m".split(",")
HBASE_REST = "http://hbase:8085"
TABLE = "rc_clean_minute"
COLUMN_FAMILY = "m"

def b64(s):
    if isinstance(s, str):
        s = s.encode("utf-8")
    return base64.b64encode(s).decode("ascii")

def rowkey(d):
    ts = d.get("ts_bucket")
    minute = ts[:16].replace("-","").replace(":","").replace("T","")
    return f"{d.get('wiki')}|{d.get('title')}|{minute}"

def to_cells(d):
    m = {
      "edit_count": d.get("edit_count",0),
      "unique_editors": d.get("unique_editors",0),
      "sum_abs_delta": d.get("sum_abs_delta",0),
      "revert_count": d.get("revert_count",0),
      "minor_share": d.get("minor_share",0.0),
      "bot_share": d.get("bot_share",0.0),
      "score": d.get("score",0.0),
      "window_size": d.get("window_size","1m")
    }
    cells = []
    for k,v in m.items():
        cells.append({"column": b64(f"{COLUMN_FAMILY}:{k}"), "$": b64(str(v))})
    return cells

def ensure_table(session):
    """Check if table exists, create if missing"""
    schema_url = f"{HBASE_REST}/{TABLE}/schema"
    r = session.get(schema_url)
    if r.status_code == 404:
        print(f"Table {TABLE} not found, creating...")
        payload = {
            "name": TABLE,
            "ColumnSchema": [{"name": COLUMN_FAMILY}]
        }
        r2 = session.put(schema_url, json=payload)
        if r2.status_code >= 300:
            raise RuntimeError(f"Failed to create table {TABLE}: {r2.status_code} {r2.text}")
        print(f"Table {TABLE} created")
    else:
        print(f"Table {TABLE} already exists")

def main():
    session = requests.Session()

    # Wait for HBase REST API to be ready
    ready = False
    for _ in range(30):
        try:
            r = session.get(HBASE_REST)
            if r.status_code == 200:
                ready = True
                break
        except requests.exceptions.RequestException:
            pass
        time.sleep(2)
    if not ready:
        raise RuntimeError("HBase REST API not reachable")

    ensure_table(session)

    consumer = KafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: ujson.loads(v.decode("utf-8")),
    )

    for msg in consumer:
        d = msg.value
        rk = rowkey(d)
        payload = {"Row":[{"key": b64(rk), "Cell": to_cells(d)}]}
        # Stargate bulk PUT
        r = session.put(f"{HBASE_REST}/{TABLE}/fakerow", json=payload, timeout=5)
        if r.status_code >= 300:
            print("HBase write error", r.status_code, r.text)

if __name__ == "__main__":
    main()
