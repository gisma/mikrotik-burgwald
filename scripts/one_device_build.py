import os, json, pathlib, requests, pandas as pd
import plotly.express as px, plotly.io as pio
from datetime import datetime, timezone

APP   = os.environ["TTN_APP_ID"]
REG   = os.environ["TTN_REGION"]
KEY   = os.environ["TTN_API_KEY"]
DEV   = os.environ["DEVICE_ID"]

DOCS  = pathlib.Path("docs"); DOCS.mkdir(exist_ok=True, parents=True)
DATA  = pathlib.Path("data"); DATA.mkdir(exist_ok=True, parents=True)
PARQ  = DATA / f"{DEV}.parquet"

# 1) Pull TTN Storage (NDJSON)
url = f"https://{REG}.cloud.thethings.network/api/v3/as/applications/{APP}/packages/storage/uplink_message?limit=1000"
r = requests.get(url, headers={"Authorization": f"Bearer {KEY}"}, timeout=30)
r.raise_for_status()
lines = [json.loads(x) for x in r.text.splitlines() if x.strip()]

# 2) Nur ein DEVICE_ID filtern
rows=[]
for o in lines:
    if o.get("end_device_ids",{}).get("device_id") != DEV:
        continue
    up = o.get("uplink_message", {})
    rx0 = (up.get("rx_metadata") or [{}])[0]
    rows.append({
        "received_at": o.get("received_at"),
        "device_id":   DEV,
        "f_port":      up.get("f_port"),
        "rssi":        rx0.get("rssi"),
        "snr":         rx0.get("snr"),
        "payload":     up.get("decoded_payload", {})
    })

df_new = pd.DataFrame(rows)

# 3) Historie mergen (Parquet)
if PARQ.exists():
    df_old = pd.read_parquet(PARQ)
    df = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=["received_at"])
else:
    df = df_new.copy()

if df.empty:
    # Minimal leere Seite schreiben
    (DOCS / "index.html").write_text(f"<h1>{DEV}</h1><p>Keine Daten gefunden.</p>", encoding="utf-8")
    raise SystemExit(0)

# 4) Payload-Felder entpacken (→ an deinen Decoder anpassen)
def unpack(d):
    if not isinstance(d, dict): return {}
    return {
        "pm25": d.get("pm25"),
        "co2": d.get("co2"),
        "temperature": d.get("temperature") or d.get("temp") or d.get("t"),
        "humidity": d.get("humidity") or d.get("rh"),
        "battery": d.get("battery") or d.get("vbat"),
        "pressure": d.get("pressure") or d.get("p"),
        "no2": d.get("no2"), "o3": d.get("o3")
    }

df["received_at"] = pd.to_datetime(df["received_at"], utc=True, errors="coerce")
pl = df["payload"].apply(unpack).apply(pd.Series)
df = pd.concat([df.drop(columns=["payload"]), pl], axis=1).sort_values("received_at")
df.to_parquet(PARQ, index=False)

# 5) Kleine Kennzahlen
stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
last_ts = df["received_at"].max()
n_rows  = len(df)

# 6) Einfache Liniendiagramme (nur für vorhandene Spalten)
plots = []
def add_plot(col, title):
    if col in df and df[col].notna().any():
        d = df[["received_at", col]].dropna()
        if not d.empty:
            f = px.line(d, x="received_at", y=col, title=title)
            f.update_layout(margin=dict(l=10,r=10,t=40,b=10))
            plots.append(pio.to_html(f, include_plotlyjs="cdn", full_html=False,
                                     default_width="100%", default_height="420px"))

labels = {
    "pm25": "PM2.5 (µg/m³)",
    "co2": "CO₂ (ppm)",
    "temperature": "Temperatur (°C)",
    "humidity": "Relative Feuchte (%)",
    "pressure": "Luftdruck (hPa)",
    "battery": "Batterie/Spannung",
    "no2": "NO₂ (ppb)",
    "o3": "O₃ (ppb)"
}
for c,t in labels.items():
    add_plot(c, f"{t} – {DEV}")

# 7) HTML ausgeben
html = f"""<!doctype html>
<html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{DEV} – TTN Dashboard</title>
<style>
body{{font-family:system-ui,Segoe UI,Roboto,Ubuntu,Arial,sans-serif;margin:20px;}}
h1{{margin:0 0 .25rem}} small{{color:#555}}
.card{{background:#fff;border:1px solid #eee;border-radius:12px;box-shadow:0 1px 6px rgba(0,0,0,.04);padding:16px;margin:16px 0}}
table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #ddd;padding:6px 8px;text-align:left}}
</style>
</head><body>
<h1>Device: {DEV}</h1>
<small>Stand: {stamp} • Quelle: TTN Storage ({APP}@{REG})</small>

<div class="card">
  <b>Records:</b> {n_rows} &nbsp;|&nbsp;
  <b>Letzter Uplink (UTC):</b> {last_ts}
  <p>Parquet: <a href="../data/{DEV}.parquet">{DEV}.parquet</a></p>
</div>

{"".join(f'<div class="card">{p}</div>' for p in plots)}

</body></html>"""
(DOCS / "index.html").write_text(html, encoding="utf-8")
