#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, pathlib, requests, pandas as pd
import plotly.express as px, plotly.io as pio
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

# --- Environment ---
APP   = os.environ["TTN_APP_ID"]            # z.B. gisma-hydro-testbed
REG   = os.environ["TTN_REGION"]            # z.B. eu1
KEY   = os.environ["TTN_API_KEY"]           # Secret (NNSXS....)
DEVS  = [d for d in os.environ["DEVICES"].split() if d.strip()]  # whitespace-getrennt

DOCS  = pathlib.Path("docs"); DOCS.mkdir(exist_ok=True, parents=True)
DATA  = pathlib.Path("data"); DATA.mkdir(exist_ok=True, parents=True)

# Sandbox: 24h Retention → 1 Tag rückwärts. In Cloud ggf. größer wählen.
AFTER = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
HDRS  = {"Authorization": f"Bearer {KEY}", "Accept": "text/event-stream"}

# --- Helpers -----------------------------------------------------------------

def device_pull(dev: str) -> pd.DataFrame:
    """
    Holt NDJSON nur für dieses Device (Storage-HTTP), merged mit bestehendem Parquet
    und liefert DataFrame mit Spalten: received_at, device_id, f_port, rssi, snr, payload_json
    """
    parq = DATA / f"{dev}.parquet"
    base = f"https://{REG}.cloud.thethings.network/api/v3/as/applications/{APP}/devices/{quote(dev)}/packages/storage/uplink_message"
    r = requests.get(base, headers=HDRS, params={"limit": "1000", "after": AFTER}, timeout=30)
    r.raise_for_status()
    lines = [json.loads(x) for x in r.text.splitlines() if x.strip()]

    rows = []
    for o in lines:
        up  = o.get("uplink_message", {})
        rx0 = (up.get("rx_metadata") or [{}])[0]
        rows.append({
            "received_at": o.get("received_at"),
            "device_id":   dev,
            "f_port":      up.get("f_port"),
            "rssi":        rx0.get("rssi"),
            "snr":         rx0.get("snr"),
            # Dict → JSON-Text (Parquet kann keine leeren structs)
            "payload_json": json.dumps(up.get("decoded_payload", {}), ensure_ascii=False)
        })
    df_new = pd.DataFrame(rows)

    # Historie mergen
    if parq.exists():
        try:
            df_old = pd.read_parquet(parq)
            df = pd.concat([df_old, df_new], ignore_index=True)
            df = df.drop_duplicates(subset=["received_at"])  # reicht, device_id ist fix
        except Exception:
            df = df_new
    else:
        df = df_new

    if not df.empty:
        df["received_at"] = pd.to_datetime(df["received_at"], utc=True, errors="coerce")
        df = df.sort_values("received_at")
        df.to_parquet(parq, index=False)

    return df


def unpack_payload(d: dict) -> dict:
    """Hier deine Decoder-Keys anpassen."""
    if not isinstance(d, dict):
        return {}
    return {
        "pm25": d.get("pm25"),
        "co2": d.get("co2"),
        "temperature": d.get("temperature") or d.get("temp") or d.get("t"),
        "humidity": d.get("humidity") or d.get("rh"),
        "pressure": d.get("pressure") or d.get("p"),
        "battery": d.get("battery") or d.get("vbat"),
        "distance_cm": d.get("distance_cm") or d.get("distance"),
        "water_cm": d.get("water_cm") or d.get("waterlevel_cm") or d.get("wl_cm"),
        "no2": d.get("no2"),
        "o3": d.get("o3"),
    }


def make_line(df: pd.DataFrame, col: str, title: str) -> str | None:
    if col not in df.columns or not df[col].notna().any():
        return None
    d = df[["received_at", col]].dropna()
    if d.empty:
        return None
    f = px.line(d, x="received_at", y=col, title=title)
    f.update_layout(margin=dict(l=10, r=10, t=40, b=10))
    return pio.to_html(f, include_plotlyjs="cdn", full_html=False,
                       default_width="100%", default_height="420px")


# --- Main --------------------------------------------------------------------

overview_rows = []
plot_cards = []

for dev in DEVS:
    df = device_pull(dev)

    if df.empty:
        overview_rows.append({"device_id": dev, "records": 0, "last_seen_utc": None})
        continue

    # payload_json -> payload (Dict) -> Spalten
    if "payload_json" in df.columns:
        df["payload"] = df["payload_json"].apply(lambda s: json.loads(s) if isinstance(s, str) and s else {})
    else:
        df["payload"] = [{}] * len(df)

    pl = df["payload"].apply(unpack_payload).apply(pd.Series)
    df = pd.concat([df.drop(columns=["payload"]), pl], axis=1)

    last_ts = df["received_at"].max()
    overview_rows.append({"device_id": dev, "records": len(df), "last_seen_utc": last_ts})

    # Plots pro Device (nur vorhandene Spalten)
    labels = {
        "pm25": "PM2.5 (µg/m³)",
        "co2": "CO₂ (ppm)",
        "temperature": "Temperatur (°C)",
        "humidity": "Relative Feuchte (%)",
        "pressure": "Luftdruck (hPa)",
        "battery": "Batterie/Spannung",
        "distance_cm": "Distanz (cm)",
        "water_cm": "Wasserstand (cm)",
    }
    dev_parts = []
    for col, title in labels.items():
        html_plot = make_line(df, col, f"{title} – {dev}")
        if html_plot:
            dev_parts.append(html_plot)

    if dev_parts:
        plot_cards.append(f'<div class="card"><h3>{dev}</h3>{"".join(dev_parts)}</div>')


# Übersicht
ov = pd.DataFrame(overview_rows)
if not ov.empty:
    ov["last_seen_utc"] = ov["last_seen_utc"].astype("string")
overview_html = (ov.sort_values("device_id").to_html(index=False)
                 if not ov.empty else "<p>Keine Geräte-/Records gefunden.</p>")

# HTML ausgeben
stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
html = f"""<!doctype html>
<html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>TTN – Alle Devices</title>
<style>
body{{font-family:system-ui,Segoe UI,Roboto,Ubuntu,Arial,sans-serif;margin:20px;}}
h1{{margin:0 0 .25rem}} small{{color:#555}}
.card{{background:#fff;border:1px solid #eee;border-radius:12px;box-shadow:0 1px 6px rgba(0,0,0,.04);padding:16px;margin:16px 0}}
table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #ddd;padding:6px 8px;text-align:left}}
.grid{{display:grid;grid-template-columns:1fr;gap:16px}}
@media (min-width:1000px){{.grid{{grid-template-columns:1fr;}}}}
</style>
</head><body>
<h1>TTN Dashboard – Alle Geräte</h1>
<small>Stand: {stamp} • Quelle: TTN Storage ({APP}@{REG}) • Fenster: letzte 24 h</small>

<div class="card">
  <h2>Übersicht</h2>
  {overview_html}
  <p style="margin-top:8px">Parquet pro Gerät: <code>data/&lt;device&gt;.parquet</code></p>
</div>

<div class="grid">
  {"".join(plot_cards) if plot_cards else '<div class="card">Keine plottbaren Felder gefunden (Decoder-Keys in Script anpassen).</div>'}
</div>

</body></html>"""
(DOCS / "data.html").write_text(html, encoding="utf-8")
