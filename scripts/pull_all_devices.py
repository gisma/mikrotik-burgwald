#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, pathlib, requests, pandas as pd
import plotly.express as px, plotly.io as pio
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

# ── Env ────────────────────────────────────────────────────────────────────────
APP   = os.environ["TTN_APP_ID"]            # z.B. gisma-hydro-testbed
REG   = os.environ["TTN_REGION"]            # z.B. eu1
KEY   = os.environ["TTN_API_KEY"]           # Secret (NNSXS....)
DEVS  = [d for d in os.environ["DEVICES"].split() if d.strip()]  # whitespace-getrennt

DOCS  = pathlib.Path("docs"); DOCS.mkdir(exist_ok=True, parents=True)
DATA  = pathlib.Path("data"); DATA.mkdir(exist_ok=True, parents=True)

# Sandbox: 24h Retention → 1 Tag rückwärts. In Cloud ggf. erhöhen.
AFTER = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
HDRS  = {"Authorization": f"Bearer {KEY}", "Accept": "text/event-stream"}

# ── Helpers: Pull + Persist ───────────────────────────────────────────────────
def device_pull(dev: str) -> pd.DataFrame:
    """
    Holt NDJSON nur für dieses Device (Storage-HTTP), merged mit bestehendem Parquet
    und liefert DataFrame mit Spalten:
      received_at, device_id, f_port, rssi, snr, payload_json
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

    # Historie mergen (de-dupe auf received_at)
    if parq.exists():
        try:
            df_old = pd.read_parquet(parq)
            df = pd.concat([df_old, df_new], ignore_index=True)
            df = df.drop_duplicates(subset=["received_at"])
        except Exception:
            df = df_new
    else:
        df = df_new

    if not df.empty:
        df["received_at"] = pd.to_datetime(df["received_at"], utc=True, errors="coerce")
        df = df.sort_values("received_at")
        df.to_parquet(parq, index=False)

    return df

# ── Helpers: Flatten & Normalisieren ──────────────────────────────────────────
def flatten_payload(df: pd.DataFrame) -> pd.DataFrame:
    """payload_json -> dict -> flache Spalten (beliebige Decoder-Keys werden abgelegt)."""
    if df.empty or "payload_json" not in df.columns:
        return df
    dicts = df["payload_json"].apply(lambda s: json.loads(s) if isinstance(s, str) and s else {})
    if dicts.map(bool).any():
        flat = pd.json_normalize(dicts)     # verschachtelte Keys werden zu a.b.c
        flat.rename(columns={c: c.replace(".", "_") for c in flat.columns}, inplace=True)
        dup = [c for c in flat.columns if c in df.columns]
        flat.drop(columns=dup, inplace=True, errors="ignore")
        df = pd.concat([df.reset_index(drop=True), flat.reset_index(drop=True)], axis=1)
    return df

# ── Decoder-spezifische Mappings (aus deinen JS-Decoder-Snippets) ─────────────
def normalize_dds75(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    if "Bat" in df.columns and "battery" not in df.columns:
        df["battery"] = pd.to_numeric(df["Bat"], errors="coerce")
    if "BAT" in df.columns and "battery" not in df.columns:
        df["battery"] = pd.to_numeric(df["BAT"], errors="coerce")
    if "Distance_mm" in df.columns and "distance_cm" not in df.columns:
        df["distance_cm"] = pd.to_numeric(df["Distance_mm"], errors="coerce") / 10.0
    if "Additional_Distance_mm" in df.columns and "additional_distance_mm" not in df.columns:
        df["additional_distance_mm"] = df["Additional_Distance_mm"].apply(
            lambda v: json.dumps(v) if isinstance(v, list) else None
        )
    if "TempC_DS18B20" in df.columns and "temperature" not in df.columns:
        df["temperature"] = pd.to_numeric(df["TempC_DS18B20"], errors="coerce")
    for src, dst in [("Interrupt_flag","interrupt_flag"), ("Sensor_flag","sensor_flag")]:
        if src in df.columns and dst not in df.columns:
            df[dst] = pd.to_numeric(df[src], errors="coerce")
    if "Node_type" in df.columns and "node_type" not in df.columns:
        df["node_type"] = df["Node_type"]
    return df

def normalize_pslb(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    for src in ("Bat_V", "BAT"):
        if src in df.columns and "battery" not in df.columns:
            df["battery"] = pd.to_numeric(df[src], errors="coerce")
    mapping = {
        "Water_deep_cm": "water_cm",
        "Water_pressure_kPa": "pressure_kpa",
        "Water_pressure_MPa": "pressure_mpa",
        "Differential_pressure_Pa": "diff_pressure_pa",
        "VDC_intput_V": "vdc_input_v",
        "IDC_intput_mA": "idc_input_ma",
        "Probe_mod": "probe_mode",
        "IN1_pin_level": "in1_pin",
        "IN2_pin_level": "in2_pin",
        "Exti_pin_level": "exti_pin",
        "Exti_status": "exti_status",
        "SENSOR_MODEL": "sensor_model",
        "FIRMWARE_VERSION": "fw_version",
        "FREQUENCY_BAND": "freq_band",
        "SUB_BAND": "sub_band"
    }
    for src, dst in mapping.items():
        if src in df.columns and dst not in df.columns:
            if src in ("IN1_pin_level","IN2_pin_level","Exti_pin_level","Exti_status"):
                df[dst] = df[src]
            else:
                df[dst] = pd.to_numeric(df[src], errors="coerce")
    if "DATALOG" in df.columns and "datalog_pslb" not in df.columns:
        df["datalog_pslb"] = df["DATALOG"].astype(str)
    return df

def normalize_sensecap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Der V2-Decoder liefert typ. { data: { messages: [...] } } als JSON in payload_json.
    Extrahiere sinnvolle Kennwerte (letzter Eintrag je Typ) in flache Spalten.
    """
    if df.empty or "payload_json" not in df.columns:
        return df

    def extract_metrics(s):
        try:
            o = json.loads(s) if isinstance(s, str) else {}
        except Exception:
            return {}
        data = o.get("data") or {}
        msgs = data.get("messages") or []
        out = {}
        kmap = {
            "Air Temperature": "temperature",
            "Air Humidity": "humidity",
            "Light Intensity": "illumination",
            "UV Index": "uv_index",
            "Wind Speed": "wind_speed",
            "Wind Direction Sensor": "wind_dir",
            "Rain Gauge": "rainfall",
            "Barometric Pressure": "pressure",
            "Battery(%)": "battery_pct",
            "measureInterval": "measure_interval_s",
            "gpsInterval": "gps_interval_s",
            "status": "status",
            "channelType": "channel_type",
            "sensorEui": "sensor_eui",
            "errCode": "sensor_error_code",
            "type": "type",
        }
        for m in msgs:
            if not isinstance(m, dict): continue
            for k, v in m.items():
                key = kmap.get(k)
                if not key: continue
                if isinstance(v, (int, float)):
                    out[key] = float(v)
                else:
                    try:
                        out[key] = float(v) if isinstance(v, str) and v.replace('.', '', 1).isdigit() else v
                    except Exception:
                        out[key] = v
        return out

    metrics = df["payload_json"].apply(extract_metrics).apply(pd.Series)
    if metrics is not None and not metrics.empty:
        dup = [c for c in metrics.columns if c in df.columns]
        metrics.drop(columns=dup, inplace=True, errors="ignore")
        df = pd.concat([df.reset_index(drop=True), metrics.reset_index(drop=True)], axis=1)
    return df

def normalize_all_known(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_dds75(df)
    df = normalize_pslb(df)
    df = normalize_sensecap(df)
    for col in ("rssi","snr"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ── Plot Helper ───────────────────────────────────────────────────────────────
def to_plot_html(df: pd.DataFrame, y: str, title: str) -> str | None:
    if y not in df.columns or not pd.api.types.is_numeric_dtype(df[y]):
        return None
    d = df[["received_at", y]].dropna()
    if d.empty:
        return None
    f = px.line(d, x="received_at", y=y, title=title)
    f.update_layout(margin=dict(l=10, r=10, t=40, b=10))
    return pio.to_html(f, include_plotlyjs="cdn", full_html=False,
                       default_width="100%", default_height="420px")

# ── Main ──────────────────────────────────────────────────────────────────────
overview_rows, plot_cards = [], []

for dev in DEVS:
    df = device_pull(dev)
    if df.empty:
        overview_rows.append({"device_id": dev, "records": 0, "last_seen_utc": None})
        continue

    # payload_json → flach; bekannte Felder normieren
    df = flatten_payload(df)
    df = normalize_all_known(df)

    # Kennzahlen
    last_ts = pd.to_datetime(df["received_at"], utc=True, errors="coerce").max()
    overview_rows.append({"device_id": dev, "records": len(df), "last_seen_utc": last_ts})

    # Plotbare (numerische) Spalten bestimmen
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in {"f_port"}]
    preferred = ["temperature","humidity","pressure","pm25","co2",
                 "distance_cm","water_cm","battery","vbat","rssi","snr",
                 "pressure_kpa","pressure_mpa","diff_pressure_pa","idc_input_ma","vdc_input_v"]
    ordered = [c for c in preferred if c in numeric_cols] + [c for c in numeric_cols if c not in preferred]

    dev_parts, used = [], set()
    for col in ordered:
        if col in used: continue
        html_plot = to_plot_html(df, col, f"{col} – {dev}")
        if html_plot:
            dev_parts.append(html_plot)
            used.add(col)
        if len(dev_parts) >= 6:  # Seite schlank halten
            break

    if dev_parts:
        plot_cards.append(f'<div class="card"><h3>{dev}</h3>{"".join(dev_parts)}</div>')

# Übersichtstabelle
ov = pd.DataFrame(overview_rows)
if not ov.empty:
    ov["last_seen_utc"] = pd.to_datetime(ov["last_seen_utc"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%SZ")
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
  {"".join(plot_cards) if plot_cards else '<div class="card">Keine plottbaren numerischen Felder gefunden. Prüfe Decoder / Payload.</div>'}
</div>

</body></html>"""
(DOCS / "data.html").write_text(html, encoding="utf-8")
