#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Dieses Script:
# - zieht TTN-Storage-Daten je Device (letzte 24h, Sandbox),
# - speichert Rohdaten (NDJSON) + Parquet,
# - normalisiert Felder (DDS75-LB, PS-LB, SenseCAP-V2),
# - baut docs/data.html + docs/debug.html.

import os, json, pathlib, requests, pandas as pd
import plotly.express as px, plotly.io as pio
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

# ── ENV (über Workflow/Terminal setzen) ───────────────────────────────────────
APP   = os.environ["TTN_APP_ID"]            # z.B. "gisma-hydro-testbed"
REG   = os.environ["TTN_REGION"]            # z.B. "eu1"
KEY   = os.environ["TTN_API_KEY"]           # "NNSXS...."
DEVS  = [d for d in os.environ["DEVICES"].split() if d.strip()]  # space-getrennt

print("DEBUG ENV:", "APP=", APP, "REG=", REG, "KEY=", KEY[:8]+"..." if KEY else "MISSING")

# ── Verzeichnisse ─────────────────────────────────────────────────────────────
DOCS  = pathlib.Path("docs"); DOCS.mkdir(exist_ok=True, parents=True)
DATA  = pathlib.Path("data"); DATA.mkdir(exist_ok=True, parents=True)

# ── Fenster: Sandbox 24h; in Cloud ggf. erhöhen ──────────────────────────────
AFTER = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
HDRS  = {"Authorization": f"Bearer {KEY}", "Accept": "text/event-stream"}

# ── PULL je Device (robust: Timestamps/Raw persist) ───────────────────────────
def device_pull(dev: str) -> pd.DataFrame:
    """
    Holt NDJSON nur für dieses Device (Storage-HTTP),
    merged mit bestehendem Parquet und gibt DataFrame zurück.
    Spalten: received_at, device_id, f_port, rssi, snr, payload_json
    """
    parq = DATA / f"{dev}.parquet"
    raw  = DATA / f"{dev}_raw.ndjson"

    base = f"https://{REG}.cloud.thethings.network/api/v3/as/applications/{APP}/devices/{quote(dev)}/packages/storage/uplink_message"
    r = requests.get(base, headers=HDRS, params={"limit": "1000", "after": AFTER}, timeout=30)
    r.raise_for_status()
    raw_text = r.text.strip()
    # Rohantwort speichern (Diagnose)
    raw.write_text(raw_text + ("\n" if raw_text and not raw_text.endswith("\n") else ""), encoding="utf-8")

    lines = [json.loads(x) for x in raw_text.splitlines() if x.strip()]

    def pick_ts(o):
        """Nimmt bestmöglichen Zeitstempel aus verschiedenen Feldern."""
        up = o.get("uplink_message", {}) if isinstance(o, dict) else {}
        rx = (up.get("rx_metadata") or [{}])
        return (
            o.get("received_at") or
            up.get("received_at") or
            (rx[0].get("time") if rx and isinstance(rx[0], dict) else None) or
            o.get("created_at")
        )

    rows = []
    for o in lines:
        up  = o.get("uplink_message", {})
        rx0 = (up.get("rx_metadata") or [{}])[0] if isinstance(up.get("rx_metadata"), list) else {}
        rows.append({
            "received_at": pick_ts(o),
            "device_id":   dev,
            "f_port":      up.get("f_port"),
            "rssi":        rx0.get("rssi"),
            "snr":         rx0.get("snr"),
            # Dict -> JSON-Text (Parquet mag keine leeren Structs)
            "payload_json": json.dumps(up.get("decoded_payload", {}), ensure_ascii=False)
        })
    df_new = pd.DataFrame(rows)

    # Merge mit vorhandener Historie
    if not parq.exists():
        df = df_new
    else:
        try:
            df_old = pd.read_parquet(parq)
        except Exception:
            df_old = pd.DataFrame()
        df = pd.concat([df_old, df_new], ignore_index=True) if not df_new.empty else df_old

    if df.empty:
        return df

    # Zeitstempel parsen + auf Fenster beschränken
    df["received_at"] = pd.to_datetime(df["received_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["received_at"]).sort_values("received_at")
    window_start = pd.to_datetime(AFTER, utc=True)
    df = df[df["received_at"] >= window_start]

    # Persist
    if not df.empty:
        df.to_parquet(parq, index=False)
    return df

# ── Payload flatten ───────────────────────────────────────────────────────────
def flatten_payload(df: pd.DataFrame) -> pd.DataFrame:
    """payload_json -> dict -> flache Spalten (a.b -> a_b)."""
    if df.empty or "payload_json" not in df.columns:
        return df
    dicts = df["payload_json"].apply(lambda s: json.loads(s) if isinstance(s, str) and s else {})
    if dicts.map(bool).any():
        flat = pd.json_normalize(dicts)
        flat.rename(columns=lambda c: c.replace(".", "_"), inplace=True)
        # Kollisionen vermeiden
        dup = [c for c in flat.columns if c in df.columns]
        flat.drop(columns=dup, inplace=True, errors="ignore")
        df = pd.concat([df.reset_index(drop=True), flat.reset_index(drop=True)], axis=1)
    return df

# ── Normalisierung (aus deinen JS-Decodern) ───────────────────────────────────
def normalize_dds75(df: pd.DataFrame) -> pd.DataFrame:
    # Battery
    if "Bat" in df.columns and "battery" not in df.columns:
        df["battery"] = pd.to_numeric(df["Bat"], errors="coerce")
    if "BAT" in df.columns and "battery" not in df.columns:
        df["battery"] = pd.to_numeric(df["BAT"], errors="coerce")
    # Distanz mm -> cm
    if "Distance_mm" in df.columns and "distance_cm" not in df.columns:
        df["distance_cm"] = pd.to_numeric(df["Distance_mm"], errors="coerce") / 10.0
    # Zusatzliste als JSON-Text
    if "Additional_Distance_mm" in df.columns and "additional_distance_mm" not in df.columns:
        df["additional_distance_mm"] = df["Additional_Distance_mm"].apply(
            lambda v: json.dumps(v) if isinstance(v, list) else None
        )
    # Temperatur
    if "TempC_DS18B20" in df.columns and "temperature" not in df.columns:
        df["temperature"] = pd.to_numeric(df["TempC_DS18B20"], errors="coerce")
    # Flags
    for src, dst in [("Interrupt_flag","interrupt_flag"), ("Sensor_flag","sensor_flag")]:
        if src in df.columns and dst not in df.columns:
            df[dst] = pd.to_numeric(df[src], errors="coerce")
    # Node type
    if "Node_type" in df.columns and "node_type" not in df.columns:
        df["node_type"] = df["Node_type"]
    return df

def normalize_pslb(df: pd.DataFrame) -> pd.DataFrame:
    # Battery
    for src in ("Bat_V", "BAT"):
        if src in df.columns and "battery" not in df.columns:
            df["battery"] = pd.to_numeric(df[src], errors="coerce")
    # Hauptgrößen
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
                df[dst] = df[src]  # Strings beibehalten
            else:
                df[dst] = pd.to_numeric(df[src], errors="coerce")
    # Datalog als Text beibehalten (optional später explodieren)
    if "DATALOG" in df.columns and "datalog_pslb" not in df.columns:
        df["datalog_pslb"] = df["DATALOG"].astype(str)
    return df

def normalize_sensecap(df: pd.DataFrame) -> pd.DataFrame:
    """
    V2-Decoder-Struktur: payload_json enthält { data: { messages:[...] } }.
    Wir ziehen daraus sinnvolle Kennwerte (z. B. temperature/humidity/...).
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
    # rssi/snr numerisch
    for col in ("rssi","snr"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ── Plot Helper ───────────────────────────────────────────────────────────────
def to_plot_html(df: pd.DataFrame, y: str, title: str) -> str | None:
    """Erzeugt ein Plotly-Line-Chart für Spalte y (falls numerisch & vorhanden)."""
    if y not in df.columns or not pd.api.types.is_numeric_dtype(df[y]):
        return None
    d = df[["received_at", y]].dropna()
    if d.empty:
        return None
    f = px.line(d, x="received_at", y=y, title=title)
    f.update_layout(margin=dict(l=10, r=10, t=40, b=10))
    return pio.to_html(f, include_plotlyjs="cdn", full_html=False,
                       default_width="100%", default_height="420px")

# ── MAIN: Pull → Flatten → Norm → Plot → HTML ─────────────────────────────────
overview_rows, plot_cards = [], []

for dev in DEVS:
    df = device_pull(dev)

    if df.empty:
        overview_rows.append({"device_id": dev, "records": 0, "last_seen_utc": None})
        continue

    # payload_json → flach
    df = flatten_payload(df)

    # Versuch, alle "zahlartig" aussehenden Spalten zu numerisieren
    for c in df.columns:
        if c not in ("device_id",) and df[c].dtype == "object":
            df[c] = pd.to_numeric(df[c], errors="ignore")

    # bekannte Felder normieren
    df = normalize_all_known(df)

    # Kennzahlen
    last_ts = pd.to_datetime(df["received_at"], utc=True, errors="coerce").max()
    overview_rows.append({"device_id": dev, "records": len(df), "last_seen_utc": last_ts})

    # Plotbare (numerische) Spalten
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
        if len(dev_parts) >= 6:  # max 6 Plots/Device
            break

    if dev_parts:
        plot_cards.append(f'<div class="card"><h3>{dev}</h3>{"".join(dev_parts)}</div>')

# Übersichtstabelle
ov = pd.DataFrame(overview_rows)
if not ov.empty:
    ov["last_seen_utc"] = pd.to_datetime(ov["last_seen_utc"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%SZ")
overview_html = (ov.sort_values("device_id").to_html(index=False)
                 if not ov.empty else "<p>Keine Geräte-/Records gefunden.</p>")

# HTML Dashboard
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

# ── Debug-Seite: Dateigrößen, head(3) je Device ───────────────────────────────
dbg_parts = []
for dev in DEVS:
    parq = DATA / f"{dev}.parquet"
    raw  = DATA / f"{dev}_raw.ndjson"
    size_parq = parq.stat().st_size if parq.exists() else 0
    size_raw  = raw.stat().st_size if raw.exists() else 0
    try:
        dfx = pd.read_parquet(parq) if parq.exists() else pd.DataFrame()
    except Exception:
        dfx = pd.DataFrame()
    head_html = dfx.head(3).to_html(index=False) if not dfx.empty else "<i>leer</i>"
    dbg_parts.append(f"""
    <div class="card">
      <h3>{dev}</h3>
      <p>Parquet: {size_parq} B, Raw: {size_raw} B, Rows: {len(dfx)}</p>
      {head_html}
    </div>""")
dbg_html = f"""<!doctype html><meta charset="utf-8"><title>Debug</title>
<style>body{{font-family:system-ui;margin:20px}} .card{{border:1px solid #eee;border-radius:12px;padding:12px;margin:12px 0}}</style>
<h1>Debug – Pull & Persist</h1>
{"".join(dbg_parts)}
"""
(DOCS / "debug.html").write_text(dbg_html, encoding="utf-8")
