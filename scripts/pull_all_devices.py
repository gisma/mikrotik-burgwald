#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, json, pathlib, requests, pandas as pd
import plotly.express as px, plotly.io as pio
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

# ===== ENV =====
APP   = os.environ["TTN_APP_ID"]              # z.B. gisma-hydro-testbed
REG   = os.environ["TTN_REGION"]              # z.B. eu1
KEY   = os.environ["TTN_API_KEY"]             # NNSXS....
DEVS  = [d for d in os.environ["DEVICES"].split() if d.strip()]
AFTER_DAYS = int(os.environ.get("TTN_AFTER_DAYS", "2"))
AFTER = (datetime.now(timezone.utc) - timedelta(days=AFTER_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
HDRS  = {"Authorization": f"Bearer {KEY}"}     # wir parsen NDJSON & SSE robust

DOCS  = pathlib.Path("docs"); DOCS.mkdir(exist_ok=True, parents=True)
DATA  = pathlib.Path("data"); DATA.mkdir(exist_ok=True, parents=True)

# ===== Parser-Utils =====
def _robust_json_lines(raw_text: str):
    """Verträgt NDJSON, SSE ('data: {...}') und TTN-Wrapper {"result": {...}}."""
    out = []
    for ln in raw_text.splitlines():
        s = ln.strip()
        if not s:
            continue
        if s.startswith("data:"):
            s = s[5:].strip()
        if "{" in s and "}" in s:
            s = s[s.find("{"): s.rfind("}")+1]
        try:
            o = json.loads(s)
        except Exception:
            continue
        if isinstance(o, dict) and "result" in o and isinstance(o["result"], dict):
            o = o["result"]
        if isinstance(o, dict):
            out.append(o)
    return out

def _best_ts(o: dict):
    up = o.get("uplink_message", {}) if isinstance(o, dict) else {}
    rx = (up.get("rx_metadata") or [{}])
    return (o.get("received_at")
            or up.get("received_at")
            or (rx and isinstance(rx[0], dict) and rx[0].get("time"))
            or o.get("created_at"))

# ===== Pull je Device =====
def device_pull(dev: str) -> pd.DataFrame:
    parq = DATA / f"{dev}.parquet"
    raw  = DATA / f"{dev}_raw.ndjson"
    url  = f"https://{REG}.cloud.thethings.network/api/v3/as/applications/{APP}/devices/{quote(dev)}/packages/storage/uplink_message"

    r = requests.get(url, headers=HDRS, params={"limit": "1000", "after": AFTER}, timeout=30)
    r.raise_for_status()
    raw_text = r.text
    raw.write_text(raw_text, encoding="utf-8")

    objs = _robust_json_lines(raw_text)
    rows = []
    for o in objs:
        up  = o.get("uplink_message", {})
        rx0 = (up.get("rx_metadata") or [{}])
        rx0 = rx0[0] if isinstance(rx0, list) and rx0 else {}
        rows.append({
            "received_at": _best_ts(o),
            "device_id":   dev,
            "f_port":      up.get("f_port"),
            "rssi":        rx0.get("rssi"),
            "snr":         rx0.get("snr"),
            "payload_json": json.dumps(up.get("decoded_payload", {}), ensure_ascii=False)
        })

    df_new = pd.DataFrame(rows)

    # wenn leer, ggf. Altbestand zurückgeben
    if df_new.empty:
        return pd.read_parquet(parq) if parq.exists() else df_new

    # Zeit + Fenster
    df_new["received_at"] = pd.to_datetime(df_new["received_at"], utc=True, errors="coerce")
    df_new = df_new.dropna(subset=["received_at"]).sort_values("received_at")
    window_start = pd.to_datetime(AFTER, utc=True)
    df_new = df_new[df_new["received_at"] >= window_start]

    if df_new.empty:
        return pd.read_parquet(parq) if parq.exists() else df_new

    df_new.to_parquet(parq, index=False)
    return df_new

# ===== Flatten & Normalisierung =====
def flatten_payload(df: pd.DataFrame) -> pd.DataFrame:
    """payload_json (Text) -> dict -> flache Spalten (a.b -> a_b)."""
    if df.empty or "payload_json" not in df.columns:
        return df
    dicts = df["payload_json"].apply(lambda s: json.loads(s) if isinstance(s, str) and s else {})
    if dicts.map(bool).any():
        flat = pd.json_normalize(dicts).rename(columns=lambda c: c.replace(".", "_"))
        dup = [c for c in flat.columns if c in df.columns]
        flat.drop(columns=dup, inplace=True, errors="ignore")
        df = pd.concat([df.reset_index(drop=True), flat.reset_index(drop=True)], axis=1)
    return df

def normalize_dds75(df: pd.DataFrame) -> pd.DataFrame:
    if "battery" not in df.columns:
        for src in ("Bat","BAT","Bat_V"):
            if src in df.columns:
                df["battery"] = pd.to_numeric(df[src], errors="coerce"); break
    if "distance_cm" not in df.columns:
        if "Distance_mm" in df.columns:
            df["distance_cm"] = pd.to_numeric(df["Distance_mm"], errors="coerce") / 10.0
        elif "Distance" in df.columns:
            df["distance_cm"] = pd.to_numeric(df["Distance"], errors="coerce") / 10.0
    if "temperature" not in df.columns and "TempC_DS18B20" in df.columns:
        df["temperature"] = pd.to_numeric(df["TempC_DS18B20"], errors="coerce")
    for src, dst in [("Interrupt_flag","interrupt_flag"), ("Sensor_flag","sensor_flag")]:
        if src in df.columns and dst not in df.columns:
            df[dst] = pd.to_numeric(df[src], errors="coerce")
    return df

def normalize_pslb(df: pd.DataFrame) -> pd.DataFrame:
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
    }
    for src, dst in mapping.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = pd.to_numeric(df[src], errors="coerce")
    for s in ("IN1_pin_level","IN2_pin_level","Exti_pin_level","Exti_status"):
        if s in df.columns and s.lower() not in df.columns:
            df[s.lower()] = df[s]
    return df

def normalize_sensecap_messages(df: pd.DataFrame) -> pd.DataFrame:
    """SenseCAP decoded_payload.messages -> flache Spalten."""
    if df.empty or "payload_json" not in df.columns:
        return df
    def extract_from_messages(s):
        try:
            o = json.loads(s) if isinstance(s, str) else {}
        except Exception:
            return {}
        msgs = o.get("messages") or []
        flat_msgs = []
        for m in msgs:
            if isinstance(m, list): flat_msgs.extend(m)
            elif isinstance(m, dict): flat_msgs.append(m)
        by_type = {}
        for m in flat_msgs:
            t = m.get("type"); v = m.get("measurementValue")
            if t is None: continue
            try: v = float(v)
            except Exception: pass
            by_type[t] = v
        res = {}
        if "Air Temperature" in by_type:  res["temperature"]  = by_type["Air Temperature"]
        if "Air Humidity" in by_type:     res["humidity"]     = by_type["Air Humidity"]
        if "Light Intensity" in by_type:  res["illumination"] = by_type["Light Intensity"]
        if "UV Index" in by_type:         res["uv_index"]     = by_type["UV Index"]
        if "Wind Speed" in by_type:       res["wind_speed"]   = by_type["Wind Speed"]
        if "Wind Direction Sensor" in by_type: res["wind_dir"] = by_type["Wind Direction Sensor"]
        if "Rain Gauge" in by_type:       res["rainfall"]     = by_type["Rain Gauge"]
        if "Barometric Pressure" in by_type:
            p = by_type["Barometric Pressure"]
            try: p = float(p); res["pressure_hpa"] = p/100.0 if p > 5000 else p
            except Exception: pass
        return res
    metrics = df["payload_json"].apply(extract_from_messages).apply(pd.Series)
    if metrics is not None and not metrics.empty:
        dup = [c for c in metrics.columns if c in df.columns]
        metrics.drop(columns=dup, inplace=True, errors="ignore")
        df = pd.concat([df.reset_index(drop=True), metrics.reset_index(drop=True)], axis=1)
    return df

def normalize_all(df: pd.DataFrame) -> pd.DataFrame:
    # generische Numerik-Casts
    for c in df.columns:
        if c not in ("device_id",) and df[c].dtype == "object":
            df[c] = pd.to_numeric(df[c], errors="ignore")
    df = normalize_dds75(df)
    df = normalize_pslb(df)
    df = normalize_sensecap_messages(df)
    for col in ("rssi","snr"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ===== Typ-Erkennung & Rendering =====
def detect_sensor_type(df: pd.DataFrame, device_id: str) -> str:
    """Bestimme Sensortyp für Gruppierung."""
    # Hinweise aus Payload
    for col, val in [("node_type", None), ("Node_type", None), ("sensor_model", None), ("SENSOR_MODEL", None)]:
        if col in df.columns:
            v = str(df[col].dropna().iloc[-1]) if not df[col].dropna().empty else ""
            if v: return v
    name = device_id.lower()
    if "sensecap" in name: return "SenseCAP"
    if "dds75" in name:    return "DDS75-LB"
    if "ps-lb" in name:    return "PS-LB"
    # Heuristik über Spalten
    cols = set(df.columns)
    if {"illumination","uv_index","wind_speed","pressure_hpa"} & cols: return "SenseCAP"
    if {"distance_cm","TempC_DS18B20","Interrupt_flag"} & cols:        return "DDS75-LB"
    if {"water_cm","idc_input_ma","vdc_input_v"} & cols:                return "PS-LB"
    return "Other"

def to_plot_html(df: pd.DataFrame, y: str, title: str) -> str | None:
    if y not in df.columns or not pd.api.types.is_numeric_dtype(df[y]): return None
    d = df[["received_at", y]].dropna()
    if d.empty: return None
    fig = px.line(d, x="received_at", y=y, title=title)
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10))
    return pio.to_html(fig, include_plotlyjs="cdn", full_html=False,
                       default_width="100%", default_height="350px")

# Welche Metriken je Typ bevorzugt gezeigt werden (bis zu 4 pro Gerät)
PREFERRED_BY_TYPE = {
    "DDS75-LB": ["distance_cm","temperature","battery","rssi"],
    "PS-LB":    ["water_cm","idc_input_ma","vdc_input_v","battery"],
    "SenseCAP": ["temperature","humidity","pressure_hpa","illumination"],
    "Other":    ["battery","rssi","snr"]
}

# ===== Main =====
overview_rows, debug_cards = [], []
# Typ -> Liste von (device_id, df)
by_type: dict[str, list[tuple[str, pd.DataFrame]]] = {}

for dev in DEVS:
    df = device_pull(dev)
    if df.empty:
        overview_rows.append({"device_id": dev, "records": 0, "last_seen_utc": None})
    else:
        df = flatten_payload(df)
        df = normalize_all(df)
        last_ts = pd.to_datetime(df["received_at"], utc=True, errors="coerce").max()
        overview_rows.append({"device_id": dev, "records": len(df), "last_seen_utc": last_ts})

        typ = detect_sensor_type(df, dev)
        by_type.setdefault(typ, []).append((dev, df))

    # Debug
    raw = DATA / f"{dev}_raw.ndjson"
    sample = ""
    if raw.exists():
        sample = "".join(raw.read_text(encoding="utf-8").splitlines(True)[:5])
        sample = sample.replace("<", "&lt;").replace(">", "&gt;")
    head_html = (pd.read_parquet(DATA / f"{dev}.parquet").head(3).to_html(index=False)
                 if (DATA / f"{dev}.parquet").exists() else "<i>kein Parquet</i>")
    debug_cards.append(f"""
    <div class="card">
      <h3>{dev}</h3>
      <pre style="white-space:pre-wrap;max-height:220px;overflow:auto">{sample}</pre>
      {head_html}
    </div>""")

# Übersicht
ov = pd.DataFrame(overview_rows)
if not ov.empty:
    ov["last_seen_utc"] = pd.to_datetime(ov["last_seen_utc"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%SZ")
overview_html = ov.sort_values("device_id").to_html(index=False) if not ov.empty else "<p>Keine Geräte-/Records gefunden.</p>"

# ===== HTML bauen: große Typ-Kacheln → zweispaltige Gerätekacheln
stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

# CSS: Typ-Kachel ganz, darin zweispaltige Device-Kacheln
style = """
<style>
body{font-family:system-ui,Segoe UI,Roboto,Ubuntu,Arial,sans-serif;margin:20px}
.card{background:#fff;border:1px solid #eee;border-radius:12px;box-shadow:0 1px 6px rgba(0,0,0,.05);padding:16px;margin:16px 0}
.card h2{margin:0 0 .25rem}
.card h3{margin:.25rem 0 .5rem}
table{border-collapse:collapse;width:100%} th,td{border:1px solid #ddd;padding:6px 8px;text-align:left}

.type-grid{display:grid;grid-template-columns:1fr;gap:16px}
.device-grid{display:grid;grid-template-columns:repeat(2,minmax(280px,1fr));gap:14px;margin-top:12px}
.device-tile{border:1px solid #ddd;border-radius:12px;padding:10px}
.device-tile h4{margin:.2rem 0 .6rem;font-size:15px}
.plot-wrap{border:1px solid #eee;border-radius:10px;padding:4px;margin-bottom:8px}
@media (max-width:800px){ .device-grid{grid-template-columns:1fr} }
</style>
"""

type_cards_html = []
for typ, items in by_type.items():
    device_tiles = []
    preferred = PREFERRED_BY_TYPE.get(typ, PREFERRED_BY_TYPE["Other"])
    for dev, df in sorted(items, key=lambda x: x[0]):
        # wähle bis zu 4 Plots passend zum Typ
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in {"f_port"}]
        ordered = [c for c in preferred if c in numeric_cols] + [c for c in numeric_cols if c not in preferred]
        plots, used = [], set()
        for col in ordered:
            if col in used: continue
            html_plot = to_plot_html(df, col, f"{col}")
            if html_plot:
                plots.append(f'<div class="plot-wrap">{html_plot}</div>')
                used.add(col)
            if len(plots) >= 4:  # max 4 Plots pro Gerät
                break
        if not plots:
            plots.append('<div class="plot-wrap"><em>Keine numerischen Felder gefunden.</em></div>')
        device_tiles.append(f'<div class="device-tile"><h4>{dev}</h4>{"".join(plots)}</div>')
    type_cards_html.append(f'<div class="card"><h2>{typ}</h2><div class="device-grid">{"".join(device_tiles)}</div></div>')

html = f"""<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TTN – Alle Geräte (nach Typ gruppiert)</title>
{style}
<h1>TTN Dashboard – Nach Sensortyp gruppiert</h1>
<small>Stand: {stamp} • Quelle: TTN Storage ({APP}@{REG}) • Fenster: letzte {AFTER_DAYS} Tage</small>
<div class="card"><h2>Übersicht</h2>{overview_html}<p style="margin-top:8px">Parquet: <code>data/&lt;device&gt;.parquet</code></p></div>
<div class="type-grid">
{"".join(type_cards_html) if type_cards_html else '<div class="card">Keine Daten zum Anzeigen.</div>'}
</div>
"""
(DOCS / "data.html").write_text(html, encoding="utf-8")

# Debug-Seite
dbg = f"""<!doctype html><meta charset="utf-8"><title>Debug</title>
<style>body{{font-family:system-ui;margin:20px}} .card{{border:1px solid #eee;border-radius:12px;padding:12px;margin:12px 0}}</style>
<h1>Debug – Pull & Persist</h1>
{"".join(debug_cards)}
"""
(DOCS / "debug.html").write_text(dbg, encoding="utf-8")
