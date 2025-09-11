#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, pathlib, requests, pandas as pd
import plotly.express as px, plotly.io as pio
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

# ===== ENV =====
APP   = os.environ["TTN_APP_ID"]              # z.B. gisma-hydro-testbed
REG   = os.environ["TTN_REGION"]              # z.B. eu1
KEY   = os.environ["TTN_API_KEY"]             # NNSXS....
DEVS  = [d for d in os.environ["DEVICES"].split() if d.strip()]  # space-getrennt
AFTER_DAYS = int(os.environ.get("TTN_AFTER_DAYS", "2"))          # Testweise 2 Tage
AFTER = (datetime.now(timezone.utc) - timedelta(days=AFTER_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
HDRS  = {"Authorization": f"Bearer {KEY}"}     # kein Accept -> wir parsen NDJSON & SSE

DOCS  = pathlib.Path("docs"); DOCS.mkdir(exist_ok=True, parents=True)
DATA  = pathlib.Path("data"); DATA.mkdir(exist_ok=True, parents=True)

# ===== Parser-Utils =====
def _robust_json_lines(raw_text: str):
    """
    Akzeptiert:
      - NDJSON: {"..."}\n{"..."}\n
      - SSE:    data: {"..."}\n\n
      - TTN:    {"result": {...}}
    Liefert Liste von dicts (ein Objekt je Uplink).
    """
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
    """Wähle bestmöglichen Zeitstempel."""
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
    # Battery
    if "battery" not in df.columns:
        for src in ("Bat","BAT","Bat_V"):
            if src in df.columns:
                df["battery"] = pd.to_numeric(df[src], errors="coerce"); break
    # Distance -> cm (dein Sample: "Distance")
    if "distance_cm" not in df.columns:
        if "Distance_mm" in df.columns:
            df["distance_cm"] = pd.to_numeric(df["Distance_mm"], errors="coerce") / 10.0
        elif "Distance" in df.columns:
            df["distance_cm"] = pd.to_numeric(df["Distance"], errors="coerce") / 10.0
    # Temperatur
    if "temperature" not in df.columns and "TempC_DS18B20" in df.columns:
        df["temperature"] = pd.to_numeric(df["TempC_DS18B20"], errors="coerce")
    # Flags
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
    # Digitale Eingänge als Text belassen
    for s in ("IN1_pin_level","IN2_pin_level","Exti_pin_level","Exti_status"):
        if s in df.columns and s.lower() not in df.columns:
            df[s.lower()] = df[s]
    return df

def normalize_sensecap_messages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Zieht aus decoded_payload.messages (Liste von Listen) flache Spalten:
    temperature, humidity, illumination, uv_index, wind_speed, wind_dir, rainfall, pressure_hpa.
    """
    if df.empty or "payload_json" not in df.columns:
        return df

    def extract_from_messages(s):
        try:
            o = json.loads(s) if isinstance(s, str) else {}
        except Exception:
            return {}
        msgs = o.get("messages") or []  # bei SenseCAP liegt das direkt im decoded_payload
        flat_msgs = []
        for m in msgs:
            if isinstance(m, list):
                flat_msgs.extend(m)
            elif isinstance(m, dict):
                flat_msgs.append(m)

        out_by_type = {}
        for m in flat_msgs:
            t = m.get("type")
            v = m.get("measurementValue")
            if t is None:
                continue
            try:
                v = float(v)
            except Exception:
                pass
            out_by_type[t] = v  # letzter gewinnt

        res = {}
        if "Air Temperature" in out_by_type:  res["temperature"]  = out_by_type["Air Temperature"]
        if "Air Humidity" in out_by_type:     res["humidity"]     = out_by_type["Air Humidity"]
        if "Light Intensity" in out_by_type:  res["illumination"] = out_by_type["Light Intensity"]
        if "UV Index" in out_by_type:         res["uv_index"]     = out_by_type["UV Index"]
        if "Wind Speed" in out_by_type:       res["wind_speed"]   = out_by_type["Wind Speed"]
        if "Wind Direction Sensor" in out_by_type: res["wind_dir"] = out_by_type["Wind Direction Sensor"]
        if "Rain Gauge" in out_by_type:       res["rainfall"]     = out_by_type["Rain Gauge"]
        if "Barometric Pressure" in out_by_type:
            p = out_by_type["Barometric Pressure"]
            try:
                p = float(p)
                res["pressure_hpa"] = p/100.0 if p > 5000 else p
            except Exception:
                pass
        return res

    metrics = df["payload_json"].apply(extract_from_messages).apply(pd.Series)
    if metrics is not None and not metrics.empty:
        dup = [c for c in metrics.columns if c in df.columns]
        metrics.drop(columns=dup, inplace=True, errors="ignore")
        df = pd.concat([df.reset_index(drop=True), metrics.reset_index(drop=True)], axis=1)
    return df

def normalize_all(df: pd.DataFrame) -> pd.DataFrame:
    # Zweiter Schritt: alles, was wie Zahl aussieht, numerisch casten
    for c in df.columns:
        if c not in ("device_id",) and df[c].dtype == "object":
            df[c] = pd.to_numeric(df[c], errors="ignore")
    # Gerätespezifisch
    df = normalize_dds75(df)
    df = normalize_pslb(df)
    df = normalize_sensecap_messages(df)
    # rssi/snr numerisch
    for col in ("rssi","snr"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ===== Plot =====
def to_plot_html(df: pd.DataFrame, y: str, title: str) -> str | None:
    if y not in df.columns or not pd.api.types.is_numeric_dtype(df[y]): return None
    d = df[["received_at", y]].dropna()
    if d.empty: return None
    f = px.line(d, x="received_at", y=y, title=title)
    f.update_layout(margin=dict(l=10, r=10, t=40, b=10))
    return pio.to_html(f, include_plotlyjs="cdn", full_html=False,
                       default_width="100%", default_height="420px")

# ===== Main =====
overview_rows, plot_cards, debug_cards = [], [], []

for dev in DEVS:
    df = device_pull(dev)
    if df.empty:
        overview_rows.append({"device_id": dev, "records": 0, "last_seen_utc": None})
    else:
        df = flatten_payload(df)
        df = normalize_all(df)
        last_ts = pd.to_datetime(df["received_at"], utc=True, errors="coerce").max()
        overview_rows.append({"device_id": dev, "records": len(df), "last_seen_utc": last_ts})

        # Plots
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in {"f_port"}]
        preferred = [
            "temperature","humidity","pressure_hpa","pressure","pm25","co2",
            "distance_cm","water_cm","battery","rssi","snr",
            "pressure_kpa","pressure_mpa","diff_pressure_pa","idc_input_ma","vdc_input_v",
            "illumination","uv_index","wind_speed","wind_dir","rainfall"
        ]
        ordered = [c for c in preferred if c in numeric_cols] + [c for c in numeric_cols if c not in preferred]
        parts, used = [], set()
        for col in ordered:
            if col in used: continue
            h = to_plot_html(df, col, f"{col} – {dev}")
            if h: parts.append(h); used.add(col)
            if len(parts) >= 6: break
        if parts:
            plot_cards.append(f'<div class="card"><h3>{dev}</h3>{"".join(parts)}</div>')

    # Debug: Roh-Sample + Parquet-Head
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

# Dashboard
stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
html = f"""<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TTN – Alle Geräte</title>
<style>
body{{font-family:system-ui,Segoe UI,Roboto,Ubuntu,Arial,sans-serif;margin:20px}}
.card{{background:#fff;border:1px solid #eee;border-radius:12px;box-shadow:0 1px 6px rgba(0,0,0,.04);padding:16px;margin:16px 0}}
table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #ddd;padding:6px 8px;text-align:left}}
.grid{{display:grid;grid-template-columns:1fr;gap:16px}}
</style>
<h1>TTN Dashboard – Alle Geräte</h1>
<small>Stand: {stamp} • Quelle: TTN Storage ({APP}@{REG}) • Fenster: letzte {AFTER_DAYS} Tage</small>
<div class="card"><h2>Übersicht</h2>{overview_html}<p style="margin-top:8px">Parquet: <code>data/&lt;device&gt;.parquet</code></p></div>
<div class="grid">
{"".join(plot_cards) if plot_cards else '<div class="card">Keine plottbaren numerischen Felder gefunden. Prüfe Decoder / Payload.</div>'}
</div>"""
(DOCS / "data.html").write_text(html, encoding="utf-8")

# Debug-Seite
dbg = f"""<!doctype html><meta charset="utf-8"><title>Debug</title>
<style>body{{font-family:system-ui;margin:20px}} .card{{border:1px solid #eee;border-radius:12px;padding:12px;margin:12px 0}}</style>
<h1>Debug – Pull & Persist</h1>
{"".join(debug_cards)}
"""
(DOCS / "debug.html").write_text(dbg, encoding="utf-8")
