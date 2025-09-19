#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, pathlib, requests, pandas as pd, time
import plotly.express as px, plotly.io as pio
import plotly.graph_objects as go  # Multi-Traces
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from typing import List, Dict, Tuple, Optional
from pathlib import Path

# ===== .env laden – robust & installationsfrei =====
def _load_env_file(path: Path, override: bool = False) -> bool:
    """Einfacher .env-Parser; wird verwendet, falls python-dotenv fehlt oder nichts lädt."""
    try:
        if not path.exists():
            return False
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if override or k not in os.environ:
                os.environ[k] = v
        return True
    except Exception:
        return False

try:
    from dotenv import load_dotenv
    DOTENV_PATH = Path(__file__).with_name(".env")
    loaded = load_dotenv(DOTENV_PATH, override=True)
    loaded = load_dotenv(override=False) or loaded  # zusätzlich CWD
    if not loaded:
        _load_env_file(DOTENV_PATH, override=False)
        _load_env_file(Path.cwd() / ".env", override=False)
except Exception:
    _load_env_file(Path(__file__).with_name(".env"), override=False)
    _load_env_file(Path.cwd() / ".env", override=False)

# -------- ENV: mit klarer Fehlermeldung --------
def _require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(
            f"ENV '{name}' fehlt. Setze es in .env (neben dem Script oder im CWD) "
            f"oder per Shell-ENV. Pflicht: TTN_APP_ID, TTN_REGION, TTN_API_KEY."
        )
    return v

# ===== ENV =====
APP   = _require_env("TTN_APP_ID")            # e.g., gisma-hydro-testbed
REG   = _require_env("TTN_REGION")            # e.g., eu1
KEY   = _require_env("TTN_API_KEY")           # NNSXS....
AFTER_DAYS = int(os.environ.get("TTN_AFTER_DAYS", "2"))
AFTER = (datetime.now(timezone.utc) - timedelta(days=AFTER_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
HDRS  = {"Authorization": f"Bearer {KEY}"}    # robust NDJSON/SSE parsing
DELAY_BETWEEN_DEVICES = float(os.environ.get("DELAY_BETWEEN_DEVICES", "0.3"))
DEBUG_RECENT_MINUTES = int(os.environ.get("DEBUG_RECENT_MINUTES", "90"))

# Health report configuration
STALE_HOURS  = int(os.environ.get("STALE_HOURS", "3"))
DEV_INCLUDE  = os.environ.get("DEV_INCLUDE", ".*")  # regex include (default: all)
DEV_EXCLUDE  = os.environ.get("DEV_EXCLUDE", "")    # regex exclude (default: none)

# Optional: Rohdaten-Verhalten steuern
RAW_APPEND   = os.environ.get("RAW_APPEND", "0") == "1"     # 1 = _raw.ndjson anhängen statt überschreiben
RAW_SNAPSHOT = os.environ.get("RAW_SNAPSHOT", "0") == "1"   # 1 = zusätzlich Snapshot-Datei je Lauf schreiben

# Folders: Templates & Runtime-Builds sauber trennen
DATA   = pathlib.Path("data");   DATA.mkdir(exist_ok=True, parents=True)
ASSETS = pathlib.Path("assets"); ASSETS.mkdir(exist_ok=True, parents=True)
ASSETS_BUILD_SUBDIR = os.environ.get("ASSETS_BUILD_SUBDIR", "build")  # z.B. "build"
ASSETS_BUILD = ASSETS / ASSETS_BUILD_SUBDIR if ASSETS_BUILD_SUBDIR else ASSETS
ASSETS_BUILD.mkdir(exist_ok=True, parents=True)

# ===== Discover device list from TTN (if DEVICES env is not set) =====
def list_ttn_devices(app: str) -> List[str]:
    """Return all end-device IDs for the given application (paginated)."""
    url = f"https://{REG}.cloud.thethings.network/api/v3/applications/{quote(app)}/devices"
    devs, page = [], ""
    while True:
        params = {"limit": "100"}
        if page:
            params["page"] = page
        r = requests.get(url, headers=HDRS, params=params, timeout=30)
        r.raise_for_status()
        js = r.json() if r.text.strip() else {}
        for ed in js.get("end_devices", []):
            ids = ed.get("ids", {})
            did = ids.get("device_id")
            if did:
                devs.append(did)
        page = js.get("next_page_token")
        if not page:
            break
    return sorted(set(devs))

DEVICES_ENV = os.environ.get("DEVICES", "").strip()
if DEVICES_ENV:
    DEVS = [d for d in DEVICES_ENV.split() if d.strip()]
else:
    # Auto-discovery fallback if DEVICES is not set
    DEVS = list_ttn_devices(APP)

# Deterministic order
DEVS = sorted(set(DEVS))

# (Initial) snapshot der gefundenen Devices – wird später von Health überschrieben
try:
    (ASSETS_BUILD / "devices_used.txt").write_text("\n".join(DEVS), encoding="utf-8")
except Exception:
    pass

# ===== Parsers =====
def _robust_json_lines(raw_text: str):
    """Accepts NDJSON, SSE ('data: {...}') and TTN wrapper {'result': {...}}."""
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
    """Pick the most reliable timestamp from a TTN uplink."""
    up = o.get("uplink_message", {}) if isinstance(o, dict) else {}
    rx = (up.get("rx_metadata") or [{}])
    return (o.get("received_at")
            or up.get("received_at")
            or (rx and isinstance(rx[0], dict) and rx[0].get("time"))
            or o.get("created_at"))

# ===== HTTP: retries/backoff =====
RETRY_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
BACKOFF_BASE = 0.6  # seconds

def _do_get_with_retries(url, params, headers, timeout, dev):
    """GET with exponential backoff; resilient against 429/5xx/network glitches."""
    last_exc = None
    for i in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code in RETRY_CODES:
                wait = BACKOFF_BASE * (2 ** i)
                print(f"[{dev}] RETRY {i+1}/{MAX_RETRIES} HTTP {resp.status_code} → wait {wait:.1f}s")
                time.sleep(wait)
                continue
            return resp
        except requests.RequestException as e:
            last_exc = e
            wait = BACKOFF_BASE * (2 ** i)
            print(f"[{dev}] RETRY {i+1}/{MAX_RETRIES} network error: {e} → {wait:.1f}s pause")
            time.sleep(wait)
    if last_exc:
        raise last_exc
    return requests.get(url, headers=headers, params=params, timeout=timeout)

# ===== Pull per device =====
def device_pull(dev: str) -> pd.DataFrame:
    """Fetch TTN Storage data for a device (with paging + fallbacks)."""
    parq = DATA / f"{dev}.parquet"
    csv  = DATA / f"{dev}.csv"
    raw  = DATA / f"{dev}_raw.ndjson"
    url  = f"https://{REG}.cloud.thethings.network/api/v3/as/applications/{APP}/devices/{quote(dev)}/packages/storage/uplink_message"

    limit_primary = 200
    limit_fallback = 100

    def _empty_df():
        return pd.DataFrame(columns=["device_id","received_at","f_port","rssi","snr","payload_json"])

    try:
        # Start time: AFTER; wenn persistente Daten vorhanden, weiter ab last_ts
        current_after = pd.to_datetime(AFTER, utc=True, errors="coerce")
        used_after = False

        if parq.exists() or csv.exists():
            try:
                if parq.exists():
                    df_old_ts = pd.read_parquet(parq)
                else:
                    raise FileNotFoundError("parquet missing, try CSV")
            except Exception as e:
                print(f"[{dev}] WARN: failed reading parquet ({e}) -> try CSV")
                try:
                    df_old_ts = pd.read_csv(csv)
                    if "received_at" in df_old_ts.columns:
                        df_old_ts["received_at"] = pd.to_datetime(df_old_ts["received_at"], utc=True, errors="coerce")
                except Exception as e2:
                    print(f"[{dev}] WARN: failed reading CSV too: {e2}")
                    df_old_ts = None

            if df_old_ts is not None and not df_old_ts.empty:
                last_old = pd.to_datetime(df_old_ts["received_at"], utc=True, errors="coerce").max()
                if pd.notna(last_old):
                    current_after = max(current_after, last_old + pd.Timedelta(seconds=1))
                    used_after = True

        all_rows: List[Dict] = []
        ndjson_chunks: List[str] = []

        while True:
            params = {"limit": str(limit_primary)}
            if used_after:
                params["after"] = current_after.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

            r = _do_get_with_retries(url, params, HDRS, 30, dev)

            # 400 fallbacks (be conservative with 'after' and 'limit')
            if r.status_code == 400:
                body = (r.text or "").strip()
                print(f"[{dev}] WARN HTTP 400 for params={params} - BODY: {body[:300]}")
                # 1) smaller limit
                params_fb = dict(params); params_fb["limit"] = str(limit_fallback)
                r2 = _do_get_with_retries(url, params_fb, HDRS, 30, dev)
                print(f"[{dev}] retry limit={limit_fallback} -> HTTP {r2.status_code}, bytes={len(r2.text)}")
                if not r2.ok or not r2.text.strip():
                    # 2) try without 'after'
                    params_no_after = {"limit": str(limit_fallback)}
                    r3 = _do_get_with_retries(url, params_no_after, HDRS, 30, dev)
                    print(f"[{dev}] retry no 'after' limit={limit_fallback} -> HTTP {r3.status_code}, bytes={len(r3.text)}")
                    r = r3
                    used_after = False
                else:
                    r = r2

            if r.status_code == 204 or not r.text.strip():
                break

            if not r.ok:
                print(f"[{dev}] ERROR HTTP {r.status_code} {r.reason} for {r.url}")
                body = (r.text or "").strip()
                if body:
                    print(f"[{dev}] BODY: {body[:400]}")
                return _empty_df()

            raw_text = r.text
            ndjson_chunks.append(raw_text)

            objs = _robust_json_lines(raw_text)
            if not objs:
                break

            batch_rows, max_ts = [], None
            for o in objs:
                up  = o.get("uplink_message", {}) if isinstance(o, dict) else {}
                rx0 = (up.get("rx_metadata") or [{}])
                rx0 = rx0[0] if isinstance(rx0, list) and rx0 else {}
                ts  = _best_ts(o)
                batch_rows.append({
                    "received_at": ts,
                    "device_id":   dev,
                    "f_port":      up.get("f_port"),
                    "rssi":        rx0.get("rssi"),
                    "snr":         rx0.get("snr"),
                    "payload_json": json.dumps(up.get("decoded_payload", {}), ensure_ascii=False),
                })
                if ts:
                    tsv = pd.to_datetime(ts, utc=True, errors="coerce")
                    if pd.notna(tsv):
                        max_ts = tsv if max_ts is None or tsv > max_ts else max_ts

            all_rows.extend(batch_rows)

            # Paging: if batch is smaller than limit or no max ts found, stop
            eff_limit = int((params.get("limit") or limit_primary))
            if len(objs) < eff_limit or max_ts is None:
                break
            current_after = max_ts + pd.Timedelta(seconds=1)
            used_after = True

        # --- Rohdaten NDJSON schreiben (Debug) ---
        if ndjson_chunks:
            mode = "a" if RAW_APPEND else "w"
            with raw.open(mode, encoding="utf-8") as f:
                if mode == "a":
                    f.write("\n")
                f.write("".join(ndjson_chunks))
            if RAW_SNAPSHOT:
                snap = DATA / f"{dev}_raw_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.ndjson"
                snap.write_text("".join(ndjson_chunks), encoding="utf-8")

        # Build dataframe for new rows
        df_new = pd.DataFrame(all_rows)
        if not df_new.empty:
            df_new["received_at"] = pd.to_datetime(df_new["received_at"], utc=True, errors="coerce")
            df_new = df_new.dropna(subset=["received_at"]).sort_values("received_at")

        # Merge mit bestehenden Daten (Parquet bevorzugt, CSV Fallback)
        if parq.exists() or csv.exists():
            try:
                if parq.exists():
                    df_old = pd.read_parquet(parq)
                else:
                    raise FileNotFoundError("parquet missing, try CSV")
            except Exception as e:
                print(f"[{dev}] WARN: read old parquet failed ({e}) -> try CSV")
                try:
                    df_old = pd.read_csv(csv)
                    if "received_at" in df_old.columns:
                        df_old["received_at"] = pd.to_datetime(df_old["received_at"], utc=True, errors="coerce")
                except Exception as e2:
                    print(f"[{dev}] WARN: read old CSV failed: {e2}")
                    df_old = pd.DataFrame()

            if not df_old.empty and not df_new.empty:
                df = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df = df_old if df_new.empty else df_new
        else:
            df = df_new

        # De-duplicate und Persistenz: immer Parquet + CSV schreiben (Parquet kann ggf. scheitern)
        if not df.empty:
            subset_cols = [c for c in ["device_id","received_at","f_port","payload_json"] if c in df.columns]
            df = df.drop_duplicates(subset=subset_cols).sort_values("received_at")

            try:
                df.to_parquet(parq, index=False)
            except Exception as e:
                print(f"[{dev}] WARN: parquet write failed: {e}")

            try:
                df.to_csv(csv, index=False)
            except Exception as e:
                print(f"[{dev}] WARN: csv write failed: {e}")

            return df

        return _empty_df()

    except Exception as e:
        print(f"[{dev}] FATAL: device_pull() exception: {repr(e)}")
        return _empty_df()

# ===== Flatten & normalization =====
def flatten_payload(df: pd.DataFrame) -> pd.DataFrame:
    """payload_json (string) -> dict -> flat columns (a.b -> a_b)."""
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
    """SenseCAP decoded_payload.messages -> flat columns."""
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
    # Generic numeric casting
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

# ===== Type detection & plotting =====
def detect_sensor_type(df: pd.DataFrame, device_id: str) -> str:
    """Rudimentary sensor type detection for grouping."""
    for col in ("node_type", "Node_type", "sensor_model", "SENSOR_MODEL"):
        if col in df.columns:
            vser = df[col].dropna()
            if not vser.empty:
                v = str(vser.iloc[-1])
                if v:
                    return v
    name = device_id.lower()
    if "sensecap" in name: return "SenseCAP"
    if "dds75" in name:    return "DDS75-LB"
    if "ps-lb" in name:    return "PS-LB"
    cols = set(df.columns)
    if {"illumination","uv_index","wind_speed","pressure_hpa"} & cols: return "SenseCAP"
    if {"distance_cm","TempC_DS18B20","Interrupt_flag"} & cols:        return "DDS75-LB"
    if {"water_cm","idc_input_ma","vdc_input_v"} & cols:                return "PS-LB"
    return "Other"

def to_plot_html(df: pd.DataFrame, y: str, title: str) -> Optional[str]:
    if y not in df.columns or not pd.api.types.is_numeric_dtype(df[y]): return None
    d = df[["received_at", y]].dropna()
    if d.empty: return None
    fig = px.line(d, x="received_at", y=y, title=title)
    fig.update_layout(
      template="plotly_white",                     # Helles Theme
      paper_bgcolor="rgba(0,0,0,0)",
      plot_bgcolor="rgba(0,0,0,0)",
      margin=dict(l=10, r=10, t=40, b=10),
      font=dict(size=14),
      xaxis=dict(showgrid=True, zeroline=False),
      yaxis=dict(showgrid=True, zeroline=False),
    )
    return pio.to_html(fig, include_plotlyjs="cdn", full_html=False,
                       default_width="100%", default_height="350px")

# --- Multi-Trace-Plot (Battery/RSSI/SNR/Power) ---
def to_plot_multi_html(df: pd.DataFrame, y_cols: List[str], title: str) -> Optional[str]:
    y_cols = [c for c in y_cols if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]
    if not y_cols:
        return None
    d = df[["received_at"] + y_cols].copy()
    d = d.dropna(how="all", subset=y_cols)
    if d.empty:
        return None
    d = d.sort_values("received_at")

    fig = go.Figure()
    for c in y_cols:
        fig.add_trace(go.Scatter(x=d["received_at"], y=d[c], mode="lines", name=c))

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=40, b=10),
        font=dict(size=14),
        xaxis=dict(showgrid=True, zeroline=False),
        yaxis=dict(showgrid=True, zeroline=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
    )
    return pio.to_html(fig, include_plotlyjs="cdn", full_html=False,
                       default_width="100%", default_height="350px")

PREFERRED_BY_TYPE = {
    "DDS75-LB": ["distance_cm","temperature","battery","rssi"],
    "PS-LB":    ["water_cm","idc_input_ma","vdc_input_v","battery"],
    "SenseCAP": ["temperature","humidity","pressure_hpa","illumination"],
    "Other":    ["battery","rssi","snr"]
}

# --- Inhalt vs. Betriebsmetriken + Labels/Units ---
NON_CONTENT = {
    "battery","rssi","snr","vdc_input_v","idc_input_ma",
    "probe_mode","interrupt_flag","sensor_flag","f_port"
}
CONTENT_BY_TYPE = {
    "DDS75-LB": ["distance_cm","temperature"],
    "PS-LB":    ["water_cm","pressure_kpa","pressure_mpa","diff_pressure_pa"],
    "SenseCAP": ["temperature","humidity","pressure_hpa","illumination","uv_index","wind_speed","wind_dir","rainfall"],
    "Other":    ["temperature","humidity","pressure_hpa","distance_cm","water_cm"]
}
LABELS = {
    "temperature":"Temperature", "humidity":"Humidity", "pressure_hpa":"Pressure",
    "illumination":"Light", "uv_index":"UV Index", "wind_speed":"Wind Speed",
    "wind_dir":"Wind Dir", "rainfall":"Rain", "distance_cm":"Distance",
    "water_cm":"Water", "pressure_kpa":"Pressure (kPa)", "pressure_mpa":"Pressure (MPa)",
    "diff_pressure_pa":"ΔPressure (Pa)"
}
UNITS = {
    "temperature":"°C","humidity":"%","pressure_hpa":"hPa",
    "illumination":"lx","uv_index":"","wind_speed":"m/s","wind_dir":"°",
    "rainfall":"mm","distance_cm":"cm","water_cm":"cm","pressure_kpa":"kPa",
    "pressure_mpa":"MPa","diff_pressure_pa":"Pa"
}
def _fmt_val(key: str, v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    try:
        fv = float(v)
        if key in ("temperature","pressure_hpa","water_cm","distance_cm","pressure_kpa","pressure_mpa"):
            fv = round(fv, 1)
        elif key in ("humidity","uv_index","wind_dir","rainfall"):
            fv = round(fv, 0)
        else:
            fv = round(fv, 2)
        unit = UNITS.get(key,"")
        return f"{fv:g}{(' ' + unit) if unit else ''}"
    except Exception:
        return str(v)

# --- Kachel mit aktuellen Werten pro Device ---
def device_value_card_html(device_id: str, df: pd.DataFrame, typ: str) -> str:
    if df.empty:
        return f'<div class="val-card"><h4>{device_id}</h4><div class="vals"><em>No data</em></div></div>'

    dfl = df.sort_values("received_at")
    row = dfl.iloc[-1]

    cand = [c for c in CONTENT_BY_TYPE.get(typ, CONTENT_BY_TYPE["Other"]) if c in df.columns]
    if not cand:
        num = [c for c in df.columns if c not in NON_CONTENT and c != "received_at" and pd.api.types.is_numeric_dtype(df[c])]
        cand = num[:4]

    items = []
    for c in cand:
        val = _fmt_val(c, row.get(c))
        if val is not None:
            label = LABELS.get(c, c)
            items.append(f'<div class="kv"><div class="k">{label}</div><div class="v">{val}</div></div>')

    if not items:
        items = ['<div class="kv"><div class="k">—</div><div class="v">—</div></div>']

    return f'<div class="val-card"><h4>{device_id}</h4><div class="vals">{"".join(items)}</div></div>'

# ===== Main (Dashboard/HTML) =====
RUN_DASH = os.environ.get("RUN_DASH", "1") == "1"
if RUN_DASH:
    overview_rows, debug_cards = [], []
    by_type: Dict[str, List[Tuple[str, pd.DataFrame]]] = {}

    for dev in DEVS:
        status = "ok"
        last_ts = None
        try:
            df = device_pull(dev)
            if df.empty:
                status = "empty"
            else:
                df = flatten_payload(df)
                df = normalize_all(df)
                last_ts = pd.to_datetime(df["received_at"], utc=True, errors="coerce").max()
                typ = detect_sensor_type(df, dev)
                by_type.setdefault(typ, []).append((dev, df))
        except Exception as e:
            status = "error"
            print(f"[{dev}] ERROR: {e!r}")
            df = pd.DataFrame()

        overview_rows.append({
            "device_id": dev,
            "records": 0 if df.empty else len(df),
            "last_seen_utc": last_ts,
            "status": status
        })

        # DEBUG (letzte Minuten)
        recent_html = "<i>no recent data</i>"
        try:
            df_dev = df.copy()
            if not df_dev.empty:
                df_dev["received_at"] = pd.to_datetime(df_dev["received_at"], utc=True, errors="coerce")
                cutoff = datetime.now(timezone.utc) - timedelta(minutes=DEBUG_RECENT_MINUTES)
                dfr = df_dev[df_dev["received_at"] >= cutoff].sort_values("received_at")

                prefer = ["received_at","f_port","battery","water_cm","idc_input_ma","vdc_input_v","rssi","snr"]
                cols = [c for c in prefer if c in dfr.columns]
                if not cols:
                    num_cols = [c for c in dfr.columns
                                if c != "received_at" and pd.api.types.is_numeric_dtype(dfr[c])]
                    cols = ["received_at"] + num_cols[:6]
                if cols:
                    recent_html = dfr[cols].tail(12).to_html(index=False)
        except Exception:
            pass

        debug_cards.append(f"""
        <div class="card">
          <h3>{dev}</h3>
          <div>{recent_html}</div>
        </div>""")

        time.sleep(DELAY_BETWEEN_DEVICES)

    # Overview table for health/tab
    ov = pd.DataFrame(overview_rows)
    def _badge(s: str) -> str:
       cls = "badge"
       if s == "ok": cls += " ok"
       elif isinstance(s, str) and s.startswith("STALE"): cls += " stale"
       elif s == "empty": cls += " empty"
       elif s == "error": cls += " err"
       return f'<span class="{cls}">{s}</span>'

    if not ov.empty:
       ov["status"] = ov["status"].apply(_badge)
       ov["last_seen_utc"] = pd.to_datetime(ov["last_seen_utc"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%SZ")

    # ---------- Styling (hell) ----------
    style = """
    <style>
    :root{--card-bg:#fff;--muted:#334155;--border:#cbd5e1}
    html,body{background:#f8fafc}
    body{font-family:system-ui,Segoe UI,Roboto,Ubuntu,Arial,sans-serif;margin:20px;color:#0b1221}
    h1,h2,h3,h4{margin:0 0 .4rem}
    .card{background:var(--card-bg);border:1px solid var(--border);border-radius:12px;box-shadow:0 1px 10px rgba(2,6,23,.06);padding:16px;margin:16px 0}
    table{border-collapse:collapse;width:100%} th,td{border:1px solid var(--border);padding:8px 10px;text-align:left}

    .type-grid{display:grid;grid-template-columns:1fr;gap:16px}
    .device-grid{display:grid;grid-template-columns:repeat(2,minmax(320px,1fr));gap:14px;margin-top:12px}
    .device-tile{border:1px solid var(--border);border-radius:12px;padding:12px;background:#fff;box-shadow:0 1px 10px rgba(2,6,23,.06)}
    .device-tile h4{margin:.2rem 0 .6rem;font-size:15px}
    .plot-wrap{border:1px solid var(--border);border-radius:10px;padding:6px;margin-bottom:10px;background:#fff}

    /* Tabs */
    .tabs{display:flex;gap:8px;margin:8px 0 12px}
    .tabs button{border:1px solid var(--border);background:#fff;border-radius:10px;padding:8px 12px;cursor:pointer;font-weight:600}
    .tabs button.active{background:#111827;color:#fff;border-color:#111827}
    [hidden]{display:none !important}

    /* Value cards (Übersicht) */
    .val-grid{display:grid;grid-template-columns:repeat(3,minmax(260px,1fr));gap:14px}
    .val-card{background:#fff;border:1px solid var(--border);border-radius:12px;padding:14px;box-shadow:0 1px 10px rgba(2,6,23,.06)}
    .val-card h4{margin:.1rem 0 .7rem;font-weight:700}
    .vals{display:grid;grid-template-columns:1fr 1fr;gap:8px 12px}
    .kv{display:contents}
    .kv .k{color:var(--muted);font-weight:600}
    .kv .v{text-align:right;font-weight:700;font-size:1.05rem}
    @media (max-width:1100px){ .val-grid{grid-template-columns:repeat(2,minmax(240px,1fr))} }
    @media (max-width:900px){ .device-grid{grid-template-columns:1fr} .val-grid{grid-template-columns:1fr} }

    /* Badges */
    .badge{display:inline-block;padding:.15rem .5rem;border-radius:999px;font-size:.8rem;font-weight:700;border:1px solid transparent}
    .badge.ok{background:#e1f7e6;color:#065f46;border-color:#a7f3d0}
    .badge.stale{background:#fff4e6;color:#92400e;border-color:#fed7aa}
    .badge.empty{background:#f1f5f9;color:#334155;border-color:#cbd5e1}
    .badge.err{background:#fee2e2;color:#991b1b;border-color:#fecaca}
    </style>
    """

    # ---------- Kachel-Übersicht ----------
    cards_by_type_html = []
    for typ, items in by_type.items():
        card_list = []
        for dev, df in sorted(items, key=lambda x: x[0]):
            card_list.append(device_value_card_html(dev, df, typ))
        cards_by_type_html.append(f'<div class="card"><h2>{typ}</h2><div class="val-grid">{"".join(card_list)}</div></div>')
    overview_cards_html = "".join(cards_by_type_html) if cards_by_type_html else '<div class="card">Keine Daten zum Anzeigen.</div>'

    # ---------- Diagramme (inkl. Sammel-Plot der NON_CONTENT) ----------
    type_cards_html = []
    for typ, items in by_type.items():
        device_tiles = []
        preferred = PREFERRED_BY_TYPE.get(typ, PREFERRED_BY_TYPE["Other"])
        for dev, df in sorted(items, key=lambda x: x[0]):
            numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in {"f_port"}]
            misc_cols = [c for c in ("battery","rssi","snr","vdc_input_v","idc_input_ma") if c in numeric_cols]
            plots = []
            if misc_cols:
                m = to_plot_multi_html(df, misc_cols, "Betrieb (Battery/RSSI/SNR/Power)")
                if m: plots.append(f'<div class="plot-wrap">{m}</div>')
            ordered = [c for c in preferred if c in numeric_cols and c not in misc_cols] \
                    + [c for c in numeric_cols if c not in set(preferred) | set(misc_cols)]
            used = set()
            for col in ordered:
                if col in used: continue
                html_plot = to_plot_html(df, col, f"{col}")
                if html_plot:
                    plots.append(f'<div class="plot-wrap">{html_plot}</div>')
                    used.add(col)
                if len(plots) >= 4:  # 1 Sammel + bis zu 3 Einzel
                    break
            if not plots:
                plots.append('<div class="plot-wrap"><em>No numeric fields found.</em></div>')
            device_tiles.append(f'<div class="device-tile"><h4>{dev}</h4>{"".join(plots)}</div>')
        type_cards_html.append(f'<div class="card"><h2>{typ}</h2><div class="device-grid">{"".join(device_tiles)}</div></div>')

    # ---------- Tabs + Template-Renderer ----------
    import re
    def _render_template(tpl: str, ctx: dict) -> str:
        pattern = re.compile(r"{{\s*([A-Z0-9_]+)\s*}}")
        return pattern.sub(lambda m: str(ctx.get(m.group(1), "")), tpl)

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    overview_table_html = ov[["device_id","records","last_seen_utc","status"]].sort_values("device_id") \
        .to_html(index=False, escape=False)

    tabs_js = """
    <script>
      const tabs = document.querySelectorAll('.tabs button');
      const sections = { overview: document.getElementById('tab-overview'),
                         charts:   document.getElementById('tab-charts'),
                         debug:    document.getElementById('tab-debug'),
                         info:     document.getElementById('tab-info') };
      tabs.forEach(btn => btn.addEventListener('click', () => {
        tabs.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        for (const k in sections) sections[k].hidden = (btn.dataset.tab !== k);
      }));
    </script>
    """

    ctx = {
        "STYLE": style,
        "STAMP": stamp,
        "APP": APP,
        "REG": REG,
        "AFTER_DAYS": AFTER_DAYS,
        "AFTER": AFTER,
        "OVERVIEW_CARDS": overview_cards_html,
        "OVERVIEW_TABLE": overview_table_html,
        "TYPE_CARDS": "".join(type_cards_html) if type_cards_html else '<div class="card">Keine Daten zum Anzeigen.</div>',
        "DEBUG_CARDS": "".join(debug_cards) if debug_cards else "<div class='card'><i>Keine aktuellen Daten.</i></div>",
        "TABS_JS": tabs_js
    }

    # Template-Kandidaten: lokale Overrides > templates/ > root
    tpl_candidates = [
        ASSETS / "templates/dashboard_template.local.html",
        ASSETS / "dashboard_template.local.html",
        ASSETS / "templates/dashboard_template.html",
        ASSETS / "dashboard_template.html",
    ]
    tpl_path = next((p for p in tpl_candidates if p.exists()), None)

    if tpl_path:
        tpl_html = tpl_path.read_text(encoding="utf-8")
        html = _render_template(tpl_html, ctx)
    else:
        # eingebaute helle Fallback-Variante
        html = f"""<!doctype html>
        <meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
        <title>TTN – All Devices (grouped by sensor type)</title>
        {style}

        <h1>TTN Dashboard – Grouped by Sensor Type</h1>
        <small>As of: {stamp} • Source: TTN Storage ({APP}@{REG}) • Window: last {AFTER_DAYS} days • AFTER={AFTER}</small>

        <div class="tabs">
          <button class="active" data-tab="overview">Übersicht</button>
          <button data-tab="charts">Diagramme</button>
          <button data-tab="debug">Debug</button>
          <button data-tab="info">Info</button>
        </div>

        <section id="tab-overview">
          <div class="card"><h2>Übersicht (aktuelle Werte)</h2>{overview_cards_html}</div>
          <div class="card"><h2>Geräte-Tabelle</h2>{overview_table_html}
            <p style="margin-top:8px">Parquet: <code>data/&lt;device&gt;.parquet</code> • CSV: <code>data/&lt;device&gt;.csv</code> • NDJSON: <code>data/&lt;device&gt;_raw.ndjson</code></p>
          </div>
        </section>

        <section id="tab-charts" hidden>
          <div class="type-grid">
            {"".join(type_cards_html) if type_cards_html else '<div class="card">Keine Daten zum Anzeigen.</div>'}
          </div>
        </section>

        <section id="tab-debug" hidden>
          {"".join(debug_cards) if debug_cards else "<div class='card'><i>Keine aktuellen Daten.</i></div>"}
        </section>

        <section id="tab-info" hidden>
          <div class="card"><h2>Info</h2><p>Lege <code>assets/templates/dashboard_template.html</code> an, um dieses Fallback zu ersetzen.</p></div>
        </section>

        {tabs_js}
        """

    # --- Debug-Seite schreiben (nur recent) ---
    dbg = f"""<!doctype html><meta charset="utf-8"><title>Debug (recent)</title>
    <style>body{{font-family:system-ui;margin:20px}} .card{{border:1px solid #eee;border-radius:12px;padding:12px;margin:12px 0}}</style>
    <h1>Debug – letzte {DEBUG_RECENT_MINUTES} Minuten</h1>
    {"".join(debug_cards) if debug_cards else "<div class='card'><i>Keine aktuellen Daten.</i></div>"}
    """

    # Write HTML files → in ASSETS_BUILD (Runtime)
    (ASSETS_BUILD / "data.html").write_text(html, encoding="utf-8")
    (ASSETS_BUILD / "debug.html").write_text(dbg, encoding="utf-8")

    # ===== Health report (text + CSV) =====
    import re as _re
    inc_re = _re.compile(DEV_INCLUDE)
    exc_re = _re.compile(DEV_EXCLUDE) if DEV_EXCLUDE else None

    ov_for_health = ov.copy()
    if not ov_for_health.empty:
        ov_for_health["last_seen_utc"] = pd.to_datetime(ov_for_health["last_seen_utc"], utc=True, errors="coerce")

    health_rows = []
    for _, row in ov_for_health.iterrows():
        dev = row["device_id"]
        if not inc_re.search(dev):
            continue
        if exc_re and exc_re.search(dev):
            continue

        last_seen = row.get("last_seen_utc")
        records   = int(row.get("records", 0))

        status = "OK"
        last_seen_str = None
        if pd.isna(last_seen):
            status = "NO DATA"
            last_seen_str = "–"
        else:
            last_seen_str = last_seen.strftime("%Y-%m-%d %H:%M:%SZ")
            age_h = (datetime.now(timezone.utc) - last_seen).total_seconds() / 3600
            if age_h > STALE_HOURS:
                status = f"STALE ({age_h:.1f}h)"

        health_rows.append({
            "device_id": dev,
            "records": records,
            "last_seen": last_seen_str,
            "status": status
        })

    # Human-readable text summary
    health_txt = "\n".join(
        f"{r['device_id']:24s} | {r['records']:5d} rec | last: {r['last_seen'] or '–':20s} | {r['status']}"
        for r in health_rows
    )
    (ASSETS_BUILD / "devices_used.txt").write_text(health_txt, encoding="utf-8")

    # CSV for machines
    pd.DataFrame(health_rows).to_csv(ASSETS_BUILD / "devices_used.csv", index=False)

# ========= Smoke-test / CLI =========
if __name__ == "__main__":
    import argparse, sys
    # In dashboard mode: only render, do not run smoke-test
    if os.environ.get("RUN_DASH", "1") == "1":
        sys.exit(0)

    parser = argparse.ArgumentParser(description="TTN Storage Pull – Smoke Test")
    parser.add_argument("--device","-d")
    parser.add_argument("--hours","-H", type=int, default=None)
    parser.add_argument("--verbose","-v", action="store_true")
    args = parser.parse_args()

    if args.hours is not None:
        AFTER = (datetime.now(timezone.utc) - timedelta(hours=args.hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
        if args.verbose:
            print(f"[TEST] Override AFTER -> {AFTER}")

    devs = DEVS[:]
    if args.device:
        devs = [args.device]
    if not devs:
        print("[TEST] No devices found in DEVS. Set ENV DEVICES='dev1 dev2' or use --device.", file=sys.stderr)
        sys.exit(2)

    print(f"[TEST] APP={APP} REG={REG} AFTER={AFTER} (AFTER_DAYS={AFTER_DAYS})")
    print(f"[TEST] Devices: {devs}")

    dev = devs[0]
    parq = DATA / f"{dev}.parquet"
    before_rows = None
    if parq.exists():
        try:
            before_rows = len(pd.read_parquet(parq))
        except Exception:
            before_rows = None

    if args.verbose:
        print(f"[{dev}] Start pull… (existing parquet rows: {before_rows})")

    df = device_pull(dev)

    after_rows = None
    if parq.exists():
        try:
            after_rows = len(pd.read_parquet(parq))
        except Exception:
            after_rows = None

    print(f"[{dev}] Pull OK. df_returned={len(df)} rows; parquet before={before_rows}, after={after_rows}")

    # Show tail & last timestamp
    if not df.empty:
        try:
            last_ts = pd.to_datetime(df["received_at"], utc=True, errors="coerce").max()
            print(f"[{dev}] last timestamp (UTC): {last_ts}")
            print(f"[{dev}] columns: {list(df.columns)[:12]}{' …' if len(df.columns)>12 else ''}")
            print(df.tail(3).to_string(index=False))
        except Exception as e:
            print(f"[{dev}] note: could not render preview: {e}")

    if df.empty:
        print(f"[{dev}] No data from storage in selected window.")
        print("  -> Check: device ID, Data Storage enabled, API key rights, retention/window?")
