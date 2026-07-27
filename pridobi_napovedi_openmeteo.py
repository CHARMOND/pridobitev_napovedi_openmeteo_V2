"""
Pridobivanje zgodovinskih napovedi iz Open-Meteo Single Runs API
=================================================================

Za vse tri modele (best_match/ecmwf_ifs, icon_d2, icon_eu) HKRATI, dan za dnem:
  - za vsak dan poskusimo VSE tri modele, preden gremo na naslednji dan
  - EN klic na posamezen tek zajame lokacije po blokih (GET ali POST, samodejno
    glede na dolžino URL-ja - GET za majhno število lokacij, POST za veliko)
  - iz vrnjene urne serije izluščimo vrednosti pri run+1h, +2h, +3h, +6h, +24h
  - rezultat sproti shranjujemo v output/<model>.csv, napredek pa v output/<model>.done

Namestitev:
    pip install requests pandas --break-system-packages

Vhod:
    CSV (ločen s podpičjem ";") s stolpci: UniqID, Longitude, Latitude
    (Longitude/Latitude so lahko celoštevilske *1e6 - npr. 14070300 = 14.0703 -
    koda to sama zazna in pretvori; če so že decimalne, jih pusti pri miru)
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import pandas as pd
import requests
import urllib3

BASE_URL = "https://single-runs-api.open-meteo.com/v1/forecast"

# --- SSL preverjanje ------------------------------------------------------
VERIFY_SSL = False

if VERIFY_SSL is False:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Spremenljivke, ki jih pridobivamo ---------------------------------
HOURLY_VARS = ["temperature_2m", "shortwave_radiation", "precipitation"]

# --- Horizonti -----------------------------------------------------------
HORIZONS = [1, 2, 3, 6, 24]
MAX_HORIZON = max(HORIZONS)

# --- Modeli ---------------------------------------------------------------
MODELS = {
    "best_match": {
        "model_param": "ecmwf_ifs",
        "run_hours": [0, 6, 12, 18],
        "start_date": "2026-04-01",
    },
    "icon_d2": {
        "model_param": "icon_d2",
        "run_hours": [0, 3, 6, 9, 12, 15, 18, 21],
        "start_date": "2026-04-02",

    },
    "icon_eu": {
        "model_param": "icon_eu",
        "run_hours": [0, 3, 6, 9, 12, 15, 18, 21],
        "start_date": "2026-04-02",
    },
}

END_DATE = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

# POZOR: preimenovano v ASCII-only ime, da se izognemo morebitnim težavam z
# Unicode normalizacijo imen datotek (š/č/ž) med Windows in GitHub Actions
# (Ubuntu). Preimenujte tudi dejansko datoteko v repozitoriju v "koordinate.csv",
# ali spremenite spodnjo vrstico nazaj na svoje dejansko ime.
STATIONS_CSV = "koordinate_manjse.csv"  # CSV s podpičjem: UniqID;Longitude;Latitude # za 14 lokacij
# STATIONS_CSV = "koordinate_kvadratov.csv"  # CSV s podpičjem: WPointID;longitude;latitude
OUTPUT_DIR = "output1" # za 14 lokacij: output1

# Če je postaj veliko (100+), manjši bloki + daljši timeout preprečujejo, da bi
# ena zahteva na strežniku trajala predolgo (glej diagnozo o "1 concurrent
# request / queue 6" iz prejšnjih zagonov). Če je postaj malo (<30), to sploh
# ni pomembno - vse gredo v en blok.s
LOCATION_BATCH_SIZE = 50 # za 14 lokacij: 50
TIMEOUT_SEC = 120

REQUEST_PAUSE_SEC = 2.0
MAX_RETRIES = 2
RETRY_BASE_WAIT_SEC = 10

DAILY_QUOTA_KEYWORDS = ("daily", "per day", "tomorrow")

COLS = ["TimeStampUTC", "WPointID", "tplusH", "GHI", "temp", "Rainfall", "latitude", "longitude"]

COLS = ["TimeStampUTC", "WPointID", "tplusH", "GHI", "temp", "Rainfall", "latitude", "longitude"]


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def load_stations(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", decimal=".")
    df = df.rename(columns={"UniqID": "WPointID", "Latitude": "latitude", "Longitude": "longitude"})
    print(df.head())
    required = {"WPointID", "latitude", "longitude"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Manjkajo stolpci v {path}: {missing}")

    # Koordinate so lahko celoštevilske (*1e6) - npr. 14070300 = 14.0703.
    # Če so že decimalne (majhne vrednosti, npr. <180), pretvorbe ne naredimo.
    if df["longitude"].abs().max() > 1000:
        df["longitude"] = df["longitude"] / 1_000_000
    if df["latitude"].abs().max() > 1000:
        df["latitude"] = df["latitude"] / 1_000_000

    return df[["WPointID", "latitude", "longitude"]]


def daterange(start_date: str, end_date: str):
    d = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    while d <= end:
        yield d
        d += timedelta(days=1)


# --- Samoprilagodljivo omejevanje hitrosti -------------------------------
_rate_state = {"pause": REQUEST_PAUSE_SEC, "streak": 0}
RATE_MIN_PAUSE = REQUEST_PAUSE_SEC
RATE_MAX_PAUSE = 25.0
RATE_INCREASE_FACTOR = 1.6
RATE_DECREASE_FACTOR = 0.85
RATE_STREAK_FOR_DECREASE = 25


def _rate_on_429():
    old = _rate_state["pause"]
    _rate_state["pause"] = min(RATE_MAX_PAUSE, _rate_state["pause"] * RATE_INCREASE_FACTOR)
    _rate_state["streak"] = 0
    if _rate_state["pause"] != old:
        print(f"  [RATE] Povečujem premor med klici: {old:.1f}s -> {_rate_state['pause']:.1f}s")


def _rate_on_success():
    _rate_state["streak"] += 1
    if _rate_state["streak"] >= RATE_STREAK_FOR_DECREASE and _rate_state["pause"] > RATE_MIN_PAUSE:
        old = _rate_state["pause"]
        _rate_state["pause"] = max(RATE_MIN_PAUSE, _rate_state["pause"] * RATE_DECREASE_FACTOR)
        _rate_state["streak"] = 0
        print(f"  [RATE] Zmanjšujem premor med klici: {old:.1f}s -> {_rate_state['pause']:.1f}s")


_DEBUG_SHOWN = {}
MAX_DEBUG_PRINTS_PER_MODEL = 8


def _debug_once(key: str, label: str, status_code, text: str, params: dict = None):
    shown = _DEBUG_SHOWN.get(key, 0)
    if shown < MAX_DEBUG_PRINTS_PER_MODEL:
        print(f"  [DIAGNOSTIKA - {label}, {key}, status={status_code}] {text[:300]}")
        if params is not None:
            print(f"  [DIAGNOSTIKA - poslani parametri] run={params.get('run')} "
                  f"past_hours={params.get('past_hours')} forecast_hours={params.get('forecast_hours')} "
                  f"models={params.get('models')}")
        _DEBUG_SHOWN[key] = shown + 1
        if _DEBUG_SHOWN[key] == MAX_DEBUG_PRINTS_PER_MODEL:
            print(f"  [DIAGNOSTIKA za {key} - nadaljnji izpisi utišani]")


def fetch_run(session: requests.Session, lat_list, lon_list, model_param: str, run_dt: datetime):
    """
    Vrne:
      dict/list        - uspešen odgovor
      None              - prehodna napaka / neveljaven tek, smiselno preskočiti
      dict/list        - uspešen odgovor
      None              - prehodna napaka / neveljaven tek, smiselno preskočiti
      "FATAL_ERROR"     - napačen model/parameter (400/422), ustavi ta model
      "QUOTA_EXCEEDED"  - dnevna kvota izčrpana, ustavi CEL backfill
    """
    params = {
        "latitude": ",".join(f"{x:.4f}" for x in lat_list),
        "longitude": ",".join(f"{x:.4f}" for x in lon_list),
        "hourly": ",".join(HOURLY_VARS),
        "models": model_param,
        "run": run_dt.strftime("%Y-%m-%dT%H:%M"),
        "past_hours": 0,
        "forecast_hours": MAX_HORIZON + 1,
        "timezone": "UTC",
    }

    # Samodejno GET ali POST glede na dolžino URL-ja - GET za majhno število
    # lokacij, POST (v telesu, ne v URL-ju) če bi URL presegel varno dolžino
    # (preprečuje 414 Request-URI Too Large pri večjem številu postaj).
    query_string = urlencode(params)
    full_url_len = len(BASE_URL) + 1 + len(query_string)
    use_post = full_url_len > 3500

    for attempt in range(MAX_RETRIES):
        try:
            if use_post:
                r = session.post(BASE_URL, data=params, timeout=TIMEOUT_SEC)
            else:
                r = session.get(BASE_URL, params=params, timeout=TIMEOUT_SEC)
        except requests.exceptions.Timeout:
            wait_time = TIMEOUT_SEC
            print(f"  [TIMEOUT po {TIMEOUT_SEC}s] Čakam še {wait_time}s...")
            time.sleep(wait_time)
            continue
        except requests.RequestException as e:
            print(f"  [NAPAKA POVEZAVE] {type(e).__name__}: {e}")
            time.sleep(2 ** attempt)
            continue

        if r.status_code != 200:
            text_lower = r.text.lower()
            if any(kw in text_lower for kw in DAILY_QUOTA_KEYWORDS):
                print(f"  [DNEVNA KVOTA IZČRPANA] {r.text[:200]}")
                return "QUOTA_EXCEEDED"

        if r.status_code == 429:
            _rate_on_429()
            wait_time = RETRY_BASE_WAIT_SEC * (attempt + 1)
            print(f"  [429 Too Many Requests] Čakam {wait_time}s (poskus {attempt + 1}/{MAX_RETRIES})...")
            time.sleep(wait_time)
            continue

        if r.status_code in (400, 422):
            print(f"  [KRITIČNA NAPAKA {r.status_code}] Odziv: {r.text[:300]}")
            print(f"  [Poslan URL] {r.url}")
            return "FATAL_ERROR"

        if r.status_code != 200:
            _debug_once(model_param, f"nepredviden status {r.status_code}", r.status_code, r.text, params)
            return None

        try:
            payload = r.json()
        except ValueError:
            _debug_once(model_param, "odgovor ni veljaven JSON", r.status_code, r.text, params)
            return None

        if isinstance(payload, dict) and payload.get("error"):
            _debug_once(model_param, "status 200, a JSON vsebuje error=true", r.status_code, r.text, params)
            return None

        _rate_on_success()
        _rate_on_success()
        return payload

    # Vsi poskusi izčrpani (npr. ponavljajoč se 429) - obravnavamo kot
    # prehodno manjkajoč podatek, ne ustavimo cele skripte zaradi tega.
    return None


def parse_response(data, station_ids, run_dt: datetime):
    if data is None or isinstance(data, str):
        return []
    if isinstance(data, dict) and "error" in data:
        return []
    if isinstance(data, dict) and "hourly" in data:
        data = [data]

    rows = []
    for wid, loc in zip(station_ids, data):
        if not isinstance(loc, dict) or "hourly" not in loc:
            continue
        hourly = loc["hourly"]
        times = hourly.get("time")
        if not times:
            continue
        idx_by_time = {t: i for i, t in enumerate(times)}
        ghi_arr = hourly.get("shortwave_radiation", [])
        temp_arr = hourly.get("temperature_2m", [])
        rain_arr = hourly.get("precipitation", [])
        for h in HORIZONS:
            target_time = run_dt + timedelta(hours=h)
            key = target_time.strftime("%Y-%m-%dT%H:%M")
            i = idx_by_time.get(key)
            if i is None:
                continue
            rows.append({
                "TimeStampUTC": target_time,
                "WPointID": wid,
                "tplusH": h,
                "GHI": ghi_arr[i] if i < len(ghi_arr) else None,
                "temp": temp_arr[i] if i < len(temp_arr) else None,
                "Rainfall": rain_arr[i] if i < len(rain_arr) else None,
                "latitude": loc.get("latitude"),
                "longitude": loc.get("longitude"),
            })
    return rows


def checkpoint_path(model_key: str) -> str:
    return os.path.join(OUTPUT_DIR, f"{model_key}.done")


def read_checkpoint(model_key: str):
    path = checkpoint_path(model_key)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        content = f.read().strip()
    return content or None


def write_checkpoint(model_key: str, day: datetime):
    with open(checkpoint_path(model_key), "w") as f:
        f.write(day.strftime("%Y-%m-%d"))


def process_one_day(session, model_key, cfg, day, station_batches):
    day_rows = []
    for hour in cfg["run_hours"]:
        run_dt = day.replace(hour=hour, tzinfo=timezone.utc)
        for batch in station_batches:
            lat_list = [s["latitude"] for s in batch]
            lon_list = [s["longitude"] for s in batch]
            wid_list = [s["WPointID"] for s in batch]

            data = fetch_run(session, lat_list, lon_list, cfg["model_param"], run_dt)


            if data == "FATAL_ERROR":
                return "FATAL", day_rows
            if data == "QUOTA_EXCEEDED":
                return "QUOTA", day_rows

            time.sleep(_rate_state["pause"])
            rows = parse_response(data, wid_list, run_dt)
            if data is not None and not rows:
                key = model_key + "_norows"
                shown = _DEBUG_SHOWN.get(key, 0)
                if shown < MAX_DEBUG_PRINTS_PER_MODEL:
                    sample_times = None
                    if isinstance(data, list) and data and isinstance(data[0], dict):
                        sample_times = data[0].get("hourly", {}).get("time", [])[:3]
                    elif isinstance(data, dict):
                        sample_times = data.get("hourly", {}).get("time", [])[:3]
                    print(f"  [DIAGNOSTIKA - 0 vrstic kljub odgovoru, {model_key}] "
                          f"zahtevan run={run_dt.isoformat()}, prve vrnjene ure: {sample_times}")
                    _DEBUG_SHOWN[key] = shown + 1
            day_rows.extend(rows)
    return "OK", day_rows


def run_all_backfills(models: dict, stations: pd.DataFrame, session: requests.Session):
    station_batches = list(chunked(stations.to_dict("records"), LOCATION_BATCH_SIZE))

    state = {}
    for model_key, cfg in models.items():
        out_path = os.path.join(OUTPUT_DIR, f"{model_key}.csv")
        if not os.path.exists(out_path):
            pd.DataFrame(columns=COLS).to_csv(out_path, index=False)

        last_done = read_checkpoint(model_key)
        start_date = cfg["start_date"]
        if last_done:
            resume_from = (datetime.strptime(last_done, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            start_date = max(start_date, resume_from)
            print(f"[{model_key}] Najden checkpoint ({last_done}) - nadaljujem od {start_date}.")

        state[model_key] = {
            "out_path": out_path,
            "start_date": start_date,
            "dead": start_date > END_DATE,
            "total": 0,
        }
        if state[model_key]["dead"]:
            print(f"[{model_key}] Že v celoti pridobljeno do {END_DATE}. Nič za narediti.")

    active_models = {k: v for k, v in models.items() if not state[k]["dead"]}
    if not active_models:
        print("Vsi modeli so že v celoti pridobljeni.")
        return

    global_start = min(state[k]["start_date"] for k in active_models)

    for day in daterange(global_start, END_DATE):
        day_str = day.strftime("%Y-%m-%d")
        for model_key, cfg in active_models.items():
            st = state[model_key]
            if st["dead"]:
                continue
            if day_str < st["start_date"]:
                continue

            status, rows = process_one_day(session, model_key, cfg, day, station_batches)

            if status == "FATAL":
                print(f"[{model_key}] Model onemogočen zaradi napake v zahtevi - ostali se nadaljujejo.")
                st["dead"] = True
                continue

            if status == "QUOTA":
                if rows:
                    df_partial = pd.DataFrame(rows)[COLS].sort_values(["TimeStampUTC", "WPointID", "tplusH"])
                    df_partial.to_csv(st["out_path"], mode="a", header=False, index=False)
                    st["total"] += len(df_partial)
                print(f"\n[{model_key}] DNEVNA KVOTA IZČRPANA sredi dneva {day_str}.")
                print("Skripta se ustavlja. Poženite jo znova po resetu kvote.")
                sys.exit(0)

            if rows:
                df_day = pd.DataFrame(rows)[COLS].sort_values(["TimeStampUTC", "WPointID", "tplusH"])
                df_day.to_csv(st["out_path"], mode="a", header=False, index=False)
                st["total"] += len(df_day)
                print(f"[{model_key}] {day_str}: +{len(df_day)} vrstic (skupaj: {st['total']})")
            else:
                print(f"[{model_key}] {day_str}: 0 vrstic")

            write_checkpoint(model_key, day)
            write_checkpoint(model_key, day)

    print("\nVsi modeli dokončani do", END_DATE)

    print("\nVsi modeli dokončani do", END_DATE)


def preflight_check(session: requests.Session, model_param: str, test_stations: pd.DataFrame, run_dt: datetime = None):
    if run_dt is None:
        run_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        run_dt -= timedelta(hours=run_dt.hour % 6 + 6)

    data = fetch_run(session,
                      test_stations["latitude"].tolist(),
                      test_stations["longitude"].tolist(),
                      model_param, run_dt)

    rows = parse_response(data, test_stations["WPointID"].tolist(), run_dt)
    df = pd.DataFrame(rows)
    print(f"Preflight za model={model_param}, run={run_dt}:")
    if df.empty:
        print("  BREZ PODATKOV - preveri model_param, run uro ali domeno.")
    else:
        missing = set(test_stations["WPointID"]) - set(df["WPointID"])
        print(df.head())
        if missing:
            print(f"  Postaje BREZ podatkov (verjetno izven domene): {missing}")
    return df


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stations = load_stations(STATIONS_CSV)
    print(f"Naloženih {len(stations)} postaj:")
    print(stations)

    session = requests.Session()
    session.verify = VERIFY_SSL

    print("\n=== ZAGON PREFLIGHT PREVERJANJA ===")
    for model_key, cfg in MODELS.items():
        preflight_check(session, cfg["model_param"], stations)

    print("\nPreverite zgornje rezultate: so vrednosti GHI/temp/Rainfall smiselne "
          "za VSE tri modele? Če ne, PREKINITE (Ctrl+C).")

    try:
        input("\nPritisnite ENTER za nadaljevanje s polnim backfillom, ali Ctrl+C za prekinitev... ")
    except EOFError:
        print("\n[Ni interaktivnega vnosa - nadaljujem samodejno]")
    except KeyboardInterrupt:
        print("\nPrekinjeno s strani uporabnika.")
        sys.exit(0)

    print("\n=== Začenjam/nadaljujem backfill za vse modele (dan za dnem) ===")
    run_all_backfills(MODELS, stations, session)

    print("\n=== Konec zagona ===")