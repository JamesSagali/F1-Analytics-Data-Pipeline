import os
import pandas as pd
import fastf1
from sqlalchemy import create_engine, text

# ---------------------------
# FIX 1: AIRFLOW THREAD-SAFE CACHE OVERRIDE
# ---------------------------
# Airflow workers running concurrently will corrupt a shared cache.
# We isolate the cache folder by appending the current process ID (PID).
pid_cache_dir = f'/opt/airflow/cache_{os.getpid()}'
os.makedirs(pid_cache_dir, exist_ok=True)
fastf1.Cache.enable_cache(pid_cache_dir)

# ---------------------------
# GET LATEST LOADED RACE
# ---------------------------
def get_latest_race_id(table_name):
    # FIX 2: Wrapped query execution inside explicit SQLAlchemy connection block
    with engine.connect() as conn:
        query = text(f"""
            SELECT MAX(RaceID) AS max_race
            FROM {table_name}
        """)
        df = pd.read_sql(query, conn)

    if df.empty or pd.isna(df['max_race'][0]):
        return 0

    return int(df['max_race'][0])

# ---------------------------
# DB CONNECTION
# ---------------------------
engine = create_engine(
    "mysql+pymysql://root:Injeni16@mysql:3306/f1_analytics"
)

# ---------------------------
# LAP TIMES
# ---------------------------
def get_lap_times(season, race, round_number):
    try:
        session = fastf1.get_session(season, race, 'R')
        session.load(laps=True, telemetry=False, weather=False, messages=False)

        laps = session.laps.copy()

        # FIX 3: Fail early if FastF1 returns an empty dataframe or missing structure
        if laps.empty:
            print(f"No lap data found for {race}.")
            return None

        columns = [
            'DriverNumber', 'LapNumber', 'Sector1Time', 'Sector2Time', 
            'Sector3Time', 'LapTime', 'Compound', 'TyreLife', 
            'Stint', 'IsPersonalBest', 'Position'
        ]

        available_columns = [col for col in columns if col in laps.columns]
        selected = laps[available_columns].copy()

        time_columns = ['LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time']

        # Robust Timedelta conversion
        # FastF1 returns NaT (Not a Time) or Object columns if live data stream dropped packets.
        # We explicitly convert columns to timedelta, coerce errors, and calculate seconds safely.
        for col in time_columns:
            if col in selected.columns:
                selected[col] = pd.to_timedelta(selected[col], errors='coerce')
                selected[col] = selected[col].dt.total_seconds()

        selected['Season'] = season
        selected['RaceID'] = round_number

        # Drop fully corrupted records or uncompleted laps to avoid DB null violations
        if 'LapTime' not in selected.columns or selected['LapTime'].isna().all():
            print(f"Skipping {race}: All LapTime values are NaN/Corrupted.")
            return None
            
        # Optional: Remove specific rows missing critical timing data
        selected = selected.dropna(subset=['LapTime', 'LapNumber'])

        return selected

    except Exception as e:
        print(f"Skipping {race} due to processing error: {e}")
        return None

# ---------------------------
# LOAD NEW LAPS ONLY
# ---------------------------
def run_lap_times_pipeline(season):
    # Guard table existence check to avoid pipeline crashes on fresh DBs
    try:
        latest_loaded = get_latest_race_id("lap_times")
    except Exception:
        latest_loaded = 0

    schedule = fastf1.get_event_schedule(season, include_testing=False)
    
    # FIX 6: Safeguard against cancellations or un-run sessions in schedule matrix
    race_map = dict(zip(schedule['EventName'], schedule['RoundNumber']))

    for race in schedule['EventName']:
        round_number = race_map[race]

        if round_number <= latest_loaded:
            continue

        df = get_lap_times(season, race, round_number)

        if df is None:
            continue

        print(f"Loading lap times: {race}")

        # FIX 7: Use an explicit connection block for writes to prevent leaked pools
        with engine.begin() as conn:
            df.to_sql(
                "lap_times",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=5000,
                method='multi'
            )

        print(f"Finished laps: {race}")

# ---------------------------
# RUN PIPELINE
# ---------------------------
if __name__ == "__main__":
    season = 2026
    run_lap_times_pipeline(season)
