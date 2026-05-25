import pandas as pd
import fastf1
from sqlalchemy import create_engine, text

# ---------------------------
# DB CONNECTION
# ---------------------------
engine = create_engine(
    "mysql+pymysql://root:Injeni16@mysql:3306/f1_analytics"
)

# ---------------------------
# HELPER: DELETE EXISTING DATA (IDEMPOTENT LOAD)
# ---------------------------
def delete_existing_race(engine, table, race_id):
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table} WHERE RaceID = :race_id"),
            {"race_id": race_id}
        )

# ---------------------------
# RACE RESULTS
# ---------------------------
def get_race_results(season, race, round_number):

    session = fastf1.get_session(season, race, 'R')
    session.load()

    results = session.results.copy()

    selected = results[[
        'DriverNumber',
        'Abbreviation',
        'TeamId',
        'ClassifiedPosition',
        'GridPosition',
        'Status',
        'Time',
        'Laps',
        'Points'
    ]].copy()

    def format_time(row):
        time = row['Time']
        status = row['Status']

        if pd.isna(time):
            return "DNS" if status == 'Did not start' else "DNF"

        return f"{time.components.hours:02}:{time.components.minutes:02}:{time.components.seconds:02}.{int(time.components.milliseconds):03}"

    selected['Time'] = selected.apply(format_time, axis=1)

    selected['Points'] = selected['Points'].astype('Int64')
    selected['GridPosition'] = selected['GridPosition'].astype('Int64')

    selected = selected.copy()
    selected['Season'] = season
    selected['RaceID'] = round_number

    return selected


def run_race_results_pipeline(season):

    schedule = fastf1.get_event_schedule(season, include_testing=False)

    race_map = dict(zip(schedule['EventName'], schedule['RoundNumber']))

    for race in schedule['EventName']:
        round_number = race_map[race]

        try:
            df = get_race_results(season, race, round_number)

            delete_existing_race(engine, "race_results", round_number)

            df.to_sql(
                "race_results",
                con=engine,
                if_exists="append",
                index=False
            )

            print(f"Loaded race results: {race}")

        except Exception as e:
            print(f"Skipping {race}: {e}")


# ---------------------------
# LAP TIMES
# ---------------------------
def get_lap_times(season, race, round_number):

    try:
        session = fastf1.get_session(season, race, 'R')
        session.load()

        laps = session.laps.copy()

        selected = laps[[
            'DriverNumber',
            'LapNumber',
            'Sector1Time',
            'Sector2Time',
            'Sector3Time',
            'LapTime',
            'Compound',
            'TyreLife',
            'Stint',
            'IsPersonalBest',
            'Position'
        ]].copy()

        # convert times
        selected['LapTime'] = selected['LapTime'].dt.total_seconds()
        selected['Sector1Time'] = selected['Sector1Time'].dt.total_seconds()
        selected['Sector2Time'] = selected['Sector2Time'].dt.total_seconds()
        selected['Sector3Time'] = selected['Sector3Time'].dt.total_seconds()

        selected = selected.copy()
        selected['Season'] = season
        selected['RaceID'] = round_number

        if selected['LapTime'].isna().all():
            print(f"Skipping {race} (no lap data)")
            return None

        return selected

    except Exception as e:
        print(f"Skipping {race}: {e}")
        return None


def run_lap_times_pipeline(season):

    schedule = fastf1.get_event_schedule(season, include_testing=False)
    race_map = dict(zip(schedule['EventName'], schedule['RoundNumber']))

    for race in schedule['EventName']:
        round_number = race_map[race]

        df = get_lap_times(season, race, round_number)

        if df is None:
            continue

        delete_existing_race(engine, "lap_times", round_number)

        df.to_sql(
            "lap_times",
            con=engine,
            if_exists="append",
            index=False
        )

        print(f"Loaded lap times: {race}")


# ---------------------------
# RUN PIPELINE
# ---------------------------
if __name__ == "__main__":

    season = 2026

    run_race_results_pipeline(season)
    run_lap_times_pipeline(season)