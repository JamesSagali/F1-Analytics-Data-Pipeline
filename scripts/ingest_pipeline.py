import pandas as pd
import fastf1
from sqlalchemy import create_engine, text

# ---------------------------
# FASTF1 CACHE
# ---------------------------
fastf1.Cache.enable_cache('/opt/airflow/cache')

# ---------------------------
# DB CONNECTION
# ---------------------------
engine = create_engine(
    "mysql+pymysql://root:Injeni16@mysql:3306/f1_analytics"
)

# ---------------------------
# GET LATEST LOADED RACE
# ---------------------------
def get_latest_race_id(table_name):

    query = f"""
        SELECT MAX(RaceID) AS max_race
        FROM {table_name}
    """

    df = pd.read_sql(query, engine)

    if pd.isna(df['max_race'][0]):
        return 0

    return int(df['max_race'][0])

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

        return (
            f"{time.components.hours:02}:"
            f"{time.components.minutes:02}:"
            f"{time.components.seconds:02}."
            f"{int(time.components.milliseconds):03}"
        )

    selected['Time'] = selected.apply(format_time, axis=1)

    selected['Points'] = selected['Points'].astype('Int64')
    selected['GridPosition'] = selected['GridPosition'].astype('Int64')

    selected['Season'] = season
    selected['RaceID'] = round_number

    return selected

# ---------------------------
# LOAD NEW RACE RESULTS ONLY
# ---------------------------
def run_race_results_pipeline(season):

    latest_loaded = get_latest_race_id("race_results")

    schedule = fastf1.get_event_schedule(
        season,
        include_testing=False
    )

    race_map = dict(
        zip(schedule['EventName'], schedule['RoundNumber'])
    )

    for race in schedule['EventName']:

        round_number = race_map[race]

        # skip already loaded races
        if round_number <= latest_loaded:
            continue

        try:

            print(f"Loading race results: {race}")

            df = get_race_results(
                season,
                race,
                round_number
            )

            df.to_sql(
                "race_results",
                con=engine,
                if_exists="append",
                index=False,
                chunksize=1000,
                method='multi'
            )

            print(f"Finished: {race}")

        except Exception as e:
            print(f"Skipping {race}: {e}")

# ---------------------------
# RUN PIPELINE
# ---------------------------
if __name__ == "__main__":

    season = 2026

    run_race_results_pipeline(season)

