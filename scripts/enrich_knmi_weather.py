from __future__ import annotations

from pathlib import Path
import re

import numpy as np
import pandas as pd


WEATHER_FILE_GLOB = "team_data/weather/*.csv"
KNMI_COLUMNS = ["STN", "YYYYMMDD", "HH", "FH", "T", "DR", "RH", "P", "U", "M", "R", "S", "O", "Y"]
WEATHER_OUTPUT_COLUMNS = [
    "weather_station_code",
    "weather_home_city",
    "weather_source_file",
    "walk_weather_timestamp_end_utc",
    "walk_temp_c",
    "walk_wind_m_s",
    "walk_precip_mm",
    "walk_precip_raw_0_1mm",
    "walk_trace_precip_flag",
    "bedtime_weather_timestamp_end_utc",
    "bedtime_temp_c",
    "bedtime_wind_m_s",
    "bedtime_precip_mm",
    "bedtime_precip_raw_0_1mm",
    "bedtime_trace_precip_flag",
]


def ensure_single_weather_file() -> Path:
    files = sorted(Path().glob(WEATHER_FILE_GLOB))
    if not files:
        raise FileNotFoundError(
            "No KNMI weather CSV was found under team_data/weather. "
            "Add exactly one weather file such as team_data/weather/knmi_hourly_eindhoven_370.csv."
        )
    if len(files) > 1:
        names = ", ".join(path.as_posix() for path in files)
        raise FileExistsError(
            "Multiple weather CSV files were found under team_data/weather. "
            f"Keep exactly one active KNMI file there. Found: {names}"
        )
    return files[0]


def format_utc_iso(series: pd.Series) -> pd.Series:
    formatted = series.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return formatted.str.replace(r"([+-]\d{2})(\d{2})$", r"\1:\2", regex=True)


def timestamp_to_hour_end_iso(local_series: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(local_series, utc=True, errors="coerce")
    ceiled = timestamps.dt.ceil("h")
    return format_utc_iso(ceiled).where(timestamps.notna(), None)


def load_knmi_weather(weather_file: Path) -> pd.DataFrame:
    weather_df = pd.read_csv(
        weather_file,
        comment="#",
        skip_blank_lines=True,
        header=None,
        names=KNMI_COLUMNS,
        skipinitialspace=True,
    )
    weather_df = weather_df.apply(pd.to_numeric, errors="coerce")
    weather_df = weather_df.dropna(subset=["STN", "YYYYMMDD", "HH"]).copy()

    weather_df["STN"] = weather_df["STN"].astype(int)
    weather_df["YYYYMMDD"] = weather_df["YYYYMMDD"].astype(int)
    weather_df["HH"] = weather_df["HH"].astype(int)

    date_utc = pd.to_datetime(weather_df["YYYYMMDD"].astype(str), format="%Y%m%d", utc=True)
    weather_df["timestamp_end_utc_dt"] = date_utc + pd.to_timedelta(weather_df["HH"], unit="h")
    weather_df["timestamp_end_utc"] = format_utc_iso(weather_df["timestamp_end_utc_dt"])

    weather_df["wind_m_s"] = weather_df["FH"] / 10.0
    weather_df["temp_c"] = weather_df["T"] / 10.0
    weather_df["precip_raw_0_1mm"] = weather_df["RH"]
    weather_df["trace_precip_flag"] = weather_df["precip_raw_0_1mm"].eq(-1)
    weather_df["precip_mm"] = np.where(
        weather_df["trace_precip_flag"],
        0.0,
        weather_df["precip_raw_0_1mm"] / 10.0,
    )

    return weather_df


def load_setup_rows() -> pd.DataFrame:
    setup_files = sorted(Path("metadata").glob("*_study_setup.csv"))
    if not setup_files:
        raise FileNotFoundError("No participant setup files were found under metadata/*_study_setup.csv")

    rows = []
    for path in setup_files:
        setup_df = pd.read_csv(path, dtype=str).fillna("")
        if setup_df.empty:
            continue
        row = setup_df.iloc[0].to_dict()
        row["setup_path"] = path.as_posix()
        rows.append(row)

    setup_rows = pd.DataFrame(rows)
    if setup_rows.empty:
        raise ValueError("Participant setup files were found, but none contained usable rows.")
    return setup_rows


def build_weather_lookup(weather_df: pd.DataFrame, station_code: int) -> pd.DataFrame:
    station_weather = weather_df[weather_df["STN"].eq(station_code)].copy()
    if station_weather.empty:
        available = sorted(weather_df["STN"].dropna().astype(int).unique().tolist())
        raise ValueError(
            f"Requested KNMI station {station_code}, but the weather file only contains stations: {available}"
        )

    return station_weather[
        [
            "timestamp_end_utc",
            "temp_c",
            "wind_m_s",
            "precip_mm",
            "precip_raw_0_1mm",
            "trace_precip_flag",
        ]
    ].drop_duplicates(subset=["timestamp_end_utc"])


def enrich_participant_study_file(
    study_path: Path,
    setup_row: pd.Series,
    weather_df: pd.DataFrame,
    weather_file: Path,
) -> Path:
    study_df = pd.read_csv(study_path)

    participant_id = setup_row["participant_id"]
    home_city = setup_row.get("home_city", "").strip()
    station_code_text = setup_row.get("knmi_station_code", "").strip()
    if not station_code_text:
        raise ValueError(f"{participant_id} is missing knmi_station_code in {setup_row['setup_path']}")
    station_code = int(station_code_text)

    weather_lookup = build_weather_lookup(weather_df, station_code)

    study_df["weather_station_code"] = station_code
    study_df["weather_home_city"] = home_city
    study_df["weather_source_file"] = weather_file.as_posix()

    study_df["walk_weather_timestamp_end_utc"] = timestamp_to_hour_end_iso(study_df["matched_walk_end_local"])
    study_df["bedtime_weather_timestamp_end_utc"] = timestamp_to_hour_end_iso(study_df["bedtime_local"])

    walk_lookup = weather_lookup.rename(
        columns={
            "timestamp_end_utc": "walk_weather_timestamp_end_utc",
            "temp_c": "walk_temp_c",
            "wind_m_s": "walk_wind_m_s",
            "precip_mm": "walk_precip_mm",
            "precip_raw_0_1mm": "walk_precip_raw_0_1mm",
            "trace_precip_flag": "walk_trace_precip_flag",
        }
    )
    bedtime_lookup = weather_lookup.rename(
        columns={
            "timestamp_end_utc": "bedtime_weather_timestamp_end_utc",
            "temp_c": "bedtime_temp_c",
            "wind_m_s": "bedtime_wind_m_s",
            "precip_mm": "bedtime_precip_mm",
            "precip_raw_0_1mm": "bedtime_precip_raw_0_1mm",
            "trace_precip_flag": "bedtime_trace_precip_flag",
        }
    )

    study_df = study_df.merge(walk_lookup, on="walk_weather_timestamp_end_utc", how="left")
    study_df = study_df.merge(bedtime_lookup, on="bedtime_weather_timestamp_end_utc", how="left")

    output_path = study_path.with_name("study_dataset_weather.csv")
    study_df.to_csv(output_path, index=False)
    return output_path


def write_group_weather_outputs(participant_weather_paths: list[Path]) -> list[Path]:
    participant_frames = []
    for path in participant_weather_paths:
        df = pd.read_csv(path)
        participant_frames.append(df[["participant_id", "night_date_local", *WEATHER_OUTPUT_COLUMNS]])

    weather_lookup = pd.concat(participant_frames, ignore_index=True).drop_duplicates(
        subset=["participant_id", "night_date_local"]
    )

    written_paths: list[Path] = []
    for input_name, output_name in (
        ("group_study_dataset.csv", "group_study_dataset_weather.csv"),
        ("group_primary_analysis_dataset.csv", "group_primary_analysis_dataset_weather.csv"),
    ):
        input_path = Path("processed/group") / input_name
        if not input_path.exists():
            raise FileNotFoundError(f"Expected group dataset at {input_path.as_posix()}")
        group_df = pd.read_csv(input_path)
        merged = group_df.merge(weather_lookup, on=["participant_id", "night_date_local"], how="left")
        output_path = input_path.with_name(output_name)
        merged.to_csv(output_path, index=False)
        written_paths.append(output_path)

    return written_paths


def write_parsed_weather(weather_df: pd.DataFrame) -> Path:
    output_dir = Path("processed/weather")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "knmi_hourly_eindhoven_370_parsed.csv"
    parsed = weather_df[
        [
            "STN",
            "YYYYMMDD",
            "HH",
            "timestamp_end_utc",
            "wind_m_s",
            "temp_c",
            "precip_mm",
            "precip_raw_0_1mm",
            "trace_precip_flag",
            "P",
            "U",
            "R",
        ]
    ].copy()
    parsed.to_csv(output_path, index=False)
    return output_path


def main() -> None:
    weather_file = ensure_single_weather_file()
    weather_df = load_knmi_weather(weather_file)
    setup_rows = load_setup_rows()

    participant_weather_paths: list[Path] = []
    for row in setup_rows.itertuples(index=False):
        participant_id = row.participant_id
        study_path = Path("processed") / participant_id / "study_dataset.csv"
        if not study_path.exists():
            raise FileNotFoundError(
                f"Expected participant study dataset at {study_path.as_posix()} for {participant_id}. "
                "Run the personal and group pipeline first."
            )
        participant_weather_paths.append(
            enrich_participant_study_file(study_path, pd.Series(row._asdict()), weather_df, weather_file)
        )

    parsed_weather_path = write_parsed_weather(weather_df)
    group_weather_paths = write_group_weather_outputs(participant_weather_paths)

    print(f"Wrote {parsed_weather_path.as_posix()}")
    for path in participant_weather_paths:
        print(f"Wrote {path.as_posix()}")
    for path in group_weather_paths:
        print(f"Wrote {path.as_posix()}")


if __name__ == "__main__":
    main()
