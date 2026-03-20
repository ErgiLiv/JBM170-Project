from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


RAW_DIR_DEFAULT = "20260309_8298429185_MiFitness_ams1_data_copy"


@dataclass(frozen=True)
class Paths:
    raw_dir: Path
    participant_id: str
    processed_dir: Path
    reports_dir: Path
    metadata_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build canonical JBM170 sleep and walk tables from a Mi Fitness raw export."
    )
    parser.add_argument(
        "--raw-dir",
        default=RAW_DIR_DEFAULT,
        help="Path to the raw Mi Fitness export directory.",
    )
    parser.add_argument(
        "--participant-id",
        default="P01",
        help="Pseudonymous participant id to use in generated outputs.",
    )
    return parser.parse_args()


def parse_offset_minutes(raw_value: object) -> int:
    if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
        return 0
    value = int(raw_value)
    if -48 <= value <= 56:
        return value * 15
    if -840 <= value <= 840:
        return value
    return 0


def first_present(*values: object) -> object | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        return value
    return None


def find_export_file(raw_dir: Path, suffix: str) -> Path:
    matches = sorted(raw_dir.glob(f"*{suffix}"))
    if not matches:
        raise FileNotFoundError(f"Could not find a file ending with '{suffix}' in {raw_dir}")
    if len(matches) > 1:
        raise FileExistsError(f"Expected one file ending with '{suffix}' in {raw_dir}, found {len(matches)}")
    return matches[0]


def ts_to_utc(ts: int | float | None) -> str | None:
    if ts is None or pd.isna(ts):
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def ts_to_local(ts: int | float | None, offset_minutes: int) -> str | None:
    if ts is None or pd.isna(ts):
        return None
    tz = timezone(timedelta(minutes=offset_minutes))
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(tz).isoformat()


def ts_to_local_date(ts: int | float | None, offset_minutes: int) -> str | None:
    local = ts_to_local(ts, offset_minutes)
    if local is None:
        return None
    return local[:10]


def load_sleep_sessions(paths: Paths) -> pd.DataFrame:
    file_path = find_export_file(paths.raw_dir, "_MiFitness_hlth_center_fitness_data.csv")
    raw = pd.read_csv(file_path)
    raw = raw[raw["Key"] == "sleep"].copy()

    records: list[dict[str, object]] = []
    for row in raw.itertuples(index=False):
        payload = json.loads(row.Value)
        offset_minutes = parse_offset_minutes(payload.get("timezone"))
        bedtime_ts = first_present(payload.get("bedtime"), payload.get("device_bedtime"))
        bed_timestamp_ts = first_present(payload.get("bed_timestamp"), bedtime_ts)
        wake_ts = first_present(payload.get("wake_up_time"), payload.get("device_wake_up_time"), row.Time)
        duration_min = payload.get("duration")
        light_min = payload.get("sleep_light_duration")
        deep_min = payload.get("sleep_deep_duration")
        rem_min = payload.get("sleep_rem_duration")
        stage_total_min = sum(
            int(value)
            for value in (light_min, deep_min, rem_min)
            if value is not None and not pd.isna(value)
        )

        records.append(
            {
                "participant_id": paths.participant_id,
                "session_source_time": int(row.Time),
                "session_source_time_utc": ts_to_utc(row.Time),
                "session_id": f"{paths.participant_id}_sleep_{int(row.Time)}",
                "timezone_offset_minutes": offset_minutes,
                "night_date_local": ts_to_local_date(bedtime_ts or wake_ts, offset_minutes),
                "bed_timestamp": bed_timestamp_ts,
                "bedtime_timestamp": bedtime_ts,
                "wake_timestamp": wake_ts,
                "bed_timestamp_local": ts_to_local(bed_timestamp_ts, offset_minutes),
                "bedtime_local": ts_to_local(bedtime_ts, offset_minutes),
                "wake_local": ts_to_local(wake_ts, offset_minutes),
                "total_sleep_time_min": duration_min,
                "raw_sleep_duration_field": payload.get("sleep_duration"),
                "time_in_bed_min": round(payload.get("bed_duration", 0) / 60, 2)
                if payload.get("bed_duration") is not None
                else None,
                "sleep_efficiency": payload.get("sleep_efficiency"),
                "awake_count": payload.get("awake_count"),
                "awake_duration_min": payload.get("sleep_awake_duration"),
                "light_sleep_min": light_min,
                "deep_sleep_min": deep_min,
                "rem_sleep_min": rem_min,
                "stage_total_min": stage_total_min,
                "duration_minus_stage_total_min": (
                    int(duration_min) - stage_total_min if duration_min is not None else None
                ),
                "avg_hr": payload.get("avg_hr"),
                "min_hr": payload.get("min_hr"),
                "max_hr": payload.get("max_hr"),
                "avg_breath": payload.get("avg_breath"),
                "has_stage": bool(payload.get("has_stage")),
                "has_rem": bool(payload.get("has_rem")),
                "item_count": len(payload.get("items", [])),
                "is_main_sleep_candidate": bool(payload.get("has_stage"))
                or (duration_min is not None and int(duration_min) >= 180),
                "is_short_or_fragment": not bool(payload.get("has_stage"))
                and (duration_min is None or int(duration_min) < 180),
            }
        )

    sleep_df = pd.DataFrame(records).sort_values(
        ["night_date_local", "is_main_sleep_candidate", "total_sleep_time_min", "bedtime_timestamp"],
        ascending=[True, False, False, True],
    )
    return sleep_df


def load_walk_sessions(paths: Paths) -> pd.DataFrame:
    file_path = find_export_file(paths.raw_dir, "_MiFitness_hlth_center_sport_record.csv")
    raw = pd.read_csv(file_path)
    raw = raw[raw["Key"] == "outdoor_walking"].copy()

    records: list[dict[str, object]] = []
    for row in raw.itertuples(index=False):
        payload = json.loads(row.Value)
        offset_minutes = parse_offset_minutes(payload.get("timezone"))
        start_ts = first_present(payload.get("start_time"), row.Time)
        end_ts = payload.get("end_time")
        duration_sec = payload.get("duration")
        records.append(
            {
                "participant_id": paths.participant_id,
                "walk_id": f"{paths.participant_id}_walk_{int(start_ts)}",
                "timezone_offset_minutes": offset_minutes,
                "walk_date_local": ts_to_local_date(start_ts, offset_minutes),
                "start_timestamp": start_ts,
                "end_timestamp": end_ts,
                "start_local": ts_to_local(start_ts, offset_minutes),
                "end_local": ts_to_local(end_ts, offset_minutes),
                "duration_min": round(int(duration_sec) / 60, 2) if duration_sec is not None else None,
                "steps": payload.get("steps"),
                "distance_m": payload.get("distance"),
                "avg_hrm": payload.get("avg_hrm"),
                "min_hrm": payload.get("min_hrm"),
                "max_hrm": payload.get("max_hrm"),
                "avg_speed_m_s": payload.get("avg_speed"),
                "avg_pace_raw": payload.get("avg_pace"),
                "calories": payload.get("calories"),
                "train_load": payload.get("train_load"),
                "train_effect": payload.get("train_effect"),
            }
        )

    walk_df = pd.DataFrame(records).sort_values(["start_timestamp"])
    return walk_df


def load_daily_sleep_reports(paths: Paths) -> pd.DataFrame:
    file_path = find_export_file(paths.raw_dir, "_MiFitness_hlth_center_aggregated_fitness_data.csv")
    raw = pd.read_csv(file_path)
    raw = raw[(raw["Tag"] == "daily_report") & (raw["Key"] == "sleep")].copy()

    records: list[dict[str, object]] = []
    for row in raw.itertuples(index=False):
        payload = json.loads(row.Value)
        segments = payload.get("segment_details", [])
        longest_segment = max(segments, key=lambda segment: segment.get("duration", -1), default={})
        offset_minutes = parse_offset_minutes(longest_segment.get("timezone"))
        bedtime_ts = longest_segment.get("bedtime")
        records.append(
            {
                "night_date_local": ts_to_local_date(bedtime_ts or row.Time, offset_minutes),
                "sleep_score": payload.get("sleep_score"),
                "nap_duration_min": payload.get("sleep_nap_duration"),
                "reported_total_duration_min": payload.get("total_duration"),
                "reported_long_duration_min": payload.get("total_long_duration"),
                "day_sleep_evaluation": payload.get("day_sleep_evaluation"),
                "long_sleep_evaluation": payload.get("long_sleep_evaluation"),
            }
        )

    return pd.DataFrame(records).drop_duplicates(subset=["night_date_local"])


def choose_main_sleep_sessions(sleep_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    main_sleep = sleep_df.drop_duplicates(subset=["night_date_local"], keep="first").copy()
    session_counts = (
        sleep_df.groupby("night_date_local", dropna=False)
        .agg(
            sleep_session_count=("session_id", "count"),
            extra_sleep_session_count=("session_id", lambda values: max(len(values) - 1, 0)),
            extra_sleep_duration_min=("total_sleep_time_min", lambda values: max(values.sum() - values.max(), 0)),
        )
        .reset_index()
    )
    main_sleep = main_sleep.merge(session_counts, on="night_date_local", how="left")
    return main_sleep, session_counts


def pick_pre_sleep_walks(nights_df: pd.DataFrame, walk_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for row in nights_df.itertuples(index=False):
        matched_walk = {
            "matched_walk_id": None,
            "matched_walk_start_local": None,
            "matched_walk_end_local": None,
            "matched_walk_duration_min": None,
            "matched_walk_steps": None,
            "matched_walk_distance_m": None,
            "matched_walk_avg_hrm": None,
            "matched_walk_max_hrm": None,
            "minutes_from_matched_walk_end_to_bedtime": None,
            "tracked_walks_pre_sleep_6h": 0,
            "tracked_walks_pre_sleep_3h": 0,
            "tracked_walk_in_last_3h": False,
            "tracked_walk_30_90_min": False,
            "tracked_walk_90_min_or_less": False,
            "auto_condition_observed": "missing",
        }

        if pd.isna(row.bedtime_timestamp):
            rows.append(matched_walk)
            continue

        candidate_walks = walk_df[
            (walk_df["end_timestamp"] <= row.bedtime_timestamp)
            & (walk_df["end_timestamp"] >= row.bedtime_timestamp - (6 * 60 * 60))
        ].copy()

        if candidate_walks.empty:
            matched_walk["auto_condition_observed"] = "control"
            rows.append(matched_walk)
            continue

        candidate_walks["gap_to_bedtime_min"] = (
            row.bedtime_timestamp - candidate_walks["end_timestamp"]
        ) / 60.0
        candidate_walks = candidate_walks.sort_values("end_timestamp", ascending=False)

        matched_walk["tracked_walks_pre_sleep_6h"] = int(len(candidate_walks))
        matched_walk["tracked_walks_pre_sleep_3h"] = int((candidate_walks["gap_to_bedtime_min"] <= 180).sum())
        matched_walk["tracked_walk_in_last_3h"] = bool((candidate_walks["gap_to_bedtime_min"] <= 180).any())
        matched_walk["tracked_walk_30_90_min"] = bool(
            ((candidate_walks["gap_to_bedtime_min"] >= 30) & (candidate_walks["gap_to_bedtime_min"] <= 90)).any()
        )
        matched_walk["tracked_walk_90_min_or_less"] = bool(
            ((candidate_walks["gap_to_bedtime_min"] >= 0) & (candidate_walks["gap_to_bedtime_min"] <= 90)).any()
        )

        latest_walk = candidate_walks.iloc[0]
        matched_walk.update(
            {
                "matched_walk_id": latest_walk["walk_id"],
                "matched_walk_start_local": latest_walk["start_local"],
                "matched_walk_end_local": latest_walk["end_local"],
                "matched_walk_duration_min": latest_walk["duration_min"],
                "matched_walk_steps": latest_walk["steps"],
                "matched_walk_distance_m": latest_walk["distance_m"],
                "matched_walk_avg_hrm": latest_walk["avg_hrm"],
                "matched_walk_max_hrm": latest_walk["max_hrm"],
                "minutes_from_matched_walk_end_to_bedtime": round(
                    float(latest_walk["gap_to_bedtime_min"]), 2
                ),
            }
        )

        if matched_walk["tracked_walk_90_min_or_less"]:
            matched_walk["auto_condition_observed"] = "walk"
        elif matched_walk["tracked_walk_in_last_3h"]:
            matched_walk["auto_condition_observed"] = "protocol_deviation"
        else:
            matched_walk["auto_condition_observed"] = "control"

        rows.append(matched_walk)

    return pd.DataFrame(rows)


def build_nightly_dataset(
    participant_id: str,
    main_sleep: pd.DataFrame,
    walk_df: pd.DataFrame,
    daily_reports: pd.DataFrame,
) -> pd.DataFrame:
    start_date = pd.to_datetime(main_sleep["night_date_local"].min())
    end_date = pd.to_datetime(main_sleep["night_date_local"].max())
    nights = pd.DataFrame(
        {
            "participant_id": participant_id,
            "night_date_local": pd.date_range(start_date, end_date, freq="D").strftime("%Y-%m-%d"),
        }
    )
    nights["study_day_index"] = range(1, len(nights) + 1)
    nights["weekday_name"] = pd.to_datetime(nights["night_date_local"]).dt.day_name()
    nights["is_weekend"] = nights["weekday_name"].isin(["Friday", "Saturday"])

    nightly = nights.merge(main_sleep, on=["participant_id", "night_date_local"], how="left")
    nightly = nightly.merge(daily_reports, on="night_date_local", how="left")
    walk_matches = pick_pre_sleep_walks(nightly, walk_df)
    nightly = pd.concat([nightly.reset_index(drop=True), walk_matches.reset_index(drop=True)], axis=1)
    nightly["sleep_record_present"] = nightly["session_id"].notna()
    nightly["missing_reason"] = nightly["sleep_record_present"].map(
        lambda present: "" if present else "no_main_sleep_record"
    )
    nightly["main_sleep_selection_rule"] = nightly["session_id"].map(
        lambda value: "longest_session_per_local_night" if pd.notna(value) else ""
    )
    return nightly


def load_device_metadata(paths: Paths) -> pd.DataFrame:
    file_path = find_export_file(paths.raw_dir, "_MiFitness_hlth_center_data_source.csv")
    device_df = pd.read_csv(file_path)
    for column in ("Sid", "Identifier", "Model", "Name"):
        device_df[column] = device_df[column].astype(str).str.strip()
    return device_df


def load_study_setup(paths: Paths) -> dict[str, str]:
    file_path = paths.metadata_dir / f"{paths.participant_id}_study_setup.csv"
    if not file_path.exists():
        return {}
    setup_df = pd.read_csv(file_path, dtype=str).fillna("")
    if setup_df.empty:
        return {}
    return setup_df.iloc[0].to_dict()


def in_date_window(night_date_local: str, start_date: str, end_date: str) -> bool:
    if not start_date or not end_date:
        return False
    return start_date <= night_date_local <= end_date


def add_planned_study_labels(nightly_df: pd.DataFrame, study_setup: dict[str, str]) -> pd.DataFrame:
    planned = nightly_df.copy()
    control_start = study_setup.get("control_start_night_local", "")
    control_end = study_setup.get("control_end_night_local", "")
    walk_start = study_setup.get("walk_start_night_local", "")
    walk_end = study_setup.get("walk_end_night_local", "")

    planned["include_in_study"] = False
    planned["condition_planned"] = ""
    planned["condition_window"] = ""

    for idx, row in planned.iterrows():
        night_date = row["night_date_local"]
        if in_date_window(night_date, control_start, control_end):
            planned.at[idx, "include_in_study"] = True
            planned.at[idx, "condition_planned"] = "control"
            planned.at[idx, "condition_window"] = f"{control_start} to {control_end}"
        if in_date_window(night_date, walk_start, walk_end):
            if planned.at[idx, "condition_planned"]:
                raise ValueError(f"Overlapping planned windows for {night_date}")
            planned.at[idx, "include_in_study"] = True
            planned.at[idx, "condition_planned"] = "walk"
            planned.at[idx, "condition_window"] = f"{walk_start} to {walk_end}"

    protocol_status: list[str] = []
    valid_primary_analysis: list[bool] = []
    for row in planned.itertuples(index=False):
        if not row.include_in_study:
            protocol_status.append("not_in_study")
            valid_primary_analysis.append(False)
            continue
        if not row.sleep_record_present:
            protocol_status.append("missing_sleep_record")
            valid_primary_analysis.append(False)
            continue
        if row.condition_planned == "control":
            if row.auto_condition_observed == "control":
                protocol_status.append("adherent_control")
                valid_primary_analysis.append(True)
            else:
                protocol_status.append("control_violation")
                valid_primary_analysis.append(False)
            continue
        if row.condition_planned == "walk":
            if row.auto_condition_observed == "walk":
                protocol_status.append("adherent_walk")
                valid_primary_analysis.append(True)
            elif row.auto_condition_observed == "protocol_deviation":
                protocol_status.append("walk_timing_violation")
                valid_primary_analysis.append(False)
            else:
                protocol_status.append("walk_missing_or_untracked")
                valid_primary_analysis.append(False)
            continue
        protocol_status.append("planned_condition_missing")
        valid_primary_analysis.append(False)

    planned["protocol_status"] = protocol_status
    planned["valid_primary_analysis_night"] = valid_primary_analysis
    planned["included_study_night_index"] = pd.NA
    planned["condition_night_index"] = pd.NA

    included_indices = planned.index[planned["include_in_study"]].tolist()
    for position, idx in enumerate(included_indices, start=1):
        planned.at[idx, "included_study_night_index"] = position

    for condition in ("control", "walk"):
        condition_indices = planned.index[
            planned["include_in_study"] & planned["condition_planned"].eq(condition)
        ].tolist()
        for position, idx in enumerate(condition_indices, start=1):
            planned.at[idx, "condition_night_index"] = position
    return planned


def write_csv(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def create_participant_template(
    paths: Paths,
    device_df: pd.DataFrame,
    nightly_df: pd.DataFrame,
    sleep_df: pd.DataFrame,
) -> Path:
    output_path = paths.metadata_dir / f"{paths.participant_id}_study_setup.csv"
    if output_path.exists():
        return output_path

    watch_row = device_df[device_df["Status"].eq(1)].head(1)
    timezone_mode = int(sleep_df["timezone_offset_minutes"].mode().iloc[0])
    template = pd.DataFrame(
        [
            {
                "participant_id": paths.participant_id,
                "raw_export_dir": paths.raw_dir.as_posix(),
                "device_model": watch_row["Name"].iloc[0] if not watch_row.empty else "",
                "device_identifier": watch_row["Identifier"].iloc[0] if not watch_row.empty else "",
                "timezone_offset_minutes": timezone_mode,
                "first_available_night_local": nightly_df["night_date_local"].min(),
                "last_available_night_local": nightly_df["night_date_local"].max(),
                "control_start_night_local": "",
                "control_end_night_local": "",
                "walk_start_night_local": "",
                "walk_end_night_local": "",
                "home_city": "",
                "knmi_station_code": "",
                "notes": "",
            }
        ]
    )
    write_csv(template, output_path)
    return output_path


def create_night_annotation_template(paths: Paths, study_df: pd.DataFrame) -> Path:
    output_path = paths.metadata_dir / f"{paths.participant_id}_night_annotations.csv"
    template = study_df[
        [
            "participant_id",
            "night_date_local",
            "auto_condition_observed",
            "tracked_walk_90_min_or_less",
            "minutes_from_matched_walk_end_to_bedtime",
            "sleep_record_present",
            "include_in_study",
            "condition_planned",
        ]
    ].copy()
    template["manual_condition_override"] = ""
    template["known_issue"] = ""
    template["notes"] = ""
    if output_path.exists():
        existing = pd.read_csv(output_path, dtype=str).fillna("")
        keep_columns = ["participant_id", "night_date_local", "manual_condition_override", "known_issue", "notes"]
        existing = existing[[column for column in keep_columns if column in existing.columns]]
        template = template.merge(existing, on=["participant_id", "night_date_local"], how="left", suffixes=("", "_existing"))
        for column in ("manual_condition_override", "known_issue", "notes"):
            existing_column = f"{column}_existing"
            if existing_column in template.columns:
                template[column] = template[existing_column].fillna(template[column])
                template = template.drop(columns=[existing_column])
    write_csv(template, output_path)
    return output_path


def write_summary(
    paths: Paths,
    nightly_df: pd.DataFrame,
    study_df: pd.DataFrame,
    study_setup: dict[str, str],
    sleep_df: pd.DataFrame,
    walk_df: pd.DataFrame,
    participant_template_path: Path,
    night_template_path: Path,
) -> Path:
    output_path = paths.reports_dir / f"{paths.participant_id}_personal_summary.md"
    auto_counts = nightly_df["auto_condition_observed"].value_counts(dropna=False).to_dict()
    main_sleep_nights = int(nightly_df["sleep_record_present"].sum())
    extra_sleep_nights = int((nightly_df["extra_sleep_session_count"].fillna(0) > 0).sum())
    short_fragments = int(sleep_df["is_short_or_fragment"].sum())
    planned_counts = study_df["protocol_status"].value_counts(dropna=False).to_dict()
    included_nights = int(study_df["include_in_study"].sum())
    valid_nights = int(study_df["valid_primary_analysis_night"].sum())
    control_configured = bool(study_setup.get("control_start_night_local")) and bool(
        study_setup.get("control_end_night_local")
    )
    walk_configured = bool(study_setup.get("walk_start_night_local")) and bool(
        study_setup.get("walk_end_night_local")
    )
    if control_configured and walk_configured:
        study_window_note = (
            f"- The planned study windows are configured as control "
            f"{study_setup.get('control_start_night_local')} to {study_setup.get('control_end_night_local')} "
            f"and walk {study_setup.get('walk_start_night_local')} to {study_setup.get('walk_end_night_local')}."
        )
    else:
        study_window_note = (
            "- The raw export spans more than two weeks, so you still need to mark the exact study window in the setup template."
        )
    summary = f"""# {paths.participant_id} personal dataset summary

## What was generated
- Canonical sleep sessions: `processed/{paths.participant_id}/sleep_sessions.csv`
- Canonical walk sessions: `processed/{paths.participant_id}/walk_sessions.csv`
- Night-level study table: `processed/{paths.participant_id}/nightly_dataset.csv`
- Planned-window study table: `processed/{paths.participant_id}/study_dataset.csv`
- Participant setup template: `{participant_template_path.as_posix()}`
- Night annotation template: `{night_template_path.as_posix()}`

## Raw export facts
- Sleep session rows found: {len(sleep_df)}
- Outdoor walking rows found: {len(walk_df)}
- Main sleep nights reconstructed: {main_sleep_nights}
- Nights with extra sleep fragments or naps: {extra_sleep_nights}
- Short or fragment sleep sessions flagged: {short_fragments}

## Auto-observed condition counts
- walk: {auto_counts.get('walk', 0)}
- control: {auto_counts.get('control', 0)}
- protocol_deviation: {auto_counts.get('protocol_deviation', 0)}
- missing: {auto_counts.get('missing', 0)}

## Planned-study counts
- Nights inside the configured study windows: {included_nights}
- Nights valid for the primary per-protocol analysis: {valid_nights}
- adherent_control: {planned_counts.get('adherent_control', 0)}
- adherent_walk: {planned_counts.get('adherent_walk', 0)}
- walk_timing_violation: {planned_counts.get('walk_timing_violation', 0)}
- control_violation: {planned_counts.get('control_violation', 0)}
- walk_missing_or_untracked: {planned_counts.get('walk_missing_or_untracked', 0)}

## Important interpretation notes
- {study_window_note[2:] if study_window_note.startswith('- ') else study_window_note}
- `total_sleep_time_min` comes from the raw `duration` field because the separate `sleep_duration` field is inconsistent in this export.
- Auto condition labels treat a tracked walk ending at most 90 minutes before bedtime as a walk night.
- Weather enrichment is handled later by the dedicated KNMI step in `run_full_pipeline.py`, using the participant setup metadata plus the shared Eindhoven weather file.
- Teammates can use the same pipeline with different date windows. The raw schema stays the same; only the participant setup file changes.

## Immediate next actions
1. Fill or verify `control_start_night_local`, `control_end_night_local`, `walk_start_night_local`, `walk_end_night_local`, `home_city`, and `knmi_station_code` in the participant setup template.
2. Review the night annotation template and add manual notes only where the tracker-based label is incomplete or misleading.
3. Run the full pipeline after the KNMI file is present to generate weather-enriched datasets and report assets.
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(summary, encoding="utf-8")
    return output_path


def main() -> None:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    paths = Paths(
        raw_dir=raw_dir,
        participant_id=args.participant_id,
        processed_dir=Path("processed") / args.participant_id,
        reports_dir=Path("reports"),
        metadata_dir=Path("metadata"),
    )

    sleep_df = load_sleep_sessions(paths)
    walk_df = load_walk_sessions(paths)
    daily_reports = load_daily_sleep_reports(paths)
    main_sleep, _ = choose_main_sleep_sessions(sleep_df)
    nightly_df = build_nightly_dataset(paths.participant_id, main_sleep, walk_df, daily_reports)
    device_df = load_device_metadata(paths)

    write_csv(sleep_df, paths.processed_dir / "sleep_sessions.csv")
    write_csv(walk_df, paths.processed_dir / "walk_sessions.csv")
    write_csv(nightly_df, paths.processed_dir / "nightly_dataset.csv")

    study_setup = load_study_setup(paths)
    study_df = add_planned_study_labels(nightly_df, study_setup)
    write_csv(study_df, paths.processed_dir / "study_dataset.csv")
    participant_template_path = create_participant_template(paths, device_df, nightly_df, sleep_df)
    night_template_path = create_night_annotation_template(paths, study_df)
    summary_path = write_summary(
        paths,
        nightly_df,
        study_df,
        study_setup,
        sleep_df,
        walk_df,
        participant_template_path,
        night_template_path,
    )

    print(f"Wrote {paths.processed_dir / 'sleep_sessions.csv'}")
    print(f"Wrote {paths.processed_dir / 'walk_sessions.csv'}")
    print(f"Wrote {paths.processed_dir / 'nightly_dataset.csv'}")
    print(f"Wrote {paths.processed_dir / 'study_dataset.csv'}")
    print(f"Wrote {participant_template_path}")
    print(f"Wrote {night_template_path}")
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
