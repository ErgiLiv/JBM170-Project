from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd


def load_setup_files() -> list[Path]:
    return sorted(Path("metadata").glob("*_study_setup.csv"))


def run_personal_pipeline(setup_path: Path) -> None:
    setup_df = pd.read_csv(setup_path, dtype=str).fillna("")
    if setup_df.empty:
        raise ValueError(f"{setup_path} is empty")

    row = setup_df.iloc[0]
    participant_id = row.get("participant_id", "").strip()
    raw_export_dir = row.get("raw_export_dir", "").strip()
    if not participant_id:
        raise ValueError(f"{setup_path} does not define participant_id")
    if not raw_export_dir:
        raise ValueError(f"{setup_path} does not define raw_export_dir")
    if not Path(raw_export_dir).exists():
        raise FileNotFoundError(f"Raw export directory does not exist: {raw_export_dir}")

    command = [
        sys.executable,
        "scripts/build_personal_dataset.py",
        "--participant-id",
        participant_id,
        "--raw-dir",
        raw_export_dir,
    ]
    print(f"Running personal pipeline for {participant_id}")
    subprocess.run(command, check=True)


def main() -> None:
    setup_files = load_setup_files()
    if not setup_files:
        raise FileNotFoundError("No participant setup files were found under metadata/*_study_setup.csv")

    for setup_path in setup_files:
        run_personal_pipeline(setup_path)

    print("Running group pipeline")
    subprocess.run([sys.executable, "scripts/build_group_dataset.py"], check=True)
    print("Running KNMI weather enrichment")
    subprocess.run([sys.executable, "scripts/enrich_knmi_weather.py"], check=True)
    print("Running report asset generation")
    subprocess.run([sys.executable, "scripts/generate_report_assets.py"], check=True)


if __name__ == "__main__":
    main()
