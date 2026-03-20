# JBM170 Evening Walk And Sleep Pipeline

This repository turns raw Mi Fitness exports and KNMI weather data into a reproducible analysis pipeline for a simple question:

Does a 20-minute evening walk ending at most 90 minutes before sleep change same-night sleep outcomes compared with a no-walk control week?

The pipeline is designed for a repeated-measures study:
- 7 control nights
- 7 walk nights
- participant-specific calendar windows
- one pooled analysis after all participant data is available

The practical goal is straightforward: drop in each participant's raw export, define their study windows, run one command, and get cleaned datasets, weather-enriched tables, and report-ready figures.

## What This Repository Does

- Parses raw Mi Fitness sleep and outdoor-walk exports.
- Reconstructs one participant-night dataset per person.
- Applies exact study windows from participant metadata.
- Merges KNMI hourly weather data.
- Builds pooled group datasets for the whole study.
- Generates QC tables, outcome summaries, effect tables, and report-ready figures.
- Keeps raw, processed, and report outputs separated so the workflow stays FAIR and rerunnable.

## Repository Layout

```text
JBM170-Project/
  context/
  metadata/
    study_setup_template.csv.example
    P01_study_setup.csv
    P02_study_setup.csv
    ...
  team_data/
    raw/
      P01/
        <original_export_folder>/
      P02/
        <original_export_folder>/
      ...
    weather/
      knmi_hourly_eindhoven_370.csv
  processed/
  reports/
  scripts/
  requirements.txt
```

## What You Need Before Running

For each participant:
- One untouched Mi Fitness export folder.
- One pseudonymous participant id such as `P01`, `P02`, `P03`.
- Exact `7 control + 7 walk` night windows.

For the study as a whole:
- Exactly one KNMI hourly weather CSV in `team_data/weather/`.
- In the current project setup, the weather source is Eindhoven, station `370`.

## Step-By-Step Setup

### 1. Create and activate a Python environment

Python 3.11 or newer is recommended.

PowerShell example:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Place the raw participant data

Put each participant's untouched Mi Fitness export in its own folder:

```text
team_data/raw/P01/<original_export_folder>/
team_data/raw/P02/<original_export_folder>/
...
```

Do not rename files inside the original export folder unless you have a very good reason. The parser expects the original Mi Fitness export structure.

### 3. Place the KNMI weather file

Put exactly one KNMI hourly CSV in:

```text
team_data/weather/
```

Recommended filename:

```text
team_data/weather/knmi_hourly_eindhoven_370.csv
```

Important:
- Keep only one active weather CSV in that folder.
- The file should cover the full union of all participant study dates.

### 4. Create one metadata file per participant

Copy the template:

```text
metadata/study_setup_template.csv.example
```

into files like:

```text
metadata/P01_study_setup.csv
metadata/P02_study_setup.csv
```

Then fill these fields:
- `participant_id`
- `raw_export_dir`
- `control_start_night_local`
- `control_end_night_local`
- `walk_start_night_local`
- `walk_end_night_local`
- `home_city`
- `knmi_station_code`

Example:

```csv
participant_id,raw_export_dir,control_start_night_local,control_end_night_local,walk_start_night_local,walk_end_night_local,home_city,knmi_station_code
P01,team_data/raw/P01/<original_export_folder>,2026-02-22,2026-02-28,2026-03-01,2026-03-07,Eindhoven,370
```

Rules:
- Each participant must have exactly 7 control nights and exactly 7 walk nights.
- Participants may have different actual calendar dates.
- Use pseudonymous ids only. Do not use names.

### 5. Run the full pipeline

From the repository root:

```powershell
python scripts/run_full_pipeline.py
```

That one command will:
1. Build the participant-level datasets from every `metadata/*_study_setup.csv`.
2. Build the pooled group datasets.
3. Enrich the study tables with KNMI weather.
4. Generate report tables, figures, and report-planning files.

## What The Pipeline Produces

### Participant-level outputs

- `processed/P0X/sleep_sessions.csv`
- `processed/P0X/walk_sessions.csv`
- `processed/P0X/nightly_dataset.csv`
- `processed/P0X/study_dataset.csv`
- `processed/P0X/study_dataset_weather.csv`
- `reports/P0X_personal_summary.md`
- `metadata/P0X_night_annotations.csv`

### Group-level outputs

- `processed/group/group_study_dataset.csv`
- `processed/group/group_primary_analysis_dataset.csv`
- `processed/group/group_study_dataset_weather.csv`
- `processed/group/group_primary_analysis_dataset_weather.csv`
- `processed/group/group_protocol_counts.csv`
- `reports/group_summary.md`

### Report assets

- `reports/tables/tbl01_data_qc_by_participant.csv`
- `reports/tables/tbl02_outcome_summary_by_condition.csv`
- `reports/tables/tbl03_primary_effects.csv`
- `reports/figures/fig01_protocol_timeline.png`
- `reports/figures/fig02_primary_outcome_comparison.png`
- `reports/figures/fig03_secondary_outcomes.png`
- `reports/figures/fig04_walk_protocol_context.png`
- `reports/figures/fig05_weather_context.png`
- `reports/report_outline.md`
- `reports/report_analysis_strategy.md`
- `reports/report_asset_manifest.md`

## How To Read The Outputs

- `total_sleep_time_min` is the main endpoint.
- Sleep efficiency, time in bed, and device sleep score are supporting outcomes.
- REM and deep sleep are exploratory only.
- Weather is included as context and later sensitivity information, not as the headline result.

Report figures only use the actual two-week study windows. They do not show extra nights from the raw export.

Cross-participant figures align nights as:
- `study night 1..14`
- `walk night 1..7`

This is important because different teammates can have different calendar windows while still being analyzed together.

## FAIR And Reproducibility Notes

- Keep raw exports untouched.
- Keep pseudonyms separate from real identities.
- Keep raw, processed, and report outputs in separate folders.
- Keep exactly one active KNMI weather CSV in `team_data/weather/`.
- The pipeline is fully rerunnable from raw inputs plus metadata files.
- Avoid manual spreadsheet editing after the pipeline starts producing outputs.
- Do not commit personal raw or processed data to git.

## Known Limitation

`metadata/P0X_night_annotations.csv` is currently a manual QC log. It is generated for documentation and review, but the pipeline does not automatically apply manual overrides from that file.
