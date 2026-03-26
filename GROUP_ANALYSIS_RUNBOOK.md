# Group Analysis Runbook

## What you need

- One untouched Mi Fitness export folder per participant.
- One pseudonymous participant id per person: `P01`, `P02`, `P03`, `P04`, `P05`.
- Exact 7 control nights and exact 7 walk nights for each participant.
- A local path to each raw export folder. This can be a OneDrive-synced local folder or a repo-local folder.
- Eindhoven as the home city for all participants.
- KNMI station code `370` for Eindhoven.

## Where things go

- Raw exports:
  - Recommended: `team_data/raw/P0X/<original_export_folder>/`
  - Alternative: keep them in OneDrive and put that local synced path in `raw_export_dir`.
- Participant setup files:
  - `metadata/P0X_study_setup.csv`
- Auto-generated annotation files:
  - `metadata/P0X_night_annotations.csv`
- Parsed outputs:
  - `processed/P0X/`
- Pooled outputs:
  - `processed/group/`
- Summaries:
  - `reports/`
- Report assets:
  - `reports/figures/`
  - `reports/tables/`

## Step by step

1. Put each member's untouched raw export in a separate local folder.
2. Copy `metadata/study_setup_template.csv.example` to `metadata/P0X_study_setup.csv` for each participant.
3. In each participant setup file, fill:
   - `participant_id`
   - `raw_export_dir`
   - `control_start_night_local`
   - `control_end_night_local`
   - `walk_start_night_local`
   - `walk_end_night_local`
   - `home_city = Eindhoven`
   - `knmi_station_code = 370`
4. Make sure each participant has exactly 7 control nights and exactly 7 walk nights in the setup file.
5. Run:
   - `python scripts/run_full_pipeline.py`
6. Check for each participant:
   - `processed/P0X/study_dataset.csv`
   - `reports/P0X_personal_summary.md`
   - `metadata/P0X_night_annotations.csv`
7. Check the pooled outputs:
   - `processed/group/group_study_dataset.csv`
   - `processed/group/group_primary_analysis_dataset.csv`
   - `reports/group_summary.md`
8. Check the report assets:
   - `reports/figures/fig01_protocol_timeline.png`
   - `reports/figures/fig02_primary_outcome_comparison.png`
   - `reports/figures/fig03_secondary_outcomes.png`
   - `reports/figures/fig04_walk_protocol_context.png`
   - `reports/figures/fig05_weather_context.png`
   - `reports/tables/tbl01_data_qc_by_participant.csv`
   - `reports/tables/tbl02_outcome_summary_by_condition.csv`
   - `reports/tables/tbl03_primary_effects.csv`
   - `reports/report_outline.md`
   - `reports/report_analysis_strategy.md`

## KNMI weather data

1. Because all participants are in Eindhoven, use one KNMI station for everyone: `370`.
2. Download one hourly KNMI weather dataset for station `370` that covers the full union of all participant study windows:
   - start = earliest `control_start_night_local` across all setup files
   - end = latest `walk_end_night_local` across all setup files
3. Store that file locally in a single place, for example:
   - `team_data/weather/knmi_hourly_eindhoven_370.<ext>`
4. Use the KNMI hourly validated dataset, not a forecast product.
5. Match weather timestamps in UTC, because KNMI stores hourly timestamps in UTC and the timestamp marks the end of the preceding hourly interval.
6. For the proposal-required variables, keep:
   - temperature
   - wind speed
   - precipitation
7. For walk nights, match weather to the hour containing the walk end time.
8. For control nights, do not invent a fake walk weather field. If you want a symmetric contextual variable for both conditions, add a separate bedtime-hour weather variable as an extension, not as a replacement.

## Practical rule for teammates

- Teammates can have different calendar dates.
- The code does not need to change.
- Only their `metadata/P0X_study_setup.csv` values change.
- Report figures handle these different dates by aligning nights as `study night 1-14` and `walk night 1-7`.
- Report figures only show the actual two-week study windows, not extra nights from the raw export.
- After all setup files are ready, rerun `python scripts/run_full_pipeline.py`.
