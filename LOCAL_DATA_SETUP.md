# Local Data Setup

Use pseudonymous participant ids locally: `P01`, `P02`, `P03`, `P04`, `P05`.

## Recommended layout

```text
JBM170-Project/
  scripts/
  metadata/
    P01_study_setup.csv
    P01_night_annotations.csv
    P02_study_setup.csv
    ...
  team_data/
    raw/
      P01/
        <original Mi Fitness export folder>/
      P02/
        <original Mi Fitness export folder>/
      ...
  processed/
    P01/
    P02/
    ...
    group/
  reports/
```

## How to use it

1. Put each member's untouched Mi Fitness export in their own folder under `team_data/raw/<participant_id>/`.
2. Set `raw_export_dir` in `metadata/<participant_id>_study_setup.csv` to that member's export folder.
3. Fill the participant-specific date windows in the same setup file:
   - `control_start_night_local`
   - `control_end_night_local`
   - `walk_start_night_local`
   - `walk_end_night_local`
4. Keep `home_city` and `knmi_station_code` consistent unless a teammate slept in a different city for the study week.
5. Run `python scripts/run_full_pipeline.py` after all participant setup files are ready.

## What gets generated

- `processed/<participant_id>/sleep_sessions.csv`: parsed raw sleep sessions
- `processed/<participant_id>/walk_sessions.csv`: parsed tracked walks
- `processed/<participant_id>/nightly_dataset.csv`: all reconstructed nights from the export
- `processed/<participant_id>/study_dataset.csv`: only the metadata-driven study interpretation
- `processed/<participant_id>/study_dataset_weather.csv`: study table enriched with KNMI weather
- `processed/group/group_study_dataset.csv`: all included study nights across participants
- `processed/group/group_primary_analysis_dataset.csv`: only nights valid for the primary per-protocol analysis
- `processed/group/group_study_dataset_weather.csv`: pooled study nights with weather
- `processed/group/group_primary_analysis_dataset_weather.csv`: pooled primary-analysis nights with weather
- `reports/figures/`: report-ready figures using only the configured 2-week study windows
- `reports/tables/`: report-ready tables for QC, condition summaries, and primary effects

## Compliance notes

- Keep raw exports unchanged.
- Do not use names in filenames or participant ids.
- Keep the OneDrive folder access-restricted to the project group.
- Treat this repo as a local analysis workspace; do not commit raw or processed personal data to git.
