# JBM170 Evening Walk vs Sleep Analysis Plan

## Summary
- Base the work on the study in [JBM170 Proposal.pdf](C:/Users/Ergi%20Livanaj/Desktop/University/Year%203/Quarter%203/JBM170%20(Field%20Data%20Acquisition%20and%20Analysis)/JBM170-Project/context/JBM170%20Proposal.pdf) and the course requirements in [handouts JBM170 2025 2026 V1.pdf](C:/Users/Ergi%20Livanaj/Desktop/University/Year%203/Quarter%203/JBM170%20(Field%20Data%20Acquisition%20and%20Analysis)/JBM170-Project/context/handouts%20JBM170%202025%202026%20V1.pdf).
- Treat this as a repeated-night, within-participant pilot study with a planned walk condition and control condition, not as a strong causal medical trial.
- Use raw export files as the source of truth, because you already have your own two-week data and teammates will add comparable exports later.
- Inference from the wearable-validation literature: make `total_sleep_time` the primary endpoint, keep `sleep_efficiency`, `time_in_bed`, `WASO/awakenings`, and the device sleep score as supporting secondary outcomes, and treat sleep stages as exploratory only.

## Canonical Dataset
- Build one night-level master table with one row per participant-night.
- Include at minimum: `participant_id`, `date`, `study_day`, `weekday/weekend`, `condition_planned`, `condition_observed`, `protocol_deviation_flag`, `walk_start`, `walk_end`, `walk_duration_min`, `walk_steps_or_distance`, `walk_intensity_proxy`, `sleep_start`, `sleep_end`, `total_sleep_time_min`, `time_in_bed_min`, `sleep_efficiency`, `awakenings_or_WASO`, `deep_sleep_min`, `REM_min`, `weather_station`, `walk_temp`, `walk_wind`, `walk_precip`, `bedtime_temp`, `missing_reason`.
- Reconstruct `condition_observed` from actual behavior, not only planned week labels, because there were some deviations.
- Define a valid walk night as a tracked walk ending at most 90 minutes before sleep; define a valid control night as no outdoor walk in the last 3 hours before sleep; keep all other nights flagged rather than silently dropping them.
- Preserve raw exports unchanged in `raw`, cleaned tables in `processed`, and the analysis-ready table in `analysis`, with a README, data dictionary, consent record, and transformation log to satisfy FAIR and course ethics requirements.

## Step-by-Step Work Plan
1. Freeze your current inputs now: collect your raw Mi Fitness export, note your date range, timezone, app/device version if visible, city or nearest KNMI station, and any known missed walks, dead-battery days, or sync failures.
2. Do a schema audit on your own export first: list every available sleep, heart-rate, and activity field, then lock a minimal cross-participant schema containing only variables that should exist for everyone.
3. Build your own participant-night table and run QC before touching team data: normalize timestamps, remove duplicates, detect impossible durations, verify that each night maps to exactly one participant and one observed condition, and log every missing or ambiguous night.
4. Enrich your table with official KNMI hourly validated weather data, matched to the nearest station and nearest hourly interval; record the extraction date, because KNMI states validated hourly data can be revised and the timestamps denote the end of the hourly interval in UTC.
5. Run a dry-run analysis on your data only: produce adherence counts, valid-night counts by condition, a two-week nightly timeline plot using only study-window nights, and your personal walk-versus-control effect estimates for the primary and secondary outcomes.
6. Freeze the analysis protocol before teammate data arrives: lock the variable names, valid-night rules, primary outcome, secondary outcomes, plots, and model formulas so the pooled analysis is not changed after seeing group results.
7. When teammates finish, process each export through the exact same QC pipeline, produce a per-participant validation sheet, and only then merge them into the pooled master table.
8. Run the pooled analysis on nightly data with a mixed-effects model of the form `outcome ~ condition_observed + study_day + weekend + (1 | participant)` as the primary model; report the condition effect with 95% confidence intervals and use p-values as secondary evidence only.
9. Add sensitivity analyses: per-protocol nights only, exclusion of nights with severe protocol deviations, and an exploratory model adding walk intensity and weather to check whether any observed effect is driven by unusually intense walks or bad weather rather than the walk condition itself.
10. Present results in the report as estimation-first: participant-level paired plots, pooled effect estimates, adherence/missingness tables, weather-context figures, and explicit limitations about small sample size, order deviations, and consumer-wearable sleep measurement.

## Test Plan
- Every raw file is traceable to one pseudonymous participant and remains unchanged after import.
- Every participant-night is classified as `walk`, `control`, `protocol_deviation`, or `missing`, with no unlabeled nights.
- All timestamps are in one timezone and the walk-to-sleep interval is computable for every valid walk night.
- Each participant has reported valid-night counts per condition; anyone below 5 valid nights in either condition is flagged as low-reliability.
- The pooled table is reproducible from raw files plus the logged transformations, and the final results can be rerun without manual spreadsheet edits.

## Assumptions And Research Basis
- Default stack: Python, not spreadsheets, for the actual cleaning and analysis, because that is the safest route for reproducibility.
- Default interpretation: exploratory pilot evidence, not a medical conclusion.
- Default reporting rule: do not make strong claims from REM/deep/light stage differences unless they align with stronger metrics and are clearly labeled exploratory.
- Research basis: [Stutz et al. 2019](https://pubmed.ncbi.nlm.nih.gov/30374942/), [Frimpong et al. 2021](https://pubmed.ncbi.nlm.nih.gov/34416428/), [Atoui et al. 2021](https://pubmed.ncbi.nlm.nih.gov/33571893/), [Lee et al. 2025](https://pubmed.ncbi.nlm.nih.gov/39484805/), [Yuan et al. 2024](https://pubmed.ncbi.nlm.nih.gov/38384163/), [Lau et al. 2022](https://pubmed.ncbi.nlm.nih.gov/37193398/), [KNMI hourly validated dataset](https://dataplatform.knmi.nl/dataset/hourly-in-situ-meteorological-observations-validated-1-0), [KNMI access/API docs](https://dataplatform.knmi.nl/dataset/access/hourly-in-situ-meteorological-observations-validated-1-0).
