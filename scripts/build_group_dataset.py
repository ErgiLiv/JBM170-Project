from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_participant_study_tables() -> list[pd.DataFrame]:
    tables: list[pd.DataFrame] = []
    for path in sorted(Path("processed").glob("*/study_dataset.csv")):
        df = pd.read_csv(path)
        if "participant_id" not in df.columns:
            continue
        df["source_path"] = path.as_posix()
        tables.append(df)
    return tables


def write_group_outputs(group_df: pd.DataFrame) -> None:
    output_dir = Path("processed") / "group"
    output_dir.mkdir(parents=True, exist_ok=True)

    in_study = group_df[group_df["include_in_study"]].copy()
    primary = in_study[in_study["valid_primary_analysis_night"]].copy()

    in_study.to_csv(output_dir / "group_study_dataset.csv", index=False)
    primary.to_csv(output_dir / "group_primary_analysis_dataset.csv", index=False)

    counts = (
        in_study.groupby(["participant_id", "condition_planned", "protocol_status"])
        .size()
        .reset_index(name="n_nights")
        .sort_values(["participant_id", "condition_planned", "protocol_status"])
    )
    counts.to_csv(output_dir / "group_protocol_counts.csv", index=False)

    summary_lines = [
        "# Group dataset summary",
        "",
        f"- Participant tables found: {group_df['participant_id'].nunique()}",
        f"- Nights inside study windows: {len(in_study)}",
        f"- Nights valid for primary per-protocol analysis: {len(primary)}",
        "",
        "## Counts by participant, planned condition, and protocol status",
        counts.to_markdown(index=False),
    ]
    (Path("reports") / "group_summary.md").write_text("\n".join(summary_lines), encoding="utf-8")


def main() -> None:
    tables = load_participant_study_tables()
    if not tables:
        raise FileNotFoundError("No processed participant study tables were found under processed/*/study_dataset.csv")
    group_df = pd.concat(tables, ignore_index=True)
    write_group_outputs(group_df)
    print("Wrote processed/group/group_study_dataset.csv")
    print("Wrote processed/group/group_primary_analysis_dataset.csv")
    print("Wrote processed/group/group_protocol_counts.csv")
    print("Wrote reports/group_summary.md")


if __name__ == "__main__":
    main()
