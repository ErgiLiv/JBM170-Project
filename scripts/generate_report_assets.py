from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import pandas as pd
import seaborn as sns
import statsmodels.formula.api as smf


CONDITION_ORDER = ["control", "walk"]
CONDITION_LABELS = {"control": "Control week", "walk": "Walk week"}
PRIMARY_OUTCOME = "total_sleep_time_min"
PALETTE = {"control": "#4c78a8", "walk": "#f58518"}
NEUTRAL = "#334155"
TEXTBOX_STYLE = {
    "facecolor": "white",
    "edgecolor": "#cbd5e1",
    "alpha": 0.9,
    "boxstyle": "round,pad=0.3",
}
OUTCOME_SPECS = [
    {
        "column": "total_sleep_time_min",
        "label": "Total sleep time",
        "role": "primary",
        "units": "min",
        "note": "Main endpoint because it is direct and interpretable.",
    },
    {
        "column": "sleep_efficiency",
        "label": "Sleep efficiency",
        "role": "secondary",
        "units": "%",
        "note": "Secondary quality metric summarizing time asleep while in bed.",
    },
    {
        "column": "sleep_score",
        "label": "Sleep score",
        "role": "secondary_device_score",
        "units": "score",
        "note": "Useful user-facing composite, but still vendor-defined.",
    },
    {
        "column": "time_in_bed_min",
        "label": "Time in bed",
        "role": "secondary",
        "units": "min",
        "note": "Helps separate longer sleep from simply spending longer in bed.",
    },
    {
        "column": "awake_duration_min",
        "label": "Awake duration",
        "role": "exploratory_continuity",
        "units": "min",
        "note": "Exploratory continuity metric related to interruptions.",
    },
    {
        "column": "awake_count",
        "label": "Awake count",
        "role": "exploratory_continuity",
        "units": "count",
        "note": "Exploratory count of awakenings from the wearable export.",
    },
    {
        "column": "deep_sleep_min",
        "label": "Deep sleep",
        "role": "exploratory_stage",
        "units": "min",
        "note": "Wearable-estimated stage metric; use descriptively only.",
    },
    {
        "column": "rem_sleep_min",
        "label": "REM sleep",
        "role": "exploratory_stage",
        "units": "min",
        "note": "Wearable-estimated stage metric; use descriptively only.",
    },
]
NUMERIC_COLUMNS = [
    "included_study_night_index",
    "condition_night_index",
    "total_sleep_time_min",
    "sleep_efficiency",
    "sleep_score",
    "time_in_bed_min",
    "awake_count",
    "awake_duration_min",
    "deep_sleep_min",
    "rem_sleep_min",
    "matched_walk_duration_min",
    "matched_walk_distance_m",
    "matched_walk_steps",
    "matched_walk_avg_hrm",
    "minutes_from_matched_walk_end_to_bedtime",
    "walk_temp_c",
    "walk_wind_m_s",
    "walk_precip_mm",
    "bedtime_temp_c",
    "bedtime_wind_m_s",
    "bedtime_precip_mm",
]


def round_or_none(value: float | int | None, digits: int = 3) -> float | int | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def coerce_bool(series: pd.Series) -> pd.Series:
    truthy = {"true", "1", "yes", "y"}
    return series.astype("string").fillna("").str.strip().str.lower().isin(truthy)


def ensure_relative_indices(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    if "include_in_study" not in data.columns:
        return data

    data["condition_planned"] = data.get("condition_planned", "").fillna("")
    data = data.sort_values(["participant_id", "night_date_local"]).reset_index(drop=True)

    if "included_study_night_index" not in data.columns or data["included_study_night_index"].isna().all():
        data["included_study_night_index"] = pd.NA
        for _, participant_df in data.groupby("participant_id"):
            included_indices = participant_df.index[participant_df["include_in_study"]].tolist()
            for position, row_idx in enumerate(included_indices, start=1):
                data.at[row_idx, "included_study_night_index"] = position

    if "condition_night_index" not in data.columns or data["condition_night_index"].isna().all():
        data["condition_night_index"] = pd.NA
        for _, participant_df in data.groupby("participant_id"):
            for condition in CONDITION_ORDER:
                condition_indices = participant_df.index[
                    participant_df["include_in_study"] & participant_df["condition_planned"].eq(condition)
                ].tolist()
                for position, row_idx in enumerate(condition_indices, start=1):
                    data.at[row_idx, "condition_night_index"] = position

    for column in ("included_study_night_index", "condition_night_index"):
        data[column] = pd.to_numeric(data[column], errors="coerce")
    return data


def load_participant_datasets() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for participant_dir in sorted(Path("processed").iterdir()):
        if not participant_dir.is_dir() or participant_dir.name in {"group", "weather"}:
            continue
        weather_path = participant_dir / "study_dataset_weather.csv"
        base_path = participant_dir / "study_dataset.csv"
        dataset_path = weather_path if weather_path.exists() else base_path
        if not dataset_path.exists():
            continue
        df = pd.read_csv(dataset_path)
        df["dataset_path"] = dataset_path.as_posix()
        frames.append(df)

    if not frames:
        raise FileNotFoundError("No participant study datasets were found under processed/*/")

    data = pd.concat(frames, ignore_index=True)
    data["participant_id"] = data["participant_id"].astype(str)
    data["include_in_study"] = coerce_bool(data["include_in_study"])
    data["valid_primary_analysis_night"] = coerce_bool(data["valid_primary_analysis_night"])
    data["sleep_record_present"] = coerce_bool(data["sleep_record_present"])
    data["night_date"] = pd.to_datetime(data["night_date_local"], errors="coerce")
    for column in NUMERIC_COLUMNS:
        if column in data.columns:
            data[column] = pd.to_numeric(data[column], errors="coerce")
    return ensure_relative_indices(data)


def load_group_dataset() -> pd.DataFrame:
    weather_path = Path("processed/group/group_primary_analysis_dataset_weather.csv")
    base_path = Path("processed/group/group_primary_analysis_dataset.csv")
    dataset_path = weather_path if weather_path.exists() else base_path
    if not dataset_path.exists():
        raise FileNotFoundError("No group primary-analysis dataset was found under processed/group/")

    df = pd.read_csv(dataset_path)
    df["participant_id"] = df["participant_id"].astype(str)
    df["valid_primary_analysis_night"] = coerce_bool(df["valid_primary_analysis_night"])
    df["include_in_study"] = coerce_bool(df["include_in_study"])
    df["night_date"] = pd.to_datetime(df["night_date_local"], errors="coerce")
    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return ensure_relative_indices(df)


def ensure_output_dirs() -> tuple[Path, Path]:
    figures_dir = Path("reports/figures")
    tables_dir = Path("reports/tables")
    figures_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    return figures_dir, tables_dir


def save_figure(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def get_participant_palette(df: pd.DataFrame) -> dict[str, tuple[float, float, float]]:
    participants = sorted(df["participant_id"].dropna().unique())
    colors = sns.color_palette("crest", n_colors=max(len(participants), 1))
    return dict(zip(participants, colors))


def build_qc_table(full_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for participant_id, participant_df in full_df.groupby("participant_id"):
        included = participant_df[participant_df["include_in_study"]].copy()
        walk_nights = included[included["condition_planned"].eq("walk")].copy()
        rainy_walks = 0
        if len(walk_nights):
            rainy_walks = int(
                (
                    walk_nights.get("walk_precip_mm", pd.Series(dtype=float)).fillna(0).gt(0)
                    | coerce_bool(walk_nights.get("walk_trace_precip_flag", pd.Series(dtype=object)))
                ).sum()
            )

        rows.append(
            {
                "participant_id": participant_id,
                "raw_nights_total": len(participant_df),
                "included_nights": int(included.shape[0]),
                "valid_primary_nights": int(participant_df["valid_primary_analysis_night"].sum()),
                "missing_sleep_records_included": int((included["sleep_record_present"] == False).sum()),
                "control_nights": int(included["condition_planned"].eq("control").sum()),
                "walk_nights": int(included["condition_planned"].eq("walk").sum()),
                "adherent_control_nights": int(included["protocol_status"].eq("adherent_control").sum()),
                "adherent_walk_nights": int(included["protocol_status"].eq("adherent_walk").sum()),
                "walk_adherence_pct": round_or_none(
                    100 * included["protocol_status"].eq("adherent_walk").sum() / max(len(walk_nights), 1),
                    1,
                ),
                "mean_walk_gap_min": round_or_none(walk_nights["minutes_from_matched_walk_end_to_bedtime"].mean(), 2),
                "mean_walk_duration_min": round_or_none(walk_nights["matched_walk_duration_min"].mean(), 2),
                "mean_walk_avg_hrm": round_or_none(walk_nights["matched_walk_avg_hrm"].mean(), 2),
                "mean_walk_temp_c": round_or_none(walk_nights["walk_temp_c"].mean(), 2)
                if "walk_temp_c" in walk_nights.columns
                else None,
                "mean_walk_wind_m_s": round_or_none(walk_nights["walk_wind_m_s"].mean(), 2)
                if "walk_wind_m_s" in walk_nights.columns
                else None,
                "rainy_walk_nights": rainy_walks,
                "bedtime_weather_coverage_pct": round_or_none(
                    100 * included["bedtime_temp_c"].notna().mean(),
                    1,
                )
                if "bedtime_temp_c" in included.columns
                else None,
                "walk_weather_coverage_pct": round_or_none(
                    100 * walk_nights["walk_temp_c"].notna().mean(),
                    1,
                )
                if "walk_temp_c" in walk_nights.columns and len(walk_nights)
                else None,
            }
        )
    return pd.DataFrame(rows).sort_values("participant_id")


def build_outcome_summary_table(primary_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for spec in OUTCOME_SPECS:
        outcome = spec["column"]
        if outcome not in primary_df.columns:
            continue
        for condition in CONDITION_ORDER:
            subset = primary_df[primary_df["condition_planned"].eq(condition)][["participant_id", outcome]].dropna()
            rows.append(
                {
                    "outcome": outcome,
                    "metric_label": spec["label"],
                    "metric_role": spec["role"],
                    "units": spec["units"],
                    "interpretation_note": spec["note"],
                    "summary_type": "condition_nights",
                    "condition": condition,
                    "n_participants": int(subset["participant_id"].nunique()),
                    "n_nights": int(len(subset)),
                    "mean": round_or_none(subset[outcome].mean()),
                    "sd": round_or_none(subset[outcome].std(ddof=1)) if len(subset) > 1 else None,
                    "median": round_or_none(subset[outcome].median()),
                    "min": round_or_none(subset[outcome].min()),
                    "max": round_or_none(subset[outcome].max()),
                }
            )

        participant_means = (
            primary_df.pivot_table(index="participant_id", columns="condition_planned", values=outcome, aggfunc="mean")
            .reindex(columns=CONDITION_ORDER)
            .dropna()
        )
        diff = participant_means["walk"] - participant_means["control"] if not participant_means.empty else pd.Series(dtype=float)
        rows.append(
            {
                "outcome": outcome,
                "metric_label": spec["label"],
                "metric_role": spec["role"],
                "units": spec["units"],
                "interpretation_note": spec["note"],
                "summary_type": "paired_participant_difference",
                "condition": "walk_minus_control",
                "n_participants": int(diff.shape[0]),
                "n_nights": None,
                "mean": round_or_none(diff.mean()),
                "sd": round_or_none(diff.std(ddof=1)) if len(diff) > 1 else None,
                "median": round_or_none(diff.median()),
                "min": round_or_none(diff.min()),
                "max": round_or_none(diff.max()),
            }
        )
    return pd.DataFrame(rows)


def build_primary_effects_table(primary_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    control = primary_df[primary_df["condition_planned"].eq("control")][PRIMARY_OUTCOME].dropna()
    walk = primary_df[primary_df["condition_planned"].eq("walk")][PRIMARY_OUTCOME].dropna()
    participant_means = (
        primary_df.pivot_table(index="participant_id", columns="condition_planned", values=PRIMARY_OUTCOME, aggfunc="mean")
        .reindex(columns=CONDITION_ORDER)
        .dropna()
    )
    paired_diff = participant_means["walk"] - participant_means["control"] if not participant_means.empty else pd.Series(dtype=float)

    rows.append(
        {
            "analysis_type": "descriptive_nightly",
            "n_participants": int(primary_df["participant_id"].nunique()),
            "n_nights": int(len(primary_df)),
            "mean_control": round_or_none(control.mean()),
            "mean_walk": round_or_none(walk.mean()),
            "effect_walk_minus_control": round_or_none(walk.mean() - control.mean()) if not control.empty and not walk.empty else None,
            "ci_lower": None,
            "ci_upper": None,
            "p_value": None,
            "notes": "Difference in nightly means across all valid study nights.",
        }
    )
    rows.append(
        {
            "analysis_type": "descriptive_paired_participant_means",
            "n_participants": int(paired_diff.shape[0]),
            "n_nights": None,
            "mean_control": round_or_none(participant_means["control"].mean()) if not participant_means.empty else None,
            "mean_walk": round_or_none(participant_means["walk"].mean()) if not participant_means.empty else None,
            "effect_walk_minus_control": round_or_none(paired_diff.mean()) if not paired_diff.empty else None,
            "ci_lower": None,
            "ci_upper": None,
            "p_value": None,
            "notes": "Difference based on participant-level condition means in the within-person design.",
        }
    )

    model_df = primary_df[["participant_id", "weekday_name", "condition_planned", PRIMARY_OUTCOME]].dropna().copy()
    model_df["condition_walk"] = model_df["condition_planned"].eq("walk").astype(int)
    if model_df["participant_id"].nunique() >= 2 and model_df["condition_walk"].nunique() == 2:
        try:
            model = smf.mixedlm(
                f"{PRIMARY_OUTCOME} ~ condition_walk + C(weekday_name)",
                data=model_df,
                groups=model_df["participant_id"],
            )
            result = model.fit(reml=False, method="lbfgs", disp=False)
            ci = result.conf_int().loc["condition_walk"]
            rows.append(
                {
                    "analysis_type": "pooled_mixed_model",
                    "n_participants": int(model_df["participant_id"].nunique()),
                    "n_nights": int(len(model_df)),
                    "mean_control": None,
                    "mean_walk": None,
                    "effect_walk_minus_control": round_or_none(result.params["condition_walk"]),
                    "ci_lower": round_or_none(ci.iloc[0]),
                    "ci_upper": round_or_none(ci.iloc[1]),
                    "p_value": round_or_none(result.pvalues["condition_walk"], 4),
                    "notes": "Model: total_sleep_time_min ~ condition_walk + C(weekday_name) + (1 | participant_id)",
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "analysis_type": "pooled_mixed_model_error",
                    "n_participants": int(model_df["participant_id"].nunique()),
                    "n_nights": int(len(model_df)),
                    "mean_control": None,
                    "mean_walk": None,
                    "effect_walk_minus_control": None,
                    "ci_lower": None,
                    "ci_upper": None,
                    "p_value": None,
                    "notes": f"Model fitting failed: {exc}",
                }
            )
    return pd.DataFrame(rows)


def format_delta(value: float | None, units: str) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    if units == "%":
        return f"{value:+.1f} percentage points"
    if units == "score":
        return f"{value:+.1f} points"
    if units == "count":
        return f"{value:+.1f}"
    return f"{value:+.1f} {units}"


def add_distribution_panel(
    ax: plt.Axes,
    data: pd.DataFrame,
    outcome: str,
    title: str,
    ylabel: str,
    units: str,
    note: str | None = None,
) -> None:
    plot_df = data[["participant_id", "condition_planned", outcome]].dropna().copy()
    if plot_df.empty:
        ax.set_axis_off()
        return

    sns.boxplot(
        data=plot_df,
        x="condition_planned",
        y=outcome,
        order=CONDITION_ORDER,
        hue="condition_planned",
        hue_order=CONDITION_ORDER,
        palette=PALETTE,
        dodge=False,
        width=0.55,
        showfliers=False,
        linewidth=1.1,
        boxprops={"alpha": 0.35},
        medianprops={"color": NEUTRAL, "linewidth": 1.6},
        whiskerprops={"color": "#64748b", "linewidth": 1.1},
        capprops={"color": "#64748b", "linewidth": 1.1},
        ax=ax,
    )
    sns.stripplot(
        data=plot_df,
        x="condition_planned",
        y=outcome,
        order=CONDITION_ORDER,
        hue="condition_planned",
        palette=PALETTE,
        dodge=False,
        jitter=0.09,
        size=6.8,
        alpha=0.78,
        ax=ax,
    )
    legend = ax.get_legend()
    if legend is not None:
        legend.remove()

    participant_means = (
        plot_df.pivot_table(index="participant_id", columns="condition_planned", values=outcome, aggfunc="mean")
        .reindex(columns=CONDITION_ORDER)
        .dropna()
    )
    for _, row in participant_means.iterrows():
        ax.plot([0, 1], row.values, color="#94a3b8", linewidth=1.0, alpha=0.5, zorder=2)

    condition_means = plot_df.groupby("condition_planned")[outcome].mean().reindex(CONDITION_ORDER)
    if condition_means.notna().all():
        ax.plot([0, 1], condition_means.values, color=NEUTRAL, linewidth=2.2, marker="D", markersize=6, zorder=4)
        delta = round_or_none(condition_means["walk"] - condition_means["control"], 2)
        ax.text(
            0.03,
            0.96,
            f"Mean change: {format_delta(delta, units)}",
            transform=ax.transAxes,
            va="top",
            ha="left",
            fontsize=9,
            bbox=TEXTBOX_STYLE,
        )

    if note:
        ax.text(
            0.03,
            0.06,
            note,
            transform=ax.transAxes,
            va="bottom",
            ha="left",
            fontsize=8.5,
            color="#475569",
        )

    ax.set_title(title)
    ax.set_xlabel("")
    ax.set_ylabel(ylabel)
    ax.set_xticks([0, 1], [CONDITION_LABELS[c] for c in CONDITION_ORDER])
    ax.grid(True, axis="y", alpha=0.25)


def create_protocol_timeline_figure(full_df: pd.DataFrame, output_path: Path) -> None:
    study_df = full_df[full_df["include_in_study"]].copy()
    participants = sorted(study_df["participant_id"].unique())
    fig, axes = plt.subplots(len(participants), 1, figsize=(12.5, max(3.8, 3.0 * len(participants))), sharex=True)
    if len(participants) == 1:
        axes = [axes]

    for ax, participant_id in zip(axes, participants):
        participant_df = study_df[study_df["participant_id"].eq(participant_id)].sort_values("included_study_night_index")
        observed = participant_df[participant_df[PRIMARY_OUTCOME].notna()].copy()

        for condition in CONDITION_ORDER:
            window = participant_df[participant_df["condition_planned"].eq(condition)]
            if window.empty:
                continue
            ax.axvspan(
                window["included_study_night_index"].min() - 0.5,
                window["included_study_night_index"].max() + 0.5,
                color=PALETTE[condition],
                alpha=0.12,
                zorder=0,
            )
            ax.text(
                window["included_study_night_index"].mean(),
                1.02,
                CONDITION_LABELS[condition],
                transform=ax.get_xaxis_transform(),
                ha="center",
                va="bottom",
                fontsize=9,
                color=PALETTE[condition],
                fontweight="bold",
            )

        ax.plot(
            observed["included_study_night_index"],
            observed[PRIMARY_OUTCOME],
            color=NEUTRAL,
            linewidth=1.6,
            alpha=0.9,
            zorder=1,
        )
        for condition in CONDITION_ORDER:
            subset = observed[observed["condition_planned"].eq(condition)]
            if subset.empty:
                continue
            ax.scatter(
                subset["included_study_night_index"],
                subset[PRIMARY_OUTCOME],
                color=PALETTE[condition],
                edgecolor="white",
                linewidth=0.8,
                s=72,
                zorder=3,
            )

        flagged = observed[~observed["valid_primary_analysis_night"]]
        if not flagged.empty:
            ax.scatter(
                flagged["included_study_night_index"],
                flagged[PRIMARY_OUTCOME],
                facecolor="none",
                edgecolor="#b91c1c",
                linewidth=1.5,
                s=135,
                zorder=4,
            )

        missing_sleep = participant_df[participant_df[PRIMARY_OUTCOME].isna()]
        if not missing_sleep.empty and not observed.empty:
            baseline = observed[PRIMARY_OUTCOME].min() - 20
            ax.scatter(
                missing_sleep["included_study_night_index"],
                [baseline] * len(missing_sleep),
                marker="x",
                color="#b91c1c",
                s=48,
                zorder=4,
            )
            ax.set_ylim(bottom=baseline - 15)

        ax.set_title(f"{participant_id}", loc="left", fontsize=12, fontweight="bold")
        ax.set_ylabel("Sleep duration (min)")
        ax.grid(True, axis="y", alpha=0.25)
        ax.set_xlim(0.5, max(participant_df["included_study_night_index"].max(), 14) + 0.5)

    legend_handles = [
        Patch(facecolor=PALETTE["control"], alpha=0.12, edgecolor="none", label="Planned control window"),
        Patch(facecolor=PALETTE["walk"], alpha=0.12, edgecolor="none", label="Planned walk window"),
        Line2D([0], [0], color=NEUTRAL, marker="o", markerfacecolor="white", label="Observed nightly sleep"),
        Line2D([0], [0], color="#b91c1c", marker="o", markerfacecolor="none", linestyle="None", label="Flagged study night"),
    ]
    axes[0].legend(handles=legend_handles, loc="upper left", ncol=2, frameon=True)
    axes[-1].set_xlabel("Aligned study night (1-14)")
    axes[-1].set_xticks(range(1, 15))
    save_figure(fig, output_path)


def create_primary_outcome_figure(primary_df: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12.8, 4.8), gridspec_kw={"width_ratios": [1.0, 1.2]})
    participant_means = (
        primary_df.pivot_table(index="participant_id", columns="condition_planned", values=PRIMARY_OUTCOME, aggfunc="mean")
        .reindex(columns=CONDITION_ORDER)
        .dropna()
    )

    for participant_id, row in participant_means.iterrows():
        axes[0].plot([0, 1], row.values, color="#94a3b8", linewidth=1.5, alpha=0.85, zorder=1)
        axes[0].scatter(0, row["control"], s=82, color=PALETTE["control"], edgecolor="white", linewidth=0.8, zorder=2)
        axes[0].scatter(1, row["walk"], s=82, color=PALETTE["walk"], edgecolor="white", linewidth=0.8, zorder=2)
        if participant_means.shape[0] <= 6:
            axes[0].text(1.06, row["walk"], participant_id, fontsize=8.5, va="center", color=NEUTRAL)

    if not participant_means.empty:
        pooled_means = participant_means.mean()
        pooled_delta = round_or_none((participant_means["walk"] - participant_means["control"]).mean(), 2)
    else:
        pooled_means = primary_df.groupby("condition_planned")[PRIMARY_OUTCOME].mean().reindex(CONDITION_ORDER)
        pooled_delta = round_or_none(pooled_means["walk"] - pooled_means["control"], 2) if pooled_means.notna().all() else None

    axes[0].plot([0, 1], pooled_means.values, color=NEUTRAL, linewidth=2.8, zorder=3)
    axes[0].scatter([0, 1], pooled_means.values, color=NEUTRAL, marker="D", s=62, zorder=4)
    axes[0].set_title("Within-participant mean change")
    axes[0].set_xticks([0, 1], [CONDITION_LABELS[c] for c in CONDITION_ORDER])
    axes[0].set_ylabel("Total sleep time (min)")
    axes[0].set_xlim(-0.15, 1.3 if participant_means.shape[0] <= 6 else 1.1)
    axes[0].grid(True, axis="y", alpha=0.25)
    axes[0].text(
        0.03,
        0.96,
        f"Each thin line = one participant\nPooled mean change: {format_delta(pooled_delta, 'min')}",
        transform=axes[0].transAxes,
        va="top",
        ha="left",
        fontsize=9,
        bbox=TEXTBOX_STYLE,
    )

    add_distribution_panel(
        axes[1],
        primary_df,
        PRIMARY_OUTCOME,
        "Night-level distribution",
        "Total sleep time (min)",
        "min",
        note="Boxes show nightly spread within each condition; the dark line shows condition means.",
    )
    save_figure(fig, output_path)


def create_secondary_outcomes_figure(primary_df: pd.DataFrame, output_path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12.8, 4.8))
    add_distribution_panel(
        axes[0],
        primary_df,
        "sleep_efficiency",
        "Sleep efficiency",
        "Sleep efficiency (%)",
        "%",
        note="Secondary quality metric because it adjusts for time spent in bed.",
    )
    add_distribution_panel(
        axes[1],
        primary_df,
        "sleep_score",
        "Sleep score",
        "Sleep score",
        "score",
        note="User-facing composite metric; keep interpretation more cautious than duration or efficiency.",
    )
    save_figure(fig, output_path)


def create_walk_protocol_context_figure(primary_df: pd.DataFrame, output_path: Path) -> None:
    walk_df = primary_df[primary_df["condition_planned"].eq("walk")].copy().sort_values(["participant_id", "condition_night_index"])
    participant_palette = get_participant_palette(walk_df)
    fig, axes = plt.subplots(3, 1, figsize=(12.5, 8.6), sharex=True)
    specs = [
        ("minutes_from_matched_walk_end_to_bedtime", "Minutes from walk end to bedtime", {"ymin": 0, "ymax": 90, "label": "Planned <=90 min window"}),
        ("matched_walk_duration_min", "Tracked walk duration (min)", {"ymin": 18, "ymax": 22, "label": "20-minute target band"}),
        ("matched_walk_avg_hrm", "Average walk heart rate (bpm)", None),
    ]

    for ax, (column, ylabel, guide) in zip(axes, specs):
        if guide:
            ax.axhspan(guide["ymin"], guide["ymax"], color=PALETTE["walk"], alpha=0.08, zorder=0)
            ax.text(0.99, 0.93, guide["label"], transform=ax.transAxes, ha="right", va="top", fontsize=8.5, color=PALETTE["walk"])
        for participant_id, participant_df in walk_df.groupby("participant_id"):
            ax.plot(
                participant_df["condition_night_index"],
                participant_df[column],
                marker="o",
                linewidth=1.8,
                markersize=5.5,
                color=participant_palette[participant_id],
                label=participant_id,
            )
        ax.set_ylabel(ylabel)
        ax.grid(True, axis="y", alpha=0.25)

    axes[0].set_title("Walk-week intervention fidelity")
    if walk_df["participant_id"].nunique() > 1:
        axes[0].legend(title="Participant", loc="upper left", ncol=min(3, walk_df["participant_id"].nunique()))
    axes[-1].set_xlabel("Aligned walk night (1-7)")
    axes[-1].set_xticks(range(1, 8))
    save_figure(fig, output_path)


def create_weather_context_figure(primary_df: pd.DataFrame, output_path: Path) -> None:
    weather_df = primary_df[primary_df["condition_planned"].eq("walk")].copy().sort_values(["participant_id", "condition_night_index"])
    participant_palette = get_participant_palette(weather_df)
    fig, axes = plt.subplots(3, 1, figsize=(12.5, 8.4), sharex=True)
    specs = [
        ("walk_temp_c", "Walk-time temperature (deg C)"),
        ("walk_wind_m_s", "Walk-time wind speed (m/s)"),
        ("walk_precip_mm", "Walk-time precipitation (mm)"),
    ]

    for ax, (column, ylabel) in zip(axes, specs):
        for participant_id, participant_df in weather_df.groupby("participant_id"):
            valid = participant_df[participant_df[column].notna()].copy()
            if valid.empty:
                continue
            ax.plot(
                valid["condition_night_index"],
                valid[column],
                marker="o",
                linewidth=1.8,
                markersize=5.5,
                color=participant_palette[participant_id],
                label=participant_id,
            )
            if column == "walk_precip_mm" and "walk_trace_precip_flag" in valid.columns:
                trace_df = valid[coerce_bool(valid["walk_trace_precip_flag"])]
                if not trace_df.empty:
                    ax.scatter(
                        trace_df["condition_night_index"],
                        trace_df[column],
                        facecolor="white",
                        edgecolor=participant_palette[participant_id],
                        linewidth=1.2,
                        s=80,
                        zorder=4,
                    )
        ax.set_ylabel(ylabel)
        ax.grid(True, axis="y", alpha=0.25)

    axes[0].set_title("Outdoor context during the walk week")
    axes[0].text(
        0.99,
        0.93,
        "Weather is descriptive context and a future sensitivity covariate, not the headline result.",
        transform=axes[0].transAxes,
        ha="right",
        va="top",
        fontsize=8.5,
        color="#475569",
    )
    if weather_df["participant_id"].nunique() > 1:
        axes[0].legend(title="Participant", loc="upper left", ncol=min(3, weather_df["participant_id"].nunique()))
    axes[-1].set_xlabel("Aligned walk night (1-7)")
    axes[-1].set_xticks(range(1, 8))
    save_figure(fig, output_path)


def write_report_outline() -> Path:
    output_path = Path("reports/report_outline.md")
    outline = """# JBM170 Report Outline

## 1. Research Question And Stakeholder Perspective
- Research question: does a 20-minute evening walk ending at most 90 minutes before sleep change same-night sleep outcomes compared with a no-walk control week?
- Stakeholder perspective: individual fitness, framed as a realistic consumer-wellness habit for people who are not naturally sport-oriented.
- Position this as a repeated-night pilot study with within-person comparison, not a medical trial.

## 2. Data Sources And Data Actually Used
- Raw Mi Fitness exports from each participant.
- KNMI hourly validated weather data for Eindhoven station 370.
- Exact 7 control nights and exact 7 walk nights per participant, with participant-specific calendar windows.
- Use [tbl01_data_qc_by_participant.csv](tables/tbl01_data_qc_by_participant.csv) to document what was included, adherence, and weather coverage.

## 3. Dataflow, Cleaning, And Feature Extraction
- Describe sleep-session parsing, tracked-walk parsing, main-sleep reconstruction, and participant-night assembly.
- Explain why report assets use only study-window nights, while the raw pipeline still preserves the full export.
- State that all cross-participant plots align nights by `study night 1-14` or `walk night 1-7`, because teammate calendar dates differ.
- Explain weather-hour matching and the decision to treat weather as contextual or sensitivity information rather than as the primary causal driver.

## 4. Results
- Protocol and execution:
  - [fig01_protocol_timeline.png](figures/fig01_protocol_timeline.png)
  - [fig04_walk_protocol_context.png](figures/fig04_walk_protocol_context.png)
- Primary finding:
  - [fig02_primary_outcome_comparison.png](figures/fig02_primary_outcome_comparison.png)
  - [tbl03_primary_effects.csv](tables/tbl03_primary_effects.csv)
- Secondary and exploratory sleep findings:
  - [fig03_secondary_outcomes.png](figures/fig03_secondary_outcomes.png)
  - [tbl02_outcome_summary_by_condition.csv](tables/tbl02_outcome_summary_by_condition.csv)
- Environmental context:
  - [fig05_weather_context.png](figures/fig05_weather_context.png)

## 5. Interpretation Guidance
- Lead with the within-person change in total sleep time, because that is the clearest and strongest endpoint.
- Use sleep efficiency and time in bed as supporting sleep-quality/context measures.
- Treat the device sleep score as useful for a consumer-facing narrative, but still acknowledge that it is a vendor-defined composite.
- Treat REM and deep-sleep values as exploratory only.
- Discuss weather as a plausible contextual influence on comfort or adherence, not as the main explanatory claim.

## 6. FAIR, Ethics, And Limitations
- Explain pseudonymization, raw-versus-processed separation, and reproducible reruns.
- State that consumer wearables estimate sleep and stages rather than measure them clinically.
- Discuss sample size, self-tracking bias, order effects, and the fact that the walk week is behaviorally richer than the control week.

## 7. Enterprise Relevance
- Frame the outcome as a low-friction recommendation engine for lifestyle improvement.
- Emphasize habit feasibility, adherence tracking, and weather-aware nudging rather than diagnosis.

## 8. Individual Reflection
- Keep this section manual per group member.
- Explicitly separate common-group findings from each member's contribution and reflection.
"""
    output_path.write_text(outline, encoding="utf-8")
    return output_path


def write_asset_manifest() -> Path:
    output_path = Path("reports/report_asset_manifest.md")
    assets = [
        ("fig01_protocol_timeline.png", "figure", "Generated", "Two-week sleep-duration timeline per participant", "Shows the intervention story while keeping participants comparable despite different calendar dates.", "Results"),
        ("fig02_primary_outcome_comparison.png", "figure", "Generated", "Primary outcome: total sleep time", "Combines within-person mean change with nightly spread, which matches the repeated-measures design.", "Results"),
        ("fig03_secondary_outcomes.png", "figure", "Generated", "Secondary outcomes: sleep efficiency and sleep score", "Keeps the report focused on the strongest supporting metrics without turning stages into headline evidence.", "Results"),
        ("fig04_walk_protocol_context.png", "figure", "Generated", "Walk-week protocol fidelity", "Shows whether the intervention itself was delivered consistently before interpreting the sleep results.", "Results"),
        ("fig05_weather_context.png", "figure", "Generated", "Walk-week weather context", "Uses KNMI data descriptively so the report can discuss external conditions without over-claiming weather effects.", "Results"),
        ("tbl01_data_qc_by_participant.csv", "table", "Generated", "Participant-level QC, adherence, and weather coverage", "Documents what was actually analyzed and whether the intervention/weather linkage is trustworthy.", "Data/Methods"),
        ("tbl02_outcome_summary_by_condition.csv", "table", "Generated", "Condition summaries across primary, secondary, and exploratory metrics", "Keeps the broader proposal metrics available while clearly labeling weaker exploratory endpoints.", "Results"),
        ("tbl03_primary_effects.csv", "table", "Generated", "Primary descriptive effects and pooled model when available", "Holds the formal effect estimate for the main research question.", "Results"),
        ("report_analysis_strategy.md", "markdown", "Generated", "Report-writing strategy and guardrails", "Explains what to emphasize, what to keep exploratory, and how to write the final report consistently.", "Preparation"),
    ]
    lines = [
        "# Report Asset Manifest",
        "",
        "| Asset | Type | Status | Purpose | Why this asset exists | Suggested section |",
        "|:------|:-----|:-------|:--------|:----------------------|:------------------|",
    ]
    for name, asset_type, status, purpose, rationale, section in assets:
        lines.append(f"| `{name}` | {asset_type} | {status} | {purpose} | {rationale} | {section} |")
    lines.extend(["", "Figures directory: `reports/figures`", "Tables directory: `reports/tables`"])
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def write_report_analysis_strategy() -> Path:
    output_path = Path("reports/report_analysis_strategy.md")
    strategy = """# Report Analysis Strategy

## Core Analytic Stance
- Use an exact `7 control + 7 walk` within-person design for every participant.
- Treat `total_sleep_time_min` as the main endpoint because it is direct and interpretable.
- Keep the report aligned to the `Individual fitness` stakeholder perspective: practical, behavior-oriented, and preventive rather than clinical.

## Metric Hierarchy
- Primary:
  - `total_sleep_time_min`
- Secondary:
  - `sleep_efficiency`
  - `time_in_bed_min`
  - `sleep_score` as a user-facing composite, with explicit caution that it is vendor-defined
- Exploratory:
  - `awake_count`
  - `awake_duration_min`
  - `deep_sleep_min`
  - `rem_sleep_min`
  - walk effort variables and KNMI weather variables

## Visualization Strategy
- `fig01_protocol_timeline.png`:
  - Use only study-window nights.
  - Align participants by `study night 1-14`, not calendar date, so different teammate windows stay comparable.
  - Use shaded background bands to mark the planned control and walk weeks.
- `fig02_primary_outcome_comparison.png`:
  - Show within-participant mean change because that matches the repeated-measures logic.
  - Also show nightly spread so the report does not hide variability.
- `fig03_secondary_outcomes.png`:
  - Focus on sleep efficiency and sleep score because they are the clearest support metrics for the main story.
  - Keep sleep stages out of the headline figures.
- `fig04_walk_protocol_context.png`:
  - Demonstrate that the intervention was actually delivered as intended before claiming anything about sleep changes.
- `fig05_weather_context.png`:
  - Use KNMI variables as context, not as headline causal evidence.
  - Save weather-sleep association claims for pooled sensitivity analysis later.

## Statistical Strategy
- Always report descriptive condition means and participant-level paired differences.
- Once at least two participants are available, use the pooled mixed model in `tbl03_primary_effects.csv`.
- Keep weather out of the main model for the headline result.
- If the final pooled sample is large enough, use weather only in a sensitivity analysis or exploratory appendix.

## Writing Guardrails
- Do not discuss `not in study` nights in the report figures.
- Do not over-interpret sleep stages or the vendor sleep score.
- Do not present weather as the explanation unless sensitivity analysis supports it.
- Keep the results section ordered as:
  1. study execution and adherence,
  2. primary outcome,
  3. secondary outcomes,
  4. exploratory/contextual findings,
  5. limitations and enterprise relevance.
"""
    output_path.write_text(strategy, encoding="utf-8")
    return output_path


def main() -> None:
    sns.set_theme(
        style="whitegrid",
        context="notebook",
        rc={
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.titleweight": "bold",
            "axes.labelcolor": NEUTRAL,
            "text.color": NEUTRAL,
            "axes.edgecolor": "#cbd5e1",
            "grid.color": "#cbd5e1",
        },
    )

    figures_dir, tables_dir = ensure_output_dirs()
    full_df = load_participant_datasets()
    primary_df = load_group_dataset()

    qc_table = build_qc_table(full_df)
    outcome_table = build_outcome_summary_table(primary_df)
    primary_effects_table = build_primary_effects_table(primary_df)

    qc_path = tables_dir / "tbl01_data_qc_by_participant.csv"
    outcome_path = tables_dir / "tbl02_outcome_summary_by_condition.csv"
    effects_path = tables_dir / "tbl03_primary_effects.csv"
    qc_table.to_csv(qc_path, index=False)
    outcome_table.to_csv(outcome_path, index=False)
    primary_effects_table.to_csv(effects_path, index=False)

    timeline_path = figures_dir / "fig01_protocol_timeline.png"
    primary_path = figures_dir / "fig02_primary_outcome_comparison.png"
    secondary_path = figures_dir / "fig03_secondary_outcomes.png"
    walk_context_path = figures_dir / "fig04_walk_protocol_context.png"
    weather_context_path = figures_dir / "fig05_weather_context.png"

    create_protocol_timeline_figure(full_df, timeline_path)
    create_primary_outcome_figure(primary_df, primary_path)
    create_secondary_outcomes_figure(primary_df, secondary_path)
    create_walk_protocol_context_figure(primary_df, walk_context_path)
    create_weather_context_figure(primary_df, weather_context_path)

    outline_path = write_report_outline()
    manifest_path = write_asset_manifest()
    strategy_path = write_report_analysis_strategy()

    print(f"Wrote {qc_path.as_posix()}")
    print(f"Wrote {outcome_path.as_posix()}")
    print(f"Wrote {effects_path.as_posix()}")
    print(f"Wrote {timeline_path.as_posix()}")
    print(f"Wrote {primary_path.as_posix()}")
    print(f"Wrote {secondary_path.as_posix()}")
    print(f"Wrote {walk_context_path.as_posix()}")
    print(f"Wrote {weather_context_path.as_posix()}")
    print(f"Wrote {outline_path.as_posix()}")
    print(f"Wrote {manifest_path.as_posix()}")
    print(f"Wrote {strategy_path.as_posix()}")


if __name__ == "__main__":
    main()
