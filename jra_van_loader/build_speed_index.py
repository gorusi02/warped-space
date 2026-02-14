import argparse
import logging
import os
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_DATASET = "jra_common"
DEFAULT_SOURCE_TABLE = "analysis_view"
DEFAULT_OUTPUT_TABLE = "speed_index_master"
DEFAULT_BASELINE_TABLE = "speed_index_baseline"
DEFAULT_LOCATION = "asia-northeast1"

SURFACE_TURF = "\u829d"
SURFACE_DIRT = "\u30c0"
VALID_SURFACES = [SURFACE_TURF, SURFACE_DIRT]


@dataclass
class SourceColumns:
    horse_key: str
    horse_name: str
    surface: str
    time_sec: str
    distance: str
    weight: str | None
    num_horses: str | None
    age: str | None
    sex: str | None
    track_condition: str | None
    venue: str | None
    class_name: str | None


def resolve_project_id(arg_project: str | None) -> str | None:
    if arg_project:
        return arg_project
    if os.environ.get("GOOGLE_CLOUD_PROJECT"):
        return os.environ["GOOGLE_CLOUD_PROJECT"]
    return os.environ.get("GCLOUD_PROJECT")


def pick_column(columns: set[str], candidates: list[str], required: bool = False) -> str | None:
    for name in candidates:
        if name in columns:
            return name
    if required:
        raise ValueError(f"Required column is missing. candidates={candidates}")
    return None


def inspect_source_columns(client: bigquery.Client, table_id: str) -> SourceColumns:
    table = client.get_table(table_id)
    columns = {field.name for field in table.schema}

    horse_id_col = pick_column(columns, ["ketto_num", "horse_id", "blood_reg_num"])
    horse_name_col = pick_column(columns, ["horse_name", "bamei", "horse"], required=True)

    return SourceColumns(
        horse_key=horse_id_col or horse_name_col,
        horse_name=horse_name_col,
        surface=pick_column(columns, ["surface", "course"], required=True),
        time_sec=pick_column(columns, ["time_sec", "finish_time_sec", "race_time_sec"], required=True),
        distance=pick_column(columns, ["distance", "kyori"], required=True),
        weight=pick_column(columns, ["weight", "kinryo"]),
        num_horses=pick_column(columns, ["num_horses", "field_size", "head_count"]),
        age=pick_column(columns, ["age"]),
        sex=pick_column(columns, ["sex"]),
        track_condition=pick_column(columns, ["track_condition", "baba_state", "condition"]),
        venue=pick_column(columns, ["venue", "kaisai_basho", "place"]),
        class_name=pick_column(columns, ["class_name", "race_class", "race_grade"]),
    )


def build_source_query(table_id: str, cols: SourceColumns) -> str:
    optional_cols = []
    for alias, source in [
        ("weight", cols.weight),
        ("num_horses", cols.num_horses),
        ("age", cols.age),
        ("sex", cols.sex),
        ("track_condition", cols.track_condition),
        ("venue", cols.venue),
        ("class_name", cols.class_name),
    ]:
        if source:
            optional_cols.append(f"SAFE_CAST({source} AS STRING) AS {alias}")
        else:
            optional_cols.append(f"CAST(NULL AS STRING) AS {alias}")

    return f"""
    SELECT
      TRIM(CAST({cols.horse_key} AS STRING)) AS horse_key,
      TRIM(CAST({cols.horse_name} AS STRING)) AS horse_name,
      TRIM(CAST({cols.surface} AS STRING)) AS surface,
      SAFE_CAST({cols.time_sec} AS FLOAT64) AS time_sec,
      SAFE_CAST({cols.distance} AS FLOAT64) AS distance,
      {", ".join(optional_cols)}
    FROM `{table_id}`
    WHERE {cols.time_sec} IS NOT NULL
      AND {cols.distance} IS NOT NULL
    """


def normalize_source_frame(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["horse_key"] = data["horse_key"].astype(str).str.strip()
    data["horse_name"] = data["horse_name"].astype(str).str.strip()

    # Keep first character so values like "ダート" become "ダ".
    data["surface"] = data["surface"].astype(str).str.strip().str[:1]

    data["time_sec"] = pd.to_numeric(data["time_sec"], errors="coerce")
    data["distance"] = pd.to_numeric(data["distance"], errors="coerce")
    for c in ["weight", "num_horses", "age"]:
        data[c] = pd.to_numeric(data[c], errors="coerce")
    for c in ["sex", "track_condition", "venue", "class_name"]:
        data[c] = data[c].astype(str).str.strip()
        data.loc[data[c].isin(["", "None", "nan", "NaN"]), c] = np.nan

    data = data[
        (data["horse_key"] != "")
        & (data["surface"].isin(VALID_SURFACES))
        & (data["time_sec"] > 0)
        & (data["distance"] > 0)
    ].copy()
    data["log_time"] = np.log(data["time_sec"])
    data["log_dist"] = np.log(data["distance"])
    return data


def fit_surface_index(
    data: pd.DataFrame,
    surface: str,
    shrinkage_lambda: float,
    min_rows: int,
) -> pd.DataFrame:
    d = data[data["surface"] == surface].copy()
    if len(d) < min_rows:
        logger.warning("Skip surface=%s because rows=%s < min_rows=%s", surface, len(d), min_rows)
        return pd.DataFrame()

    num_cols = ["log_dist"] + [c for c in ["weight", "num_horses", "age"] if d[c].notna().sum() > 0]
    cat_cols = [c for c in ["track_condition", "venue", "class_name", "sex"] if d[c].notna().sum() > 0]

    keep_cols = ["horse_key", "horse_name", "log_time"] + num_cols + cat_cols
    d = d[keep_cols].dropna(subset=["log_time", "horse_key"] + num_cols).copy()
    if len(d) < min_rows:
        logger.warning("Skip surface=%s after dropna because rows=%s < min_rows=%s", surface, len(d), min_rows)
        return pd.DataFrame()

    x_parts = [d[num_cols].astype(float)]
    if cat_cols:
        x_parts.append(pd.get_dummies(d[cat_cols], columns=cat_cols, drop_first=True, dtype=float))
    x_df = pd.concat(x_parts, axis=1)

    # Remove constant columns to avoid unstable least-squares solutions.
    x_df = x_df.loc[:, x_df.nunique(dropna=False) > 1]
    if x_df.shape[1] == 0:
        logger.warning("Skip surface=%s because all predictors were constant.", surface)
        return pd.DataFrame()

    y = d["log_time"].to_numpy(dtype=float)
    x = x_df.to_numpy(dtype=float)
    x = np.hstack([np.ones((x.shape[0], 1)), x])
    beta, _, _, _ = np.linalg.lstsq(x, y, rcond=None)
    y_pred = x @ beta

    d["residual"] = y - y_pred
    horse_stats = (
        d.groupby("horse_key", as_index=False)
        .agg(
            horse_name=("horse_name", "first"),
            mean_resid=("residual", "mean"),
            run_count=("residual", "size"),
        )
        .copy()
    )
    horse_stats["run_count"] = horse_stats["run_count"].astype(int)
    learning = horse_stats["run_count"] / (horse_stats["run_count"] + shrinkage_lambda)
    horse_stats["u_hat"] = -1.0 * learning * horse_stats["mean_resid"]

    mu = float(horse_stats["u_hat"].mean())
    sigma = float(horse_stats["u_hat"].std(ddof=0))
    if sigma == 0.0:
        horse_stats["speed_z"] = 0.0
    else:
        horse_stats["speed_z"] = (horse_stats["u_hat"] - mu) / sigma

    horse_stats["speed_index"] = 100.0 + 10.0 * horse_stats["speed_z"]
    horse_stats["surface"] = surface

    return horse_stats[
        ["horse_key", "horse_name", "surface", "u_hat", "speed_z", "speed_index", "run_count"]
    ].copy()


def build_speed_index_table(
    client: bigquery.Client,
    source_table_id: str,
    output_table_id: str,
    baseline_table_id: str,
    location: str,
    shrinkage_lambda: float,
    min_rows: int,
    asof_date: date,
) -> None:
    source_cols = inspect_source_columns(client, source_table_id)
    query = build_source_query(source_table_id, source_cols)
    logger.info("Loading source data from %s", source_table_id)
    rows = list(client.query(query, location=location).result())
    df = pd.DataFrame([dict(row.items()) for row in rows])
    logger.info("Loaded rows=%s", len(df))

    source = normalize_source_frame(df)
    logger.info("Rows after normalization=%s", len(source))
    if source.empty:
        raise ValueError("No usable rows after normalization.")

    parts = []
    for surface in VALID_SURFACES:
        result = fit_surface_index(
            data=source,
            surface=surface,
            shrinkage_lambda=shrinkage_lambda,
            min_rows=min_rows,
        )
        if not result.empty:
            parts.append(result)

    if not parts:
        raise ValueError("No speed index rows were produced.")

    master = pd.concat(parts, ignore_index=True)
    master["asof_date"] = asof_date.isoformat()
    master["speed_index"] = master["speed_index"].astype(float).round(4)
    master["speed_z"] = master["speed_z"].astype(float).round(6)
    master["u_hat"] = master["u_hat"].astype(float).round(8)
    master["horse_key"] = master["horse_key"].astype(str)
    master["horse_name"] = master["horse_name"].astype(str)

    baseline = (
        master.groupby("surface", as_index=False)
        .agg(
            mean_u=("u_hat", "mean"),
            sd_u=("u_hat", lambda s: float(np.std(s.to_numpy(dtype=float), ddof=0))),
            n_horses=("horse_key", "nunique"),
        )
        .copy()
    )
    baseline["period"] = "all_time"
    baseline["asof_date"] = asof_date.isoformat()
    baseline = baseline[["surface", "period", "mean_u", "sd_u", "n_horses", "asof_date"]]

    logger.info("Writing %s rows to %s", len(master), output_table_id)
    master_rows = master.to_dict(orient="records")
    client.load_table_from_json(
        master_rows,
        output_table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        ),
    ).result()

    logger.info("Writing %s rows to %s", len(baseline), baseline_table_id)
    baseline_rows = baseline.to_dict(orient="records")
    client.load_table_from_json(
        baseline_rows,
        baseline_table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        ),
    ).result()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build speed-index tables from analysis view")
    parser.add_argument("--project", "-p", help="GCP project ID")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="BigQuery dataset (default: jra_common)")
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE, help="Source table/view name")
    parser.add_argument("--output-table", default=DEFAULT_OUTPUT_TABLE, help="Output master table name")
    parser.add_argument("--baseline-table", default=DEFAULT_BASELINE_TABLE, help="Output baseline table name")
    parser.add_argument("--location", "-l", default=DEFAULT_LOCATION, help="BigQuery location")
    parser.add_argument("--key", "-k", help="Path to service account JSON")
    parser.add_argument(
        "--shrinkage-lambda",
        type=float,
        default=10.0,
        help="Shrinkage factor K in u_hat = -(n/(n+K))*mean_residual",
    )
    parser.add_argument("--min-rows", type=int, default=300, help="Minimum rows per surface to fit")
    parser.add_argument(
        "--asof-date",
        default=date.today().isoformat(),
        help="As-of date (YYYY-MM-DD) stored in output tables",
    )
    args = parser.parse_args()

    if args.key:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key

    project_id = resolve_project_id(args.project)
    if not project_id:
        raise ValueError("Project ID is required. Set --project or GOOGLE_CLOUD_PROJECT.")

    asof_date = date.fromisoformat(args.asof_date)
    client = bigquery.Client(project=project_id)

    source_table_id = f"{project_id}.{args.dataset}.{args.source_table}"
    output_table_id = f"{project_id}.{args.dataset}.{args.output_table}"
    baseline_table_id = f"{project_id}.{args.dataset}.{args.baseline_table}"

    build_speed_index_table(
        client=client,
        source_table_id=source_table_id,
        output_table_id=output_table_id,
        baseline_table_id=baseline_table_id,
        location=args.location,
        shrinkage_lambda=args.shrinkage_lambda,
        min_rows=args.min_rows,
        asof_date=asof_date,
    )
    logger.info("Completed speed-index build.")


if __name__ == "__main__":
    main()
