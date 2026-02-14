import argparse
import logging
import os
from glob import glob
from typing import Iterable

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_RAW_DATASET = "jra_raw"
DEFAULT_CORE_DATASET = "jra_core"

MERGE_KEYS: dict[str, list[str]] = {
    "RA": ["Year", "MonthDay", "JyoCD", "Kaiji", "Nichiji", "RaceNum"],
    "SE": ["Year", "MonthDay", "JyoCD", "Kaiji", "Nichiji", "RaceNum", "Umaban"],
}


def infer_record_type(file_path: str) -> str | None:
    filename = os.path.basename(file_path)
    parts = filename.split("_")
    if not parts:
        return None
    record_type = parts[0].strip()
    return record_type or None


def parse_merge_types(value: str) -> set[str]:
    return {v.strip().upper() for v in value.split(",") if v.strip()}


def get_table_id(project_id: str, dataset_id: str, table_name: str) -> str:
    return f"{project_id}.{dataset_id}.{table_name}"


def create_dataset_if_not_exists(
    client: bigquery.Client, dataset_id: str, location: str = "asia-northeast1"
) -> None:
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset %s already exists.", dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = client.create_dataset(dataset, timeout=30)
        logger.info("Created dataset %s.%s in %s", client.project, dataset.dataset_id, location)


def load_jsonl_to_table(
    client: bigquery.Client,
    file_path: str,
    table_id: str,
    write_disposition: str,
    allow_field_addition: bool,
) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
        autodetect=True,
        ignore_unknown_values=True,
    )
    if allow_field_addition:
        job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

    filename = os.path.basename(file_path)
    logger.info("Loading %s -> %s (%s)", filename, table_id, write_disposition)
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    logger.info("Loaded rows=%s columns=%s into %s", table.num_rows, len(table.schema), table_id)


def create_table_if_not_exists_from_stage(
    client: bigquery.Client, target_table_id: str, stage_table_id: str
) -> None:
    query = f"""
    CREATE TABLE IF NOT EXISTS `{target_table_id}` AS
    SELECT *
    FROM `{stage_table_id}`
    WHERE 1 = 0
    """
    client.query(query).result()


def merge_stage_into_target(
    client: bigquery.Client,
    stage_table_id: str,
    target_table_id: str,
    merge_keys: Iterable[str],
) -> bool:
    stage_table = client.get_table(stage_table_id)
    columns = [field.name for field in stage_table.schema]
    key_list = list(merge_keys)
    missing_keys = [k for k in key_list if k not in columns]
    if missing_keys:
        logger.warning(
            "Skip merge to %s because merge keys are missing in staging table: %s",
            target_table_id,
            ",".join(missing_keys),
        )
        return False

    create_table_if_not_exists_from_stage(client, target_table_id, stage_table_id)

    order_expr = (
        "SAFE_CAST(`fetched_at` AS TIMESTAMP) DESC, `fetched_at` DESC"
        if "fetched_at" in columns
        else ", ".join([f"CAST(`{k}` AS STRING) DESC" for k in key_list])
    )
    partition_expr = ", ".join([f"CAST(`{k}` AS STRING)" for k in key_list])
    on_clause = " AND ".join(
        [f"COALESCE(CAST(T.`{k}` AS STRING), '') = COALESCE(CAST(S.`{k}` AS STRING), '')" for k in key_list]
    )
    update_clause = ", ".join([f"`{c}` = S.`{c}`" for c in columns])
    insert_columns = ", ".join([f"`{c}`" for c in columns])
    insert_values = ", ".join([f"S.`{c}`" for c in columns])

    query = f"""
    MERGE `{target_table_id}` AS T
    USING (
      SELECT * EXCEPT(_rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY {partition_expr}
            ORDER BY {order_expr}
          ) AS _rn
        FROM `{stage_table_id}`
      )
      WHERE _rn = 1
    ) AS S
    ON {on_clause}
    WHEN MATCHED THEN
      UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """
    client.query(query).result()
    return True


def load_jsonl_to_raw(client: bigquery.Client, file_path: str, raw_dataset_id: str) -> str | None:
    record_type = infer_record_type(file_path)
    if not record_type:
        logger.warning("Skipping file with invalid name format: %s", os.path.basename(file_path))
        return None

    raw_table_id = get_table_id(client.project, raw_dataset_id, record_type)
    load_jsonl_to_table(
        client=client,
        file_path=file_path,
        table_id=raw_table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        allow_field_addition=True,
    )
    return record_type


def sync_core_latest_table(
    client: bigquery.Client,
    file_path: str,
    record_type: str,
    core_dataset_id: str,
    staging_prefix: str,
    core_table_suffix: str,
) -> None:
    merge_keys = MERGE_KEYS.get(record_type)
    if not merge_keys:
        logger.info("Skip core merge for record_type=%s", record_type)
        return

    stage_table_id = get_table_id(client.project, core_dataset_id, f"{staging_prefix}{record_type}")
    target_table_id = get_table_id(client.project, core_dataset_id, f"{record_type}{core_table_suffix}")

    load_jsonl_to_table(
        client=client,
        file_path=file_path,
        table_id=stage_table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_field_addition=False,
    )

    merged = merge_stage_into_target(
        client=client,
        stage_table_id=stage_table_id,
        target_table_id=target_table_id,
        merge_keys=merge_keys,
    )
    if merged:
        table = client.get_table(target_table_id)
        logger.info("Merged %s into %s. Rows=%s", record_type, target_table_id, table.num_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load JRA-VAN JSONL to BigQuery")
    parser.add_argument("--input", "-i", required=True, help="Input directory containing JSONL files")
    parser.add_argument("--project", "-p", help="GCP Project ID")
    parser.add_argument("--dataset", "-d", default=DEFAULT_RAW_DATASET, help="Raw BigQuery dataset ID")
    parser.add_argument("--core-dataset", default=DEFAULT_CORE_DATASET, help="Core BigQuery dataset ID")
    parser.add_argument(
        "--merge-types",
        default="RA,SE",
        help="Comma-separated record types to MERGE into core latest tables (default: RA,SE)",
    )
    parser.add_argument(
        "--core-table-suffix",
        default="_latest",
        help="Suffix for core canonical table names (e.g. RA_latest)",
    )
    parser.add_argument("--staging-prefix", default="_stg_", help="Prefix for core staging table names")
    parser.add_argument("--skip-core-merge", action="store_true", help="Skip core MERGE synchronization")
    parser.add_argument("--key", "-k", help="Path to Service Account JSON key")
    parser.add_argument("--location", "-l", default="asia-northeast1", help="Dataset location")
    args = parser.parse_args()

    if args.key:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key

    try:
        client = bigquery.Client(project=args.project) if args.project else bigquery.Client()
    except Exception as e:
        logger.error("Failed to create BigQuery client: %s", e)
        return

    create_dataset_if_not_exists(client, args.dataset, args.location)
    if not args.skip_core_merge:
        create_dataset_if_not_exists(client, args.core_dataset, args.location)

    merge_types = parse_merge_types(args.merge_types)
    files = sorted(glob(os.path.join(args.input, "*.jsonl")))
    logger.info("Found %s files in %s", len(files), args.input)

    for file_path in files:
        filename = os.path.basename(file_path)
        try:
            record_type = load_jsonl_to_raw(client, file_path, args.dataset)
            if not record_type:
                continue

            if not args.skip_core_merge and record_type in merge_types:
                sync_core_latest_table(
                    client=client,
                    file_path=file_path,
                    record_type=record_type,
                    core_dataset_id=args.core_dataset,
                    staging_prefix=args.staging_prefix,
                    core_table_suffix=args.core_table_suffix,
                )
        except Exception as e:
            logger.exception("Failed processing %s: %s", filename, e)


if __name__ == "__main__":
    main()
