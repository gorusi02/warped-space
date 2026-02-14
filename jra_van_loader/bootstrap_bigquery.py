import argparse
import logging
import os
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def resolve_project_id(arg_project: str | None) -> str | None:
    if arg_project:
        return arg_project
    if os.environ.get("GOOGLE_CLOUD_PROJECT"):
        return os.environ["GOOGLE_CLOUD_PROJECT"]
    return os.environ.get("GCLOUD_PROJECT")


def resolve_sql_files(sql_dir: Path, only: str | None) -> list[Path]:
    if not only:
        return sorted(sql_dir.glob("*.sql"))

    names = [name.strip() for name in only.split(",") if name.strip()]
    files: list[Path] = []
    for name in names:
        file_path = sql_dir / name
        if not file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")
        files.append(file_path)
    return files


def run_sql_files(
    client: bigquery.Client,
    sql_dir: Path,
    location: str,
    project_id: str,
    only: str | None,
) -> None:
    sql_files = resolve_sql_files(sql_dir, only)
    if not sql_files:
        raise FileNotFoundError(f"No SQL files found in {sql_dir}")

    for sql_file in sql_files:
        raw_sql = sql_file.read_text(encoding="utf-8")
        rendered_sql = raw_sql.replace("${PROJECT_ID}", project_id).replace("${BQ_LOCATION}", location)
        logger.info("Executing %s", sql_file.name)
        client.query(rendered_sql, location=location).result()
        logger.info("Completed %s", sql_file.name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap BigQuery datasets and serving tables")
    parser.add_argument("--project", "-p", help="GCP project ID")
    parser.add_argument("--key", "-k", help="Path to Service Account JSON key")
    parser.add_argument("--location", "-l", default="asia-northeast1", help="BigQuery location")
    parser.add_argument(
        "--only",
        help="Comma-separated SQL filenames to execute in order (e.g. 02_build_serving_tables.sql)",
    )
    parser.add_argument(
        "--sql-dir",
        default=str(Path(__file__).resolve().parents[1] / "bigquery"),
        help="Directory containing SQL files",
    )
    args = parser.parse_args()

    if args.key:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key

    project_id = resolve_project_id(args.project)
    if not project_id:
        raise ValueError("Project ID is required. Set --project or GOOGLE_CLOUD_PROJECT.")

    client = bigquery.Client(project=project_id)
    run_sql_files(
        client=client,
        sql_dir=Path(args.sql_dir),
        location=args.location,
        project_id=project_id,
        only=args.only,
    )


if __name__ == "__main__":
    main()
