export function getBigQueryLocation(): string {
  return process.env.BIGQUERY_LOCATION || "asia-northeast1";
}
