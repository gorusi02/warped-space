-- Run as ad-hoc monitoring queries for duplicate/null/freshness checks.
-- The bootstrap script replaces ${PROJECT_ID} with the runtime project.

SELECT
  'serving_races_duplicate_race_id' AS check_name,
  COUNT(*) AS issue_count
FROM (
  SELECT race_id
  FROM `${PROJECT_ID}.jra_serving.serving_races`
  GROUP BY race_id
  HAVING COUNT(*) > 1
);

SELECT
  'serving_entries_duplicate_race_horse' AS check_name,
  COUNT(*) AS issue_count
FROM (
  SELECT race_id, umaban
  FROM `${PROJECT_ID}.jra_serving.serving_entries`
  GROUP BY race_id, umaban
  HAVING COUNT(*) > 1
);

SELECT
  'serving_races_null_kyori_or_course_rate' AS check_name,
  SAFE_DIVIDE(
    COUNTIF(kyori IS NULL OR course IS NULL OR TRIM(course) = ''),
    COUNT(*)
  ) AS issue_rate
FROM `${PROJECT_ID}.jra_serving.serving_races`;

SELECT
  'serving_races_freshness_days' AS check_name,
  DATE_DIFF(CURRENT_DATE('Asia/Tokyo'), MAX(kaisai_date), DAY) AS lag_days
FROM `${PROJECT_ID}.jra_serving.serving_races`;
