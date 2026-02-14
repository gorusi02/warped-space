-- Build canonical and serving tables from raw ingestion layers.
-- The bootstrap script replaces ${PROJECT_ID} with the runtime project.

CREATE OR REPLACE TABLE `${PROJECT_ID}.jra_core.ra_latest` AS
SELECT * EXCEPT(_rn)
FROM (
  SELECT
    ra.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        CAST(Year AS STRING),
        LPAD(CAST(MonthDay AS STRING), 4, '0'),
        LPAD(CAST(JyoCD AS STRING), 2, '0'),
        LPAD(CAST(Kaiji AS STRING), 2, '0'),
        LPAD(CAST(Nichiji AS STRING), 2, '0'),
        LPAD(CAST(RaceNum AS STRING), 2, '0')
      ORDER BY SAFE_CAST(fetched_at AS TIMESTAMP) DESC, fetched_at DESC
    ) AS _rn
  FROM `${PROJECT_ID}.jra_raw.RA` AS ra
)
WHERE _rn = 1;

CREATE OR REPLACE TABLE `${PROJECT_ID}.jra_core.se_latest` AS
SELECT * EXCEPT(_rn)
FROM (
  SELECT
    se.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        CAST(Year AS STRING),
        LPAD(CAST(MonthDay AS STRING), 4, '0'),
        LPAD(CAST(JyoCD AS STRING), 2, '0'),
        LPAD(CAST(Kaiji AS STRING), 2, '0'),
        LPAD(CAST(Nichiji AS STRING), 2, '0'),
        LPAD(CAST(RaceNum AS STRING), 2, '0'),
        LPAD(TRIM(CAST(Umaban AS STRING)), 2, '0')
      ORDER BY SAFE_CAST(fetched_at AS TIMESTAMP) DESC, fetched_at DESC
    ) AS _rn
  FROM `${PROJECT_ID}.jra_raw.SE` AS se
)
WHERE _rn = 1;

CREATE OR REPLACE TABLE `${PROJECT_ID}.jra_core.race_summary_latest` AS
SELECT
  FORMAT_DATE('%Y%m%d', date) AS ymd,
  venue,
  kaisai,
  CAST(race_num AS INT64) AS race_num,
  MAX(TRIM(race_name)) AS race_name,
  MAX(CAST(distance AS INT64)) AS kyori,
  MAX(TRIM(surface)) AS course
FROM `${PROJECT_ID}.jra_data.raw_race_results`
WHERE
  date IS NOT NULL
  AND venue IS NOT NULL
  AND kaisai IS NOT NULL
  AND race_num IS NOT NULL
GROUP BY ymd, venue, kaisai, race_num;

CREATE OR REPLACE TABLE `${PROJECT_ID}.jra_core.entry_fallback_latest` AS
SELECT
  FORMAT_DATE('%Y%m%d', date) AS ymd,
  venue,
  kaisai,
  CAST(race_num AS INT64) AS race_num,
  LPAD(CAST(CAST(horse_num AS INT64) AS STRING), 2, '0') AS umaban,
  CAST(CEIL(CAST(horse_num AS FLOAT64) / 2.0) AS INT64) AS wakuban,
  MAX(TRIM(CAST(horse_id AS STRING))) AS ketto_num,
  MAX(TRIM(CAST(horse_name AS STRING))) AS bamei
FROM `${PROJECT_ID}.jra_data.raw_race_results`
WHERE
  date IS NOT NULL
  AND venue IS NOT NULL
  AND kaisai IS NOT NULL
  AND race_num IS NOT NULL
  AND horse_num IS NOT NULL
GROUP BY ymd, venue, kaisai, race_num, umaban, wakuban;

CREATE OR REPLACE TABLE `${PROJECT_ID}.jra_serving.serving_races` AS
WITH normalized AS (
  SELECT
    ymd,
    PARSE_DATE('%Y%m%d', ymd) AS kaisai_date,
    venue AS kaisai_basho,
    race_num AS race_no,
    COALESCE(NULLIF(TRIM(race_name), ''), CONCAT(CAST(race_num AS STRING), 'R')) AS race_name,
    kyori,
    course,
    CASE venue
      WHEN '札幌' THEN '01'
      WHEN '函館' THEN '02'
      WHEN '福島' THEN '03'
      WHEN '新潟' THEN '04'
      WHEN '東京' THEN '05'
      WHEN '中山' THEN '06'
      WHEN '中京' THEN '07'
      WHEN '京都' THEN '08'
      WHEN '阪神' THEN '09'
      WHEN '小倉' THEN '10'
      ELSE NULL
    END AS jyo_cd,
    LPAD(REGEXP_EXTRACT(kaisai, r'^[0-9]+'), 2, '0') AS kaiji,
    LPAD(REGEXP_EXTRACT(kaisai, r'[0-9]+$'), 2, '0') AS nichiji
  FROM `${PROJECT_ID}.jra_core.race_summary_latest`
)
SELECT
  CONCAT(ymd, jyo_cd, kaiji, nichiji, LPAD(CAST(race_no AS STRING), 2, '0')) AS race_id,
  kaisai_date,
  kaisai_basho,
  race_no,
  race_name,
  kyori,
  course
FROM normalized
WHERE
  jyo_cd IS NOT NULL
  AND kaiji IS NOT NULL
  AND nichiji IS NOT NULL
  AND REGEXP_CONTAINS(CONCAT(ymd, jyo_cd, kaiji, nichiji, LPAD(CAST(race_no AS STRING), 2, '0')), r'^[0-9]{16}$');

CREATE OR REPLACE TABLE `${PROJECT_ID}.jra_serving.serving_entries` AS
WITH se_entries AS (
  SELECT
    CONCAT(
      CAST(Year AS STRING),
      LPAD(CAST(MonthDay AS STRING), 4, '0'),
      LPAD(CAST(JyoCD AS STRING), 2, '0'),
      LPAD(CAST(Kaiji AS STRING), 2, '0'),
      LPAD(CAST(Nichiji AS STRING), 2, '0'),
      LPAD(CAST(RaceNum AS STRING), 2, '0')
    ) AS race_id,
    TRIM(CAST(Wakuban AS STRING)) AS wakuban,
    LPAD(TRIM(CAST(Umaban AS STRING)), 2, '0') AS umaban,
    TRIM(CAST(KettoNum AS STRING)) AS ketto_num,
    TRIM(CAST(Bamei AS STRING)) AS bamei,
    1 AS source_priority
  FROM `${PROJECT_ID}.jra_core.se_latest`
  WHERE Umaban IS NOT NULL
),
fallback_entries AS (
  SELECT
    CONCAT(
      ymd,
      CASE venue
        WHEN '札幌' THEN '01'
        WHEN '函館' THEN '02'
        WHEN '福島' THEN '03'
        WHEN '新潟' THEN '04'
        WHEN '東京' THEN '05'
        WHEN '中山' THEN '06'
        WHEN '中京' THEN '07'
        WHEN '京都' THEN '08'
        WHEN '阪神' THEN '09'
        WHEN '小倉' THEN '10'
        ELSE '00'
      END,
      LPAD(REGEXP_EXTRACT(kaisai, r'^[0-9]+'), 2, '0'),
      LPAD(REGEXP_EXTRACT(kaisai, r'[0-9]+$'), 2, '0'),
      LPAD(CAST(race_num AS STRING), 2, '0')
    ) AS race_id,
    CAST(wakuban AS STRING) AS wakuban,
    umaban,
    ketto_num,
    bamei,
    2 AS source_priority
  FROM `${PROJECT_ID}.jra_core.entry_fallback_latest`
),
unioned AS (
  SELECT race_id, wakuban, umaban, ketto_num, bamei, source_priority FROM se_entries
  UNION ALL
  SELECT race_id, wakuban, umaban, ketto_num, bamei, source_priority FROM fallback_entries
),
dedup AS (
  SELECT
    race_id,
    wakuban,
    umaban,
    ketto_num,
    bamei,
    ROW_NUMBER() OVER (
      PARTITION BY race_id, umaban
      ORDER BY source_priority ASC
    ) AS _rn
  FROM unioned
  WHERE
    race_id IS NOT NULL
    AND REGEXP_CONTAINS(race_id, r'^[0-9]{16}$')
    AND umaban IS NOT NULL
)
SELECT
  race_id,
  wakuban,
  umaban,
  ketto_num,
  bamei
FROM dedup
WHERE _rn = 1;
