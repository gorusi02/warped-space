import bigquery from "@/lib/bigquery";
import { getAnalysisDataset, getServingDataset } from "@/lib/datasets";
import { getBigQueryLocation } from "@/lib/query-options";
import { parseRaceId } from "@/lib/race";

export type AnalysisRaceHeader = {
  race_id: string;
  race_name: string;
  kaisai_date: string;
  kaisai_basho: string;
  race_no: string;
  kyori: string;
  course: string;
};

export type AnalysisEntry = {
  wakuban: string;
  umaban: string;
  ketto_num: string;
  bamei: string;
  starts: number | null;
  avg_rank: number | null;
  win_rate: number | null;
  top3_rate: number | null;
  surface_match_rate: number | null;
  distance_match_rate: number | null;
  speed_index: number | null;
  speed_source: string | null;
  analysis_score: number | null;
  last_race_date: string | null;
};

type RawRaceHeaderRow = {
  race_id: string | null;
  race_name: string | null;
  kaisai_date: string | null;
  kaisai_basho: string | null;
  race_no: number | null;
  kyori: number | null;
  course: string | null;
};

type RawAnalysisEntryRow = {
  wakuban: string | number | null;
  umaban: string | number | null;
  ketto_num: string | number | null;
  bamei: string | null;
  starts: number | null;
  avg_rank: number | null;
  win_rate: number | null;
  top3_rate: number | null;
  surface_match_rate: number | null;
  distance_match_rate: number | null;
  speed_index: number | null;
  speed_source: string | null;
  analysis_score: number | null;
  last_race_date: string | null;
};

export async function getRaceAnalysis(raceId: string): Promise<{
  race: AnalysisRaceHeader | null;
  entries: AnalysisEntry[];
  error: string | null;
}> {
  if (!parseRaceId(raceId)) {
    return { race: null, entries: [], error: "Invalid race ID format." };
  }

  const projectId = process.env.GOOGLE_CLOUD_PROJECT;
  if (!projectId) {
    return { race: null, entries: [], error: "GOOGLE_CLOUD_PROJECT is not configured." };
  }

  const servingDataset = getServingDataset();
  const analysisDataset = getAnalysisDataset();
  const location = getBigQueryLocation();
  const speedIndexTable = "speed_index_master";

  const headerQuery = `
    SELECT
      race_id,
      race_name,
      FORMAT_DATE('%Y-%m-%d', kaisai_date) AS kaisai_date,
      kaisai_basho,
      CAST(race_no AS INT64) AS race_no,
      CAST(kyori AS INT64) AS kyori,
      course
    FROM \`${projectId}.${servingDataset}.serving_races\`
    WHERE race_id = @raceId
    LIMIT 1
  `;

  let hasSpeedIndexMaster = false;
  try {
    const [exists] = await bigquery.dataset(analysisDataset).table(speedIndexTable).exists();
    hasSpeedIndexMaster = exists;
  } catch (error) {
    console.warn("Failed to check speed_index_master. Fallback mode is used.", error);
  }

  const speedMasterCte = hasSpeedIndexMaster
    ? `
    speed_index_master AS (
      SELECT
        TRIM(CAST(horse_key AS STRING)) AS horse_key,
        NORMALIZE(TRIM(CAST(horse_name AS STRING)), NFKC) AS horse_name_norm,
        TRIM(CAST(surface AS STRING)) AS surface,
        SAFE_CAST(speed_index AS FLOAT64) AS speed_index,
        SAFE_CAST(run_count AS INT64) AS run_count
      FROM \`${projectId}.${analysisDataset}.${speedIndexTable}\`
      WHERE speed_index IS NOT NULL
    ),
    `
    : `
    speed_index_master AS (
      SELECT
        CAST(NULL AS STRING) AS horse_key,
        CAST(NULL AS STRING) AS horse_name_norm,
        CAST(NULL AS STRING) AS surface,
        CAST(NULL AS FLOAT64) AS speed_index,
        CAST(NULL AS INT64) AS run_count
      WHERE FALSE
    ),
    `;

  const analysisQuery = `
    WITH target_race AS (
      SELECT
        CAST(kyori AS INT64) AS target_distance,
        CASE
          WHEN course IS NULL OR TRIM(course) = '' THEN NULL
          ELSE SUBSTR(TRIM(course), 1, 1)
        END AS target_surface
      FROM \`${projectId}.${servingDataset}.serving_races\`
      WHERE race_id = @raceId
      LIMIT 1
    ),
    entries AS (
      SELECT
        TRIM(CAST(wakuban AS STRING)) AS wakuban,
        LPAD(TRIM(CAST(umaban AS STRING)), 2, '0') AS umaban,
        TRIM(CAST(ketto_num AS STRING)) AS ketto_num,
        TRIM(CAST(bamei AS STRING)) AS bamei
      FROM \`${projectId}.${servingDataset}.serving_entries\`
      WHERE race_id = @raceId
    ),
    history_raw AS (
      SELECT
        e.wakuban,
        e.umaban,
        e.ketto_num,
        e.bamei,
        a.race_date,
        SAFE_CAST(a.rank AS FLOAT64) AS rank,
        SAFE_CAST(a.popularity AS FLOAT64) AS popularity,
        SAFE_CAST(a.distance AS FLOAT64) AS distance,
        TRIM(a.surface) AS surface,
        SAFE_CAST(a.time_sec AS FLOAT64) AS time_sec,
        ROW_NUMBER() OVER (
          PARTITION BY e.ketto_num
          ORDER BY a.race_date DESC
        ) AS rn
      FROM entries e
      LEFT JOIN \`${projectId}.${analysisDataset}.analysis_view\` a
        ON NORMALIZE(TRIM(a.horse_name), NFKC) = NORMALIZE(e.bamei, NFKC)
    ),
    history AS (
      SELECT *
      FROM history_raw
      WHERE rn <= 5
    ),
    aggregated AS (
      SELECT
        e.wakuban,
        e.umaban,
        e.ketto_num,
        e.bamei,
        COUNT(h.race_date) AS starts,
        AVG(h.rank) AS avg_rank,
        AVG(IF(h.rn <= 3, h.rank, NULL)) AS avg_rank_recent,
        AVG(IF(h.rank = 1, 1.0, 0.0)) AS win_rate,
        AVG(IF(h.rank <= 3, 1.0, 0.0)) AS top3_rate,
        AVG(IF(SUBSTR(TRIM(h.surface), 1, 1) = t.target_surface, 1.0, 0.0)) AS surface_match_rate,
        AVG(IF(ABS(h.distance - t.target_distance) <= 200, 1.0, 0.0)) AS distance_match_rate,
        AVG(IF(h.time_sec > 0, h.distance / h.time_sec, NULL)) AS avg_speed,
        AVG(h.popularity) AS avg_popularity,
        MAX(h.race_date) AS last_race_date
      FROM entries e
      CROSS JOIN target_race t
      LEFT JOIN history h
        ON h.ketto_num = e.ketto_num
      GROUP BY e.wakuban, e.umaban, e.ketto_num, e.bamei
    ),
    field_stats AS (
      SELECT
        AVG(avg_speed) AS field_avg_speed,
        STDDEV_POP(avg_speed) AS field_speed_std
      FROM aggregated
      WHERE avg_speed IS NOT NULL
    ),
    scored AS (
      SELECT
        a.*,
        CASE
          WHEN fs.field_speed_std IS NULL OR fs.field_speed_std = 0 OR a.avg_speed IS NULL THEN 0.0
          ELSE (a.avg_speed - fs.field_avg_speed) / fs.field_speed_std
        END AS speed_z
      FROM aggregated a
      CROSS JOIN field_stats fs
    ),
    ${speedMasterCte}
    speed_by_id AS (
      SELECT
        surface,
        horse_key,
        ANY_VALUE(speed_index) AS speed_index
      FROM speed_index_master
      WHERE horse_key IS NOT NULL AND horse_key != ''
      GROUP BY surface, horse_key
    ),
    speed_by_name AS (
      SELECT
        surface,
        horse_name_norm,
        ARRAY_AGG(
          STRUCT(speed_index, run_count)
          ORDER BY run_count DESC, speed_index DESC
          LIMIT 1
        )[OFFSET(0)].speed_index AS speed_index
      FROM speed_index_master
      WHERE horse_name_norm IS NOT NULL AND horse_name_norm != ''
      GROUP BY surface, horse_name_norm
    ),
    enriched AS (
      SELECT
        s.*,
        id_match.speed_index AS speed_index_id,
        name_match.speed_index AS speed_index_name,
        COALESCE(
          id_match.speed_index,
          name_match.speed_index,
          100.0 + (s.speed_z * 10.0)
        ) AS speed_index_final,
        COALESCE(
          SAFE_DIVIDE(id_match.speed_index - 100.0, 10.0),
          SAFE_DIVIDE(name_match.speed_index - 100.0, 10.0),
          s.speed_z
        ) AS speed_factor,
        CASE
          WHEN id_match.speed_index IS NOT NULL THEN 'master_id'
          WHEN name_match.speed_index IS NOT NULL THEN 'master_name'
          ELSE 'fallback'
        END AS speed_source
      FROM scored s
      CROSS JOIN target_race t
      LEFT JOIN speed_by_id id_match
        ON id_match.surface = t.target_surface
        AND id_match.horse_key = s.ketto_num
      LEFT JOIN speed_by_name name_match
        ON name_match.surface = t.target_surface
        AND name_match.horse_name_norm = NORMALIZE(s.bamei, NFKC)
    )
    SELECT
      wakuban,
      umaban,
      ketto_num,
      bamei,
      CAST(starts AS INT64) AS starts,
      ROUND(avg_rank, 2) AS avg_rank,
      ROUND(win_rate * 100.0, 1) AS win_rate,
      ROUND(top3_rate * 100.0, 1) AS top3_rate,
      ROUND(surface_match_rate * 100.0, 1) AS surface_match_rate,
      ROUND(distance_match_rate * 100.0, 1) AS distance_match_rate,
      ROUND(speed_index_final, 1) AS speed_index,
      speed_source,
      ROUND(
        LEAST(
          100.0,
          GREATEST(
            0.0,
            52.0
            + IFNULL((8.0 - avg_rank_recent) * 5.0, 0.0)
            + IFNULL(top3_rate * 16.0, 0.0)
            + IFNULL(win_rate * 10.0, 0.0)
            + IFNULL(surface_match_rate * 8.0, 0.0)
            + IFNULL(distance_match_rate * 6.0, 0.0)
            + IFNULL(speed_factor * 6.0, 0.0)
            - IFNULL(GREATEST(avg_popularity - 8.0, 0.0) * 1.2, 0.0)
            - CASE
                WHEN starts IS NULL OR starts = 0 THEN 14.0
                WHEN starts < 3 THEN 4.0
                ELSE 0.0
              END
            - CASE
                WHEN last_race_date IS NULL THEN 2.0
                WHEN DATE_DIFF(CURRENT_DATE('Asia/Tokyo'), last_race_date, DAY) > 180 THEN 3.0
                ELSE 0.0
              END
          )
        ),
        1
      ) AS analysis_score,
      FORMAT_DATE('%Y-%m-%d', last_race_date) AS last_race_date
    FROM enriched
    ORDER BY SAFE_CAST(umaban AS INT64), umaban
  `;

  try {
    const [[headerRows], [entryRows]] = await Promise.all([
      bigquery.query({ query: headerQuery, params: { raceId }, location }),
      bigquery.query({ query: analysisQuery, params: { raceId }, location }),
    ]);

    const header = (headerRows as RawRaceHeaderRow[])[0];
    if (!header) {
      return { race: null, entries: [], error: "Race not found." };
    }

    const race: AnalysisRaceHeader = {
      race_id: String(header.race_id || raceId),
      race_name: String(header.race_name || "").trim() || `${String(header.race_no ?? 0)}R`,
      kaisai_date: String(header.kaisai_date || ""),
      kaisai_basho: String(header.kaisai_basho || ""),
      race_no: String(header.race_no ?? 0),
      kyori: header.kyori ? String(header.kyori) : "",
      course: String(header.course || "").trim(),
    };

    const entries: AnalysisEntry[] = (entryRows as RawAnalysisEntryRow[]).map((row) => ({
      wakuban: String(row.wakuban || "").trim(),
      umaban: String(row.umaban || "").trim(),
      ketto_num: String(row.ketto_num || "").trim(),
      bamei: String(row.bamei || "").trim(),
      starts: row.starts === null ? null : Number(row.starts),
      avg_rank: row.avg_rank === null ? null : Number(row.avg_rank),
      win_rate: row.win_rate === null ? null : Number(row.win_rate),
      top3_rate: row.top3_rate === null ? null : Number(row.top3_rate),
      surface_match_rate: row.surface_match_rate === null ? null : Number(row.surface_match_rate),
      distance_match_rate: row.distance_match_rate === null ? null : Number(row.distance_match_rate),
      speed_index: row.speed_index === null ? null : Number(row.speed_index),
      speed_source: row.speed_source ? String(row.speed_source) : null,
      analysis_score: row.analysis_score === null ? null : Number(row.analysis_score),
      last_race_date: row.last_race_date ? String(row.last_race_date) : null,
    }));

    return { race, entries, error: null };
  } catch (error) {
    console.error("Race analysis fetch error:", error);
    return { race: null, entries: [], error: "Failed to fetch analysis data." };
  }
}
