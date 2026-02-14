import Link from "next/link";
import bigquery from "@/lib/bigquery";
import { getServingDataset } from "@/lib/datasets";
import { getBigQueryLocation } from "@/lib/query-options";
import { parseRaceId } from "@/lib/race";

type RaceHeader = {
  race_name: string;
  kaisai_date: string;
  kaisai_basho: string;
  race_no: string;
  kyori: string;
  course: string;
};

type RaceEntry = {
  wakuban: string;
  umaban: string;
  ketto_num: string;
  bamei: string;
};

type RawHeaderRow = {
  race_name: string | null;
  kaisai_date: string | null;
  kaisai_basho: string | null;
  race_no: number | null;
  kyori: number | null;
  course: string | null;
};

type RawEntryRow = {
  wakuban: string | number | null;
  umaban: string | number | null;
  ketto_num: string | number | null;
  bamei: string | null;
};

async function getRaceCardData(raceId: string): Promise<{
  race: RaceHeader | null;
  entries: RaceEntry[];
  error: string | null;
}> {
  if (!parseRaceId(raceId)) {
    return { race: null, entries: [], error: "レースIDの形式が不正です。" };
  }

  const projectId = process.env.GOOGLE_CLOUD_PROJECT;
  const servingDataset = getServingDataset();
  const location = getBigQueryLocation();
  if (!projectId) {
    return { race: null, entries: [], error: "GOOGLE_CLOUD_PROJECT が設定されていません。" };
  }

  const headerQuery = `
    SELECT
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

  const entriesQuery = `
    SELECT
      wakuban,
      umaban,
      ketto_num,
      bamei
    FROM \`${projectId}.${servingDataset}.serving_entries\`
    WHERE race_id = @raceId
    ORDER BY SAFE_CAST(umaban AS INT64), umaban
  `;

  try {
    const [[headerRows], [entryRows]] = await Promise.all([
      bigquery.query({ query: headerQuery, params: { raceId }, location }),
      bigquery.query({ query: entriesQuery, params: { raceId }, location }),
    ]);

    const headerRow = (headerRows as RawHeaderRow[])[0];
    const race = headerRow
      ? {
          race_name: String(headerRow.race_name || "").trim() || `${String(headerRow.race_no ?? 0)}R`,
          kaisai_date: String(headerRow.kaisai_date || ""),
          kaisai_basho: String(headerRow.kaisai_basho || ""),
          race_no: String(headerRow.race_no ?? 0),
          kyori: headerRow.kyori ? String(headerRow.kyori) : "",
          course: String(headerRow.course || "").trim(),
        }
      : null;

    const entries: RaceEntry[] = (entryRows as RawEntryRow[]).map((row) => ({
      wakuban: String(row.wakuban || "").trim(),
      umaban: String(row.umaban || "").trim(),
      ketto_num: String(row.ketto_num || "").trim(),
      bamei: String(row.bamei || "").trim(),
    }));

    if (!race) {
      return { race: null, entries: [], error: "指定したレースが見つかりません。" };
    }

    return { race, entries, error: null };
  } catch (error) {
    console.error("Race card fetch error:", error);
    return { race: null, entries: [], error: "出馬表データの取得に失敗しました。" };
  }
}

export default async function RaceCardPage({ params }: { params: { raceId: string } }) {
  const { race, entries, error } = await getRaceCardData(params.raceId);

  return (
    <main className="min-h-screen p-4 md:p-8 bg-gray-50 text-gray-900">
      <div className="max-w-5xl mx-auto">
        <div className="mb-4">
          <Link href="/" className="text-blue-600 hover:text-blue-800 underline">
            ← レース一覧へ戻る
          </Link>
        </div>

        {error && (
          <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-red-700">
            {error}
          </div>
        )}

        {!error && race && (
          <section className="mb-4 rounded-lg bg-white p-5 shadow">
            <h1 className="mb-3 text-2xl font-bold">
              {race.kaisai_basho} {parseInt(race.race_no, 10)}R 出馬表
            </h1>
            <div className="grid gap-2 text-sm text-gray-700 sm:grid-cols-2">
              <p>
                <span className="font-semibold">開催日:</span> {race.kaisai_date}
              </p>
              <p>
                <span className="font-semibold">レース名:</span> {race.race_name}
              </p>
              <p>
                <span className="font-semibold">距離:</span> {race.kyori ? `${race.kyori}m` : "-"}
              </p>
              <p>
                <span className="font-semibold">コース:</span> {race.course || "-"}
              </p>
            </div>
          </section>
        )}

        {!error && (
          <section className="overflow-x-auto rounded-lg bg-white shadow">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-100">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">枠番</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">馬番</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">馬名</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">血統登録番号</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {entries.map((entry, index) => (
                  <tr key={`${entry.ketto_num}-${entry.umaban || index}`} className="hover:bg-gray-50">
                    <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">{entry.wakuban || "-"}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">{entry.umaban || "-"}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm">{entry.bamei || "-"}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">{entry.ketto_num || "-"}</td>
                  </tr>
                ))}
                {entries.length === 0 && (
                  <tr>
                    <td colSpan={4} className="px-4 py-6 text-center text-sm text-gray-500">
                      出馬表データがありません。
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </section>
        )}
      </div>
    </main>
  );
}
