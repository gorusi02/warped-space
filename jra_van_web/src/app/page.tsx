import Link from "next/link";
import bigquery from "@/lib/bigquery";
import { getServingDataset } from "@/lib/datasets";
import { getBigQueryLocation } from "@/lib/query-options";

type Race = {
  race_id: string;
  kaisai_date: string;
  kaisai_basho: string;
  race_no: number;
  race_name: string;
  kyori: string;
  course: string;
};

type RaceRow = {
  race_id: string;
  kaisai_date: string;
  kaisai_basho: string;
  race_no: number;
  race_name: string | null;
  kyori: number | null;
  course: string | null;
};

async function getRecentRaces(limit: number = 50): Promise<Race[]> {
  const projectId = process.env.GOOGLE_CLOUD_PROJECT;
  const servingDataset = getServingDataset();
  const location = getBigQueryLocation();
  if (!projectId) {
    console.error("GOOGLE_CLOUD_PROJECT is not set.");
    return [];
  }

  const query = `
    SELECT
      race_id,
      FORMAT_DATE('%Y-%m-%d', kaisai_date) AS kaisai_date,
      kaisai_basho,
      CAST(race_no AS INT64) AS race_no,
      race_name,
      CAST(kyori AS INT64) AS kyori,
      course
    FROM \`${projectId}.${servingDataset}.serving_races\`
    ORDER BY kaisai_date DESC, kaisai_basho ASC, race_no ASC
    LIMIT @limit
  `;

  try {
    const [rows] = await bigquery.query({
      query,
      params: { limit },
      location,
    });

    return (rows as RaceRow[])
      .map((row) => {
        return {
          race_id: String(row.race_id || ""),
          kaisai_date: String(row.kaisai_date || ""),
          kaisai_basho: String(row.kaisai_basho || ""),
          race_no: Number(row.race_no ?? 0),
          race_name: String(row.race_name || "").trim() || `${String(row.race_no ?? 0)}R`,
          kyori: row.kyori ? String(row.kyori) : "",
          course: String(row.course || "").trim(),
        };
      })
      .filter((race): race is Race => race.race_id.length > 0);
  } catch (error) {
    console.error("BigQuery fetch error:", error);
    return [];
  }
}

export default async function Home() {
  const races = await getRecentRaces();

  return (
    <main className="min-h-screen p-4 md:p-8 bg-gray-50 text-gray-900">
      <h1 className="text-3xl font-bold mb-6">JRA レース一覧</h1>

      <div className="overflow-x-auto bg-white shadow rounded-lg">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-100">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">開催日</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">競馬場</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">R</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">レース名</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">距離</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">コース</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">出馬表</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">指数</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {races.map((race) => (
              <tr key={race.race_id} className="hover:bg-gray-50">
                <td className="px-4 py-3 whitespace-nowrap text-sm">{race.kaisai_date}</td>
                <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">{race.kaisai_basho}</td>
                <td className="px-4 py-3 whitespace-nowrap text-sm font-bold">{race.race_no}R</td>
                <td className="px-4 py-3 whitespace-nowrap text-sm">{race.race_name}</td>
                <td className="px-4 py-3 whitespace-nowrap text-sm">{race.kyori ? `${race.kyori}m` : "-"}</td>
                <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">{race.course || "-"}</td>
                <td className="px-4 py-3 whitespace-nowrap text-sm">
                  <Link
                    href={`/racecard/${race.race_id}`}
                    className="text-blue-600 hover:text-blue-800 underline"
                  >
                    表示
                  </Link>
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm">
                  <Link
                    href={`/analysis/${race.race_id}`}
                    className="text-blue-600 hover:text-blue-800 underline"
                  >
                    分析
                  </Link>
                </td>
              </tr>
            ))}
            {races.length === 0 && (
              <tr>
                <td colSpan={8} className="px-4 py-4 text-center text-gray-500">
                  データがありません。
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </main>
  );
}
