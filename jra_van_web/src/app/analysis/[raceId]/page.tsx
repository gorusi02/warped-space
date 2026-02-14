import Link from "next/link";
import { getRaceAnalysis } from "@/lib/analysis";

function scoreClass(score: number | null): string {
  if (score === null) {
    return "bg-gray-100 text-gray-500";
  }
  if (score >= 80) {
    return "bg-emerald-100 text-emerald-700";
  }
  if (score >= 65) {
    return "bg-blue-100 text-blue-700";
  }
  if (score >= 50) {
    return "bg-amber-100 text-amber-700";
  }
  return "bg-rose-100 text-rose-700";
}

function formatRate(value: number | null): string {
  return value === null ? "-" : `${value.toFixed(1)}%`;
}

function formatNumber(value: number | null, digits: number = 1): string {
  return value === null ? "-" : value.toFixed(digits);
}

export default async function AnalysisPage({ params }: { params: { raceId: string } }) {
  const { race, entries, error } = await getRaceAnalysis(params.raceId);

  return (
    <main className="min-h-screen p-4 md:p-8 bg-gray-50 text-gray-900">
      <div className="max-w-6xl mx-auto">
        <div className="mb-4 flex flex-wrap gap-4 text-sm">
          <Link href="/" className="text-blue-600 hover:text-blue-800 underline">
            ← レース一覧へ戻る
          </Link>
          <Link href={`/racecard/${params.raceId}`} className="text-blue-600 hover:text-blue-800 underline">
            出馬表へ
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
              {race.kaisai_basho} {parseInt(race.race_no, 10)}R 指数分析
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
            <p className="mt-3 text-xs text-gray-500">
              指数は直近成績、同コース適性、距離適性、速度偏差を組み合わせた簡易スコアです。
            </p>
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
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">指数</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">出走</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">勝率</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">複勝率</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">同コース率</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">距離適性率</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">速度偏差</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">平均着順</th>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">最終出走</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {entries.map((entry, index) => (
                  <tr key={`${entry.ketto_num}-${entry.umaban || index}`} className="hover:bg-gray-50">
                    <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">{entry.wakuban || "-"}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">{entry.umaban || "-"}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm">{entry.bamei || "-"}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm">
                      <span className={`rounded px-2 py-1 text-xs font-semibold ${scoreClass(entry.analysis_score)}`}>
                        {formatNumber(entry.analysis_score, 1)}
                      </span>
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm">{entry.starts ?? "-"}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm">{formatRate(entry.win_rate)}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm">{formatRate(entry.top3_rate)}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm">{formatRate(entry.surface_match_rate)}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm">{formatRate(entry.distance_match_rate)}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm">{formatNumber(entry.speed_index, 1)}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm">{formatNumber(entry.avg_rank, 2)}</td>
                    <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">{entry.last_race_date || "-"}</td>
                  </tr>
                ))}
                {entries.length === 0 && (
                  <tr>
                    <td colSpan={12} className="px-4 py-6 text-center text-sm text-gray-500">
                      指数対象データがありません。
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
