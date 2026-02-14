export const JYO_MAP: Record<string, string> = {
  "01": "札幌",
  "02": "函館",
  "03": "福島",
  "04": "新潟",
  "05": "東京",
  "06": "中山",
  "07": "中京",
  "08": "京都",
  "09": "阪神",
  "10": "小倉",
};

export const VENUE_TO_JYO_CODE: Record<string, string> = {
  札幌: "01",
  函館: "02",
  福島: "03",
  新潟: "04",
  東京: "05",
  中山: "06",
  中京: "07",
  京都: "08",
  阪神: "09",
  小倉: "10",
};

export const TRACK_MAP: Record<string, string> = {
  "10": "芝・左",
  "11": "芝・左外",
  "12": "芝・左内",
  "17": "芝・右",
  "18": "芝・右外",
  "19": "芝・右内",
  "20": "芝・直線",
  "23": "ダ・右",
  "24": "ダ・左",
  "29": "芝→ダ",
};

export type RaceKey = {
  year: string;
  monthDay: string;
  jyoCD: string;
  kaiji: string;
  nichiji: string;
  raceNum: string;
};

export function parseRaceId(raceId: string): RaceKey | null {
  if (!/^\d{16}$/.test(raceId)) {
    return null;
  }

  return {
    year: raceId.slice(0, 4),
    monthDay: raceId.slice(4, 8),
    jyoCD: raceId.slice(8, 10),
    kaiji: raceId.slice(10, 12),
    nichiji: raceId.slice(12, 14),
    raceNum: raceId.slice(14, 16),
  };
}

export function buildRaceId(key: RaceKey): string {
  return `${key.year}${key.monthDay}${key.jyoCD}${key.kaiji}${key.nichiji}${key.raceNum}`;
}

export function formatRaceDate(year: string, monthDay: string): string {
  const mmdd = monthDay.padStart(4, "0");
  return `${year}-${mmdd.slice(0, 2)}-${mmdd.slice(2, 4)}`;
}

export function parseKaijiNichiji(kaisai: string): { kaiji: string; nichiji: string } | null {
  const kaijiMatch = kaisai.match(/^(\d+)/);
  const nichijiMatch = kaisai.match(/(\d+)$/);
  if (!kaijiMatch || !nichijiMatch) {
    return null;
  }

  return {
    kaiji: kaijiMatch[1].padStart(2, "0"),
    nichiji: nichijiMatch[1].padStart(2, "0"),
  };
}
