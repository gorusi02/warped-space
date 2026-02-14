import { NextResponse } from "next/server";
import { getRaceAnalysis } from "@/lib/analysis";

type RouteContext = {
  params: {
    raceId: string;
  };
};

function resolveStatus(error: string): number {
  if (error.includes("Invalid race ID")) {
    return 400;
  }
  if (error.includes("Race not found")) {
    return 404;
  }
  return 500;
}

export async function GET(_: Request, context: RouteContext) {
  const raceId = context.params.raceId;
  const result = await getRaceAnalysis(raceId);

  if (result.error) {
    return NextResponse.json(
      {
        ok: false,
        error: result.error,
      },
      { status: resolveStatus(result.error) },
    );
  }

  return NextResponse.json({
    ok: true,
    race: result.race,
    entries: result.entries,
  });
}
