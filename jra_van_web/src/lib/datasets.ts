export function getServingDataset(): string {
  return process.env.JRA_SERVING_DATASET || "jra_serving";
}

export function getAnalysisDataset(): string {
  return process.env.JRA_ANALYSIS_DATASET || "jra_common";
}
