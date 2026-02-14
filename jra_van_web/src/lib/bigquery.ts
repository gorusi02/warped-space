import { BigQuery } from "@google-cloud/bigquery";
import fs from "fs";
import path from "path";

// Resolve credentials path only when explicitly provided.
const credentialsPath = process.env.GOOGLE_APPLICATION_CREDENTIALS
  ? path.resolve(process.env.GOOGLE_APPLICATION_CREDENTIALS)
  : undefined;

const options: { projectId?: string; keyFilename?: string } = {};

if (process.env.GOOGLE_CLOUD_PROJECT) {
  options.projectId = process.env.GOOGLE_CLOUD_PROJECT;
}

if (credentialsPath) {
  if (fs.existsSync(credentialsPath)) {
    options.keyFilename = credentialsPath;
  } else {
    console.warn(
      `GOOGLE_APPLICATION_CREDENTIALS points to a missing file: ${credentialsPath}. Falling back to ADC.`,
    );
  }
}

const bigquery = new BigQuery(options);

export default bigquery;
