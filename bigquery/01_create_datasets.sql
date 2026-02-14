-- Create datasets for a layered BigQuery architecture.
-- The bootstrap script replaces ${PROJECT_ID} with the runtime project.

CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.jra_raw`
OPTIONS (location = '${BQ_LOCATION}');

CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.jra_core`
OPTIONS (location = '${BQ_LOCATION}');

CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.jra_serving`
OPTIONS (location = '${BQ_LOCATION}');

CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.jra_ml`
OPTIONS (location = '${BQ_LOCATION}');
