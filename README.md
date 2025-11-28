# NOAA GHCN Daily Spark Medallion Architecture Project

Apache Spark pipeline for the NOAA Global Historical Climatology Network (GHCN) Daily dataset using the Medallion Architecture (Bronze → Silver → Gold). It ingests raw station data, cleans it with low-level RDD/MapReduce transformations, and produces analytics- & ML‑ready outputs.

Repository: [nafis4139/NOAA-GHCN-Project-Using-Spark-Medallion-Architecture](https://github.com/nafis4139/NOAA-GHCN-Project-Using-Spark-Medallion-Architecture)

## Dataset
Reference: https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily  
Elements: TMAX, TMIN, PRCP, SNOW, SNWD (often scaled values, e.g. tenths of °C) plus quality flags.

## What’s in this repo
Notebooks (run in order):
- Bronze (ingestion): [notebooks/bronze.ipynb](https://github.com/nafis4139/NOAA-GHCN-Project-Using-Spark-Medallion-Architecture/blob/main/notebooks/bronze.ipynb)
- Silver (RDD cleaning & standardization): [notebooks/silver.ipynb](https://github.com/nafis4139/NOAA-GHCN-Project-Using-Spark-Medallion-Architecture/blob/main/notebooks/silver.ipynb)
- Gold (serving, aggregates, anomalies, ML models): [notebooks/gold.ipynb](https://github.com/nafis4139/NOAA-GHCN-Project-Using-Spark-Medallion-Architecture/blob/main/notebooks/gold.ipynb)
  - Trains ML models: linear regression trend forecast, random forest rainfall prediction, GBT temperature anomaly regression, K‑means climate zone clustering, logistic regression heatwave year classification.
- Exploration: [notebooks/analysis.ipynb](https://github.com/nafis4139/NOAA-GHCN-Project-Using-Spark-Medallion-Architecture/blob/main/notebooks/analysis.ipynb)

Helpers: [scripts/](https://github.com/nafis4139/NOAA-GHCN-Project-Using-Spark-Medallion-Architecture/tree/main/scripts)

## Medallion Flow
- Bronze: Raw station & metadata landing (minimal transformation).
- Silver: Fixed-width parsing, sentinel handling (e.g. -9999), quality filtering, unit normalization, enrichment (RDD/MapReduce style).
- Gold: Monthly & yearly aggregates, climate normals, anomaly calculations, regional summaries, and ML modeling outputs (forecast tables, station predictions, clusters, classification).

## Quick Start
Requirements: Spark 3.x, Python 3.9+, Java 8/11. Use Jupyter, VS Code, or a managed Spark platform.  
Run notebooks sequentially (Bronze → Silver → Gold). Adjust paths, years, and station filters in the first cells.

## Example Outputs
- Monthly/annual temperature & precipitation summaries
- Climate normals (baseline window)
- Temperature anomalies & precipitation ratios
- Regional (country-level) trend metrics
- ML forecast (future anomaly trend) + station-level predictive models + clustering

## Acknowledgments
NOAA NCEI (GHCN-Daily) • Apache Spark community