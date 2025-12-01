Spark Batch Pipeline: Vehicle Market Intelligence System

Executive Summary

This notebook implements an Enterprise-Grade Batch Processing Pipeline using Apache Spark. It transforms raw vehicle listing data (CSV) into a sophisticated "Market Intelligence" dataset stored in Google BigQuery.

Unlike simple ETL pipelines, this system applies statistical analysis, fraud detection forensics, and geospatial logic to enrich the data with actionable business insights (Deal Scores, Trust Scores, and Arbitrage Opportunities).

System Architecture

Input: Raw CSV Data (Local or GCS) $\rightarrow$ Spark Processing (Cleaning, Forensics, Scoring) $\rightarrow$ Output: BigQuery Table $\rightarrow$ Viz: Looker Studio Dashboard

Pipeline Modules

1. Environment & Configuration

Hybrid Runtime: Automatically detects if the environment is Local (Windows) or Cloud (Linux/VM) and adjusts file paths and memory settings accordingly.

Windows Compatibility: Includes automated handling of winutils.exe, Hadoop binaries, and localhost network binding to prevent firewall crashes on local development machines.

2. Data Ingestion & Preprocessing

Loads raw vehicle data (supports 100-row test mode or full 1.5GB production mode).

Standardization: Normalizes manufacturer names, handles null values, and filters out pricing outliers (e.g., junk cars < $500 or errors > $200k).

3. Module A: VIN Forensics & Fraud Detection

VIN Decoding: Extracts the 10th character of the VIN to determine the actual manufacturing year.

Mismatch Detection: Flags vehicles where the listed year differs from the VIN-decoded year.

Mileage Analysis: Flags suspicious low-mileage vehicles (potential odometer rollbacks).

Trust Score: Calculates a 0-100 score for every listing based on these risk factors.

4. Module B: Geospatial Arbitrage Engine

Calculates the National Average Price for every specific Make + Model + Year combination.

Arbitrage Calculation: Compares the Local Price vs. National Average.

Sourcing Hubs: Identifies regions where specific cars are significantly cheaper than the national market, highlighting profit opportunities for dealers ("Buy Low, Sell High").

5. Module C: Financial Deal Scoring (Z-Score)

Uses Spark Window Functions to partition data by Make, Model, and Year.

Calculates the Standard Deviation and Mean for every specific car grouping.

Z-Score Analysis: Assigns a statistical score to every car indicating how far its price deviates from the norm.

Deal Rating: Classifies inventory into human-readable categories:

🟢 GREAT DEAL (Price is < -1.5 Std Dev from average)

🟡 FAIR MARKET (Price is within normal range)

🔴 OVERPRICED (Price is > +1.5 Std Dev from average)

⚫ RISKY BUY (Price is low, but Trust Score is poor)

6. Cloud Export

Writes the final, enriched dataset directly to Google BigQuery (via GCS staging) for dashboarding in Looker Studio.
