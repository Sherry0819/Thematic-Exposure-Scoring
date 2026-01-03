# Thematic Exposure Scoring for S&P 500 Companies

This project builds an end-to-end thematic exposure scoring system that extracts,
cleans, embeds, and interprets text from SEC filings to measure firm-level exposure
to economic and technological themes.

## Overview
- Data source: SEC filings (10-K / 10-Q)
- Unit of analysis: sentence-level text
- Output: firm × theme exposure scores with interpretable textual evidence

## Pipeline
1. Download and index SEC filings
2. Parse filings into sentence-level units
3. Clean and filter textual noise
4. Load documents and sentences into PostgreSQL
5. Score thematic exposure using predefined themes
6. Export firm-level scores and supporting evidence

## Repository Structure
scripts/ # data ingestion, cleaning, and scoring pipeline
sql/ # database schema and example queries
data/ # sample / intermediate outputs (large data excluded)
docker-compose.yml
requirements.txt
