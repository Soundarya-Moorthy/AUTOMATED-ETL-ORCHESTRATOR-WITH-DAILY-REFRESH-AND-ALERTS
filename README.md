# Automated ETL Orchestrator with Daily Refresh and Alerts

## 📌 Project Overview
This project implements a **production-grade automated ETL pipeline** that ingests **weather data** and **financial market data** from multiple sources, performs **data quality checks, transformations, and correlation analysis**, and loads curated data into a **PostgreSQL data warehouse**.
The pipeline is orchestrated to run **daily**, with built-in **monitoring and alerting** to detect failures, anomalies, and data freshness issues. The goal is to analyze how **environmental factors (weather)** correlate with **market behavior (stocks & indices)**.

## 🎯 Objectives
* Design an end-to-end ETL pipeline for weather and financial data
* Implement robust error handling and data quality validation
* Automate daily data refresh using workflow orchestration
* Enable monitoring and alerting for pipeline health and anomalies
* Perform correlation analysis between weather patterns and market behavior

## 📊 Data Sources
### Weather Data
* **OpenWeatherMap API** – real-time weather
* **NOAA Climate Data** – historical weather (CSV)
* Metrics: temperature, humidity, precipitation, wind speed

### Financial Data
* **Yahoo Finance (yfinance)** – stock prices & volume
* **Alpha Vantage API** – additional market indicators
* Metrics: OHLC prices, trading volume, indices (S&P 500, NASDAQ)

### Reference Data
* Public holidays calendar
* Stock ticker reference table

## 🔄 ETL Pipeline Components
### Extract
* API-based ingestion with retry logic
* CSV ingestion for historical datasets
* Error handling for API failures and rate limits

### Transform
* Missing value handling and deduplication
* Time zone and unit standardization
* Data enrichment (weather indices, stock indicators)
* Business rules and anomaly flagging
* Weather–market correlation analysis

### Load
* Incremental loading strategy
* Staging → production table pattern
* Dimension and fact table design

## ✅ Data Quality Checks
### Weather Data
* Temperature range: **-50°C to 60°C**
* Null checks
* Timestamp consistency

### Financial Data
* Positive stock prices
* Valid trading volumes
* Market hours validation

### General
* Record count reconciliation
* Data freshness (< 24 hours)
* Duplicate detection
* Referential integrity checks

## 🖥️ Monitoring & Alerting
* Pipeline execution status and history
* Data quality metrics per source
* Correlation visualizations
* API usage & quota monitoring
* Error logs and failure diagnostics
* Alerts via Email / Slack

## 🧰 Tech Stack

| Layer            | Technology                              |
| ---------------- | --------------------------------------- |
| Orchestration    | Prefect                                 |
| Programming      | Python 3.x (pandas, requests, yfinance) |
| Database         | PostgreSQL                              |
| Monitoring       | Streamlit / Grafana                     |
| Alerting         | SMTP / Slack Webhooks                   |
| Testing          | pytest                                  |
| Version Control  | Git & GitHub                            |
| Containerization | Docker (optional)                       |

## 🚀 Future Enhancements
* Dockerized deployment
* CI/CD with GitHub Actions
* Advanced anomaly detection
* Cloud deployment (AWS/GCP)
* Real-time streaming ingestion
