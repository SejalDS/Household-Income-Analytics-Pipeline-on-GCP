# Household Income Analytics Pipeline on GCP

## 📌 Project Overview

This project builds a serverless data pipeline on Google Cloud Platform (GCP) to process and analyze household income and expenditure data from Eastern Europe and former Soviet regions.

The pipeline automates data ingestion, transformation, storage, and visualization to generate insights into income distribution, spending patterns, taxation, and social assistance impact.

---

## 🎯 Objective

- Analyze household income and expenditure trends  
- Compare poverty indicators across regions  
- Evaluate the impact of social assistance programs  
- Automate batch processing of new incoming data files  

---

## 🏗 Architecture

Google Cloud Storage (Raw Data)  
↓  
Cloud Functions (Python Transformations)  
↓  
BigQuery (Data Warehouse)  
↓  
Looker Studio (Dashboards & Visualization)

The pipeline is event-driven — when new data files are uploaded to GCS, Cloud Functions automatically process and load them into BigQuery.

---

## 🛠 Tech Stack

- Python  
- SQL  
- Google Cloud Platform (GCP)  
- Google Cloud Storage (GCS)  
- Google Cloud Functions  
- Google BigQuery  
- Looker Studio  

---

## 📂 Dataset Description

### Unique Identifier
- `hhid` – Household ID  

### Income Categories
- Wage income, self-employment income, pension income  
- Social assistance, unemployment benefits  
- Rental income and other income sources  

### Expenditure Categories
- Food, health, rent, transportation  
- Education, clothing, housing, miscellaneous  

### Demographic & Regional Data
- Region, locality type (capital/city/rural)  
- Household size  
- Socioeconomic group classification  

### Tax & Asset Information
- Personal income tax, sales tax  
- Ownership of assets (car, TV, refrigerator, land)

---

## ⚙️ Implementation Steps

1. Created a GCP project to manage services.
2. Uploaded raw household data to Google Cloud Storage.
3. Configured Cloud Functions to trigger on file upload.
4. Implemented Python scripts to clean and structure data.
5. Loaded processed data into BigQuery tables.
6. Wrote SQL queries to calculate income totals and regional comparisons.
7. Built interactive dashboards in Looker Studio.

---

## 📊 Analysis Performed

- Income and expenditure breakdowns
- Poverty-level comparisons across regions
- Social assistance impact analysis
- Tax contribution distribution
- Regional spending pattern trends

---

## 🔍 Key Learnings

- Designing event-driven serverless pipelines
- Automating data transformation using Cloud Functions
- Managing analytical datasets in BigQuery
- Writing SQL queries for structured analysis
- Creating interactive dashboards for insights

---
