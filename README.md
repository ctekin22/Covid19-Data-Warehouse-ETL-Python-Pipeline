# 🦠 COVID-19 Data Engineering Pipeline with AWS Glue, Athena & Redshift

A fully serverless, production-style **ETL pipeline** project to ingest, process, transform, and load COVID-19 data from **Amazon S3** to **Amazon Redshift**, using **AWS Glue**, **Amazon Athena**, and **Python**.

---

## 🚀 Project Overview

This project demonstrates how to build a complete cloud-native data warehouse pipeline on **AWS**, including:

- Storing raw data in **Amazon S3**
- Crawling metadata with **AWS Glue Crawler**
- Querying files using **Amazon Athena**
- Designing a **star schema** dimensional model
- Transforming datasets with **Python (pandas)**
- Loading final data into **Amazon Redshift** via `COPY` command

---

## 🧰 Tech Stack

| Tool/Service         | Purpose                                      |
|----------------------|----------------------------------------------|
| **Amazon S3**        | Store raw and transformed CSV datasets       |
| **AWS Glue**         | Crawl, transform, and load data              |
| **Amazon Athena**    | SQL-based exploration of S3 data             |
| **Amazon Redshift**  | Data warehousing and analytics               |
| **IAM**              | Role-based access and security policies      |
| **Python 3**         | Transformation scripting (Glue + local)      |
| **pandas**           | In-memory data processing                    |
| **redshift_connector** | Python Redshift driver for COPY/DDL operations |
| **draw.io**          | Visual schema design (ERD + Star Schema)     |

---


## 🧱 Data Modeling
1. 📘 Relational Data Model
Generated using Athena’s DDL export after crawling.

2. ⭐ Star Schema (Dimensional Model)
Fact Table: factCovid

Dimension Tables: dimDate, dimHospital, dimRegion

See diagrams/star_schema_model.drawio for schema visualization.

## 🧪 ETL Logic (Python + Glue)
Raw datasets are loaded from S3 using pandas

Cleaned/transformed with filtering, deduplication, and type conversion

Saved as clean .csv files back to S3

Redshift schema tables created via redshift_connector

Loaded with COPY command referencing IAM role

## 📊 Redshift Tables
Table	Description
factCovid:	Confirmed cases, deaths, recovered, hospital stats
dimDate:	Year, month, day, weekday info
dimHospital:	Hospital details with location
dimRegion:	Region + geolocation data

## 🔐 IAM Roles & Permissions
IAM role used across AWS services:

• AWSGlueServiceRole
• AmazonS3FullAccess
• AWSGlueConsoleFullAccess
• Custom IAM Role: redshift-s3-access

You can now connect Redshift to visualization tools like QuickSight, Tableau, or Power BI.

## 🧠 Key Concepts Demonstrated
✔️ Serverless data processing
✔️ Glue jobs with external dependencies
✔️ Star schema design
✔️ Redshift data warehousing
✔️ IAM-based access control
✔️ Real-world Python-based ETL

## 📌 Next Steps
✅ Automate ETL with Glue Workflows

🛠 Use Glue Spark Jobs for big data

📈 Add Redshift Spectrum or QuickSight dashboard

⏱ Schedule crawler/jobs with triggers



