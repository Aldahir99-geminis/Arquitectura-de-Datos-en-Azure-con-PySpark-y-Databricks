# **Modern Data Pipeline with Azure Databricks and Medallion Architecture**
Design and implementation of a full data pipeline leveraging Azure Databricks, PySpark, and Medallion Architecture (Bronze, Silver, Gold).

------------------------------------------------------------------------------------------------------------------------------------------

## 📖**Project Overview**
This project demonstrates the design and deployment of a cloud-based data pipeline, integrating multiple Azure services to enable secure, scalable, and analytics-ready data flows.
Key components:
  - Data Architecture: Implemented Medallion Architecture with Bronze, Silver, and Gold layers.
  - Data Processing: Leveraged PySpark on Azure Databricks for distributed data transformation.
  - ETL Orchestration: Automated pipelines using Azure Data Factory (ADF).
  - Data Storage & Access: Managed ingestion and versioning in Azure Data Lake Storage Gen2 (ADLS Gen2) with governance through Unity Catalog.
  - Analytics & Visualization: Built interactive dashboards in Databricks from Gold layer tables stored in Azure SQL Database.
  - Security & Governance: Managed credentials and secrets via Azure Key Vault.

--------------------------------------------------------------------------------------------------------------------------------------------

## 🚀**Project Requirements**
### **Data Engineering**

**Objective**

Develop a cloud-native data pipeline on Azure to ingest, process, and prepare data for analytical consumption.

Specifications
  - **Ingestion**: Load raw data into ADLS Gen2 (Bronze layer).
  - **Transformation**: Apply cleansing, standardization, and business rules in Databricks (Silver layer) using PySpark.
  - **Data Modeling**: Curate and store Gold layer datasets in Azure SQL Database for BI tools and reporting.
  - **Orchestration**: Automate ETL jobs with Azure Data Factory.
  - **Governance**: Apply data access controls with Unity Catalog and manage secrets through Azure Key Vault.

--------------------------------------------------------------------------------------------------------------------------------------------------

### **Data Analysis & BI**
**Objective**
Provide business-ready data and enable advanced analytics through dashboards and reports.

**Deliverables**:
  - Interactive **Databricks dashboards** powered by Gold layer tables.
  - KPIs for monitoring **customer trends, product performance, and sales metrics**.
  - Integration-ready **Azure SQL Database** for external BI tools (e.g., Power BI, Tableau).

-----------------------------------------------------------------------------------------------------------------------------------------------------


## 🏗️**Data Architecture**
The data pipeline follows the Medallion Architecture with three layers:

<img width="900" height="435" alt="Azure Medallion Architecture" src="https://github.com/user-attachments/assets/8a1b1108-27f6-4b6a-a6b5-045592cdbd44" />

1. **Bronze Layer**
  - Raw ingestion of source data into ADLS Gen2.
  - Retains original structure for traceability and reprocessing.

2. **Silver Layer**
  - Data cleansing, deduplication, and normalization with PySpark in Databricks.
  - Harmonized schema for business alignment.

3. **Gold Layer**
  - Curated business-ready data stored in Azure SQL Database.
  - Optimized for analytics, dashboards, and BI queries.
