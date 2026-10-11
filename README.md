# EchoChain

## Circular Economy & Secondary-Market Lifecycle Analytics

EchoChain is a data engineering and analytics project that connects manufacturing-related product data with secondary-market listings to explore product resale, product condition, and circular economy opportunities.

## Problem Statement

Manufacturers may have limited visibility into products after they are sold. By combining product and manufacturing data with secondary-market listings, EchoChain demonstrates how data pipelines and analytics can help track resale activity and understand the secondary market.

## Project Architecture

**Marketplace HTML → Scrapy → JSON → Bronze Delta Lake → PySpark Cleaning → SKU Matching → Silver/Gold Data → Power BI**

## Technology Stack

- **Python & Scrapy:** Marketplace data extraction
- **Databricks:** Data processing and notebook development
- **Delta Lake:** Bronze and Silver data layers
- **PySpark:** Data cleaning and transformation
- **Fuzzy Matching:** Matching marketplace product titles with product SKUs
- **Power BI:** Dashboard development and visualization
- **Git & GitHub:** Version control and project documentation

## Work Completed

- Developed a Scrapy-based marketplace data extraction workflow.
- Collected sample secondary-market listings into JSON.
- Implemented Bronze-layer data ingestion in Databricks.
- Cleaned and transformed marketplace data using PySpark.
- Matched marketplace listings to product SKUs using token-based matching.
- Created Silver and Gold marketplace datasets.
- Built relationships between marketplace and product data in Power BI.
- Developed a dashboard showing secondary-market coverage, average resale price, product-level listings, seller information, and listing conditions.

## Dashboard

The Power BI dashboard is available in the `powerbi` folder.

**File:** `EchoChain_dashboard.pbix`

Download the file from GitHub and open it using Power BI Desktop to explore the report.

## Dataset Scope

This project uses mock marketplace data containing 8 sample listings associated with 5 product SKUs. The results demonstrate the implemented workflow and dashboard functionality; they should not be interpreted as findings from a live marketplace.

The reported secondary-market coverage represents the proportion of product SKUs represented in the marketplace dataset, not a complete measure of circularity.

## Current Project Status

The core data extraction, transformation, product matching, and dashboard development work has been completed for the current prototype. Further enhancements, such as workflow scheduling and Delta Lake optimization, are outside the scope of this submission.

## Repository Structure

- `data/mock/` — Sample input data
- `scrapy/echochain_scraper/` — Scrapy project files
- `databricks/` — Databricks notebooks
- `powerbi/` — Power BI dashboard
- `EchoChain_Week1_Lakehouse_Setup.ipynb` — Initial lakehouse setup notebook
- `README.md` — Project documentation
