# Real-Time E-commerce Product Recommendation System

## Overview

This project demonstrates the development of a Real-Time E-commerce Product Recommendation System for e-commerce platforms. The system ingests streaming data (user interactions), builds machine learning models for product recommendations, and visualizes results in an interactive dashboard. It leverages modern data engineering practices, machine learning algorithms, and DevOps tools to create a scalable, automated, and real-time recommendation system.
---

## System Architecture and Components

### 1. **Data Ingestion (Streaming Source)**
- **Tools**: PostgreSQL, Kafka, python.
- **Functionality**: 
  - Simulates user interactions from an e-commerce platform.
  - Data is streamed into PostgreSQL via Kafka stream processing.

### 2. **Data Pipeline (ETL)**
- **Tools**:, Prefect,DBT
- **Functionality**: 
  - Extract: Raw interaction data is ingested through Kafka.
  - Transform: DBT is used for transforming raw data into an analytical format. This includes:
    - Cleaning and aggregating data.
    - Joining tables, creating summary reports, and preparing data for machine learning.
  - Load: The transformed data is loaded into the PostgreSQL data warehouse.

### 3. **Data Analysis & Modeling**
- **Tools**: Python (Pandas, NumPy, Scikit-learn)
- **Functionality**:
  - **Feature Engineering**: Created user-product interaction matrix.
  - **Model Building**: Implemented Collaborative Filtering (k-NN, SVD) for product recommendations.
  - **Evaluation**: Used RMSE, Precision@k, classification_report and confusion_matrix metrics to evaluate model performance.

### 4. **Visualization**
- **Tools**: python (pandas, matplotlib, seaborn), Jupyter Notebook
- **Functionality**:
  - Created interactive dashboards to visualize:
    - Top recommended products.
    - User activity and engagement metrics.
    - Model performance (Precision, Recall).

### 5. **CI/CD & Automation**
- **Tools**: GitHub, yaml, jenkins
- **Functionality**:
  - Integrated CI/CD pipeline to automate model training and deployment.
  - Jenkins triggers the data pipeline and model retraining whenever new code is pushed to GitHub.

---

## Project Highlights

- **Real-Time Data Processing**: Utilized Kafka for real-time streaming in PostgreSQL.
- **End-to-End Automation**: Built automated ETL pipelines using Prefect, along with yaml,Jenkins for CI/CD.
- **Machine Learning**: Applied Collaborative Filtering (KNN, SVD) for product recommendations and evaluated models using industry-standard metrics.
- **Visualization**: Developed interactive dashboards in Jupyter Notebook and Tableau Public to present real-time insights on user activity, recommendations, and model performance.
- **Open-Source Tools**: Entirely built using open-source tools like PostgreSQL, Kafka, Python, Scikit-learn, and Tableau Public.
---

## Tools and Technologies

- **Data Ingestion**: 
  - PostgreSQL
  - Kafka
- **Data Pipeline**:
  - Prefect
  - DBT (Data Build Tool)
- **Machine Learning**:
  - Python (Pandas, NumPy, Scikit-learn)
- **Visualization**:
  - Tableau Public
  - Jupyter Notebook
  - python (matplotlib,seaborn)
- **CI/CD & Automation**:
  - GitHub
  - Jenkins
  - yaml

---

## Project Setup Instructions

### Required Software

1. **Java JDK (version 11 or later)**
   - Download from: [AdoptOpenJDK](https://adoptium.net/)
   - Adding to PATH (system environment variable) is recommended.
   - Install and verify by running `java -version` in your terminal.

2. **Apache Kafka (version 3.9.1 recommended)**
   - Download the Scala 2.13 binary (or the latest binary Scala version) from: [Kafka Downloads](https://kafka.apache.org/downloads).
   - Extract the `.tgz` archive (use 7-Zip or similar tool on Windows).
   - Example path after extraction: `C:\kafka\`
   - Kafka runs on Java, so **Java** must be installed first.

3. **PostgreSQL**
   - Download the installer from: [PostgreSQL Downloads](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads).
   - Install and configure as per instructions.
   - Ensure you can connect using `psql` or a GUI tool like **pgAdmin**.

---

### Starting Kafka and Zookeeper

1. Open a terminal or Command Prompt.
2. Navigate to Project directory.
3. Start **Zookeeper** (Kafka’s dependency):
   - Got to src Folder: Run `start-kafka.bat`
