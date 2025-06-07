# Project Setup Instructions

This document explains how to set up the required external dependencies to run the project.

---

## Required Software

1. **Java JDK (version 11 or later)**
   - Download from: https://adoptium.net/
   - Adding to PATH (system enviornment variable) is recommended
   - Install and verify by running `java -version` in your terminal.

2. **Apache Kafka (version 3.9.1 recommended)**
   - Download the Scala 2.13 binary (or the latest binary Scala version) from: https://kafka.apache.org/downloads
   - Extract the `.tgz` archive (use 7-Zip or similar tool on Windows).
   - Example path after extraction: `C:\kafka\`
   - Kafka runs on Java, so Java must be installed first.

3. **PostgreSQL**
   - Download the installer from: https://www.enterprisedb.com/downloads/postgres-postgresql-downloads
   - Install and configure as per instructions.
   - Ensure you can connect using `psql` or a GUI tool like pgAdmin.   

---

## Starting Kafka and Zookeeper

1. Open a terminal or Command Prompt.
2. Navigate to Kafka’s folder (e.g., `C:\kafka_2.13-3.9.1\bin\windows`).
3. Start Zookeeper (Kafka’s dependency):

## When setting up the project on a new system, install all dependencies using:
pip install -r requirements.txt