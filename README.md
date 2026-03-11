# Healthcare Pipeline
This project is a data engineer pipeline to analyze information within a general healthcare system. From here, it may be modified to fit specific use cases, scenarios, or find other types of information.

## Project Premise
Let’s say you’re a patient and you’re feeling unwell, so you go to the hospital for treatment. You arrive at the location, you fill out the information, receive the treatment, then get discharged. That’s from the user perspective.
While the hospital provides treatment, it does so for a fee and needs to set up transactional data to collect fees from the patient. In the background, the cycle has extra steps involved:
1. Patient Visit
2. Service Provisions (Hospital renders treatment)
3. Billing
4. Claims Review
5. Payments & Follow-up
6. Further Tracking & Improvement

## Overview
I’m building a data lake inside of Google Cloud Platform (GCP) catered to Revenue Cycle Management (RCM), that is scalable, in the healthcare domain. The goal is centralizing, cleaning, and transforming data from multiple sources, including patients, healthcare providers, and insurance companies.

Data and Codes utilized are based around United States of America healthcare systems.

To explain the architecture briefly after the initial setup of sources:
1. Ingestion
2. Storage
3. Processing
4. Analytics
5. Pipelines
6. Orchestration
7. Visualization

Workflow orchestration tools automate and update information going to ingestion and displayed information in visualizations.

## Services Used
- GCS (Google Cloud Storage): Store raw/processed data.
- BigQuery: Analytical engine for storing/querying data.
- Dataproc (Spark/Hadoop): Large-scale processing.
- Cloud Composer: Automates ETL pipelines and workflow orchestration.
- Cloud SQL: Stores transactional Electronic Medical Records (EMR) data.
- GitHub: Version control and CI/CD.
- Cloud Build: Automates deployment for data processing and ETL workflows.

## Techniques
- Metadata-Driven Approach
- SCD Type 2
- Incremental Loading
- Common Data Model (CRM)
- Medallion Architecture (Bronze, Silver; Gold layers)
- Logging/Monitoring
- Error Handling
- CI/CD
- Automation
- Optimizations

## Revenue Cycle Management (RCM)
Revenue Cycle Management (RCM) is the process providers use to to track patient care from registration/scheduling to final payments and manages sensitive information like patient details, billing, claims, and payments. The process is generally the following:
- Patient Visit Initiation
- Service Provision
- Billing Generation
- Claims Review
- Payments and Follow-ups
- Tracking and Improvement

## Data Sources
The pipeline can utilize the following data sources below. Normally, if it’s done while inside a company you’ll likely have readily available data sources and multiple people working on this, but I’m assuming they do NOT exist and need to be created from scratch. I’m focusing on the first three types of sources.
1. EMR (Electronic Medical Records)
    - Stores data related to patients, providers, departments, transactions, and encounters
    - Example: Three hospitals with separate databases, where A, B; C is a single letter or another identifier: Hospital A → hospital_a_db, Hospital B → hospital_b_db, and so on.
    - Unless otherwise stated, the tables for each hospital are the following: patients, providers, encounters, transactions; departments
2. Claims Data Source
    - Comes from insurance companies as flat files
    - Updates on a monthly basis for each hospital stored in the landing folder.
    - Stored in designated folder/location in data lake on recurring, monthly basis
3. CPT (Current Procedural Terminology) Codes
    - Provides descriptions for medical, surgical, and diagnostic procedures
4. NPI (National Provider Identifier) Data
    - Public API assigning unique identifiers to healthcare providers
5. ICD (International Classification of Diseases) Codes
    - Maps diagnosis codes and descriptions

# Setup Instructions
## Prerequisites
- Google Cloud Platform
    - A Free account with credits will suffice for starting.
- Access to an on-premises SQL Server database.

## Cautionary
At any point in time during the creation, running, or automation of this pipeline, if you’re expecting to leave any Clusters, Instances, etc. idle for a long time, STOP THEM. Do NOT leave them running or you'll incur unnecessary costs!

As a mantra: Idle Processes = Costly Processes.

The scale of this particular pipeline is doable within a week’s time, even if you copied all of the code here. With sufficiently large data, or starting everything from scratch, it could take a few months to fully implement with verifications and tests.

## Step 1: GCP Setup
1. Create cloud SQL mySQL Instance → e.g. hospital-a-mysql-db
2. Create a bucket within the project
    - Create initial folders for ingestion scripts.
3. Allow networks to access instance
    - Connections → Networking
    - On a local machine and local network, you can do 0.0.0.0/0 as the IP address, otherwise use another address.
4. Create Database → e.g. hospital_a_db
5. Add user accounts → e.g. “myuser” with a password.
6. Log into Cloud SQL Studio with the user account.
7. In the bucket, create a data folder and a hosp-a folder to store the incoming data.
8. Run scripts to create tables and insert data.
9. Repeat the steps above for each database/instance.
10. Create *initial* datasets for Bronze, Silver, and Gold layers. Also create a dataset for temp “landing” data. These will be filled in later.

The system default setup for a GCP database should suffice (at least 2vCPU, 8 GB RAM, and >= 20 GB SSD)

## Aside: Incremental vs Full Load
In the configs folder, tables and data sources are separated by incremental vs full load and follow a metadata-driven approach. Incremental load is for data subject to frequent updates whereas full load is suited for data requiring few, if any, updates after the first load into the database. 

For the primary tables used in our hospital db:
- Patients: Incremental 
- Providers: Full 
- Departments: Full 
- Transactions: Incremental 
- Encounters: Incremental 

## Step 2: Data Ingestion
These are the steps to go from extraction by preparing all data inside of the “landing.”

1. Create a dataproc cluster within GCP
    - This is done with the GCP wizard creation setup. Set disk sizes to 50GB and enable Jupyter Notebooks as an optional component.
    - E.g. n1-standard-2
2. Extract from hospital-a-mysql (tables) → gcs_bucket/landing/hospital-a/
    - Similar process is repeated for other hospital MySQL DBs
3. Open Jupyter Lab and create a new PySpark Notebook
    - Import the necessary libraries, initialize the sessions, and set up configurations for extraction.
    - This includes GCS, BigQuery, MySQL, Logging, Archiving Files, Audit Tables, Extraction from MySQL to GCS, and reading Config files into GCS.
    - Save notebook file as a python script ([name].py).
4. Run PySpark scripts from step 3 to ingest data
    - If there’s insufficient memory, increase the number of workers within the VM (e.g. 2 workers → 4 workers)
5. Repeat steps as needed for other hospital sources.
    - If you were to rerun the pipeline script for a source already ingested, files get moved back into the Archive (because rerun is newer against previous run)
6. Check BigQuery Studio and temp_dataset to verify pipeline and audit logs as needed.

## Step 3: Bronze Layer
**Before continuing, ensure all hospital data is extracted from their data source(s) into GCP.**

1. Create External tables through SQL for all hospitals
    - E.g. gcs_bucket/landing/hospital-a/* → external tables → bigquery/bronze_dataset/*
    - This keeps data in cheaper (object) storage and avoids duplicating data into the warehouse early on accidentally.
2. Make tables for Claims
    - E.g. gcs_bucket/landing/claims/*.csv → dataproc → bigquery/bronze_dataset/claims
3. Make tables for CPT Codes
    - E.g. gcs_bucket/landing/cptcodes/*.csv → dataproc → bigquery/bronze_dataset/cpt_codes
4. Create a PySpark script to ingest information into the claims table.
    - For future reference, differentiate rows by hospital source.
5. Set up a Composer 3 environment (this may take 20-30 minutes to deploy fully)
    - The “service account” should be the ###...###-compute@developer.gserviceaccount.com in GCP.
    - You can set up user-managed service accounts later, but not needed at this time.
    - The default settings (number of workers, small environment size, etc.), allowing access from all IP addresses, and standard resilience will suffice for initial scale.

## Step 4: Silver Layer
At this point, the relevant code tables should be inserted. The initial implementation doesn’t worry about ICD and NPI codes just yet to lower time to production.

1. Develop common data models for SCD Type 2 (slowly changing dimensions) to account for unpredictable behavior over time.
    - E.g. creating a new column called “patient_key” in case ID numbers, names, etc. match across two or more hospitals.
    - Hospital A → ID AS SRC_ID & Hospital B → patient_ID AS SRC_ID
2. Create tables within the silver layer that data from the bronze layer will “land” into.
3. Combine bronze tables with similar purposes (e.g. patients A and B → patients AB) into new silver tables.
    - E.g. bronze_dataset/*(full/incremental} → cdm, scd2, nulls, distinct, merge → silver_data/*
    - Truncate → Insert will delete any existing rows while preserving the structure of the table to mitigate the possibility of duplicate information.
    - If it’s a full load or “constant” (i.e. doesn’t change), truncate and insert is sufficient.
4. Verify SCD Type 2 logic with MERGE and MATCH.
    - The silver table is the target table and the bronze table is new data coming in on future incremental loads. If it matches with the silver table, then it is considered “active.”
    - If not active, or value has changed/is different, then mark it as historical data and insert updated records.
    - “<>” is a comparison condition for “not equal”, but will not catch NULL unless explicitly handled separately.
    - Delete the quality_check temp table so it doesn’t take up excess storage.
    - Repeat a similar process for each relevant table.

## Step 5: Gold Layer
The “simplest” layer, where we verify the use cases and modify data to be “human-readable” and ready for analysis. It’s a reasonable assumption we can use silver data because it’s already cleaned.

1. Business Use Cases → Silver Tables → Query → Gold Table → Reports
    - patients_summary (4 tables → 1 table) → patients_summary_gold
2. Business Use Cases
    - provider_charge_summary
    - patient_history table (a patient’s details)
    - provider_performance
    - department_performance

## Step 6: Workflow Orchestration
Now that we have data “business-ready” we move onto figuring out automation and doing the steps without needing human intervention every time.

1. Create a new Composer Environment if it doesn’t exist (see Bronze), otherwise get it running.
2. Jobs to do:
    - Pyspark DAG (Direct Acyclic Graphs) → 4 jobs (one for each ingestion table)
    - BigQuery DAG → 3 jobs (bronze, silver; gold tables)
3. PySpark DAG setup process
    - Import all the modules
    - Define all variables and jobs involved
    - Define the DAG
    - Define the Tasks using the variables/jobs from before
    - Define task dependencies
4. For BigQuery, the process is similar except you define constants, the SQL query, and default arguments.
5. Create a “parent” DAG to trigger multiple DAGs at once.

## Step 7: CI/CD
With the scripts and automation ready, now we'll add all files into a GitHub repository for source code management and further refine automation and CI/CD (Continuous Integration / Continuous Delivery).

When the code is updated, the DAG should use the updated code. This is where Cloud Build, a CI/CD platform, in GCP acts as a middle-man between GitHub and Cloud Composer.
- Additionally, any changes/pushes happening in GitHub should trigger a run.

1. Create a GitHub Repository
    - This may be done with the code editor of choice (I used VS Code)
2. To test it out fully, delete the datasets made so far and populate it with empty placeholders (e.g. bronze_dataset, silver_dataset, etc.).
    - Assuming the automation works, it should replicate all the steps taken before.
3. Initialize any DBs, clusters, and Composer BEFORE running.
4. Go to Cloud Build in GCP, then Triggers.
5. Create (or import) a "cloudbuild.yaml" file to run the code
    - Install dependencies and define how DAG/Data is uploaded.
6. Create a new trigger.
    - Event trigger should be "push to a branch" (any branch)
    - Select the repository created earlier in step 1 from GitHub and authenticate the choice
    - Configuration should be "Cloud Build configuration (yaml or json)" OR Autodetected.
    - The “service account” should be the ###...###-compute@developer.gserviceaccount.com in GCP.
7. For Airflow, go to the Composer environment you made, open the Airflow webserver, choose the account, and the updated DAGs can be found here.
    - From here, details can be edited like scheduling, running/stopping, marking states as failed/succeeded, etc.
    - For errors, you can check through the various components in GCP. Many with DAGs will be tracked/diagnosed through the Cluster and Jobs on Clusters in Dataproc.
8. You can view jobs in progress through BigQuery → Jobs Explorer.
9. From there, go through the various features of Google Cloud Platform and see what's happening within.

## Conclusion

- It's a BigQuery data lake, with data from multiple sources like MySQL and sometimes csv.
- Data is extracted from sources with Dataproc via PySpark, undergoes transformations like duplicate removal, null handling, column renaming, aggregations, joins, merges, and matches, then uploaded to BigQuery as tables.
- Business use case requirements define which tables are combined in the gold layer by utilizing one or more silver tables to create "insight tables" (or gold layer tables), which may be imported directly into visualization software like Tableau/PowerBI.
- Cloud Composer is the scheduler, with GitHub hosting the repository and Cloud Build holding the triggers to automate the data pipeline.