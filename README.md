Data Ingestion & Data Quality Pipeline

Overview

This Python-based data pipeline processes daily bank transaction batches. It validates data quality, routes invalid records to a quarantine file, deduplicates valid transactions, and safely handles historical corrections. The solution is idempotent and configuration-driven.

How to Set Up and Run the Solution


Prerequisites: Ensure Python 3.8+ is installed.

Dependencies: Install the required library:



pip install pandas
Directory Structure:

Place the input CSV files (e.g., batch_...csv, ...late_correction.csv) in the ./input directory.

Ensure the script (pipeline.py) is in the root directory alongside the ./input folder.

Execution:

python3 data_ingestion_file.py

Outputs: The cleaned dataset, quarantine file, and summary report will be generated in the ./output directory.

Approach Used for Duplicate Handling

Deduplication is handled strictly using the ref_no column, which acts as the unique primary key for a transaction.
Instead of dropping genuine repeated transactions (e.g., two identical $5 purchases on the same day), the pipeline uses Pandas' drop_duplicates(subset=['ref_no'], keep='last'). This ensures that if multiple records share the same transaction ID, only the final instance is retained, preserving data integrity.

How Re-processing is Handled (Idempotency)

The pipeline implements a Load-Merge-Deduplicate-Overwrite pattern to ensure it is completely safe to run multiple times:

Load: It reads the existing transaction_valid_final_file.csv from the output folder (if it exists).

Merge: It concatenates this historical data with the newly validated batch data.

Deduplicate: It drops duplicates across the entire merged dataset using ref_no (keep='last').

Overwrite: It overwrites the final output file.
If the same input files are processed twice, the exact same state is calculated and overwritten, resulting in zero duplicated rows.

How the Late Correction was Handled

The late correction file updates existing historical data seamlessly through the upsert logic described above.
Because the CSV files are read and concatenated sequentially, the records from the late correction file append to the bottom of the DataFrame. When the pipeline applies the drop_duplicates(subset=['ref_no'], keep='last') function, the newer, corrected records naturally overwrite the older historical records sharing the same ref_no.

Key Assumptions and Design Decisions

Configurable Architecture: File paths, column names, sort configurations, and file patterns are abstracted into a CONFIG dictionary. This allows the pipeline to quickly adapt to schema changes or entirely new data sources without rewriting core Python logic.

Strict Primary Key Rule: A null or missing ref_no is treated as a critical upstream data failure. These records are quarantined because a valid primary key is mandatory for safe deduplication and downstream analytics.

Datetime Consistency: To prevent schema drift and indexing errors between multiple pipeline runs, txn_date is strictly cast to a string format (%Y-%m-%d) immediately after parsing.

Quarantine Strategy: Invalid records (bad dates, missing keys, invalid debit/credit balances) are not silently dropped. They are tagged with a specific rejection_reason and isolated into a quarantine workflow for operational auditing.
