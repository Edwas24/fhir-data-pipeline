🏥 FHIR Data Processing & Healthcare Analytics Pipeline
Python | PySpark | Pandas | Matplotlib | SciPy

📋 Overview
This project implements a complete healthcare data pipeline for processing FHIR (Fast Healthcare Interoperability Resources) bulk data and performing statistical analysis on patient health records. The system transforms complex, nested FHIR JSON data into a structured master patient table suitable for healthcare analytics, clinical research, and population health management.

🎯 Key Features
Automated FHIR Data Extraction: Unzips and loads bulk FHIR datasets automatically

Nested Data Flattening: Transforms complex FHIR structures into analyzable format

Patient-Centric Aggregation: Creates comprehensive patient profiles with all health data

Statistical Analysis: Performs regression analysis on key health metrics

Interactive Exploration: Allows flexible resource loading for custom analysis

Data Quality Monitoring: Built-in validation and quality checks

Visualization Ready: Generates publication-quality plots and graphs

📚 Table of Contents
Overview

Architecture

Key Technical Challenges

Getting Started

Usage Guide

Output Schema

Statistical Analysis

Industry Applications

Performance Tips

Troubleshooting

Contributing

License

🏗️ Architecture & Data Flow
Data Pipeline

┌─────────────────────────────────────────────────────────────────────┐
│                         DATA FLOW PIPELINE                         │
└─────────────────────────────────────────────────────────────────────┘

ZIP Archive (FHIR NDJSON)
    │ sample-bulk-fhir-datasets-100-patients.zip
    ↓ [Extract]
Extracted FHIR Files
    │ Patient.ndjson, Condition.ndjson, Encounter*.ndjson, etc.
    ↓ [Load with Spark]
Spark DataFrames (Raw)
    │ Automatic schema inference from JSON
    ↓ [Transform & Aggregate]
Aggregated Views
    │ • conditions_agg (unique conditions per patient)
    │ • medications_agg (all prescriptions per patient)
    │ • encounters_agg (all visits per patient)
    ↓ [Broadcast Join]
Master Patient Table
    │ 1 row per patient with all aggregated data
    ↓ [Export]
Analysis Ready Data (master_table1.json)
    │ JSON Lines format - 1 JSON object per line
    ↓ [Statistical Analysis]
Insights & Visualizations
    │ • Age vs Medication Count
    │ • Age vs Diabetes Probability
    │ • Age vs Condition Count
    └→ regression_plots.png

Technology Stack
Component	Technology	Purpose
Data Processing	Apache Spark (PySpark)	Distributed processing of FHIR NDJSON
Data Analysis	Pandas, NumPy	Statistical analysis & data manipulation
Visualization	Matplotlib	Regression plots & data visualization
Statistics	SciPy	Linear regression & hypothesis testing
Data Format	NDJSON (JSON Lines)	Streaming-friendly JSON format

🧩 Key Technical Challenges & Solutions
1. JSON Flattening & Nested Data Structure
Challenge: FHIR data is deeply nested with complex object hierarchies that need to be flattened for analysis.

Example FHIR Patient Structure:

{
  "id": "patient-001",
  "name": [{"given": ["John"], "family": "Doe"}],
  "address": [{"city": "Boston", "country": "US"}],
  "maritalStatus": {"coding": [{"code": "M"}]}
}

Solution: Spark's spark.read.json() automatically handles nested structures, creating appropriate nested columns. I then selectively extract needed fields using SQL queries with dot notation.

SELECT 
    p.id AS patient_id,
    p.birthDate,
    p.gender,
    p.maritalStatus.coding[0].code AS marital_status
FROM patient p

2. The Join Explosion Problem ⚠️
The Challenge: When joining patients with multiple clinical records, a Cartesian product explosion occurs.

Patient (1) → Conditions (5) → Medications (10) → Encounters (8)
Direct JOIN creates: 1 × 5 × 10 × 8 = 400 rows for ONE patient!

The Solution: I aggregate BEFORE joining using COLLECT_SET and COLLECT_LIST.

Before Aggregation (Problem):

-- This creates explosion!
SELECT p.id, c.code, m.code, e.id
FROM patient p
JOIN condition c ON p.id = c.patient
JOIN medication m ON p.id = m.patient
JOIN encounter e ON p.id = e.patient
-- Results: 400 rows for patient with 5 conditions, 10 meds, 8 encounters

After Aggregation (Solution):

-- Step 1: Aggregate conditions per patient (one row per patient)
SELECT patient_id,
       COLLECT_SET(STRUCT(code, display)) AS conditions,
       COUNT(*) AS condition_count
FROM condition
GROUP BY patient_id  -- Now: 1 row per patient

-- Step 2: Similarly aggregate medications and encounters
-- Step 3: Join aggregated views (no explosion!)

3. COLLECT_SET vs COLLECT_LIST
Feature	COLLECT_SET	COLLECT_LIST
Duplicates	Removes duplicates	Keeps all duplicates
Ordering	Non-deterministic	Preserves input order
Use Case	Unique values (e.g., conditions)	All records (e.g., prescriptions)
Memory	More overhead (deduplication)	Less overhead
Performance	Slightly slower	Faster

4. Dictionary-Based Lookups for Speed
Problem: Repeatedly searching for patient data across different resource types.

Solution: I use Python dictionaries with O(1) lookup time.

# Build dictionary: patient_id → conditions list
patient_conditions = {}
for patient_id, condition in conditions_data:
    patient_conditions.setdefault(patient_id, []).append(condition)

# O(1) retrieval vs O(n) scanning
conditions = patient_conditions.get("patient-123", [])  # Fast!

Performance Impact:

Without dictionary: O(n²) - 100M operations for 10k patients

With dictionary: O(n) - 10k operations for 10k patients

1000x faster for large datasets


