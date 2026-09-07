
# 🏥 FHIR Data Processing & Healthcare Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.0+-orange.svg)](https://spark.apache.org/)
[![Pandas](https://img.shields.io/badge/Pandas-1.0+-green.svg)](https://pandas.pydata.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 📋 Overview

This project implements a **complete healthcare data pipeline** for processing **FHIR (Fast Healthcare Interoperability Resources)** bulk data and performing statistical analysis on patient health records. The system transforms complex, nested FHIR JSON data into a structured master patient table suitable for healthcare analytics, clinical research, and population health management.

### 🎯 Key Features

- **Automated FHIR Data Extraction**: Unzips and loads bulk FHIR datasets automatically
- **Nested Data Flattening**: Transforms complex FHIR structures into analyzable format
- **Patient-Centric Aggregation**: Creates comprehensive patient profiles with all health data
- **Statistical Analysis**: Performs regression analysis on key health metrics
- **Interactive Exploration**: Allows flexible resource loading for custom analysis
- **Data Quality Monitoring**: Built-in validation and quality checks
- **Visualization Ready**: Generates publication-quality plots and graphs

---

## 📚 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture--data-flow)
- [Key Technical Challenges](#-key-technical-challenges--solutions)
- [Getting Started](#-getting-started)
- [Usage Guide](#-usage-guide)
- [Output Schema](#-output-data-schema)
- [Statistical Analysis](#-statistical-analysis)
- [Industry Applications](#-industry-applications)
- [Performance Tips](#-performance-tips)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🏗️ Architecture & Data Flow


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

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Data Processing** | Apache Spark (PySpark) | Distributed processing of FHIR NDJSON |
| **Data Analysis** | Pandas, NumPy | Statistical analysis & data manipulation |
| **Visualization** | Matplotlib | Regression plots & data visualization |
| **Statistics** | SciPy | Linear regression & hypothesis testing |
| **Data Format** | NDJSON (JSON Lines) | Streaming-friendly JSON format |

---

## 🧩 Key Technical Challenges & Solutions

### 1. JSON Flattening & Nested Data Structure

**Challenge**: FHIR data is deeply nested with complex object hierarchies that need to be flattened for analysis.

**Example FHIR Patient Structure**:
```json
{
  "id": "patient-001",
  "name": [{"given": ["John"], "family": "Doe"}],
  "address": [{"city": "Boston", "country": "US"}],
  "maritalStatus": {"coding": [{"code": "M"}]}
}
. The Join Explosion Problem ⚠️
The Challenge: When joining patients with multiple clinical records, a Cartesian product explosion occurs.

text
Patient (1) → Conditions (5) → Medications (10) → Encounters (8)
Direct JOIN creates: 1 × 5 × 10 × 8 = 400 rows for ONE patient!

The Solution: Aggregate BEFORE joining using COLLECT_SET and COLLECT_LIST.
4. Dictionary-Based Lookups for Speed
Problem: Repeatedly searching for patient data across different resource types.

Solution: Python dictionaries with O(1) lookup time.

python
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

fhir_pipeline.py   - Data processing
fhir_analysis.py   - Statistical analysis
master_table1.json - Output data
regression_plots.png - Visualization

### Data Pipeline

# Pipeline
load_fhir_resource()      # Load FHIR data
create_spark_session()    # Start Spark
check_data_quality()      # Validate data

# Analysis
has_disease()            # Check for disease
get_disease_column()     # Create binary column
stats.linregress()       # Run regression
