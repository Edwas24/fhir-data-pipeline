# ========================
# IMPORTS
# ========================
import os
import pandas as pd
import zipfile
import json
import glob
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import StorageLevel
import builtins  # Add this to use Python's built-in round


# ========================
# CONFIGURATION
# ========================

# CURRENT_DIR: Gets the directory where this script is located.
# This ensures file paths work regardless of where you run the script from.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# ZIP_PATH: Points to the compressed FHIR dataset file.
# This is the input file containing all the FHIR resources in NDJSON format.
ZIP_PATH = os.path.join(CURRENT_DIR, 'sample-bulk-fhir-datasets-100-patients.zip')

# EXTRACT_PATH: Where the ZIP contents will be extracted.
# We extract once and reuse the extracted files for faster loading.
EXTRACT_PATH = os.path.join(CURRENT_DIR, 'extracted_fhir/')

# JSON_OUTPUT: The final output file path.
# This will contain the master patient table in JSON Lines format.
JSON_OUTPUT = os.path.join(CURRENT_DIR, 'master_table1.json')


# Spark configuration - set Python interpreter
# These environment variables tell Spark which Python executable to use.
# Important when you have multiple Python versions installed.
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\edrob\\AppData\\Local\\Programs\\Python\\Python314\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\edrob\\AppData\\Local\\Programs\\Python\\Python314\\python.exe"


# ========================
# UTILITY FUNCTIONS
# ========================

def list_zip_contents(zip_path):
    """
    Lists all files inside the ZIP archive without extracting them.
    
    Why? This helps you verify what's inside the ZIP before extraction.
    Each FHIR resource (Patient, Condition, Encounter, etc.) is stored as
    a separate .ndjson file.
    
    Args:
        zip_path: Path to the ZIP file to inspect.
    """
    with zipfile.ZipFile(zip_path, 'r') as z:
        print("Files in ZIP:")
        for file_info in z.infolist():
            # file_info.filename = name of the file inside ZIP
            # file_info.file_size = size in bytes
            print("  " + file_info.filename + " (" + str(file_info.file_size) + " bytes)")


def extract_zip_files(zip_path, extract_path):
    """
    Extracts all files from the ZIP archive to the specified folder.
    
    Why check if extract_path exists? To avoid re-extracting every time.
    Extraction can be slow, so we only do it once.
    
    Args:
        zip_path: Path to the ZIP file to extract.
        extract_path: Destination folder for extracted files.
    """
    if not os.path.exists(extract_path):
        print("Extracting files to: " + extract_path)
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(extract_path)  # Extracts everything
        print("Extraction complete")
    else:
        # Files already exist, skip extraction to save time
        print("Files already extracted at: " + extract_path)


def load_fhir_resource(spark, extract_path, resource_name):
    """
    Loads a FHIR resource from NDJSON file into a Spark DataFrame.
    
    How it works:
        1. Looks for files matching pattern: resource_name*.ndjson
        2. Spark reads all matching NDJSON files into one DataFrame
        3. Spark automatically infers the schema from the JSON structure
    
    FHIR resources are stored as NDJSON (Newline Delimited JSON).
    Each line is one JSON object representing one FHIR resource.
    
    Args:
        spark: The SparkSession object.
        extract_path: Where extracted files are located.
        resource_name: Name of the FHIR resource (e.g., 'Patient', 'Condition').
    
    Returns:
        Spark DataFrame with the resource data, or None if no file found.
    """
    base_path = extract_path + "sample-bulk-fhir-datasets-100-patients/"
    # glob finds all files matching the pattern
    matching_files = glob.glob(base_path + resource_name + "*.ndjson")
    
    if matching_files:
        # spark.read.json automatically handles NDJSON format
        df = spark.read.json(matching_files)
        return df
    else:
        print("No file found for resource: " + resource_name)
        return None


def create_spark_session():
    """
    Creates and configures a SparkSession.
    
    SparkSession is the entry point for Spark functionality.
    These configurations optimize performance for working with FHIR data.
    
    Configuration explanations:
        - spark.driver.memory: Memory for the driver program (8GB)
        - spark.executor.memory: Memory for each executor (8GB)
        - spark.sql.shuffle.partitions: Partitions for shuffle operations (200)
        - spark.network.timeout: Timeout for network operations (600s)
        - spark.driver.maxResultSize: Max result size driver can collect (4GB)
        - spark.sql.adaptive.enabled: Adaptive query execution (optimizes at runtime)
        - spark.serializer: Kryo is faster than Java serialization
        - spark.memory.fraction: Fraction of JVM heap for Spark memory (80%)
        - spark.memory.storageFraction: Fraction for storage (30%)
    
    Returns:
        Configured SparkSession object.
    """
    return SparkSession.builder \
        .appName("FHIR_Explore") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.network.timeout", "600s") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
        .config("spark.sql.broadcastTimeout", "600") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .getOrCreate()


def load_default_resources(spark, extract_path):
    """
    Loads a predefined set of common FHIR resources.
    
    Why these five?
        - Patient: Core demographic data (the main entity we're analyzing)
        - Condition: Medical diagnoses/conditions
        - Encounter: Healthcare visits/interactions
        - Observation: Clinical measurements (lab results, vitals)
        - MedicationRequest: Prescribed medications
    
    The data is:
        1. Loaded into DataFrames
        2. Persisted to memory/disk for faster access (caching)
        3. Registered as temporary views for SQL queries
    
    Args:
        spark: The SparkSession object.
        extract_path: Where extracted files are located.
    
    Returns:
        Dictionary mapping resource names to DataFrames.
    """
    default_resources = ['Patient', 'Condition', 'Encounter', 'Observation', 'MedicationRequest']
    loaded_dfs = {}
    
    print("Loading default resources...")
    for resource in default_resources:
        df = load_fhir_resource(spark, extract_path, resource)
        if df is not None:
            loaded_dfs[resource.lower()] = df
            # persist() caches the DataFrame in memory and disk
            # This speeds up repeated queries significantly
            df.persist(StorageLevel.MEMORY_AND_DISK)
            # createOrReplaceTempView allows SQL queries on this DataFrame
            df.createOrReplaceTempView(resource.lower())
            print("  Loaded " + resource + ": " + str(df.count()) + " rows, " + str(len(df.columns)) + " columns")
    
    return loaded_dfs


def interactive_loader(spark, extract_path):
    """
    Interactive resource loader that lets the user choose which files to load.
    
    Why interactive? Different analyses might need different FHIR resources.
    This lets you explore specific resources without loading everything.
    
    The function:
        1. Asks if you want to load a file
        2. Prompts for resource name
        3. Loads it if found
        4. Repeats until you say "no"
    
    Args:
        spark: The SparkSession object.
        extract_path: Where extracted files are located.
    
    Returns:
        Dictionary mapping resource names to DataFrames.
    """
    resource_dict = {}
    
    user_choice = input("Would you like to load a file? (yes/no): ")
    
    while user_choice.lower() != "no":
        resource_name = input("Resource name (Patient, Encounter, Condition, etc.): ").strip()
        
        df = load_fhir_resource(spark, extract_path, resource_name)
        
        if df is not None:
            resource_dict[resource_name.lower()] = df
            df.persist(StorageLevel.MEMORY_AND_DISK)
            df.createOrReplaceTempView(resource_name.lower())
            
            print("Loaded " + resource_name + ":")
            print("  Rows: " + str(df.count()))
            print("  Columns: " + str(len(df.columns)))
            df.printSchema()
        else:
            print("No data found for resource: " + resource_name)
        
        user_choice = input("Load another file? (yes/no): ")
    
    return resource_dict


def check_data_quality(df, df_name):
    """
    Performs basic data quality checks on a DataFrame.
    
    What it checks:
        - Total rows and columns
        - Column names
        - NULL values in the first 10 columns (with percentages)
    
    Why NULL analysis? Missing data can cause problems in analysis.
    Knowing which columns have NULLs helps you decide how to handle them.
    
    Args:
        df: The Spark DataFrame to check.
        df_name: Name of the DataFrame for display purposes.
    """
    print("")
    print("=" * 60)
    print("DATA QUALITY CHECK: " + df_name.upper())
    print("=" * 60)
    
    total_rows = df.count()
    total_cols = len(df.columns)
    print("Total rows: " + str(total_rows))
    print("Total columns: " + str(total_cols))
    
    print("")
    print("COLUMNS:")
    print(df.columns)
    
    print("")
    print("NULL VALUE ANALYSIS:")
    # Only check first 10 columns to keep output manageable
    for col_name in df.columns[:10]:
        # count() with filter counts rows where column is NULL
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
            # Use Python's built-in round, not Spark's round
            rounded_pct = builtins.round(null_percentage, 2)
            print("  WARNING: " + col_name + ": " + str(null_count) + " NULLs (" + str(rounded_pct) + "%)")


# ========================
# MAIN EXECUTION
# ========================

def main():
    """
    Main function that orchestrates the entire FHIR data processing pipeline.
    
    Pipeline steps:
        1. Extract FHIR data from ZIP
        2. Create Spark session
        3. Load resources interactively
        4. Load default resources
        5. Check data quality on raw data
        6. Build aggregated views (conditions, medications, encounters)
        7. Join everything into a master patient table
        8. Perform quality checks on final table
        9. Write master table to JSON Lines file
        10. Clean up cached data
        11. Stop Spark session
    """
    print("Starting FHIR data processing...")
    print("")
    
    # --- Step 1: Extract files ---
    # First inspect what's in the ZIP, then extract if needed
    list_zip_contents(ZIP_PATH)
    extract_zip_files(ZIP_PATH, EXTRACT_PATH)
    
    # --- Step 2: Create Spark session ---
    print("")
    print("Creating Spark session...")
    spark = create_spark_session()
    print("Spark session created")
    
    # --- Step 3: Interactive loader ---
    # Let the user choose which resources to load
    print("")
    print("Interactive resource loader")
    print("-" * 40)
    resource_dict = interactive_loader(spark, EXTRACT_PATH)
    
    # --- Step 4: Load default resources ---
    # Always load the core resources needed for the master table
    print("")
    default_dfs = load_default_resources(spark, EXTRACT_PATH)
    resource_dict.update(default_dfs)  # Merge dictionaries
    
    # --- Step 5: Extract individual DataFrames for easy access ---
    # Why lowercase? Because we stored them with .lower() in the dict
    patient_df = resource_dict.get('patient')
    condition_df = resource_dict.get('condition')
    encounter_df = resource_dict.get('encounter')
    observation_df = resource_dict.get('observation')
    medication_df = resource_dict.get('medicationrequest')
    
    # --- Step 6: Show what we loaded ---
    print("")
    print("Loaded resources:")
    if resource_dict:
        for i, (name, df) in enumerate(resource_dict.items()):
            print("  " + str(i+1) + ". " + name.title() + ": " + str(df.count()) + " rows")
    else:
        print("  No resources loaded")
    
    # --- Step 7: Data quality checks on raw data ---
    print("")
    print("=" * 60)
    print("RAW DATA QUALITY CHECKS")
    print("=" * 60)
    
    for name, df in resource_dict.items():
        if df is not None:
            check_data_quality(df, name)
    
    
    # ============================================================================
    # STEP 1: PATIENT BASE TABLE
    # ============================================================================
    """
    This creates the base patient table with demographic information.
    
    Why SELECT specific fields? FHIR Patient has many fields.
    We only need: id, name, address, birthDate, gender, maritalStatus.
    
    The data comes from the 'patient' temporary view we created earlier.
    """
    print("")
    print("=" * 60)
    print("STEP 1: PATIENT BASE TABLE")
    print("=" * 60)
    
    query_patients = """
    SELECT 
        p.id AS patient_id,
        p.name,
        p.address,
        p.birthDate,
        p.gender,
        p.maritalStatus
    FROM patient p
    """
    
    patient_base = spark.sql(query_patients)
    patient_base.persist(StorageLevel.MEMORY_AND_DISK)
    patient_base.createOrReplaceTempView("patient_base")
    
    print("Patient base: " + str(patient_base.count()) + " rows")
    print("Columns: " + str(patient_base.columns))
    patient_base.printSchema()
    
    
    # ============================================================================
    # STEP 2: AGGREGATE CONDITIONS PER PATIENT (USING COLLECT_SET)
    # ============================================================================
    """
    This aggregates all conditions for each patient into a single row.
    
    Key concepts:
        - SPLIT(c.subject.reference, '/')[1] extracts the patient ID from the reference
        - COLLECT_SET collects unique values (removes duplicates)
        - STRUCT creates a nested structure with code and display name
        - COUNT(DISTINCT) counts unique condition codes
    
    Why use COLLECT_SET instead of COLLECT_LIST?
        COLLECT_SET eliminates duplicates, giving us unique conditions per patient.
    
    Example: If a patient has "Diabetes" recorded twice, COLLECT_SET will only include it once.
    """
    print("")
    print("=" * 60)
    print("STEP 2: AGGREGATE CONDITIONS PER PATIENT (UNIQUE ONLY)")
    print("=" * 60)
    
    query_conditions_agg = """
    SELECT 
        SPLIT(c.subject.reference, '/')[1] AS patient_id,
        COLLECT_SET(
            STRUCT(
                c.code.coding[0].code AS condition_code,
                c.code.coding[0].display AS condition_name
            )
        ) AS conditions,
        COUNT(DISTINCT c.code.coding[0].code) AS condition_count
    FROM condition c
    WHERE c.subject.reference IS NOT NULL
    GROUP BY SPLIT(c.subject.reference, '/')[1]
    """
    
    conditions_agg = spark.sql(query_conditions_agg)
    conditions_agg.persist(StorageLevel.MEMORY_AND_DISK)
    conditions_agg.createOrReplaceTempView("conditions_agg")
    
    print("Patients with conditions: " + str(conditions_agg.count()))
    print("Columns: " + str(conditions_agg.columns))
    conditions_agg.printSchema()
    
    print("")
    print("Sample of unique conditions (no duplicates):")
    conditions_agg.limit(1).select("patient_id", "conditions", "condition_count").show(1, truncate=False)
    
    
    # ============================================================================
    # STEP 3: AGGREGATE MEDICATIONS PER PATIENT
    # ============================================================================
    """
    This aggregates all medication requests for each patient.
    
    Key differences from conditions:
        - Uses COLLECT_LIST instead of COLLECT_SET (keeps all medications)
        - Includes more fields: medication_id, status, intent, authoredOn, encounter_id
        - Medications might have duplicates if patient was prescribed the same drug multiple times
    
    Why use COLLECT_LIST? Medication records are individual transactions.
    Each prescription is unique, even if for the same drug.
    """
    print("")
    print("=" * 60)
    print("STEP 3: AGGREGATE MEDICATIONS PER PATIENT")
    print("=" * 60)
    
    query_medications_agg = """
    SELECT 
        SPLIT(m.subject.reference, '/')[1] AS patient_id,
        COLLECT_LIST(
            STRUCT(
                m.id AS medication_id,
                m.medicationCodeableConcept.coding[0].code AS medication_code,
                m.medicationCodeableConcept.coding[0].display AS medication_name,
                m.status AS medication_status,
                m.intent,
                m.authoredOn,
                SPLIT(m.encounter.reference, '/')[1] AS encounter_id
            )
        ) AS medications,
        COUNT(*) AS medication_count
    FROM medicationrequest m
    WHERE m.subject.reference IS NOT NULL
    GROUP BY SPLIT(m.subject.reference, '/')[1]
    """
    
    medications_agg = spark.sql(query_medications_agg)
    medications_agg.persist(StorageLevel.MEMORY_AND_DISK)
    medications_agg.createOrReplaceTempView("medications_agg")
    
    print("Patients with medications: " + str(medications_agg.count()))
    print("Columns: " + str(medications_agg.columns))
    medications_agg.printSchema()
    
    
    # ============================================================================
    # STEP 4: AGGREGATE ENCOUNTERS PER PATIENT
    # ============================================================================
    """
    This aggregates all encounters (healthcare visits) for each patient.
    
    Important FHIR concepts:
        - Encounter type: What kind of visit (e.g., outpatient, emergency)
        - Class: Inpatient, outpatient, emergency, etc.
        - Period: Start and end time of the encounter
        - ServiceProvider: The organization that provided the service
    
    Why aggregate encounters? Encounters provide context for other resources.
    Conditions and medications are often linked to specific encounters.
    """
    print("")
    print("=" * 60)
    print("STEP 4: AGGREGATE ENCOUNTERS PER PATIENT")
    print("=" * 60)
    
    query_encounters_agg = """
    SELECT 
        SPLIT(e.subject.reference, '/')[1] AS patient_id,
        COLLECT_LIST(
            STRUCT(
                e.id AS encounter_id,
                e.status AS encounter_status,
                e.type[0].coding[0].code AS encounter_type_code,
                e.type[0].coding[0].display AS encounter_type_name,
                e.period.start AS start_date,
                e.period.end AS end_date,
                e.class.code AS class_code,
                SPLIT(e.serviceProvider.reference, '/')[1] AS organization_id
            )
        ) AS encounters,
        COUNT(*) AS encounter_count
    FROM encounter e
    WHERE e.subject.reference IS NOT NULL
    GROUP BY SPLIT(e.subject.reference, '/')[1]
    """
    
    encounters_agg = spark.sql(query_encounters_agg)
    encounters_agg.persist(StorageLevel.MEMORY_AND_DISK)
    encounters_agg.createOrReplaceTempView("encounters_agg")
    
    print("Patients with encounters: " + str(encounters_agg.count()))
    print("Columns: " + str(encounters_agg.columns))
    encounters_agg.printSchema()
    
    
    # ============================================================================
    # STEP 5: BUILD MASTER PATIENT TABLE USING BROADCAST JOINS
    # ============================================================================
    """
    This builds the final master table by joining all the aggregated data.
    
    Key concept: Broadcast Join
        - broadcast() hints to Spark that the DataFrame is small
        - Spark will send the entire DataFrame to all nodes
        - This is MUCH faster than shuffling data across the network
    
    Why use LEFT JOIN?
        - We want ALL patients, even if they have no conditions, medications, or encounters
        - NULL values will be inserted for missing data
    
    The joins happen in sequence:
        1. Start with patient_base (all patients)
        2. LEFT JOIN conditions_agg
        3. LEFT JOIN medications_agg
        4. LEFT JOIN encounters_agg
    """
    print("")
    print("=" * 60)
    print("STEP 5: BUILDING MASTER PATIENT TABLE")
    print("=" * 60)
    
    from pyspark.sql.functions import broadcast
    
    # Start with the base patient table
    patient_master = patient_base
    
    # Left join with conditions
    patient_master = patient_master.join(
        broadcast(conditions_agg.select("patient_id", "conditions", "condition_count")),
        patient_master.patient_id == conditions_agg.patient_id,
        "left"
    ).drop(conditions_agg.patient_id)  # Drop duplicate column
    
    print("After adding conditions: " + str(patient_master.count()) + " rows")
    
    # Left join with medications
    patient_master = patient_master.join(
        broadcast(medications_agg.select("patient_id", "medications", "medication_count")),
        patient_master.patient_id == medications_agg.patient_id,
        "left"
    ).drop(medications_agg.patient_id)
    
    print("After adding medications: " + str(patient_master.count()) + " rows")
    
    # Left join with encounters
    patient_master = patient_master.join(
        broadcast(encounters_agg.select("patient_id", "encounters", "encounter_count")),
        patient_master.patient_id == encounters_agg.patient_id,
        "left"
    ).drop(encounters_agg.patient_id)
    
    print("After adding encounters: " + str(patient_master.count()) + " rows")
    
    # Cache the final table for faster querying
    patient_master.persist(StorageLevel.MEMORY_AND_DISK)
    patient_master.createOrReplaceTempView("patient_master")
    
    print("")
    print("Master patient table: " + str(patient_master.count()) + " rows")
    print("Columns: " + str(patient_master.columns))
    patient_master.printSchema()
    
    
    # ============================================================================
    # DATA QUALITY CHECKS ON FINAL TABLE
    # ============================================================================
    """
    Final quality checks to understand the completeness of our master table.
    
    What we calculate:
        - Total patients
        - Percentage with conditions, medications, encounters
        - Average counts per patient (for those who have data)
    
    These metrics help us understand:
        - Data completeness (are we missing many patients?)
        - Data density (how much data do we have per patient?)
    """
    print("")
    print("=" * 60)
    print("FINAL DATA QUALITY CHECKS")
    print("=" * 60)
    
    total_patients = patient_master.count()
    with_conditions = patient_master.filter(col("conditions").isNotNull()).count()
    with_medications = patient_master.filter(col("medications").isNotNull()).count()
    with_encounters = patient_master.filter(col("encounters").isNotNull()).count()
    
    cond_pct = (with_conditions / total_patients * 100) if total_patients > 0 else 0
    med_pct = (with_medications / total_patients * 100) if total_patients > 0 else 0
    enc_pct = (with_encounters / total_patients * 100) if total_patients > 0 else 0
    
    print("Total patients: " + str(total_patients))
    print("Patients with conditions: " + str(with_conditions) + " (" + str(builtins.round(cond_pct, 1)) + "%)")
    print("Patients with medications: " + str(with_medications) + " (" + str(builtins.round(med_pct, 1)) + "%)")
    print("Patients with encounters: " + str(with_encounters) + " (" + str(builtins.round(enc_pct, 1)) + "%)")
    
    # Calculate averages using Spark's avg() aggregation function
    avg_conditions_val = patient_master.select(avg("condition_count")).collect()[0][0] if with_conditions > 0 else 0
    avg_medications_val = patient_master.select(avg("medication_count")).collect()[0][0] if with_medications > 0 else 0
    avg_encounters_val = patient_master.select(avg("encounter_count")).collect()[0][0] if with_encounters > 0 else 0
    
    print("")
    print("Average conditions per patient: " + str(builtins.round(avg_conditions_val, 2)))
    print("Average medications per patient: " + str(builtins.round(avg_medications_val, 2)))
    print("Average encounters per patient: " + str(builtins.round(avg_encounters_val, 2)))
    
    
    # ============================================================================
    # WRITE MASTER TABLE AS JSON LINES
    # ============================================================================
    """
    Writes the master table to a JSON Lines file.
    
    Why JSON Lines (NDJSON)?
        - Each line is a complete JSON object
        - Easy to read line by line (streaming)
        - Compatible with many tools and languages
        - Human-readable for inspection
    
    What does toPandas() do?
        - Converts Spark DataFrame to Pandas DataFrame
        - This pulls ALL data into the driver memory
        - Only safe for small datasets (we have 100 patients)
    
    The 'default=str' in json.dumps() handles non-serializable types.
    """
    print("")
    print("=" * 60)
    print("WRITING MASTER TABLE AS JSON LINES")
    print("=" * 60)
    
    # Convert Spark DataFrame to Pandas (only safe for small data)
    master_pd = patient_master.toPandas()
    
    # Write each row as a JSON object on its own line
    with open(JSON_OUTPUT, 'w') as f:
        for idx, row in master_pd.iterrows():
            row_dict = row.to_dict()
            f.write(json.dumps(row_dict, default=str) + '\n')
    
    print("")
    print("JSON Lines file written to: " + JSON_OUTPUT)
    print("Total patients (lines): " + str(len(master_pd)))
    print("Total columns: " + str(len(master_pd.columns)))
    print("Columns: " + str(list(master_pd.columns)))
    
    
    # ============================================================================
    # CLEANUP - UNPERSISTING INTERMEDIATE DATA
    # ============================================================================
    """
    Releases cached data from memory/disk.
    
    Why unpersist?
        - Frees up memory and disk space
        - Prevents memory pressure on the driver
    
    We unpersist intermediate DataFrames (conditions_agg, medications_agg, encounters_agg)
    because they're no longer needed after building patient_master.
    
    We keep patient_base and patient_master cached until the end.
    """
    print("")
    print("=" * 60)
    print("CLEANING UP - UNPERSISTING INTERMEDIATE DATA")
    print("=" * 60)
    
    conditions_agg.unpersist()
    medications_agg.unpersist()
    encounters_agg.unpersist()
    
    print("Intermediate DataFrames unpersisted")
    print("Keeping patient_base and patient_master cached for future queries")
    
    
    # ============================================================================
    # STOP SPARK
    # ============================================================================
    """
    Properly shuts down the Spark session.
    
    Why stop Spark?
        - Releases cluster resources
        - Frees up JVM memory
        - Clean termination of executors
    
    Always stop Spark at the end of your application.
    """
    print("")
    print("=" * 60)
    print("STOPPING SPARK SESSION")
    print("=" * 60)
    
    # Unpersist final DataFrames before stopping
    patient_base.unpersist()
    patient_master.unpersist()
    
    spark.stop()
    print("")
    print("All done. Spark session stopped successfully.")


# ========================
# ENTRY POINT
# ========================

if __name__ == "__main__":
    """
    This ensures the main() function only runs when this script is executed directly.
    If this script is imported as a module, main() won't run automatically.
    This is a Python best practice for scripts that can be imported.
    """
    main()