# === IMPORTS ===
# pandas: For data manipulation and DataFrames
# numpy: For numerical operations and arrays
# matplotlib.pyplot: For creating plots and visualizations
# scipy.stats: For statistical tests and regression
# datetime: For calculating age from birth dates
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from datetime import datetime

# === LOAD DATA ===
# Read the master_table.json file (NDJSON format - one JSON object per line)
# lines=True tells pandas to read each line as a separate JSON object
df = pd.read_json('master_table.json', lines=True)

# === CALCULATE AGES ===
# Get today's date to calculate age
today = datetime.now()

# Create an empty list to store all patient ages
ages = []

# Loop through each patient's birthDate in the DataFrame
for x in df["birthDate"]:
    # Convert the birthDate string (e.g., "1991-09-01") to a datetime object
    bday = datetime.strptime(x, "%Y-%m-%d")
    
    # Calculate age by subtracting birth year from current year
    age = today.year - bday.year
    
    # Check if the birthday hasn't happened yet this year
    # If today is before the birthday, subtract 1 from age
    if today.month < bday.month or (today.month == bday.month and today.day < bday.day):
        age -= 1
    
    # Add the calculated age to the ages list
    ages.append(age)

# === FUNCTIONS ===

def has_disease(conditions, disease_name):
    """
    Check if a patient has a specific disease.
    
    Args:
        conditions: List of conditions for a patient (each condition is [code, name])
        disease_name: The name of the disease to check for
    
    Returns:
        1 if patient has the disease, 0 if they don't
    """
    # Loop through each condition in the patient's conditions list
    for cond in conditions:
        # cond[1] is the condition name (e.g., "Diabetes mellitus type 2 (disorder)")
        # Check if it matches the disease we're looking for
        if cond[1] == disease_name:
            return 1  # Found the disease - return 1
    
    # If we checked all conditions and didn't find it, return 0
    return 0

def get_disease_column(disease_name):
    """
    Create a list of 1s and 0s indicating which patients have a specific disease.
    
    Args:
        disease_name: The name of the disease to check for
    
    Returns:
        A list where each entry is 1 (has disease) or 0 (doesn't have disease)
    """
    # Create an empty list to store results
    result = []
    
    # Loop through each patient's conditions
    for conditions in df['conditions']:
        # Use the has_disease function to check this patient
        # Append 1 or 0 to the result list
        result.append(has_disease(conditions, disease_name))
    
    return result

# === GET DATA ===
# Get medication count for each patient (from the DataFrame column)
medication_counts = df['medication_count'].tolist()

# Get diabetes column (1 if patient has diabetes, 0 if not)
diabetes = get_disease_column('Diabetes mellitus type 2 (disorder)')

# Get condition count for each patient (from the DataFrame column)
condition_counts = df['condition_count'].tolist()

# === REGRESSION 1: AGE vs MEDICATION COUNT ===
# This tests: Does age predict how many medications a patient takes?
print("=" * 50)
print("Age vs Medication Count")
print("=" * 50)

# linregress does linear regression and returns:
# slope: How much Y changes when X increases by 1
# intercept: Value of Y when X = 0
# r: Correlation coefficient (-1 to 1)
# p: p-value (if < 0.05, the relationship is significant)
# se: Standard error of the estimate
slope1, intercept1, r1, p1, se1 = stats.linregress(ages, medication_counts)

# Print the results
print("R-squared:", r1**2)  # How much variance is explained (0 to 1)
print("p-value:", p1)       # Is it significant? (< 0.05 = yes)
print("Slope:", slope1)     # How much medication count increases per year of age
print("Intercept:", intercept1)  # Predicted medication count at age 0

# Interpret the results
if p1 < 0.05:
    print("✅ Significant - Age predicts medication count")
else:
    print("❌ Not significant")
print()

# === REGRESSION 2: AGE vs DIABETES ===
# This tests: Does age predict whether a patient has diabetes?
print("=" * 50)
print("Age vs Diabetes")
print("=" * 50)

# Run regression: Age (X) predicts Diabetes (Y)
slope2, intercept2, r2, p2, se2 = stats.linregress(ages, diabetes)

# Print the results
print("R-squared:", r2**2)  # How much variance is explained
print("p-value:", p2)       # Is it significant?
print("Slope:", slope2)     # How much diabetes probability increases per year
print("Intercept:", intercept2)  # Diabetes probability at age 0

# Interpret the results
if p2 < 0.05:
    print("✅ Significant - Age predicts diabetes")
else:
    print("❌ Not significant")
print()

# === REGRESSION 3: AGE vs CONDITION COUNT ===
# This tests: Does age predict how many conditions a patient has?
print("=" * 50)
print("Age vs Condition Count")
print("=" * 50)

# Run regression: Age (X) predicts Condition Count (Y)
slope3, intercept3, r3, p3, se3 = stats.linregress(ages, condition_counts)

# Print the results
print("R-squared:", r3**2)  # How much variance is explained
print("p-value:", p3)       # Is it significant?
print("Slope:", slope3)     # How many more conditions per year of age
print("Intercept:", intercept3)  # Predicted condition count at age 0

# Interpret the results
if p3 < 0.05:
    print("✅ Significant - Age predicts condition count")
else:
    print("❌ Not significant")
print()

# === PREDICTIONS ===
# Use the regression equations to predict outcomes at specific ages
print("=" * 50)
print("Predictions at Age 50")
print("=" * 50)

# Set the age we want to predict for
age = 50

# Use each regression equation: Y = slope * X + intercept
# This predicts medication count for a 50-year-old
pred_meds = slope1 * age + intercept1

# This predicts diabetes probability for a 50-year-old
pred_diabetes = slope2 * age + intercept2

# This predicts condition count for a 50-year-old
pred_conditions = slope3 * age + intercept3

# Diabetes probability should be between 0 and 1
# If prediction is less than 0, set to 0
if pred_diabetes < 0:
    pred_diabetes = 0
# If prediction is greater than 1, set to 1
if pred_diabetes > 1:
    pred_diabetes = 1

# Print the predictions
print("At age 50:")
print("  Predicted medications:", pred_meds)
print("  Predicted diabetes probability:", pred_diabetes * 100, "%")
print("  Predicted conditions:", pred_conditions)
print()

# === CREATE PLOTS ===
print("=" * 50)
print("Creating Plots")
print("=" * 50)

# Create a figure with 3 plots side by side
# figsize=(width, height) in inches
fig, axes = plt.subplots(1, 3, figsize=(15, 5))

# === PLOT 1: Age vs Medication Count ===
# Scatter plot: each point is a patient
axes[0].scatter(ages, medication_counts, alpha=0.5, color='blue')
axes[0].set_xlabel('Age')  # X-axis label
axes[0].set_ylabel('Medication Count')  # Y-axis label
axes[0].set_title('Age vs Medication Count')  # Title
axes[0].grid(True, alpha=0.3)  # Add grid with 30% opacity

# Add the regression line
# Create X values from min age to max age (100 points)
x_line = np.linspace(min(ages), max(ages), 100)
# Calculate predicted Y values using the regression equation
y_line = slope1 * x_line + intercept1
# Plot the line in red with label showing R-squared
axes[0].plot(x_line, y_line, 'r-', label=f'R²={r1**2:.3f}')
axes[0].legend()  # Show the legend

# === PLOT 2: Age vs Diabetes ===
# Scatter plot: each point is a patient (0 = no diabetes, 1 = diabetes)
axes[1].scatter(ages, diabetes, alpha=0.5, color='green')
axes[1].set_xlabel('Age')
axes[1].set_ylabel('Diabetes (0/1)')
axes[1].set_title('Age vs Diabetes')
axes[1].grid(True, alpha=0.3)

# Add the regression line
y_line2 = slope2 * x_line + intercept2
axes[1].plot(x_line, y_line2, 'r-', label=f'R²={r2**2:.3f}')
axes[1].legend()

# === PLOT 3: Age vs Condition Count ===
# Scatter plot: each point is a patient
axes[2].scatter(ages, condition_counts, alpha=0.5, color='purple')
axes[2].set_xlabel('Age')
axes[2].set_ylabel('Condition Count')
axes[2].set_title('Age vs Condition Count')
axes[2].grid(True, alpha=0.3)

# Add the regression line
y_line3 = slope3 * x_line + intercept3
axes[2].plot(x_line, y_line3, 'r-', label=f'R²={r3**2:.3f}')
axes[2].legend()

# Adjust layout so plots don't overlap
plt.tight_layout()

# Save the figure to a file
plt.savefig('regression_plots.png', dpi=150)
print("✅ Plots saved to: regression_plots.png")

# Display the figure on screen
plt.show()

# === SUMMARY ===
# Print a quick summary of all three regressions
print("\nSUMMARY")
print("-" * 40)
print("Age vs Medication Count: R²=", r1**2, "p=", p1)
print("Age vs Diabetes: R²=", r2**2, "p=", p2)
print("Age vs Condition Count: R²=", r3**2, "p=", p3)
print("-" * 40)
print("✅ All done!")