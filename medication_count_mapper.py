#!/usr/bin/python3

import sys
import csv


medications = [
    "metformin", "repaglinide", "nateglinide", "chlorpropamide", "glimepiride", "acetohexamide", "glipizide", "glyburide", "tolbutamide", "pioglitazone", 
    "rosiglitazone", "acarbose", "miglitol", "troglitazone", "tolazamide", "examide", "citoglipton", "insulin", 
    "glyburide-metformin", "glipizide-metformin", "glimepiride-pioglitazone", "metformin-rosiglitazone", "metformin-pioglitazone"]
def data_cleanup_mapper():
    # Skip the header line
    header_line = next(sys.stdin)

    for line in sys.stdin:
        # Parse the CSV line
        medication_values = line.strip().split(",")[3:len(medications)]
        
        
        # Extract relevant columns and emit them
        for i in range(len(medication_values)):
            count=0
            if medication_values[i] !='No':
                count = 1  # Set count to 1 if the medication is "Steady" or "Yes"

            print(f"{medications[i]}\t{count}")

if __name__ == "__main__":
    data_cleanup_mapper()
