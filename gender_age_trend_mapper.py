#!/usr/bin/python3

import csv
import sys

medical_record = [
    "metformin", "repaglinide", "nateglinide",
    "chlorpropamide", "glimepiride", "acetohexamide", "glipizide", "glyburide", "tolbutamide",
    "pioglitazone", "rosiglitazone", "acarbose", "miglitol", "troglitazone", "tolazamide",
    "examide", "citoglipton", "insulin", "glyburide-metformin", "glipizide-metformin",
    "glimepiride-pioglitazone", "metformin-rosiglitazone", "metformin-pioglitazone"
]



patient_details_column=["encounter_id","patient_nbr","race","gender","age","weight"]


def patient_data_mapper():
    # Input comes from the patients data file
    for line in sys.stdin:
        patient_data = line.strip().split(',')
        
        # Assuming gender is the third column and age is the fifth column
        gender = patient_data[3]
        age = patient_data[4][1:-1]
        number =patient_data[0]
        
        # Emit composite key: gender_age, value: patient data
        
        gender_age=gender+","+age
        print(f"{number}\t{gender_age}\tperson")



def medical_data_mapper():


    
    
    for line in sys.stdin:
        record=line.strip().split(",")
        #record = next(csv.reader([line]))[4:len(medical_record)]
        # Ensure the record has the expected number of fields
        

        # Emit key-value pair to standard output
        key = record[0]
        value = ",".join(record[3:len(medical_record)])  # Join the remaining items as a comma-separated string
        print(f"{key}\t{value}\tmedical")




if __name__=='__main__':
    try:
        line=next(sys.stdin)
        line=line.strip().split(",")
        if len(line)<8:
            patient_data_mapper()
        else:
            medical_data_mapper()
    except:
        pass
    
    