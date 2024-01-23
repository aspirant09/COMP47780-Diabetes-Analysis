#!/usr/bin/python3

import sys

result={"Male":{},"Female":{}}


medications = [
    "metformin", "repaglinide", "nateglinide", "chlorpropamide", "glimepiride", "acetohexamide", "glipizide", "glyburide", "tolbutamide", "pioglitazone", 
    "rosiglitazone", "acarbose", "miglitol", "troglitazone", "tolazamide", "examide", "citoglipton", "insulin", 
    "glyburide-metformin", "glipizide-metformin", "glimepiride-pioglitazone", "metformin-rosiglitazone", "metformin-pioglitazone"]


def reducer():
    current_key = None
    person_data = None
    medical_data = None

    for line in sys.stdin:
        # Split the line into key, value, and type
        key, value, data_type = line.strip().split('\t')
        #print(key,value,data_type)
        # Check if the key has changed
        if current_key != key:
            # Output the result for the previous key
            if current_key is not None and person_data is not None and medical_data is not None:
                process_data(current_key, person_data, medical_data)

            # Reset for the new key
            current_key = key
            person_data = None
            medical_data = None
        
        # Assign data based on the type
        if data_type == 'person':
            person_data = value
        elif data_type == 'medical':
            medical_data = value

    # Output the result for the last key
    if current_key is not None and person_data is not None and medical_data is not None:
        process_data(current_key, person_data, medical_data)

def process_data(key, person_data, medical_data):
    # Process and combine the data as needed
    # You can customize this part based on your requirements
    gender,age=person_data.split(",")
    #print(key,gender,age)
    if gender not in result:
        result[gender] = {}
    
    if age not in result[gender]:
        result[gender][age] = {}
    
    #gender_data=result.get(gender)[age]
    medication_values=medical_data.split(",")[0:len(medications)]
    for i in range(len(medication_values)):
            if medication_values[i]!='No':
                #print(f'{gender} {age} {medications[i]}')
                try:
                    result[gender][age][medications[i]]+=1
                except KeyError:
                    result[gender][age][medications[i]]=1
    

    #print(f"{key}\t{person_data}\t{medical_data}")

if __name__ == "__main__":
    reducer()
    print(f'{result}')
