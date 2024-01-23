#!/usr/bin/env python3

from hdfs import InsecureClient
import os
import uuid
import io
import subprocess
import json
import ast


hdfs_host = 'ec2-34-205-16-134.compute-1.amazonaws.com'
hdfs_port = '9870'  # Default is 50070
index=None
hdfs_path='/user/ubuntu/test'

def init():
    global index
    index=uuid.uuid4()
    os.system(f'hadoop fs -mkdir {hdfs_path}/{index}')

def upload_to_hdfs(data, file_name):
    global index

    # Write the local file content to a temporary file
    temp_file_path = f'/tmp/{file_name}'
    with open(temp_file_path, 'wb') as temp_file:
        with data.file as file:
            temp_file.write(file.read())

    # Construct the HDFS command to copy the file
    os.system(f'hadoop fs -copyFromLocal {temp_file_path} {hdfs_path}/{index}/{file_name}')
    print(f"File uploaded to {hdfs_path}/{index}/{file_name}")
    # Optionally, you can remove the temporary local file
    os.remove(temp_file_path)

    # print(f'Successfully uploaded {file_name} to HDFS at {hdfs_path}

def analyse():
    global index
    os.system(f'''hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -files medication_count_mapper.py,medication_count_reducer.py \
    -mapper medication_count_mapper.py \
    -reducer medication_count_reducer.py \
    -input /user/ubuntu/test/{index}/diabetic_data.csv \
    -output /user/ubuntu/test/{index}/drug_count/result/
    ''')

    os.system(f'hadoop fs -text /user/ubuntu/test/{index}/drug_count/result/* | hadoop fs -put - /user/ubuntu/test/{index}/drug_count/result/drug_count.txt')


    os.system(f'''hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -files gender_age_trend_mapper.py,gender_age_trend_reducer.py \
    -mapper gender_age_trend_mapper.py -input /user/ubuntu/test/{index}/patient_details.csv \
    -input /user/ubuntu/test/{index}/diabetic_data.csv \
    -reducer gender_age_trend_reducer.py -output /user/ubuntu/test/{index}/gender_age/result/
    ''')

    os.system(f'hadoop fs -text /user/ubuntu/test/{index}/gender_age/result/* | hadoop fs -put - /user/ubuntu/test/{index}/gender_age/result/gender_age.txt')


def read_result_files():
    global index
    try:
        print("Reading contents created by MR job....")
        hadoop_command = f'hadoop fs -cat {hdfs_path}/{index}/drug_count/result/drug_count.txt'
        result = subprocess.check_output(hadoop_command, shell=True, text=True)
        file_contents = result.strip() 
        drug_count=ast.literal_eval(file_contents)

        hadoop_command = f'hadoop fs -cat {hdfs_path}/{index}/gender_age/result/gender_age.txt'
        result = subprocess.check_output(hadoop_command, shell=True, text=True)
        file_contents = result.strip() 
        gender_age_trend=ast.literal_eval(file_contents)

        res={"drugs":drug_count,
            "gender_age":gender_age_trend}
        return res

    except subprocess.CalledProcessError as e:
        print(f'Error: {e}')
        
