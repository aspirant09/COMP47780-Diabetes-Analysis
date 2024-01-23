from hdfs import InsecureClient
import os
import uuid
import io
import sst


hdfs_host = 'ec2-34-205-16-134.compute-1.amazonaws.com'
hdfs_port = '9870'  # Default is 50070
index=None
hdfs_path='/user/ubuntu/test/'

def init():
    index=uuid.uuid4()
def upload_to_hdfs(data, file_name):
    
    # Create an HDFS client
    hdfs_client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user='ubuntu')

    # Upload the local file to HDFS
    
    # Upload the data to HDFS
    with data.file as file:
        # Read the file content into bytes
        file_content = io.BytesIO(file.read())
        hdfs_client.write(f'{hdfs_path}/{index}/{file_name}', file_content)

   # print(f'Successfully uploaded {local_path} to HDFS at {hdfs_path}')


def analyse():
    os.system(f'''hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -files medication_count_mapper.py,medication_count_reducer.py \
    -mapper nedication_count_mapper.py \
    -reducer medication_count_reducer.py \
    -input /user/ubuntu/test/{index}/diabetic_data.csv \
    -output /user/ubuntu/test/{index}/drug_count/result/
    ''')

    os.system(f'hadoop fs -text /user/ubuntu/test/{index}/drug_count/result/* | hadoop fs -put - /user/ubuntu/test/{index}/drug_count/result/test_result.csv')


    os.system(f'''hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -files gender_age_trend_mapper.py_age_trend_reducer.py \
    -mapper gender_age_trend_mapper.py -input /user/ubuntu/test/{index}/patient_details.csv \
    -input /user/ubuntu/test/{index}/diabetic_data.csv \
    -reducer gender_age_trend_reducer.py -output /user/ubuntu/test/{index}/gender_age/result/
    ''')

    os.system(f'hadoop fs -text /user/ubuntu/test/{index}/gender_age/result/* | hadoop fs -put - /user/ubuntu/test/{index}/result/gender_age/patient_gender.txt')


def read_result_file(file_path):
    # Create an HDFS client
    hdfs_client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user='ubuntu')

    # Read the content of the file
    with hdfs_client.read(f'{hdfs_path}/{index}/{file_path}') as reader:
        content = reader.read()
    return content.decode("utf-8")
    
