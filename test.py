from hdfs import InsecureClient
import os

def upload_to_hdfs(local_path, hdfs_path, hdfs_host, hdfs_port):
    # Create an HDFS client
    hdfs_client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user='ubuntu')

    # Upload the local file to HDFS
    with open(local_path, 'rb') as local_file:
        hdfs_client.write(hdfs_path, local_file)

    print(f'Successfully uploaded {local_path} to HDFS at {hdfs_path}')

if __name__ == "__main__":
    current_directory = os.getcwd()
    print("Current Directory:", current_directory)
    # Specify your local file path, HDFS destination path, HDFS host, and HDFS port
    local_file_path = file_path = os.path.join(current_directory, 'data', 'diabetic_data.csv')
    hdfs_destination_path = '/user/ubuntu/test/diabetic_data.csv'
    hdfs_host = 'ec2-34-205-16-134.compute-1.amazonaws.com'
    hdfs_port = '9870'  # Default is 50070

    upload_to_hdfs(local_file_path, hdfs_destination_path, hdfs_host, hdfs_port)
