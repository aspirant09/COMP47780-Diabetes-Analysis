#!/usr/bin/python3
import os
import uuid


index=uuid.uuid4()

os.system(f'''hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -files mapper.py,reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /user/ubuntu/test/diabetic_data.csv \
    -output /user/ubuntu/test/{index}/result/
''')

os.system(f'hadoop fs -text /user/ubuntu/test/{index}/result/* | hadoop fs -put - /user/ubuntu/test/{index}/result/test_result.csv')
