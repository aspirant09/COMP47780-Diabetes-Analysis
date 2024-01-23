#!/usr/bin/env python3

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.datastructures import UploadFile
from fastapi.params import Body, File
from fastapi.responses import JSONResponse
import job_runner_aws

app = FastAPI()



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/uploadFiles")
def upload(
    patient_data: UploadFile = File(...),
    medical_data: UploadFile = File(...)
):
    try:
        print(patient_data)
        job_runner_aws.init()
        job_runner_aws.upload_to_hdfs(patient_data,'patient_details.csv')
        job_runner_aws.upload_to_hdfs(medical_data,'diabetic_data.csv')
        print("Start MR job..")
        job_runner_aws.analyse()
        result=job_runner_aws.read_result_files()
        return JSONResponse(content=result)
        
    except Exception as e:
        raise BaseException()
    #print("Succesfully uploaded!!!")

