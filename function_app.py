import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import os
import yaml
import re
from script_util import convertDict
from cf1 import runCF,createSonocoFiles
import shutil
import time
import json

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="stage1/input/{name}",
                               connection="AzureWebJobsStorage") 
def extractSonocoData(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    connection_string = os.environ['AzureWebJobsStorage']

    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage1.yaml"), "r") as f:
        stage1_config = yaml.load(f, Loader=yaml.FullLoader)

    stage1_input_dir = stage1_config["directories"]["input"]
    stage1_output_dir = stage1_config["directories"]["output"]

    stage1_bucket_input_folder = stage1_config["bucket-directories"]["input"]
    stage1_bucket_errors_folder = stage1_config["bucket-directories"]["errors"]

    ### (2) Apply trigger filter (based on file name/path & extension) #####################
    # File name pattern: stage1/input/*.zip
    if not re.search(r"\A" + stage1_bucket_input_folder + r"/.*\.zip\Z", myblob.name, re.IGNORECASE):
        return
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1")
    blop_name = myblob.name[(myblob.name.find('input/')):]
    blob_client = input_container.get_blob_client(blop_name)

    ### (3) Set up /tmp/stage1 tree structure ##############################################
    logging.info(f"Processing file: {blop_name}")

    if not os.path.exists(stage1_input_dir): os.makedirs(stage1_input_dir)
    if not os.path.exists(stage1_output_dir): os.makedirs(stage1_output_dir)

    zip_file_on_tmp = os.path.join("/tmp/", myblob.name)

    logging.info(f"zip Path: {zip_file_on_tmp}")
    #Downloading Zip to local system
    with open(file=zip_file_on_tmp, mode="wb") as sample_blob:
        download_stream = blob_client.download_blob()
        sample_blob.write(download_stream.readall())
    try:
        logging.info(f"############ START: Stage 1 for zip file: {zip_file_on_tmp} ############")
        output_dir = runCF(stage1_config, zip_file_on_tmp)
        data = {}
        data['working_directory'] = output_dir
        json_data = json.dumps(data)
        container_client_upload = blob_service_client.get_container_client(container="stage1/processData")
        blob = container_client_upload.get_blob_client("processData.trigger.txt")
        blob.upload_blob(json_data)
        logging.info(f"############ END: Stage 1 for zip file: {zip_file_on_tmp} ############")
    except:
        logging.info(f"############ ERROR: Stage 1 for zip file: {zip_file_on_tmp} ############")
    # Delete stage 1 zip file
    blob = input_container.get_blob_client(blop_name)     

    if blob.exists():
        logging.debug(f"Deleting bucket file: {blop_name}")
        blob.delete_blob()

@app.blob_trigger(arg_name="myblob", path="stage1/processData/{name}",
                               connection="AzureWebJobsStorage") 
def processData(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    connection_string = os.environ['AzureWebJobsStorage']

    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage1.yaml"), "r") as f:
        stage1_config = yaml.load(f, Loader=yaml.FullLoader)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1/processData")
    blob = input_container.get_blob_client("processData.trigger.txt")
    data = convertDict(myblob) 

    working_directory = data['working_directory']
    logging.info(f"Merge Directory: {working_directory}")

    output_directory = createSonocoFiles(stage1_config,working_directory)

    if blob.exists():
        logging.debug(f"Deleting bucket file: merge.trigger.txt")
        blob.delete_blob()

    dataf = {}
    dataf['output_directory'] = output_directory
    json_data = json.dumps(dataf)
    container_client_upload = blob_service_client.get_container_client(container="stage1/uploadData")
    blobCurrent = container_client_upload.get_blob_client("uploadData.trigger.txt")
    blobCurrent.upload_blob(json_data)



@app.blob_trigger(arg_name="myblob", path="stage1/uploadData/{name}",
                               connection="AzureWebJobsStorage")

def uploadSonocoData(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    connection_string = os.environ['AzureWebJobsStorage']
    connection_cf2 = os.environ['CF2StorageAccount']

    blob_cf2_client = BlobServiceClient.from_connection_string(connection_cf2)
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1/uploadData")
    blob = input_container.get_blob_client("uploadData.trigger.txt")
    data = convertDict(myblob)
    output_directory = data['output_directory']
    logging.info(f"Current Order Directory: {output_directory}")

    fileList = os.listdir(output_directory)
    for filename in fileList:
        logging.info(f"FilesList: {filename}")
        container_client_upload = blob_cf2_client.get_container_client(container="stage2/sonoco")
        with open(file=os.path.join(output_directory, filename), mode="rb") as data: 
            blob_client = container_client_upload.upload_blob(name=filename, data=data, overwrite=True)

    if blob.exists():
        logging.debug(f"Deleting bucket file: digital.trigger.txt")
        blob.delete_blob()
    
    blob_client = blob_cf2_client.get_blob_client(container="stage2/sonoco", blob="SendToPrM.trigger.txt")
    input_stream = 'StartPrM'
    blob_client.upload_blob(input_stream, blob_type="BlockBlob")