import os
import shutil
import logging
import time
import script_util as util
from stage1 import extractZip,formatData


def runCF(config, zip_file_path=None):
    ########################################################################################
    ### (1) Load configuration                                                             #
    stage1_input_dir = config["directories"]["input"]
    stage1_work_dir = util.ensure_dir_exists(config["directories"]["work"])
    stage2_input_dir = util.ensure_dir_exists(config["directories"]["output"])
    output_dir = None
    exist = os.path.exists(zip_file_path)

    if zip_file_path is None:
        logging.info("Tets de CF2")
        for filename in os.listdir(stage1_input_dir):
            # Only process the first file we find
            if filename.endswith(".zip"):
                zip_file_path = os.path.join(stage1_input_dir, filename)
                break

    # Current timestamp as milliseconds
    timestamp = str(int(time.time() * 1000))

    # e.g. stage1/.working/126357624673
    stage1_batch_work_dir = util.ensure_dir_exists(os.path.join(stage1_work_dir, timestamp))

    try:
        ### (2) Move zip file to working directory #############################################
        logging.info(f"Processing file: {zip_file_path}")
        logging.info(f"Stage1 Temp: {stage1_batch_work_dir}")
        # Create new temp/work directory - e.g. stage1/.working/126357624673/tmp
        stage2_tmp_dir = os.path.join(stage1_batch_work_dir, "tmp")
        logging.info(f"Stage2 Temp: {stage2_tmp_dir}")
        os.makedirs(stage2_tmp_dir)

        # Move the zip file to a working directory
        zip_file_path = shutil.move(zip_file_path, stage2_tmp_dir)
        logging.info(f"Working temp directory: {stage2_tmp_dir}")
        logging.info(f"Zip File Path: {zip_file_path}")
        
        ### (3) Applying Stage 1 logic to zip file #############################################
        extractZip(config, zip_file_path, stage2_tmp_dir, stage1_batch_work_dir)    
        
        ### (4) Cleaning up and moving resulting CSV directory to stage2/input #################
        #shutil.rmtree(stage2_tmp_dir)

        stage2_csv_dir = os.path.join(stage1_batch_work_dir, f"csv-{timestamp}")

        # Create new directory to move to stage 2 input
        os.makedirs(stage2_csv_dir)

        # Move any CSV file to stage2_csv_dir
        """
        for path in os.listdir(stage1_batch_work_dir):
            full_path = os.path.join(stage1_batch_work_dir, path)

            if re.search(r"\.csv\Z", path, re.IGNORECASE):
                shutil.move(full_path, stage2_csv_dir)

        # Move the batch-specific csv output directory to stage2_input_dir
        output_dir = shutil.move(stage2_csv_dir, stage2_input_dir)
        """
        output_dir = stage2_tmp_dir
    finally:
        # Delete current batch working directory
        logging.info("Test here")
        #shutil.rmtree(stage1_batch_work_dir)
    #                                                                                      #
    ########################################################################################
    
    return output_dir
    
def createSonocoFiles(config,workDirectory):

    currentOrder = config["directories"]["output"]
    timestamp = str(int(time.time() * 1000))

    final_directory = os.path.join(currentOrder,timestamp)
    if not os.path.exists(final_directory): os.makedirs(final_directory)

    try:
        logging.info("Calling create CurrentOrder")
        formatData(workDirectory,final_directory)
    except:
        raise Exception('Was nos posible to create CurrentOrder')

    return final_directory

if( __name__ == "__main__"):
    runCF()
    createSonocoFiles()