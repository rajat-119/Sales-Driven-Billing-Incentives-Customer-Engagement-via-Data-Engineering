from src.main.utility.logging_config import *
import traceback
import datetime
import os
from datetime import date
class UploadToS3:
    def __init__(self,s3_client):
        self.s3_client = s3_client

    def upload_to_s3(self,s3_directory,s3_bucket,local_file_path):
        # here we are creating an epoch date folder and in that folder we will upload the data into s3,
        #so that we can get to know that on which date the data is upoaded.
        #current_epoch = int(datetime.datetime.now().timestamp()) * 1000

        current_epoch = date.today()
        s3_prefix = f"{s3_directory}/{current_epoch}"
        try:
            for root, dirs, files in os.walk(local_file_path):
                for file in files:
                    local_file_path = os.path.join(root, file)# below "upload_file" internal function of s3 needs full local file path..so for that we are connecting the root with the file name
                    s3_key = f"{s3_prefix}/{file}"
                    logger.info(f"************ s3 customer dada mart file location is {s3_key} **************")
                    self.s3_client.upload_file(local_file_path, s3_bucket, s3_key)
            return f"Data Successfully uploaded in {s3_directory} data mart "
        except Exception as e:
            logger.error(f"Error uploading file : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e