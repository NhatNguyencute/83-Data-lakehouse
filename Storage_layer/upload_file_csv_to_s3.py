import boto3
from botocore.exceptions import NoCredentialsError
import glob
import os 

def upload_to_log_data_aws():
    BUCKET_NAME = 'aws-s3-data-lake-house'
    FOLDER_NAME = 'log_data/2018/11'
    ACCESS_KEY = 'AKIATMWODXN2OXUJZZZY'
    SECRET_KEY = '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'

    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY)

    json_files = glob.glob("C:/Users/Archer/Desktop/Data_warehouse_aws/source data/log_data/2018/11/*.json")

    for filename in json_files:
        key = "%s/%s" % (FOLDER_NAME, os.path.basename(filename))
        print("Putting %s as %s" % (filename,key))
        s3.upload_file(filename, BUCKET_NAME, key)
        print("Upload Successful")



def upload_to_song_data_aws():
    BUCKET_NAME = 'aws-s3-data-lake-house'
    FOLDER_NAME_A = 'song_data/A/A'
    FOLDER_NAME_B = 'song_data/A/B'
    ACCESS_KEY = 'AKIATMWODXN2OXUJZZZY'
    SECRET_KEY = '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'

    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY)

    json_files_A = glob.glob("C:/Users/Archer/Desktop/Data_warehouse_aws/source data/song_data/A/*.json")

    for filenameA in json_files_A:
        key = "%s/%s" % (FOLDER_NAME_A, os.path.basename(filenameA))
        print("Putting %s as %s" % (filenameA,key))
        s3.upload_file(filenameA, BUCKET_NAME, key)
        print("Upload Successful")
    

    json_files_B = glob.glob("C:/Users/Archer/Desktop/Data_warehouse_aws/source data/song_data/B/*.json")

    for filenameB in json_files_B:
        key = "%s/%s" % (FOLDER_NAME_B, os.path.basename(filenameB))
        print("Putting %s as %s" % (filenameB,key))
        s3.upload_file(filenameB, BUCKET_NAME, key)
        print("Upload Successful")
    




