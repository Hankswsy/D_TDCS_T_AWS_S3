import requests
import os
import csv
import time
import pandas as pd
import random
import boto3
from botocore.client import ClientError

def cheakbuket(bucket):
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)

    response = s3.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'{bucket["Name"]}')


def tos3(BUCKET_NAME, day, filename):
    s3_client = boto3.client('s3')
    with open(filename, "rb") as f:
        s3_client.upload_fileobj(f, BUCKET_NAME, "12"+str(day)+"/"+filename)


BUCKET_NAME = 'tdcs-m06a-2022-12'
year = 2022
month = 12
daystart = 1
dayend = 8
start_hour = 0
end_hour = 23

for i in range(daystart,dayend+1):
    if i < 10:
        i = "0"+str(i)
        print("0"+str(i))
    else:
        i = str(i)
        print (str(i))

    for Hour in range(start_hour, end_hour+1):
        if Hour < 10:
            HourStr = "0"+str(Hour)
            print("0"+str(Hour))
        else:
            HourStr = str(Hour)
            print (str(Hour))
        
        OneDate = str(year)+str(month)+str(i)

        OneDate_OneHour_FileName = "TDCS_M06A_"+OneDate+"_"+HourStr+"0000.csv"

        TDCS_M06A = "https://tisvcloud.freeway.gov.tw/history/TDCS/M06A/"

        url = TDCS_M06A +OneDate+'/'+HourStr+'/'+ OneDate_OneHour_FileName
        FieldNames= ['VehicleType', 'DetectionTime_O', 'GantryID_O', 'DetectionTime_D','GantryID_D','TripLength','TripEnd','TripInformation'] 
        
        #csv_data = pd.read_csv(url,encoding='utf-8', header=None)
        csv_data = pd.read_csv(url,encoding='utf-8', header=None, names=FieldNames)
        
        OutputFileLocation = OneDate_OneHour_FileName

        csv_data.to_csv(OutputFileLocation)
        print("Download=>"+OutputFileLocation)
        
        cheakbuket(BUCKET_NAME)
        tos3(BUCKET_NAME, i, OutputFileLocation)
        
        try:
            os.remove(OutputFileLocation)
        except OSError as e:
            print(e)
        else:
            print("File is deleted successfully")
        
        x = random.randint(5, 30)
        print("wait time:"+str(x))
        time.sleep(x)
        