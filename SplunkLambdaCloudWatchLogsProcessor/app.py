#################################################################################
# Application code Cloudwatch Logs Lambda subscription filters 

### Data Format
# Raw Payload data (event data) from CW Logs to Lambda
# {'awslogs': 
# {'data': 'H4sIAAAAAAAA/22Qy2rDMBREf0XctWN0JV29diZ1smhLAw7dlFBcVw0Gv7CUhhLy7yWB0Cy6G2ZghjMn6EOM9T5sf6YAHh6KbfH+XFZVsS4
# hg/E4hBk8aGU0V+gIiSCDbtyv5/EwgYfXzXLVjcenm3MNqzSHugcPYWgXvEGnpDaSN40kXodF3XWQQTx8xGZup9SOw6rtUpgj+Df46sZjN+5jCjHB7lpXfochXcITtJ/
# gQRpntFRCaOFICiHROC4tKmVQOUXWWiMEaeUInSYiQi1RasggtX2Iqe4n8Gi4cGi5lZzz7PYDeBDsHpf9y8CQ55RbzFHxi3Y857kiRpKcZUpJpplgKDj7m7mXxXJZbrbs5RHOu/Mvy6qgToUBAAA='}}
# Data after base64 decoded and unpacked (Flow Logs example)
# {
#     "messageType": "DATA_MESSAGE",
#     "owner": "647604195155",
#     "logGroup": "VPCFlowLogGroup",
#     "logStream": "eni-0c19436730cc350ae-all",
#     "subscriptionFilters": [
#         "flowlogstest"
#     ],
#     "logEvents": [
#         {
#             "id": "37976342262953223179038144714945888722564951965555163136",
#             "timestamp": 1702918083000,
#             "message": "2 647604195155 eni-0c19436730cc350ae 10.5.81.140 10.90.0.45 53598 443 6 2 120 1702918083 1702918083 ACCEPT OK"
#         }
#     ]
# }

### Environment Variables
# os.environ['HEC_HOST'] - Splunk Host Endpoint
# os.environ['HEC_TOKEN'] - Splunk HEC Token
# os.environ['ACK_REQUIRED'] - True or False for Acknowledgement check required
# os.environ['ACK_WAIT_SECS'] - Seconds to wait from data ingest to check acknowledgement 3 secs or greater is recommended
# os.environ['ELB_COOKIE_NAME'] - Load Balance cookie Name for Indexers. For AWS CLB set AWSELB as cookie name. For AWS ALB set AWSALB as cookie name
# os.environ['HEC_ENDPOINT_TYPE'] - Specifies if the data sent is raw or event data
# os.environ['SOURCE_TYPE'] - Source type for Data
# os.environ['VERIFY_SSL'] - True or False to check SSL connection for requests
# os.environ['REQUEST_TIMEOUT'] - Seconds as timeout parameter for ingest before getting Http return code
# os.environ['ACK_RETRIES'] - Number of retries to check acknowledgement
# os.environ['DEBUG_DATA'] - True or False to display debug data in Lambda execution output. Useful for debuging acknowledgement issues.




import io
import gzip
import json
import boto3
import requests
import os
import urllib3
import time
import uuid
import base64
from io import BytesIO

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
session = boto3.session.Session()
# Set Splunk url's and auth headers
url=os.environ['HEC_HOST']+'/services/collector'
authHeader = {}
authHeader['Authorization'] = 'Splunk '+ os.environ['HEC_TOKEN']
cookie_name = os.environ['ELB_COOKIE_NAME']

# Convert integer and boolean env variables
request_timeout=int(os.environ['REQUEST_TIMEOUT'])
ack_wait_secs=int(os.environ['ACK_WAIT_SECS'])
if os.environ['VERIFY_SSL'].lower() == "true":
    verify_ssl = True
else:
    verify_ssl = False    

# Transform log event function to extract the CW Logs message
def transformLogEvent(log_event):
    """Transform each log event.

    The default implementation below just extracts the message and appends a newline to it.

    Args:
    log_event (dict): The original log event. Structure is {"id": str, "timestamp": long, "message": str}

    Returns:
    str: The transformed log event.
    """
    return log_event['message'] + '\n'

# Process log records function to base64 decode and decompress
def processRecords(records):
    data = base64.b64decode(records['data'])
    striodata = BytesIO(data)
    with gzip.GzipFile(fileobj=striodata, mode='r') as f:
        data = json.loads(f.read())
    
    if data['messageType'] == 'DATA_MESSAGE':
        joinedData = ''.join([transformLogEvent(e) for e in data['logEvents']])
        return joinedData
    else:
        return False

# Splunk Acknowledgement function if acknowledgement is enabled with retries
def splunk_ack(url,channel,ack_json,cookies):
    ack_url = url+'/ack?channel='+channel
    retries = 0
    ack_retries = int(os.environ['ACK_RETRIES'])
    print("Ack Json",ack_json)
    while retries <= ack_retries:
        try:
            a = requests.post(ack_url, headers=authHeader, json=ack_json, verify=verify_ssl, cookies=cookies, timeout=request_timeout)
        except requests.exceptions.RequestException as e:
            return "Connection Error"
        if a.status_code != 200:
            retries +=1 
            continue
        else:
            a_response = json.loads(a.text)
        if a_response['acks'][str(ack_json['acks'][0])] == True:
            print("Ack Response:",a_response)
            return True
        else:
            retries +=1
    if retries >= ack_retries:
        # Troubleshooting
        if os.environ['DEBUG_DATA'].lower() == "true":
            print("*********************Acknowledgement Debug Data*********************")            
            print("Ack Retries:",ack_retries)                
            print("Retry count:",retries)
            print("Ack HTTP Status Code:",a.status_code)
            print("Ack Response:",a_response)
            print("********************************************************************") 
        return False


def lambda_handler(event, context):
    records = processRecords(event['awslogs'])
    channel = str(uuid.uuid1())
    if os.environ['HEC_ENDPOINT_TYPE'] == 'raw':
        raw_url=url+'/raw?channel='+channel+'&sourcetype='+os.environ['SOURCE_TYPE']
        ## For troubleshooting
        if os.environ['DEBUG_DATA'].lower() == "true":
            print("*********************Request Debug Data*********************")     
            print("Request URL:",raw_url)
            print("Authorization Header:",authHeader)
            print("Ingestion Data")
            print(records) 
            print("************************************************************")         
        if os.environ['ACK_REQUIRED'].lower() == "true":                        
            try:
                r = requests.post(raw_url, headers=authHeader, data=records, verify=verify_ssl, timeout=request_timeout)
            except requests.exceptions.RequestException as e:
                print("Connection Error")
                print("HTTP Response Code:",e)
            if r.status_code == 200:
                response = json.loads(r.text)
                if response['text'] == 'Success':
                    ack_json = {"acks":[response['ackId']]}
                    # Check if ELB Cookies are set
                    if len(cookie_name.strip()) > 0:
                        cookies = {cookie_name: (r.headers['Set-Cookie'].split(";")[0]).split("=")[1]}
                        print("response cookie",cookies)
                    else:
                        cookies = ""                        
                    time.sleep(ack_wait_secs)
                    ack = splunk_ack(url,channel,ack_json,cookies)                                                    
                    if ack:
                        print("Ingestion Success: With Acknowledgement")
                    else:
                        print("Acknowledgement Failed")                                
                if response['text'] != 'Success':
                    print("Ingestion failed")
                    print(response)
            else:
                print("Connection Error")
                print(r)

        else:
            try:
                r = requests.post(raw_url, headers=authHeader, data=records, verify=verify_ssl, timeout=request_timeout)
            except requests.exceptions.RequestException as e:
                print("Connection Error")
                print("HTTP Response Code:",e)
            if r.status_code == 200:
               print("Ingestion Success: Without Acknowledgement")     
            else:
                print("Connection Error")
                print(r)
    if os.environ['HEC_ENDPOINT_TYPE'] == 'event':  
        event_url=url+'/event'
        authHeader['X-Splunk-Request-Channel'] = channel   
        json_records = records.split("\n")
        ingest_data = "" 
        for item in json_records:
            if item:
                ingest_item = '{"sourcetype":"'+os.environ['SOURCE_TYPE']+'","event":'+item+'}\n'
                ingest_data += ingest_item
        ## For troubleshooting
        if os.environ['DEBUG_DATA'].lower() == "true":
            print("*********************Request Debug Data*********************")     
            print("Request URL:",event_url)
            print("Authorization Header:",authHeader)
            print("Ingestion Data")
            print(ingest_data) 
            print("************************************************************")  
        if os.environ['ACK_REQUIRED'].lower() == "true":                       
            try:
                r = requests.post(event_url, headers=authHeader, data=ingest_data, verify=verify_ssl, timeout=request_timeout)
            except requests.exceptions.RequestException as e:
                print("Connection Error")
                print("HTTP Response Code:",e)
            if r.status_code == 200:
                response = json.loads(r.text)
                if response['text'] == 'Success':
                    ack_json = {"acks":[response['ackId']]}
                    # Check if ELB Cookies are set
                    if len(cookie_name.strip()) > 0:
                        cookies = {cookie_name: (r.headers['Set-Cookie'].split(";")[0]).split("=")[1]}
                        print("response cookie",cookies)
                    else:
                        cookies = ""                        
                    time.sleep(ack_wait_secs)
                    ack = splunk_ack(url,channel,ack_json,cookies)                                                    
                    if ack:
                        print("Ingestion Success: With Acknowledgement")
                    else:
                        print("Acknowledge Failed")                                
                if response['text'] != 'Success':
                    print("Ingestion failed")
                    print(response)
            else:
                print("Connection Error")
                print("HTTP Response Code:",r)

        else:
            try:
                r = requests.post(event_url, headers=authHeader, data=ingest_data, verify=verify_ssl, timeout=request_timeout)
            except requests.exceptions.RequestException as e:
                print("Connection Error")
                print("HTTP Response Code:",e)
            if r.status_code == 200:
               print("Ingestion Success: Without Acknowledgement")     
            else:
                print("Connection Error")
                print("HTTP Response Code:",r)



