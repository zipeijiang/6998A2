import boto3
from decimal import Decimal
import json
import urllib.request
import urllib.parse
import urllib.error
import datetime
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
from botocore.client import Config

print('Loading function')

config = Config(connect_timeout=10, retries={'max_attempts': 1})

s3_client = boto3.client('s3', config=config)
rekognition = boto3.client('rekognition')

region = 'us-east-1' # For example, us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
host = "vpc-photos-7o2qxo3aqef3d2whnkras3icze.us-east-1.es.amazonaws.com"
es_client = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

# --------------- Helper Functions to call Rekognition APIs ------------------


def detect_faces(bucket, key):
    response = rekognition.detect_faces(Image={"S3Object": {"Bucket": bucket, "Name": key}})
    return response


def detect_labels(bucket, key):
    response = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}})
    labels = [label_prediction['Name'] for label_prediction in response['Labels']]
    return labels

def put_index(item):
    es_client.index(index="photo", doc_type="_doc", id=item["objectKey"], body=item)

def es_search(keyword):
    query = {
        'size': 5,
        'query': {
            'multi_match': {
            'query': keyword,
            'fields': ['labels']
            }
        }
    }
    response = es_client.search(body = query, index = 'photo')
    return response

def is_photo(key):
    for subkey in ["jpeg", "png", "jpg"]:
        if subkey in key:
            return True
    return False
    

# --------------- Main handler ------------------
def lambda_handler(event, context):

    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    try:
        # Calls rekognition DetectLabels API to detect labels in S3 object
        if is_photo(key):
            print("Detect Photo upload: ", key)
            detected = ",".join(detect_labels(bucket, key))
            headers = s3_client.head_object(Bucket=bucket, Key=key)
            if "customlabels" in headers["Metadata"]:
                customed = headers["Metadata"]["customlabels"]
                labels = detected + "," + customed
            else:
                labels = detected

            labels = labels.lower()
            print("Labels: ", labels)
            print("Image Header: ", headers)

            item = {
                "objectKey": key,
                "bucket": bucket,
                "createdTimestamp": str(datetime.datetime.now()),
                "labels": labels
            }
            print("Item: ", item)

            put_index(item)
            print("img indexing succeed")
            # print(es_client.get(index="photo", doc_type="_doc", id=key))
            # res = es_search("Dog")
            # print("search 'Dog': ", res)
        else:
            print("Non-photo upload: ", key)

        return {}
        
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
