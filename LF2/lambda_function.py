import json
import boto3
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
import time

S3 = "https://b2photoszj.s3.amazonaws.com/"
s3_client = boto3.client('s3')

lex_client = boto3.client('lex-runtime')

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

def es_search(keyword):
    query = {
        'size': 50,
        'query': {
            'multi_match': {
            'query': keyword,
            'fields': ['labels'],
            'fuzziness': 3
            }
        }
    }
    response = es_client.search(body = query, index = 'photo')
    return response

def post_lex(text):
    response = lex_client.post_text(
        botName="photo_keyword_key",
        botAlias="alpha",
        userId=str(time.time()),
        inputText=text)
    return response
    
def parse_keywords(response):
    res = []
    slots = response["slots"]
    for key in slots:
        if slots[key] is not None:
            res.append(slots[key])
    return res
    
def search_keywords(keywords):
    image_paths = []
    for keyword in keywords:
        es_response = es_search(keyword)
        for hit in es_response["hits"]["hits"]:
            objectKey = hit["_source"]["objectKey"]
            image_paths.append(S3 + objectKey)
    return image_paths

def lambda_handler(event, context):
    print("Event: ", event["params"]["querystring"]["q"])
    text = event["params"]["querystring"]["q"]
    lex_response = post_lex(text)
    keywords = parse_keywords(lex_response)
    print("Key Words: ", keywords)
    
    image_paths = search_keywords(keywords)
    
    return {
        'statusCode': 200,
        'extrated keywords: ': keywords,
        'body': {
            "image_paths": image_paths
        }
    }
