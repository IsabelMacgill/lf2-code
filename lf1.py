import json
import logging
import boto3
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import random 

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
rek_client = boto3.client('rekognition')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    logger.debug(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    photo_name = event['Records'][0]['s3']['object']['key']
    logger.debug(photo_name)


    #Analyze the image.
    response = rek_client.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': photo_name,
            },
        }
    )

    logger.debug(response)
    rekognition_labels = response['Labels']
    logger.debug(rekognition_labels)
    
    s3_response = s3_client.head_object(
        Bucket=bucket,
        Key=photo_name,
    )
    logger.debug(s3_response)
    metadata = s3_response['Metadata']
    logger.debug(metadata)
    last_modified = s3_response['LastModified']
    date_string = last_modified.strftime("%Y-%m-%d %H:%M:%S")
    
    json_labels = []
    if 'customlabels' in metadata:
        input_labels = metadata['customlabels']
        input_labels = input_labels.split(',')
        for label in input_labels:
            json_labels.append(label)
    
    for label in rekognition_labels:
        json_labels.append(label['Name'])
        
    openSearch_json = {
        'objectKey': photo_name,
        'bucket': bucket,
        'createdTimestamp': date_string,
        'labels': json_labels
    }

    host = 'search-photos-ir3dxedsasv6ddlx7gj7efuqn4.us-east-1.es.amazonaws.com' 
    region = 'us-east-1' 
    
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    
    open_search = OpenSearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    
    # index_name = 'photo-labels'
    # index_body = {
    #   'settings': {
    #     'index': {
    #       'number_of_shards': 1
    #     }
    #   }
    # }
    # response = open_search.indices.create(index_name, body=index_body)
    
    logger.debug(openSearch_json)
    index_response = open_search.index(index = 'photo-labels', body = openSearch_json, id = photo_name, refresh = True)
    print(index_response)

  
    lambda_response = {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        },
        "body": openSearch_json
    }
    
    
    # response = open_search.indices.delete(
    #     index = 'photo-labels'
    # )
    # logger.debug(response)
    
    return lambda_response

