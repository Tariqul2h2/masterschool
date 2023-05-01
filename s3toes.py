import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import configparser

# Set up the S3 client
s3 = boto3.client('s3')

config = configparser.ConfigParser()
config.read('~/.aws/config')
access_key = config.get('default', 'aws_access_key_id')
secret_key = config.get('default', 'aws_secret_access_key')
region = config.get('default', 'region')

service = 'es'
awsauth = AWS4Auth(access_key, secret_key, region, service)

# Set up the Elasticsearch client
host = 'localost'
es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

bucket = 'backup_server_log'
key = 'my-object-key'

response = s3.get_object(Bucket=bucket, Key=key)
data = response['Body'].read().decode('utf-8')

# Transforming
# Split the data into individual documents
documents = data.split('\n')

# Push each document into Elasticsearch with pagination
page_size = 1000
for i in range(0, len(documents), page_size):
    page_documents = documents[i:i+page_size]
    actions = []
    for document in page_documents:
        if document:
            parsed_document = {'mydoc': document}

            # Create an Elasticsearch "index" action for the document
            actions.append({'index': {'_index': 'server_log', '_type': '_doc'}})
            actions.append(parsed_document)

    # Push the documents into Elasticsearch
    es.bulk(index='server_log', body=actions, refresh=True)
