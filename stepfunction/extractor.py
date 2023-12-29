import json
import os
import boto3
import requests
from botocore.exceptions import ClientError

def get_secret():  #get token from aws secret
    secret_name = "GraphQL_token"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(get_secret_value_response['SecretString'])
    secret['Authorization']
    return secret['Authorization']

def upload_s3(bucket_name,key,data):        #upload value to S3
    s3_client = boto3.client("s3")
    
    concat_strings = lambda str1, str2: ''.join([str1, str2])
    
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=key)
        print('exists in S3 bucket')
    
        add = s3_client.get_object(Bucket=bucket_name,Key=key)
        exist_data = add['Body'].read().decode('utf-8')
        new_data = concat_strings(exist_data,data)
        s3_client.put_object(Bucket=bucket_name,Key=key,Body=new_data)
        
    except Exception as e:
        print('does not exist in S3 bucket')
    
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)


def query_graphql(query,token):         #return value after query (GraphQl) 
    
    base_url = 'https://api.github.com/graphql'
    headers = {
        'Authorization': f'{token}'
    }
    response = requests.post(base_url,json= {'query':query,},headers =headers)
    return response.json()
    
    
def lambda_handler(event, context):
  
    query = """
                query {
                    organization(login: "_org_") {
                        repositories(first:100 _next_){
                            pageInfo{
                                startCursor
                                endCursor
                                hasNextPage
                            }
                            nodes{
                                id
                                name
                                url
                            }
                        }
                    }
                }"""
    #get token
    token = get_secret()
    queryData = query_graphql(query,token)
   
    #get value org from configuration lambda
    if event["org"]!="":
        query = query.replace('_org_',f'{event["org"]}')
        
    #get value nextCursor from configuration lambda
    if event["next"] == "":
        queryData = query_graphql(query.replace('_next_',''),token)
    else :
        queryData = query_graphql(query.replace('_next_',f'after :"{event["next"]}"'),token)
    
    #check hasNextPage flag
    check = queryData['data']['organization']['repositories']['pageInfo']['hasNextPage']
    
    #assign endCursor
    end = queryData['data']['organization']['repositories']['pageInfo']['endCursor']
     
    #assign "hasNextPage" event
    event["hasNextPage"]=f"{check}"
    
    #store value query
    data_nodes = queryData['data']['organization']['repositories']['nodes']
    
    kq = json.dumps(data_nodes, indent =2)
    #remove "[ ]" in result
    data_out = kq[1:-1]
    
    #store result to "org" file
    temp = event["org"]
    name_file = f'{temp}.json'
    
    #check "hasNextPage" then upload to s3
    if event["hasNextPage"]=="False":
        upload_s3("nhatbusket-test",name_file,data_out)
        event["next"]="" #assign "" to event["next"] 
        return event
    else:
        upload_s3("nhatbusket-test",name_file,data_out)
        event["next"]=f'{end}'   #assign value "endCursor" to event["next"] 
    
    return event
        