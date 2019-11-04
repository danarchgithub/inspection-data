import json
import boto3
import time

def lambda_handler(event, context):
    client = boto3.client('athena')
    output='s3://dan-data-for-exercise/athena/'
    
    keyword = event['keyword']
    value = event['value']
    database = event['database']
    table = event['table']
    
    query = "SELECT DBA, CASE WHEN DBA like %s THEN %s ELSE NULL END as prediction FROM %s WHERE DBA like %s;" % (keyword, value, table, keyword)

    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output,
        }
    )
    
    return
