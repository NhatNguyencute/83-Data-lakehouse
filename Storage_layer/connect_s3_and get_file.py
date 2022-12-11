import boto3

def connect_s3_and_get_file():
    client = boto3.client(
        's3',
        aws_access_key_id = 'AKIA46SFIWN5AMWMDQVB',
        aws_secret_access_key = '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',
        region_name = 'ap-south-1'
    )
    return client 


