import boto3
import json
import urllib
import os
from datetime import datetime

def lambda_handler(event, context):

    print("Received event")
    print(json.dumps(event))

    # Retrieve the key for the uploaded S3 object that caused this function
    # to be triggered
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    filename = key.split('/')[-1]

    try:
        # Elastic Transcoder prepends 'elastictranscoder/[filename]/[timestamp]-'
        # to the names of all files that the job creates
        timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')

        client = boto3.client('elastictranscoder')
        response = client.create_job(
            PipelineId=os.environ['PipelineId'],
            Input={'Key': key},
            OutputKeyPrefix='elastictranscoder/{}/{}_'.format(filename, timestamp),
            Output={
                'Key': 'transcoded-video.mp4',
                'ThumbnailPattern': 'thumbnail-{count}',
                'PresetId': os.environ['PresetId']
            }
        )

        print('New Elastic Transcoder job created: {}'.format(response['Job']['Id']))

    except Exception as e:
        print('Unable to create a new Elastic Transcoder job')
        print(e)
        raise(e)
