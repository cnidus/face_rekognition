import boto3
import json
import os
import sys
import time
import random
import math
from datetime import datetime
from Queue import Queue
from threading import Thread
from PIL import Image, ImageDraw
from StringIO import StringIO


CONCURRENT_THREADS = 1


def lambda_handler(event, context):

    print("Received event:")
    print(json.dumps(event))
    sns_msg = json.loads(event['Records'][0]['Sns']['Message'])

    rekognition = boto3.client('rekognition', region_name=os.environ['AWS_REGION'])
    s3 = boto3.client('s3', region_name=os.environ['AWS_REGION'])

    celebs = {}

    # Create a new collection in Amazon Rekognition. I use the ID of the Elastic
    # Transcoder job for the name of the collection.
    try:
        collectionId = sns_msg['jobId']
        try:
            rekognition.delete_collection(CollectionId=collectionId)
        except:
            pass
        rekognition.create_collection(CollectionId=collectionId)
        print('Collection {} created in Amazon Rekognition'.format(collectionId))

    except Exception as e:
        print('Failed to create the collection in Amazon Rekognition')
        print(e)
        raise(e)


    # Retrieve the list of thumbnail objects in the S3 bucket that were created
    # by Amazon Elastic Transcoder. The list of keys is stored in the local
    # variable 'thumbnailKeys'.
    try:
        thumbnailKeys = []
        prefix = sns_msg['outputKeyPrefix']
        prefix += sns_msg['outputs'][0]['thumbnailPattern'].replace('{count}', '')

        paginator = s3.get_paginator('list_objects')
        response_iterator = paginator.paginate(
            Bucket=os.environ['Bucket'],
            Prefix=prefix
        )
        for page in response_iterator:
            thumbnailKeys += [i['Key'] for i in page['Contents']]

        print('Number of thumbnail objects found in the S3 bucket: {}'.format(len(thumbnailKeys)))

    except Exception as e:
        print('Failed to list the thumbnail objects')
        print(e)
        raise(e)

    #Create the findCelebsQueue and find_celebs_workers
    findCelebsQueue = Queue()

    def find_celebs_worker():
        rekognition = boto3.client('rekognition', region_name=os.environ['AWS_REGION'])

        while True:
            key = findCelebsQueue.get()
            try:
                startTime = datetime.now()
                frameNumber = int(key[:-4][-5:])

                response = rekognition.recognize_celebrities(
                    #CollectionId=collectionId,
                    Image={'S3Object': {
                        'Bucket': os.environ['Bucket'],
                        'Name': key
                    }},
                    #ExternalImageId=str(frameNumber)
                )

                Celebrities = response['CelebrityFaces']
                if Celebrities:
                    for celeb in Celebrities:
                        celebId = celeb['Id']
                        print("celeb: " + celebId + " in frame: " + str(frameNumber) + " MatchConfidence: " + str(celeb['MatchConfidence']))
                        if celeb['MatchConfidence'] >= 0.65:
                            print("Celeb: " + json.dumps(celeb))
                            #Create the Celeb top level entry
                            if celebId in celebs:
                                print("New Celeb detected in frame " + str(frameNumber) + " with Confidence of " + str(celeb['MatchConfidence']))
                                celebs[celebId] = {
                                    'Name': celeb['Name'],
                                    'Urls': celeb['Urls'],
                                    'Faces': {}
                                }
                            #Transform the detected face object
                            celebFace = {
                                    'FrameNumber': frameNumber,
                                    'MatchConfidence': celeb['MatchConfidence'],
                                    'BoundingBox': celeb['Face']['BoundingBox'],
                                    'Confidence': celeb['Face']['Confidence']
                            }
#                            print(celebFace)
                            #Add the detected face to the 'celebs' array.
                            try:
                                celebs[celebId]['Faces'].append(celebFace)
                            except:
                                print("Failed to append face: " + json.dumps(celebFace))

                endTime = datetime.now()
                delta = int((endTime - startTime).total_seconds() * 1000)
                if delta < 1000:
                    timeToWait = float(1000 - delta)/1000
                    time.sleep(timeToWait)

                print("find_celebs_worker " + key + " completed successfully")

            # I put the key back in the queue if the IndexFaces operation
            # failed
            except Exception as e:
                print('Exception: ' + key)
                print(e)
#                findCelebsQueue.put(key)

            findCelebsQueue.task_done()

    for i in range(CONCURRENT_THREADS):
        t = Thread(target=find_celebs_worker)
        t.daemon = True
        t.start()

    for key in thumbnailKeys:
        findCelebsQueue.put(key)

    findCelebsQueue.join()
    time.sleep(2)
    print('FindCelebs operation completed')
    print(json.dumps(celebs))
    celeb_json = {}
    celeb_json = {'Celebrities': celebs}

    # Upload the JSON result into the S3 bucket
    try:
        s3.put_object(
            Body=json.dumps(celeb_json, indent=4).encode(),
            Bucket=os.environ['Bucket'],
            Key=sns_msg['outputKeyPrefix'].replace('elastictranscoder/', 'output/celeb_')[:-1] + '.json'
        )
        print('JSON result uploaded into the S3 bucket')

    except Exception as e:
        print('Failed to upload the JSON result into the S3 bucket')
        print(e)
        raise(e)


    # Create a visual representation
    # Currently removed, add back in later

    # Delete the collection in Amazon Rekognition.
    try:
        rekognition.delete_collection(CollectionId=collectionId)
        print('Collection deleted from Amazon Rekognition'.format(collectionId))

    except Exception as e:
        print('Failed to delete the collection in Amazon Rekognition')
        print(e)
        raise(e)
