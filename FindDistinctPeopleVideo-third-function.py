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


CONCURRENT_THREADS = 50


def lambda_handler(event, context):

    print("Received event:")
    print(json.dumps(event))
    sns_msg = json.loads(event['Records'][0]['Sns']['Message'])

    rekognition = boto3.client('rekognition', region_name=os.environ['AWS_REGION'])
    s3 = boto3.client('s3', region_name=os.environ['AWS_REGION'])

    faces = {}
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

    #Find celebrities worker
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

                for celeb in response['CelebrityFaces']:
                    celebId = celeb['Id']
                    celebs[celebId] = {
                        'FrameNumber': frameNumber,
                        'BoundingBox': celeb['Face']['BoundingBox']
                        'Name': celeb['Name']
                        'URLs': celeb['URLs']
                        'MatchConfidence': celeb['MatchConfidence']
                    }

                endTime = datetime.now()
                delta = int((endTime - startTime).total_seconds() * 1000)
                if delta < 1000:
                    timeToWait = float(1000 - delta)/1000
                    time.sleep(timeToWait)

            # I put the key back in the queue if the IndexFaces operation
            # failed
            except:
                findCelebsQueue.put(key)

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


    # Celebrity sorting etc
    # Sort the list of Celebrity IDs in the order of which they appear in the video.
#    def getKey(item):
#        return item[1]
#    celebsFrameNumber = {k: v['FrameNumber'] for k, v in celebs.items()}
#    celebIdsSorted = [i[0] for i in sorted(celebsFrameNumber.items(), key=getKey)]

#Trying to refactor this part....############
#    def propagate_celeb_id(celebId):
#        for matchingId in celebs[celebId]['MatchingFaces']:
#            if not 'celebId' in celebs[matchingId]:
#
#                numberMatchingLoops = 0
#                for matchingId2 in faces[matchingId]['MatchingFaces']:
#                    if faceId in faces[matchingId2]['MatchingFaces']:
#                        numberMatchingLoops = numberMatchingLoops + 1
#
#                # To avoid false positives, the propagation from faceA to faceB
#                # happens only if there are at least two faces matching faceB
#                # that also match faceA
#                if numberMatchingLoops >= 2:
#                    celebId = celebs[faceId]['PersonId']
#                    faces[matchingId]['PersonId'] = personId
#                    propagate_person_id(matchingId)
#
#    celebId = 0
#    for faceId in faceIdsSorted:
#        if not 'PersonId' in faces[faceId]:
#            personId = personId + 1
#            faces[faceId]['PersonId'] = personId
#            propagate_person_id(faceId)
######################

#    print('Unique celebs identified')


    # Retain only the people that appear in at least 3 consecutive frames
    # and create the JSON output.
#    people = []
#    maxPersonId = personId
#    targetNumberConsecutiveFrames = 3
#
#    for personId in range(1, maxPersonId + 1):
#        frames = []
#        previousFrameNumber = None
#        currentNumberConsecutiveFrames = 0
#        maxNumberConsecutiveFrames = 0

#        for faceId in faceIdsSorted:
#            if faces[faceId]['PersonId'] == personId:
#
#                frameNumber = faces[faceId]['FrameNumber']
#                frameTimePosition = '{}:{:02d}:{:02d}'.format(
#                    frameNumber / 3600,
#                    (frameNumber - frameNumber / 3600 * 3600) / 60,
#                    frameNumber % 60
#                )

#                frames.append({
#                    'FrameNumber': faces[faceId]['FrameNumber'],
#                    'FrameTimePosition': frameTimePosition,
#                    'BoundingBox': faces[faceId]['BoundingBox']
#                })

#                if previousFrameNumber == frameNumber - 1:
#                    currentNumberConsecutiveFrames += 1
#                    maxNumberConsecutiveFrames = max(maxNumberConsecutiveFrames, currentNumberConsecutiveFrames)
#                else:
#                    currentNumberConsecutiveFrames = 1
#
#                previousFrameNumber = frameNumber
#
#        if maxNumberConsecutiveFrames >= targetNumberConsecutiveFrames
#            people.append({'Frames': frames})

    celeb_json = {'Celebrities': celebs}

    # Upload the JSON result into the S3 bucket
    try:
        s3.put_object(
            Body=json.dumps(output_json, indent=4).encode(),
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
