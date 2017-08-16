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


    # Call the IndexFaces operation for each thumbnail. I use 50 concurrent
    # threads. Each iteration of a thread lasts at least one second. Faces
    # detected are stored in a local variable 'faces'.
    indexFacesQueue = Queue()

    def index_faces_worker():
        rekognition = boto3.client('rekognition', region_name=os.environ['AWS_REGION'])

        while True:
            key = indexFacesQueue.get()
            try:
                startTime = datetime.now()
                frameNumber = int(key[:-4][-5:])

                response = rekognition.index_faces(
                    CollectionId=collectionId,
                    Image={'S3Object': {
                        'Bucket': os.environ['Bucket'],
                        'Name': key
                    }},
                    ExternalImageId=str(frameNumber)
                )

                for face in response['FaceRecords']:
                    faceId = face['Face']['FaceId']
                    faces[faceId] = {
                        'FrameNumber': frameNumber,
                        'BoundingBox': face['Face']['BoundingBox']
                    }

                endTime = datetime.now()
                delta = int((endTime - startTime).total_seconds() * 1000)
                if delta < 1000:
                    timeToWait = float(1000 - delta)/1000
                    time.sleep(timeToWait)

            # I put the key back in the queue if the IndexFaces operation
            # failed
            except:
                indexFacesQueue.put(key)

            indexFacesQueue.task_done()

    for i in range(CONCURRENT_THREADS):
        t = Thread(target=index_faces_worker)
        t.daemon = True
        t.start()

    for key in thumbnailKeys:
        indexFacesQueue.put(key)

    indexFacesQueue.join()
    time.sleep(2)
    print('IndexFaces operation completed')


    # Search for faces that are similar to each face detected by the IndexFaces
    # operation with a confidence in matches that is higher than 97%.
    searchFacesQueue = Queue()

    def search_faces_worker():
        rekognition = boto3.client('rekognition', region_name=os.environ['AWS_REGION'])

        while True:
            faceId = searchFacesQueue.get()
            try:
                startTime = datetime.now()

                response = rekognition.search_faces(
                    CollectionId=collectionId,
                    FaceId=faceId,
                    FaceMatchThreshold=97,
                    MaxFaces=256
                )
                matchingFaces = [i['Face']['FaceId'] for i in response['FaceMatches']]

                # Delete the face from the local variable 'faces' if it has no
                # matching faces
                if len(matchingFaces) > 0:
                    faces[faceId]['MatchingFaces'] = matchingFaces
                else:
                    del faces[faceId]

                endTime = datetime.now()
                delta = int((endTime - startTime).total_seconds() * 1000)
                if delta < 1000:
                    timeToWait = float(1000 - delta)/1000
                    time.sleep(timeToWait)

            except:
                searchFacesQueue.put(faceId)

            searchFacesQueue.task_done()

    for i in range(CONCURRENT_THREADS):
        t = Thread(target=search_faces_worker)
        t.daemon = True
        t.start()

    for faceId in list(faces):
        searchFacesQueue.put(faceId)

    searchFacesQueue.join()
    print('SearchFaces operation completed')


    # Sort the list of face IDs in the order of which they appear in the video.
    def getKey(item):
        return item[1]
    facesFrameNumber = {k: v['FrameNumber'] for k, v in faces.items()}
    faceIdsSorted = [i[0] for i in sorted(facesFrameNumber.items(), key=getKey)]


    # Identify unique people and detect the frames in which they appear.
    sys.setrecursionlimit(10000)

    def propagate_person_id(faceId):
        for matchingId in faces[faceId]['MatchingFaces']:
            if not 'PersonId' in faces[matchingId]:

                numberMatchingLoops = 0
                for matchingId2 in faces[matchingId]['MatchingFaces']:
                    if faceId in faces[matchingId2]['MatchingFaces']:
                        numberMatchingLoops = numberMatchingLoops + 1

                # To avoid false positives, the propagation from faceA to faceB
                # happens only if there are at least two faces matching faceB
                # that also match faceA
                if numberMatchingLoops >= 2:
                    personId = faces[faceId]['PersonId']
                    faces[matchingId]['PersonId'] = personId
                    propagate_person_id(matchingId)

    personId = 0
    for faceId in faceIdsSorted:
        if not 'PersonId' in faces[faceId]:
            personId = personId + 1
            faces[faceId]['PersonId'] = personId
            propagate_person_id(faceId)

    print('Unique people identified')


    # Retain only the people that appear in at least 5 consecutive frames
    # and create the JSON output.
    people = []
    maxPersonId = personId

    for personId in range(1, maxPersonId + 1):
        frames = []
        previousFrameNumber = None
        currentNumberConsecutiveFrames = 0
        maxNumberConsecutiveFrames = 0

        for faceId in faceIdsSorted:
            if faces[faceId]['PersonId'] == personId:

                frameNumber = faces[faceId]['FrameNumber']
                frameTimePosition = '{}:{:02d}:{:02d}'.format(
                    frameNumber / 3600,
                    (frameNumber - frameNumber / 3600 * 3600) / 60,
                    frameNumber % 60
                )

                frames.append({
                    'FrameNumber': faces[faceId]['FrameNumber'],
                    'FrameTimePosition': frameTimePosition,
                    'BoundingBox': faces[faceId]['BoundingBox']
                })

                if previousFrameNumber == frameNumber - 1:
                    currentNumberConsecutiveFrames += 1
                    maxNumberConsecutiveFrames = max(maxNumberConsecutiveFrames, currentNumberConsecutiveFrames)
                else:
                    currentNumberConsecutiveFrames = 1

                previousFrameNumber = frameNumber

        if maxNumberConsecutiveFrames >= 2:
            people.append({'Frames': frames})

    output_json = {'People': people}


    # Upload the JSON result into the S3 bucket
    try:
        s3.put_object(
            Body=json.dumps(output_json, indent=4).encode(),
            Bucket=os.environ['Bucket'],
            Key=sns_msg['outputKeyPrefix'].replace('elastictranscoder/', 'output/')[:-1] + '.json'
        )
        print('JSON result uploaded into the S3 bucket')

    except Exception as e:
        print('Failed to upload the JSON result into the S3 bucket')
        print(e)
        raise(e)


    # Create a visual representation
    numberPeople = len(people)
    duration = len(thumbnailKeys)
    thumbnailSize = 50
    borderSize = 20
    desiredTimeWidth = 580.0
    secondsPerPixel = math.ceil(duration/desiredTimeWidth) if duration>desiredTimeWidth else 1

    imgWidth = int(4*thumbnailSize + borderSize*3 + math.ceil(duration/secondsPerPixel))
    imgHeight = int(1*thumbnailSize*numberPeople + borderSize*(numberPeople + 1))
    img = Image.new("RGB", (imgWidth, imgHeight), "white")
    draw = ImageDraw.Draw(img)

    draw.rectangle((0, 0, img.size[0], img.size[1]), fill="black")

    # For each person
    for indexPerson, person in enumerate(people):

        # Select 4 random frames to face thumbnails
        sampleFrameNumbers = random.sample(set(range(len(person['Frames']))), 4)

        # For each face thumbnail
        for indexSample, sampleFrameNumber in enumerate(sampleFrameNumbers):
            thumb = person['Frames'][sampleFrameNumber]

            # Download the video thumbnail from Amazon S3
            key = sns_msg['outputKeyPrefix']
            key += sns_msg['outputs'][0]['thumbnailPattern'] + '.png'
            key = key.replace('{count}', '{:05d}'.format(thumb['FrameNumber']))
            response = s3.get_object(Bucket=os.environ['Bucket'], Key=key)
            imgThumb = Image.open(StringIO(response['Body'].read()))

            # Calculate the face position to crop the image
            boxLeft = int(math.floor(imgThumb.size[0] * thumb['BoundingBox']['Left']))
            boxTop = int(math.floor(imgThumb.size[1] * thumb['BoundingBox']['Top']))
            boxWidth = int(math.floor(imgThumb.size[0] * thumb['BoundingBox']['Width']))
            boxHeight = int(math.floor(imgThumb.size[1] * thumb['BoundingBox']['Height']))

            # Each face box must have equal width and height
            if boxWidth > boxHeight:
                boxLeft = int(math.floor(boxLeft + (boxWidth - boxHeight) / 2))
                boxWidth = boxHeight
            else:
                boxTop = int(math.floor(boxTop + (boxHeight - boxWidth) / 2))
                boxHeight = boxWidth

            # Paste the face thumbnail into the visual representation
            imgThumbCrop = imgThumb.crop((boxLeft, boxTop, boxLeft+boxWidth, boxTop+boxHeight))
            imgThumbSmall = imgThumbCrop.resize((thumbnailSize, thumbnailSize), Image.ANTIALIAS)
            img.paste(imgThumbSmall, (
                int(borderSize + thumbnailSize*indexSample),
                int(borderSize + (borderSize+thumbnailSize)*indexPerson)
            ))

        # Draw the time box
        rectLeft = int(borderSize*2 + thumbnailSize*4 + 1)
        rectTop = int(borderSize + (borderSize+thumbnailSize)*indexPerson)
        rectRight = int(borderSize*2 + thumbnailSize*4 + math.floor(duration/secondsPerPixel))
        rectBottom = int(borderSize + (borderSize+thumbnailSize)*indexPerson + thumbnailSize)
        draw.rectangle((rectLeft, rectTop, rectRight, rectBottom), fill="rgb(230,230,230)")

        # For each person, draw red lines to identify frames in which they
        # appear
        for frame in person['Frames']:
            lineLeft = int(rectLeft + math.floor(frame['FrameNumber'] / secondsPerPixel))
            draw.rectangle((lineLeft, rectTop, lineLeft, rectBottom), fill="red")

    img.save("/tmp/img.png", "PNG")

    try:
        s3.upload_file(
            "/tmp/img.png",
            Bucket=os.environ['Bucket'],
            Key=sns_msg['outputKeyPrefix'].replace('elastictranscoder/', 'output/')[:-1] + '.png'
        )
        print('Visual representation uploaded into the S3 bucket')

    except Exception as e:
        print('Failed to upload the visual representation into the S3 bucket')
        print(e)
        raise(e)


    # Delete the collection in Amazon Rekognition.
    try:
        rekognition.delete_collection(CollectionId=collectionId)
        print('Collection deleted from Amazon Rekognition'.format(collectionId))

    except Exception as e:
        print('Failed to delete the collection in Amazon Rekognition')
        print(e)
        raise(e)
