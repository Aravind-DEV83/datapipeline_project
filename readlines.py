import os
import time
from google.cloud import pubsub_v1

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'bi-team.json'

project_id = 'bi-team-390005'
topic_id = 'projects/bi-team-390005/topics/stream-bank-loan'
publisher = pubsub_v1.PublisherClient()

input_file = "data.csv"
with open(input_file, 'r') as read:
    data = read.readlines()
    data = data[1:]

    i,j=0,0
    try:
        while j<len(data):
            i=j
            j=j+1
            if j > len(data):
                break
            for element in data[i:j]:
                element = (element.encode("utf-8"))
                print('Publishing {} to {}'.format(element, topic_id))
                publisher.publish(topic_id, element)
            time.sleep(1)
    except KeyboardInterrupt:
        print("exit")
