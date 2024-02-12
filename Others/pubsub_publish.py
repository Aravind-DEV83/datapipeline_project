import os
import time
from google.cloud import pubsub_v1
from concurrent import futures

## GCP Variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
TOPIC = 'projects/data-project-406509/topics/topic_2'
project_id = 'data-project-406509'


def configure_pubsub():

    publisher = pubsub_v1.PublisherClient()
    return publisher

def publish_messages(publisher, messages):

    publish_futures = []
    for element in messages:
        element = element.encode("utf-8")
        publish_futures.append(publisher.publish(TOPIC, element))

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
    print(f"Published messages with batch settings to {TOPIC}.")
    
def main():

    publisher = configure_pubsub()

    input_file = "store_sales.csv"
    with open(input_file, 'r') as read:
        data = read.readlines()[1:]
        size = 3000
        count = 0
        
        for i in range(0, len(data), size):
            count+=1
            batch = data[i: i+ size]
            print("Publishing {} messaged to TOPIC counting {}".format(len(batch), count))
            # publish_messages(publisher, batch)
            # time.sleep(1)


if __name__ == "__main__":
    main()