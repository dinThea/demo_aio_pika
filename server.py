import pika
import json
import time
import os

def create_channel(user, password, host, port):
    
    credentials = pika.PlainCredentials(username=user, password=password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host, port, credentials=credentials))
    channel = connection.channel()

    return channel

def publish_message(queue_name: str, channel: pika.channel.Channel, message: dict):
    
    print ('SENDING MESSAGE')
    channel.basic_publish(
        exchange='',
        properties=pika.BasicProperties(),
        routing_key=queue_name,
        body=json.dumps(message)
    )

if __name__ == '__main__':

    with open(os.environ['RABBIT_CREDS']) as rabbit_creds:
        creds = json.load(rabbit_creds)
        channel = create_channel(**creds)
        
        while True:
            publish_message('test_queue', channel, {'oi': 'tudobem'})
            time.sleep(0.1)