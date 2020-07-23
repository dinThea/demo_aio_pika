import asyncio
import aio_pika
import os
import json
import queue
from typing import Union

class WorkerManager():

    def __init__(self, host: str, port: int, user: str, password: str, num_workers: int):
        
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._num_workers = num_workers

        self._workers_messages = {i: None for i in range(self._num_workers)}
        self._workers_tasks = asyncio.gather(*[self.worker_routine(i) for i in range(self._num_workers)])
        
        self._loop = asyncio.get_event_loop()

    async def worker_routine(self, worker_id: int):
        while True:
            
            print (f'{worker_id} esta vivo')
            if self._workers_messages[worker_id] != None:
                async with self._workers_messages[worker_id].process(requeue=True):
                    payload= json.loads(self._workers_messages[worker_id].body)
                    print (f'{payload} {worker_id}')
                    self._workers_messages[worker_id] = None

            await asyncio.sleep(3)
    
    async def on_message(self, message):

        for worker_id in range(self._num_workers):
            print ('Receiving message')
            if self._workers_messages[worker_id] == None:
                self._workers_messages[worker_id] = message
                break
        else:
            print ('No Worker Available')
            message.nack()

    async def consume(self):

        connection = await aio_pika.connect(f'amqp://{self._user}:{self._password}@{self._host}:{self._port}')
        channel = await connection.channel()

        await channel.set_qos(prefetch_count=self._num_workers)

        queue = await channel.declare_queue(
            "test_queue",
            durable=True
        )

        await queue.consume(self.on_message)

    def run(self):
        
        self._loop.run_until_complete(asyncio.gather(self._workers_tasks, self.consume()))

if __name__ == '__main__':
    
    with open(os.environ['RABBIT_CREDS']) as rabbit_creds:
        credentials = json.load(rabbit_creds)
        wm = WorkerManager(**credentials, num_workers=5)
        wm.run()