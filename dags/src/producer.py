import asyncio
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import json

class Producer:
    CONNECTION_STRING = 'sb://exp-kdr-lan-eventhubnamespace.servicebus.windows.net/;SharedAccessKeyName=policy-send_and_receive-kandoor;SharedAccessKey=BvU7I1Hd6iU48ScI8+AUZKW5C67PrUj0xZ7zlHiMhl8=;EntityPath=diyevents'
    EVENTHUB_NAME = 'diyevents'

    YOUR_NAME = 'RS'

    async def run(self):
        # Create a producer client to send messages to the event hub.
        # Specify a connection string to your event hubs namespace and
     	    # the event hub name.
        producer = EventHubProducerClient.from_connection_string(conn_str=self.CONNECTION_STRING, eventhub_name=self.EVENTHUB_NAME)
        async with producer:
            # Create a batch.
            event_data_batch = await producer.create_batch()

            # Add events to the batch.
            event_data_batch.add(EventData(json.dumps({'user': self.YOUR_NAME, 'timestamp': str(datetime.now()), 'message': 'Is this also a question?'})))

            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)

    def execute(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run())
