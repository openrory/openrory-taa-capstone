import asyncio
from azure.eventhub.aio import EventHubConsumerClient
import json

EVENTHUB_CONNECTION_STRING = 'Endpoint=sb://exp-kdr-lan-eventhubnamespace.servicebus.windows.net/;SharedAccessKeyName=policy-send_and_receive-kandoor;SharedAccessKey=BvU7I1Hd6iU48ScI8+AUZKW5C67PrUj0xZ7zlHiMhl8=;EntityPath=diyevents'
EVENTHUB_NAME = 'diyevents'

async def on_event(partition_context, event):
    # Print the event data.
    #print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))

    try:
        json_body = event.body_as_json(encoding='UTF-8')
    except: pass
    else:
        if json_body['user'] == 'RS': print(json_body['message'])

    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():
    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string(EVENTHUB_CONNECTION_STRING, consumer_group="$Default", eventhub_name=EVENTHUB_NAME)
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())
