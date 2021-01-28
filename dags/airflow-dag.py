import time

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago


import asyncio
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub import EventData
import json

import os
import threading
import time
from azure.eventhub import EventHubConsumerClient

from gremlin_python.driver import client, serializer
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.io.graphsonV3d0 import GraphSONWriter, GraphSONReader

import sys
import traceback

class Producer:
    CONNECTION_STRING = ''
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
            event_data_batch.add(EventData(json.dumps({'user': self.YOUR_NAME, 'timestamp': str(datetime.now()), 'message': 'Robert,ABP'})))
            event_data_batch.add(EventData(json.dumps({'user': self.YOUR_NAME, 'timestamp': str(datetime.now()), 'message': 'Els,ABP'})))

            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)


class Consumer:
    #!/usr/bin/env python

    # --------------------------------------------------------------------------------------------
    # Copyright (c) Microsoft Corporation. All rights reserved.
    # Licensed under the MIT License. See License.txt in the project root for license information.
    # --------------------------------------------------------------------------------------------

    """
    An example to show receiving events from an Event Hub.
    """

    EVENTHUB_CONNECTION_STRING = ''
    EVENTHUB_NAME = 'diyevents'
    RECEIVE_DURATION = 15


    def on_event(self, partition_context, event):
        # Put your code here.
        # If the operation is i/o intensive, multi-thread will have better performance.
        print("Received event from partition: {}.".format(partition_context.partition_id))
        try:
            json_body = event.body_as_json(encoding='UTF-8')
        except: pass
        else:
            if json_body['user'] == 'RS':
                message = json_body['message']
                print("Raw message: {}".format(message))
                preprocessor = Preprocessor()
                preprocessed_message = preprocessor.preprocess(message)
                print("Preprocessed message: {}".format(preprocessed_message))

                # push the message to the 'processedevents' Event Hub
                producer = Producer2()
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(producer.run(preprocessed_message))
                print("Preprocessed message pushed back to Event Hub.")


    def on_partition_initialize(self, partition_context):
        # Put your code here.
        print("Partition: {} has been initialized.".format(partition_context.partition_id))


    def on_partition_close(self, partition_context, reason):
        # Put your code here.
        print("Partition: {} has been closed, reason for closing: {}.".format(
            partition_context.partition_id,
            reason
        ))


    def on_error(self, partition_context, error):
        # Put your code here. partition_context can be None in the on_error callback.
        if partition_context:
            print("An exception: {} occurred during receiving from Partition: {}.".format(
                partition_context.partition_id,
                error
            ))
        else:
            print("An exception: {} occurred during the load balance process.".format(error))


class Preprocessor:
    def preprocess(self, message:str):
        person, pension_fund = message.split(',')
        result = "{},{},{}".format(person, "has_pension_fund", pension_fund)
        return result


class Producer2:
    CONNECTION_STRING = ''
    EVENTHUB_NAME = 'processedevents'

    YOUR_NAME = 'RS'

    async def run(self, message):
        # Create a producer client to send messages to the event hub.
        # Specify a connection string to your event hubs namespace and
     	    # the event hub name.
        producer = EventHubProducerClient.from_connection_string(conn_str=self.CONNECTION_STRING, eventhub_name=self.EVENTHUB_NAME)
        async with producer:
            # Create a batch.
            event_data_batch = await producer.create_batch()

            # Add events to the batch.
            event_data_batch.add(EventData(json.dumps({'user': self.YOUR_NAME, 'timestamp': str(datetime.now()), 'message': message})))

            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)


class Consumer2:
    #!/usr/bin/env python

    # --------------------------------------------------------------------------------------------
    # Copyright (c) Microsoft Corporation. All rights reserved.
    # Licensed under the MIT License. See License.txt in the project root for license information.
    # --------------------------------------------------------------------------------------------

    """
    An example to show receiving events from an Event Hub.
    """

    EVENTHUB_CONNECTION_STRING = ''
    EVENTHUB_NAME = 'processedevents'
    RECEIVE_DURATION = 15


    def on_event(self, partition_context, event):
        # Put your code here.
        # If the operation is i/o intensive, multi-thread will have better performance.
        print("Received event from partition: {}.".format(partition_context.partition_id))
        try:
            json_body = event.body_as_json(encoding='UTF-8')
        except: pass
        else:
            if json_body['user'] == 'RS':
                message = json_body['message']
                print("Preprocessed message from `processedevents`: {}".format(message))

                person, edge, pension_fund = message.split(',')
                client = self.get_client()

                query = "g.addV('person').property('name','{}')".format(person)
                self.send_to_cosmos_db(client, query)

                query2 = "g.V().hasLabel('person').has('name','{}').as('a').V().hasLabel('pension_fund').has('name','{}').addE('{}').from('a')".format(person, pension_fund, edge)
                self.send_to_cosmos_db(client, query2)

    def get_client(self):
        from gremlin_python.driver import client, serializer
        try:
            cosmosclient = client.Client('ws://localhost:8901', 'g',
                                   username="/dbs/db1/colls/coll1",
                                   password="C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                                   message_serializer=serializer.GraphSONSerializersV2d0()
                                   )

            print("Welcome to Azure Cosmos DB + Gremlin on Python!")
            return cosmosclient
        except Exception as e:
            print('There was an exception: {0}'.format(e))
            traceback.print_exc(file=sys.stdout)
            sys.exit(1)

    def send_to_cosmos_db(self, client, query):
            # query = "g.io('mygraph.graphml').write().iterate()"
            # fos = new FileOutputStream("my-graph.json")

            # GraphSONWriter.build().wrapAdjacencyList(true).create().writeGraph(fos,graph)
            callback = client.submitAsync(query)
            if callback.result() is not None:
                print("\tQuery result:\n\t{0}\n".format(
                    callback.result().one()))
            else:
                print("Something went wrong with this query:\n\t{0}".format(query))
            print("\n")

            # gson_writer  = GraphSONWriter(g)
            # gson_writer.writeObject()




    def on_partition_initialize(self, partition_context):
        # Put your code here.
        print("Partition: {} has been initialized.".format(partition_context.partition_id))


    def on_partition_close(self, partition_context, reason):
        # Put your code here.
        print("Partition: {} has been closed, reason for closing: {}.".format(
            partition_context.partition_id,
            reason
        ))


    def on_error(self, partition_context, error):
        # Put your code here. partition_context can be None in the on_error callback.
        if partition_context:
            print("An exception: {} occurred during receiving from Partition: {}.".format(
                partition_context.partition_id,
                error
            ))
        else:
            print("An exception: {} occurred during the load balance process.".format(error))



args = {
    'owner': 'rory',
}

dag = DAG(
    dag_id='capstone_python_operator3',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['kandoor'],
)


# [START howto_operator_python]
def produce_raw_message():
    """Produce raw message to diy event hub."""
    producer = Producer()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(producer.run())
    return 'Raw message produced.'

def preprocess_raw_message():
    consumer = Consumer()

    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=consumer.EVENTHUB_CONNECTION_STRING,
        consumer_group='$Default',
        eventhub_name=consumer.EVENTHUB_NAME,
    )
    print('Consumer will keep receiving for {} seconds, start time is {}.'.format(consumer.RECEIVE_DURATION, time.time()))

    try:
        thread = threading.Thread(
            target=consumer_client.receive,
            kwargs={
                "on_event": consumer.on_event,
                "on_partition_initialize": consumer.on_partition_initialize,
                "on_partition_close": consumer.on_partition_close,
                "on_error": consumer.on_error,
                "starting_position": "-1",  # "-1" is from the beginning of the partition.
            }
        )
        thread.daemon = True
        thread.start()
        time.sleep(consumer.RECEIVE_DURATION)
        consumer_client.close()
        thread.join()
    except KeyboardInterrupt:
        print('Stop receiving.')

    print('Consumer has stopped receiving, end time is {}.'.format(time.time()))

def consume_preprocessed_message():
    consumer = Consumer2()

    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=consumer.EVENTHUB_CONNECTION_STRING,
        consumer_group='$Default',
        eventhub_name=consumer.EVENTHUB_NAME,
    )
    print('Consumer will keep receiving for {} seconds, start time is {}.'.format(consumer.RECEIVE_DURATION, time.time()))

    try:
        thread = threading.Thread(
            target=consumer_client.receive,
            kwargs={
                "on_event": consumer.on_event,
                "on_partition_initialize": consumer.on_partition_initialize,
                "on_partition_close": consumer.on_partition_close,
                "on_error": consumer.on_error,
                "starting_position": "-1",  # "-1" is from the beginning of the partition.
            }
        )
        thread.daemon = True
        thread.start()
        time.sleep(consumer.RECEIVE_DURATION)
        consumer_client.close()
        thread.join()
    except KeyboardInterrupt:
        print('Stop receiving.')

    print('Consumer2 has stopped receiving, end time is {}.'.format(time.time()))


t1 = PythonOperator(
    task_id='produce_raw_message',
    python_callable=produce_raw_message,
    dag=dag,
)
t2 = PythonOperator(
    task_id='preprocess_raw_message',
    python_callable=preprocess_raw_message,
    dag=dag,
)

t3 = PythonOperator(
    task_id='consume_and_offload_preprocessed_message',
    python_callable=consume_preprocessed_message,
    dag=dag,
)

t1 >> t2 >> t3
# [END howto_operator_python]
