from confluent_kafka import Producer
import uuid
import json

conf = {
    # A list of host/port pairs to use for establishing
    # the initial connection to the Kafka cluster
    'bootstrap.servers': "localhost:9092"
}

producer = Producer(conf)
# The topic where the message will be send.
topic = "video-player"
# This represents the information that needs to be
# send, to be store on the Broker side for later consumption.
player_status = {
    "sec": "18.7",
    "state": "PLAY",
    "video_id": "kafka-introduction"
}
# The `key` and `value` of the message should be a bytes-like object
user_id_as_key = str(uuid.uuid1()).encode("UTF-8")
payload = json.dumps(player_status).encode("UTF-8")

# Add the message to be send (to be produced)
producer.produce(topic, key=user_id_as_key, value=payload)
# Do not wait and send the message ASAP.
producer.flush()




