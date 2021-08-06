from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "video-player-status",
    "enable.auto.commit": False
}

consumer = Consumer(conf)

topic = "video-player"

try:
    consumer.subscribe([topic])

    while True:
        message = consumer.poll()

        if message is None:
            continue
        if message.error():
            print("Consumer error: {}".format(message.error()))
            continue

        print(f"Received message: \n"
              f"Key: {message.key().decode('UTF-8')}\n"
              f"Value: {message.value().decode('UTF-8')}")

        # Inform the Kafka Broker that this message (offset)
        # was already consumed and the consumer is ready to
        # to move to the next message (next offset).
        consumer.commit()
finally:
    """
    Close down and terminate the Kafka Consumer.        
        - Stops consuming.        
        - Commits offsets, unless the consumer property `enable.auto.commit` is set to False.        
        - Leaves the consumer group.
    """
    consumer.close()
