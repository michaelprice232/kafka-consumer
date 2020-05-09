"""
Create Kafka topic(s) with 3 partitions and 1 replication factor
"""

from confluent_kafka.admin import AdminClient, NewTopic


a = AdminClient({'bootstrap.servers': 'localhost:29092, localhost:29093'})

# Create a list of Topic objects
topics = ["important-topic", "second-topic"]
new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]

# List new_topics object
print("new_topics objects=", new_topics)

# Use the Admin API to create topics, passing the NewTopic objects
# A dict of <topic: future> is returned
fs = a.create_topics(new_topics)

# Display the fs object
print("fs objects=", fs)

# Wait for the operation to finish
for topic, f in fs.items():
    try:
        f.result()      # the result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))
