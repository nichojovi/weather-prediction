from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_kafka_topic(name, partitions, replication, server):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=server, client_id='kafka_topic_creator')
        topic = NewTopic(name=name, num_partitions=partitions, replication_factor=replication)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{name}' already exists.")
    except Exception as e:
        print(f"Failed to create topic '{name}': {e}")
    finally:
        admin_client.close()