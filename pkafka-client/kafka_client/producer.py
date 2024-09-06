import datetime
import logging, argparse
from confluent_kafka import KafkaException
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


logging.basicConfig(level=logging.INFO)

def delivery_callback(error, message):
    if error:
        logging.info("Failed to send the message: %s" % error)
    else:
        logging.info(f"Message with the key {message.key()} has been produced to the topic {message.topic()}")

def load_avro_schema_str(schema_path:str, file_name:str):
    with open(f"{schema_path}/{file_name}", "r") as f:
        schema_str = f.read()
    return schema_str
    


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
             description="Python Client example to produce messages")
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    required.add_argument('-f',
                          dest="config_file",
                          help="path to Kafka configuration file",
                          required=True)
    required.add_argument('-t',
                          dest="topic",
                          help="topic name",
                          required=True)
    args = parser.parse_args()

    return args

def read_config_file(config_file):
    """Read the configuration file"""

    config = {}
    with open(config_file) as f:
        for line in f:
            line = line.strip()
            if len(line) != 0 and not line.startswith("#"):
                key, value = line.strip().split("=", 1)
                config[key] = value.strip()
    return config

def schema_registry_config(config:dict) -> dict:
    """Configure the schema registry"""

    schema_registry_config = {
        "url": config["schema.registry.url"],
        "basic.auth.user.info": config["basic.auth.user.info"],
        "ssl.ca.location": "PfizerTestRootCAG2.pem"
    }
    
    return schema_registry_config

def pop_schema_registry_params_from_config(conf:dict) -> dict:
    """Remove potential Schema Registry related configurations from dictionary"""

    conf.pop('schema.registry.url', None)
    conf.pop('basic.auth.user.info', None)
    conf.pop('basic.auth.credentials.source', None)

    return conf

def configure_producer(config:dict):
    schema_registry_configuration = schema_registry_config(config)
    kafka_config = pop_schema_registry_params_from_config(config)
    schema_registry_client = SchemaRegistryClient(schema_registry_configuration)
    key_avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client, 
                                         schema_str=load_avro_schema_str("./schemas", "Key.avsc"),
                                         to_dict=Key.key_to_dict,
                                          conf={
                                              'auto.register.schemas': False
                                            } )
    value_avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client, 
                                           schema_str=load_avro_schema_str("./schemas", "Value.avsc"),
                                           to_dict=Value.value_to_dict)
    kafka_config["key.serializer"] = key_avro_serializer
    kafka_config["value.serializer"] = value_avro_serializer
    print(kafka_config)
    producer = SerializingProducer(kafka_config)
    return producer

def produce_messages(producer:SerializingProducer, topic:str):

    key_obj = Key(Name="John", Code="EU")
    value_obj = Value(Description="Some Description",
                                Name="Some Name",
                                StartTime=datetime.datetime.now(tz=datetime.timezone.utc))
    producer.produce(topic=topic, key=key_obj, value=value_obj, on_delivery=delivery_callback)
    producer.poll(0)
    producer.flush()

def main():
    arguments = parse_args()
    topic = arguments.topic
    config = read_config_file(arguments.config_file)
    producer = configure_producer(config)
    produce_messages(producer, topic)


class Key(object): 
    __slots__ = ["Name", "Code"] 
    def __init__(self, Name:str, Code:str):
        self.Name = Name
        self.Code = Code

    @staticmethod
    def key_to_dict(key, ctx):
        return Key.to_dict(key)

    def to_dict(self):
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        return dict(Name=self.Name, Code=self.Code)
    
class Value(object): 
    __slots__ = ["Description", "Name", "StartTime"] 
    def __init__(self, Description:str, Name:str, StartTime:datetime.datetime):
        self.Description = Description
        self.Name = Name
        self.StartTime = StartTime

    @staticmethod
    def value_to_dict(value, ctx):
        return Value.to_dict(value)

    def to_dict(self):
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        return dict(Description=self.Description, Name=self.Name, StartTime=self.StartTime)

if __name__ == "__main__":
    main()