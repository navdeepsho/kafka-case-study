import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import csv

API_KEY = 'MA3XZQ5EXC7NFE36'
ENDPOINT_SCHEMA_URL  = 'https://psrc-vn38j.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'mlD2OH/1g7mvHWWMUCnIsDXtx9nd9lZIEBDB0RfzqJysvdB66ha5igP2Ya/HM8Pp'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'WVUURNQ5DHPQRHZF'
SCHEMA_REGISTRY_API_SECRET = 'qKVlqls+FcJrE03SH/7bdAt7wDlFeNAI5JHymVpE1MqbPnGROaiocaNFEWSucF5n'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Car:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_car(data:dict,ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    subjects = schema_registry_client.get_subjects()
    print(subjects)
    for subject in subjects:
        if subject=='topic_11-value':
            schema = schema_registry_client.get_latest_version(subject)
            print(schema.version)
            print(schema.schema_id)
            value_schema=schema.schema.schema_str
            print(value_schema)
    json_deserializer = JSONDeserializer(value_schema,
                                         from_dict=Car.dict_to_car)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': '5',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    count = 0
    with open('mycsvfile.csv', 'w') as f:

        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

                if car is not None:
                    print("User record {}: car: {}\n"
                          .format(msg.key(), car))
                    print(car.__dict__)

                    w = csv.DictWriter(f, car.__dict__.keys())
                    w.writeheader()
                    w.writerow(car.__dict__)



                    count = count + 1
                    if count==5:
                        break

                print(count)

            except KeyboardInterrupt:
                break

    consumer.close()


dataframe =main("topic_11")


emptylsit=[]

for i in dataframe:
    emptylsit.append(row)
print(dfList)
pd.DataFrame(dfList).to_csv('output.csv', index=False)