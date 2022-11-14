from confluent_kafka import Producer
from faker import Faker
from nameparser import HumanName
import json
from datetime import datetime
from deepparse.parser import AddressParser

fake = Faker(['de_DE'])
address_parser = AddressParser(model_type="bpemb", device=0)

class ExampleProducer:
    broker = "localhost:9092"
    topic = "blahblah"
    producer = None

    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': self.broker,
            'socket.timeout.ms': 100,
            'api.version.request': 'false',
            'broker.version.fallback': '0.9.0',
        }
        )

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(
                msg.topic(), msg.partition()))

    def send_msg_async(self, msg):
        print("Send message asynchronously")
        self.producer.produce(
            self.topic,
            msg,
            callback=lambda err, original_msg=msg: self.delivery_report(err, original_msg
                                                                        ),
        )
        self.producer.flush()

    def send_msg_sync(self, msg):
        print("Send message synchronously")
        self.producer.produce(
            self.topic,
            msg,
            callback=lambda err, original_msg=msg: self.delivery_report(
                err, original_msg
            ),
        )
        self.producer.flush()

def fake_registration():
    timestamp = {'timestamp_uct': (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()}
    names = HumanName(fake.name()).as_dict()
    address = address_parser(fake.address()).to_dict()
    del address['Province']
    del address['Unit']
    del address['Orientation']
    del address['GeneralDelivery']
    del address['EOS']
    address['city'] = address.pop('Municipality')
    result = json.dumps(timestamp | names | address)
    return result

#SENDING DATA TO KAFKA TOPIC
example_producer = ExampleProducer()
message = fake_registration()
example_producer.send_msg_async(message)

print(fake_registration())
print("End")