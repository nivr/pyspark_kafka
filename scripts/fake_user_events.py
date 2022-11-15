from confluent_kafka import Producer
from faker import Faker
import json
from datetime import datetime
import logging
import random 
from nameparser import HumanName
import numpy as np
from dateutil.relativedelta import relativedelta
from deepparse.parser import AddressParser

fake = Faker(['de_DE'])
address_parser = AddressParser(model_type="bpemb", device=0)

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

timestamp_now = datetime.utcnow()


p=Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka producer created')

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)

def fake_registration(num):
    first_time = (timestamp_now - relativedelta(months=1) - datetime(1970, 1, 1)).total_seconds()
    last_time = (timestamp_now - datetime(1970, 1, 1)).total_seconds()
    registration_times = np.random.uniform(first_time, last_time, num)
    user_ids = list(range(num))
    for i in range(num):
        timestamp = {'timestamp_uct': registration_times[i]}
        user_id = {'user_id': user_ids[i]}
        names = HumanName(fake.name()).as_dict()
        address = address_parser(fake.address()).to_dict()
        #platform = {'platform': random.choice(['Mobile', 'Laptop', 'Tablet'])}
        del address['Province']
        del address['Unit']
        del address['Orientation']
        del address['GeneralDelivery']
        del address['EOS']
        address['city'] = address.pop('Municipality')
        data = json.dumps(timestamp | user_id | names | address)
        #print(data)
        #p.poll(1)
        p.produce('event_registration', data.encode('utf-8'),callback=receipt)
        p.flush()

def fake_purchases(num):
    purchases_per_user = np.random.poisson(1,num)
    spending_per_user = np.random.poisson(5,num)
    first_time = (timestamp_now - relativedelta(months=1) - datetime(1970, 1, 1)).total_seconds()
    last_time = (timestamp_now - datetime(1970, 1, 1)).total_seconds()
    #purchase_times = np.random.uniform(first_time, last_time, sum(purchases_per_user))
    user_ids = list(range(num))
    for i in range(num):
        purchase_times = np.random.uniform(first_time, last_time, purchases_per_user[i])
        for j in range(purchases_per_user[i]):
            timestamp = {'timestamp_uct': purchase_times[j]}
            user_id = {'user_id': user_ids[i]}
            platform = {'platform': random.choice(['Mobile', 'Laptop', 'Tablet'])}
            product = {'product': random.choice(['EuroLotto', 'Jackpot 6 aus 49', 'Kenospirale','Traumheitplus'])}
            billings = {'billings': abs(spending_per_user[i]+np.random.uniform(-3,3))}
            data = json.dumps(timestamp | user_id | platform | product | billings)
            #p.poll(1)
            p.produce('event_purchase', data.encode('utf-8'),callback=receipt)
            p.flush()


fake_registration(10000)
fake_purchases(10000)