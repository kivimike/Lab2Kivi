from kafka import KafkaProducer
import random
from datetime import datetime, timedelta



def create_log_event(dt, area):
    time = dt.strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
    temp = f'{time},area{area},sensor{random.randint(1, 1000)}_temp, {random.randint(-20, 50)}'
    pres = f'{time},area{area},sensor{random.randint(1, 1000)}_pres, {random.randint(728, 812)}'
    hum = f'{time},area{area},sensor{random.randint(1, 1000)}_hum, {random.randint(0, 100)}'
    return temp, pres, hum

def create_data(n = 1000, number_of_areas=3, topic = 'testtopic' ,bootstrap_servers='localhost:9092'):
    areas = [x for x in range(number_of_areas)]
    dt = datetime.now()
    producer = KafkaProducer(bootstrap_servers=[bootstrap_servers])
    for i in range(n):
        for area in areas:
            log = create_log_event(dt, area)
            producer.send(topic, bytes(log[0], 'utf-8'))
            producer.send(topic, bytes(log[1], 'utf-8'))
            producer.send(topic, bytes(log[2], 'utf-8'))

            dt += timedelta(minutes=random.randint(0, 1), seconds=random.randint(0, 60),
                            milliseconds=random.randint(0, 10000))
        dt += timedelta(minutes=random.randint(2, 10), seconds=random.randint(0, 60), milliseconds=random.randint(0,10000))

if __name__ == '__main__':
    create_data(3, 3)