import pika

mqConnection = pika.BlockingConnection(pika.ConnectionParameters(host='10.132.51.36',credentials=pika.PlainCredentials(username='iflying', password='mq_iflying_2019')))