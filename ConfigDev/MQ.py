import pika

mqConnection = pika.BlockingConnection(pika.ConnectionParameters(host='172.16.31.241',credentials=pika.PlainCredentials(username='test', password='123456')))