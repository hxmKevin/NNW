# encoding: utf-8
# author: kevin
# date: 2019.12.2
# summary: 发送方/生产者

import os, sys, time
import pika
from bson import json_util
import datetime
from common.readconfig import ReadConfig


# 用于构建邮件头
class MQSend:
    def __init__(self):
        pass

    def main(self, param_queue='', param_exchange='', param_routing_key='', message=''):
        if not param_queue or not param_exchange or not param_routing_key or not message:
            print('参数不完整')

        MQ = ReadConfig()
        user = str(MQ.get_mq("user"))
        pwd = str(MQ.get_mq("pwd"))
        ip = str(MQ.get_mq("ip"))

        credentials = pika.PlainCredentials(user, pwd)
        parameters = pika.ConnectionParameters(host=ip,
                                               virtual_host='/',
                                               credentials=credentials)
        connection = pika.BlockingConnection(parameters)  # 连接 RabbitMQ

        channel = connection.channel()  # 创建频道

        queue = channel.queue_declare(queue=param_queue, durable=True)  # 声明或创建队列

        channel.basic_publish(exchange=param_exchange,
                              routing_key=param_routing_key,
                              body=message)

        print('send message: %s' % message)

        # 关闭连接
        connection.close()


if __name__ == '__main__':
    start = time.perf_counter()

    try:
        param_queue = ''
        param_exchange = ''
        param_routing_key = ''
        message = ''

        try:
            param_queue = str(sys.argv[1])
        except BaseException as err:
            pass

        try:
            param_exchange = str(sys.argv[2])
        except BaseException as err:
            pass

        try:
            param_routing_key = str(sys.argv[3])
        except BaseException as err:
            pass

        try:
            message = str(sys.argv[4])
        except BaseException as err:
            pass

        MQS = MQSend()
        MQS.main(param_queue, param_exchange, param_routing_key, message)
    except BaseException as err:
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Complete, Time used:", elapsed)
