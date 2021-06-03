import sys
sys.path.append('../src')
from chain_factory.task_queue.models.mongo.task import Task
import time
import os

from chain_factory.task_queue.task_queue import TaskQueue

import logging
FORMAT = (
    '%(asctime)s.%(msecs)03d %(levelname)8s: '
    '[%(pathname)10s:%(lineno)s - '
    '%(funcName)20s() ] %(message)s'
)
logging.basicConfig(
    filename='logfile.log',
    level=logging.DEBUG,
    format=FORMAT,
    datefmt='%Y-%m-%d %H:%M:%S'
)

# create the main TaskQueue object
task_queue = TaskQueue()
host = '172.16.19.15'
# the current node name. Should be later changed to an environment variable
task_queue.node_name = os.getenv('HOSTNAME', 'hostname1234')
# the amqp endpoint. Should later be changed to an environment variable
task_queue.amqp_host = os.getenv('RABBITMQ_HOST', host)
# the redis endpoint. Should later be changed to an environment variable
task_queue.redis_host = os.getenv('REDIS_HOST', host)
# the amqp username ==> guest is the default
task_queue.amqp_username = os.getenv('RABBITMQ_USER', 'guest')
# the amqp passwort ==> guest is the default
task_queue.amqp_password = os.getenv('RABBITMQ_PASSWORD', 'guest')
task_queue.mongodb_connection = os.getenv(
    'MONGODB_CONNECTION_URI',
    'mongodb://root:example@' + host + '/orchestrator_db?authSource=admin'
)
task_queue.worker_count = 10

counter = 0


@task_queue.task()
def test01(testvar01: int):
    print(testvar01)
    print("Hello World!")


@task_queue.task()
def test02():
    print('Test02')
    return 'test01', {'testvar01': '01'}


@task_queue.task('simulate')
def simulate(times: int, i: int, exclude=['i']):
    print(" [x] Received simulate task")
    # print('times: ' + str(times))
    # print('i' + str(i))
    global counter
    counter = counter + 1
    print('counter: ' + str(counter))
    for i in range(0, times):
        time.sleep(1)
    # print(" [x] Done %d" % i)
    return None


@task_queue.task('send_feedback')
def send_feedback(feedback):
    print('feedback: %s' % (feedback, ))
    return None


@task_queue.task()
def workflow_task():
    print('schedule next task: send_feedback')
    return Task(name='send_feedback', arguments={'feedback': 'success'})


@task_queue.task()
def failed_task(failed_counter: int):
    print('Error, task failed!')
    failed_counter = failed_counter + 1
    if failed_counter >= 3:
        return None
    return False, {'failed_counter': failed_counter}


@task_queue.task()
def chained_task_01():
    print('chained_task_01')
    return Task('chained_task_02', {'arg1': 'test01'})


@task_queue.task()
def chained_task_02(arg1: str = 'test02'):
    print('chained_task_02')
    print('arg1_chained_task_02: ' + arg1)
    if arg1 == 'test01':
        print('...')
        return 'chained_task_03', {'arg2': 'test'}
    else:
        return None


@task_queue.task()
def chained_task_03(arg2: str):
    print('chained_task_03')
    return chained_task_02, {'arg1': 'test02'}


if __name__ == '__main__':
    task_queue.listen()  # start the node

    # time.sleep(1000)
    while(True):
        time.sleep(0.01)  # keep mainthread running