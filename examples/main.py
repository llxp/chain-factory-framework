from chain_factory.task_queue.models.mongo.task import Task
from chain_factory.task_queue.wrapper.redis_client import RedisClient
from chain_factory.task_queue.wrapper.mongodb_client import MongoDBClient
import time
import os

import hvac

from chain_factory.task_queue.task_queue import TaskQueue

from workflows.get_users_workflow import GetUsersWorkflow
# from workflows.workspace_one import WorkspaceOne
# from workflows.customjit_workflows.ldap_access import LDAPAccess

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

# the current node name. Should be later changed to an environment variable
node_name = os.getenv('HOSTNAME', 'hostname1234')
# the amqp endpoint. Should later be changed to an environment variable
amqp_hostname = os.getenv('RABBITMQ_HOST', '192.168.2.119')
# the redis endpoint. Should later be changed to an environment variable
redis_hostname = os.getenv('REDIS_HOST', '192.168.2.119')
# the amqp username ==> guest is the default
amqp_username = os.getenv('RABBITMQ_USER', 'guest')
# the amqp passwort ==> guest is the default
amqp_password = os.getenv('RABBITMQ_PASSWORD', 'guest')
mongodb_uri = os.getenv(
    'MONGODB_CONNECTION_URI',
    'mongodb://root:example@192.168.2.119/orchestrator_db?authSource=admin'
)
ldap_host = os.getenv('LDAP_HOST', '192.168.2.119')
ldap_username = 'Administrator@ad.lan'
ldap_password = 'Start123'
print(amqp_hostname)
# number of connections to use for the TaskQueue.
# Currently work in progress
# and not fully working as intended.
worker_count = 10

redis_client = RedisClient(redis_hostname)
mongodb_client = MongoDBClient(mongodb_uri)

# create the main TaskQueue object
task_queue = TaskQueue(
    node_name=node_name,
    worker_count=worker_count,
    amqp_host=amqp_hostname,
    redis_client=redis_client,
    mongodb_client=mongodb_client,
    task_queue='task_queue',
    wait_queue='wait_queue',
    incoming_blocked_queue='incoming_blocked_queue',
    wait_blocked_queue='wait_blocked_queue',
    amqp_username=amqp_username,
    amqp_password=amqp_password)
task_handler = task_queue.task_handler

counter = 0


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


@task_queue.task('ldap')
def ldap(
    ldap_host: str,
    ldap_action: str,
    ldap_revoke_action: str,
    ldap_group: str,
    resource: str
):
    print('ldap action started')
    print('ldap action ended')


@task_queue.task('vault')
def vault(secrets_engine_path: str, vault_host: str, resource: str):
    print('vault action started')
    print('vault action ended')


def get_next_task(current_task: str):
    # return next task to be run
    # 1. fetch workflow from database
    # 2. return next workflow after current workflow
    return Task('')


@task_queue.task('jit_entry_task')
def jit_entry_task():
    # run get_task to get the next task in the defined chain
    return get_next_task('jit_entry_task')


@task_queue.task('jit_approval_needed')
def jit_approval_needed():
    return get_next_task('jit_approval_needed')


@task_queue.task('jit_check_approval')
def jit_check_approval():
    # check, if the ApprovedResource entry is in database
    return get_next_task('jit_check_approval')


@task_queue.task('jit_is_granted')
def jit_is_granted():
    return get_next_task('jit_is_granted')


@task_queue.task('jit_end_task')
def jit_end_task():
    # report to jit, that the workflow ended
    return None


# @task_queue.task('send_feedback')
# def send_feedback(feedback):
#     print('feedback: %s' % (feedback, ))
#     return None


# @task_queue.task()
# def workflow_task():
#     print('schedule next task: send_feedback')
#     return Task(name='send_feedback', arguments={'feedback': 'success'})


# @task_queue.task()
# def failed_task(failed_counter: int):
#     print('Error, task failed!')
#     failed_counter = failed_counter + 1
#     if failed_counter >= 3:
#         return None
#     return False, {'failed_counter': failed_counter}


# @task_queue.task()
# def chained_task_01():
#     print('chained_task_01')
#     return Task('chained_task_02', {'arg1': 'test01'})


# @task_queue.task()
# def chained_task_02(arg1: str = 'test02'):
#     print('chained_task_02')
#     print('arg1_chained_task_02: ' + arg1)
#     if arg1 == 'test01':
#         print('...')
#         return 'chained_task_03', {'arg2': 'test'}
#     else:
#         return None


# @task_queue.task()
# def chained_task_03(arg2: str):
#     print('chained_task_03')
#     return chained_task_02, {'arg1': 'test02'}


# @task_queue.task()
# def vault_test():
#     client = hvac.Client(url='http://127.0.0.1:8200')
#     client.token = 's.XOH9G9xrusjulIDwL7Ekzc5D'

#     authenticated = client.is_authenticated()

#     print(authenticated)

#     client.secrets.kv.v2.configure(
#         max_versions=20,
#         mount_point='kv',
#     )

#     create_response = client.secrets.kv.v2.create_or_update_secret(
#         path='hvac',
#         secret=dict(pssst='this is secret'),
#         mount_point='kv',
#     )

#     print(create_response)

#     kv_configuration = client.secrets.kv.v2.read_configuration(
#         mount_point='kv',
#     )

#     print('Config under path "kv": max_versions set to "{max_ver}"'.format(
#         max_ver=kv_configuration['data']['max_versions'],
#     ))
#     print(
#         'Config under path "kv": check-and-set require flag '
#         'set to {cas}'.format(
#             cas=kv_configuration['data']['cas_required'],
#         )
#     )

#     secret_version_response = client.secrets.kv.v2.read_secret_version(
#         path='hvac',
#         mount_point='kv',
#     )

#     print(
#         'Latest version of secret under path "hvac" '
#         'contains the following keys: {data}'.format(
#             data=secret_version_response['data']['data'].keys(),
#         )
#     )
#     print(
#         'Latest version of secret under path "hvac" '
#         'created at: {date}'.format(
#             date=secret_version_response['data']['metadata']['created_time'],
#         )
#     )
#     print(
#         'Latest version of secret under path "hvac" '
#         'is version #{ver}'.format(
#             ver=secret_version_response['data']['metadata']['version'],
#         )
#     )
#     print(
#         'Latest version of secret under path "hvac" '
#         'is version #{ver}'.format(
#             ver=secret_version_response['data']['metadata']['version'],
#         )
#     )
#     print(
#         'Latest version of secret under path "hvac" '
#         'is <s>{}</s>'.format(
#             secret_version_response['data']['data']['pssst']
#         )
#     )


# get_users_workflow = GetUsersWorkflow(ldap_host, ldap_username, ldap_password)
# task_queue.add_task(get_users_workflow.get_users, 'ldap_get_users')
# ldap_access = LDAPAccess(
#     ldap_host, ldap_username, ldap_password, ldap_username, ldap_password)
# task_queue.add_task(ldap_access.grant_access, 'grant_access')
# task_queue.add_task(ldap_access.revoke_access, 'revoke_access')
# task_queue.add_task(ldap_access.revoke_access_in_jit, 'revoke_access_in_jit')

# workspace one is still in progress. Currently disabled.
# workspace_one = WorkspaceOne()
# task_queue.task('ws1_get_users')(workspace_one.get_users)

if __name__ == '__main__':
    task_queue.listen()

    # time.sleep(1000)
    while(True):
        time.sleep(0.01)  # keep mainthread running
