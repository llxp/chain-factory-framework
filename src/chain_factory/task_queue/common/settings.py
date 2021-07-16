"""
wait_time ==>       Time to wait after a task has been
                    rejected/rescheduled/sent to another blocked/waiting queue
prefetch_count ==>  Global setting to control the QoS in the amqp library,
                    it specifies how many messages whould be prefetched
                    from the server.
worker_count ==>    Amount of worker threads to spawn.
                    Later there can be configured spcific blocklists
                    of functions to only run e.g. on nodes,
                    which have more threads for smaller tasks
"""


# seconds to wait between each queue fetch
wait_time = 5
# how many tasks should be prefetched by amqp library
prefetch_count = 1
# number of threads to listen on the queue
worker_count = 10
# when should a waiting task be put back to the main/task queue
max_task_age_wait_queue: int = 30  # seconds
# if sticky_tasks option is set,
# # only execute the full workflow on the node it started on
sticky_tasks = False
# number of times a task can be rejected,
# until it will be put on the wait queue
reject_limit = 10
# performs a check, if the current node name already exists
# throws an exception, if this option is set and the current node name
# is already registered
unique_hostnames = False
# override an existing node registration
force_register = True
# can be used to suppress all stdout output
task_log_to_stdout = True
# can be used to suppress all external logging
task_log_to_external = True
# redis key to mark a workflow as finished, if an entry is found in this list
workflow_status_redis_key = 'workflow_status'
# redis key to mark a task as finished, if an entry is found in this list
task_status_redis_key = 'task_status'
# heartbeat configuration
# redis key, which should be updated
heartbeat_redis_key = 'heartbeat'
# wait time between each update
heartbeat_sleep_time: int = 1  # seconds
# redis key, which should hold the block list for the normal block list
incoming_block_list_redis_key = 'incoming_block_list'
# redis key, which should hold the block list for the wait block list
wait_block_list_redis_key = 'wait_block_list'
# task control channel redis key
task_control_channel_redis_key = 'task_control_channel'
# node control channel redis key
node_control_channel_redis_key = 'node_control_channel'
# redis host
redis_host = 'redis'
# redis port
redis_port = 6379
# redis password
redis_password = None
# redis db
redis_db = 0
# mongodb connection string
mongodb_connection = 'mongodb://root:example@mongodb/db?authSource=admin'
# amqp username
amqp_username = 'guest'
# amqp password
amqp_password = 'guest'
# amqp host
amqp_host = ''
task_queue = 'task_queue'
wait_queue = 'wait_queue'
incoming_blocked_queue = 'incoming_blocked_queue'
wait_blocked_queue = 'wait_blocked_queue'
namespace = 'root'
# maximum time in seconds a task can run, until it will be aborted
task_timeout = None
task_repeat_on_timeout = False
