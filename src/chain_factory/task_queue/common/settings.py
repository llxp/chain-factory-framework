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


wait_time = 5  # seconds to wait between each queue fetch
prefetch_count = 1
worker_count = 10
max_task_age_wait_queue: int = 30  # seconds
sticky_tasks = False
reject_limit = 10
unique_hostnames = False
force_register = True
task_log_to_stdout = True
task_log_to_redis = True
output_log_redis_key = 'output_log'
workflow_ids_redis_key = 'workflow_ids'
workflow_tasks_redis_key = 'workflow_tasks'
task_log_redis_key = 'task_log'
workflow_status_redis_key = 'workflow_status'
task_status_redis_key = 'task_status'
task_log_status_redis_key = 'task_log_status'
node_list_redis_key = 'node_list'
task_list_redis_key = 'task_list'
heartbeat_redis_key = 'heartbeat'
incoming_block_list_redis_key = 'incoming_block_list'
heartbeat_sleep_time: int = 1  # seconds
