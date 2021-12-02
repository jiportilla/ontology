import sys
from base import RedisClient
from taskadmin import BatchWorkerEnvironmentVars, BatchStage, BatchWorkerQueues

if __name__ == "__main__":
    last_stage = ''
    restart = False
    if len(sys.argv) >= 2:
        last_stage = sys.argv[1]
        restart = last_stage.lower() == 'restart'
        if restart:
            print('Restarting failed tasks')
        else:
            print(f'Starting after stage {last_stage}')
    else:
        print('Starting on the first stage')

    redis = RedisClient(decode_responses=False).redis
    if restart:
        BatchStage.restart_failed_stage()
    else:
        redis.flushdb()
        BatchWorkerQueues.clear_max_wip()
        BatchWorkerEnvironmentVars().push()
        # BatchStage.clean_rq()
        BatchStage.next(last_stage).process()
    print('Bye!')
