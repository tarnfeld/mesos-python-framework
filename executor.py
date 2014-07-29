#!/usr/bin/env python

#                                _                                     _
#   _____  ____ _ _ __ ___  _ __ | | ___        _____  _____  ___ _   _| |_ ___  _ __
#  / _ \ \/ / _` | '_ ` _ \| '_ \| |/ _ \_____ / _ \ \/ / _ \/ __| | | | __/ _ \| '__|
# |  __/>  < (_| | | | | | | |_) | |  __/_____|  __/>  <  __/ (__| |_| | || (_) | |
#  \___/_/\_\__,_|_| |_| |_| .__/|_|\___|      \___/_/\_\___|\___|\__,_|\__\___/|_|
#                          |_|
#

import logging
import threading
import time

import pesos.api
import pesos.executor
from pesos.vendor.mesos import mesos_pb2

logger = logging.getLogger(__name__)


class ExampleExecutor(pesos.api.Executor):

    def launch_task(self, driver, task):

        logger.info("HELLO I AM TASK")

        def run_task():
            logger.info("Launching task %s", task.task_id.value)

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.send_status_update(update)

            time.sleep(15)

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED

            # Send the terminal update
            driver.send_status_update(update)

        thread = threading.Thread(target=run_task)
        thread.daemon = True
        thread.start()


if __name__ == "__main__":

    for l in ('pesos', 'compactor', 'tornado', '__main__'):
        l = logging.getLogger(l)
        l.setLevel(logging.DEBUG)

    # Launch the executor driver
    driver = pesos.executor.MesosExecutorDriver(ExampleExecutor())

    status = 0
    if driver.run() == mesos_pb2.DRIVER_STOPPED:
        status = 1

    driver.stop()
    exit(status)
