#!/usr/bin/env python

#                                _                                     _
#   _____  ____ _ _ __ ___  _ __ | | ___        _____  _____  ___ _   _| |_ ___  _ __
#  / _ \ \/ / _` | '_ ` _ \| '_ \| |/ _ \_____ / _ \ \/ / _ \/ __| | | | __/ _ \| '__|
# |  __/>  < (_| | | | | | | |_) | |  __/_____|  __/>  <  __/ (__| |_| | || (_) | |
#  \___/_/\_\__,_|_| |_| |_| .__/|_|\___|      \___/_/\_\___|\___|\__,_|\__\___/|_|
#                          |_|
#

import threading
import random
import time
import sys

import mesos
import mesos_pb2


class ExampleExecutor(mesos.Executor):

    def launchTask(self, driver, task):

        print >> sys.stderr, "HELLO"

        def run_task():
            print >> sys.stderr, "Launching task %s" % (task.task_id.value)

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            time.sleep(15)

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED

            # Send the terminal update
            driver.sendStatusUpdate(update)

        thread = threading.Thread(target=run_task)
        thread.daemon = True
        thread.start()


if __name__ == "__main__":

    # Launch the executor driver
    driver = mesos.MesosExecutorDriver(ExampleExecutor())

    status = 0
    if driver.run() == mesos_pb2.DRIVER_STOPPED:
        status = 1

    driver.stop()
    exit(status)
