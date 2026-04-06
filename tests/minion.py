from arbiter import Minion, ZeroMQEventNode, ZeroMQServerNode
from multiprocessing import Process
from time import sleep
import logging

server_node = ZeroMQServerNode(
    bind_pub="tcp://localhost:5010",
    bind_pull="tcp://localhost:5011",
)

event_node = ZeroMQEventNode(
    connect_sub="tcp://localhost:5010",
    connect_push="tcp://localhost:5011",
    topic="tasks",
)

app = Minion(event_node, queue="default")
app.raw_task_node.multiprocessing_context = "threading"
app.raw_task_node.thread_scan_interval = 0.5
app.raw_task_node.result_transport = "memory"


@app.task(name="add")
def add(x, y):
    logging.info("Running task 'add'")
    # task that initiate new task within same app
    increment = 0
    for message in app.apply('simple_add', task_args=[3, 4]):
        if isinstance(message, dict):
            increment = message["result"]
    return x + y + increment


@app.task(name="simple_add")
def adds(x, y):
    logging.info(f"Running task 'add_small' with params {x}, {y}")
    return x + y


@app.task(name="add_in_pipe")
def addp(x, y, upstream=0):
    logging.info("Running task 'add_in_pipe'")
    return x + y + upstream


@app.task(name="long_running")
def long_task():
    for _ in range(180):
        sleep(1)
    return "Long Task"


def run(rpc):
    if rpc:
        raise RuntimeError("No longer supported")
    #
    server_node.start()
    try:
        app.run(workers=10)
    finally:
        server_node.stop()


def start_minion(rpc: bool = False) -> Process:
    p = Process(target=run, args=(rpc,))
    p.start()
    sleep(5)  # some time to start Minion
    return p


def stop_minion(p: Process):
    p.terminate()
    p.join()


if __name__ == "__main__":
    run(False)
