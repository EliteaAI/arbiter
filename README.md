# Arbiter
Distributed tasks queue use Redis as broker. Consists of arbiter and minion.

## Installation

Clone git repo
```bash
git clone https://github.com/EliteaAI/arbiter.git
```
and install by running
```bash
cd arbiter
python setup.py install
```

## Basic scenario

Launch Redis, as it required for everything to work
```bash
docker run -d --rm --hostname arbiter-redis --name arbiter-redis \
           -p 6379:6379 redis:alpine redis-server
```

### Create simple task
You need to initiate minion and provide connection details:
```python
from arbiter import RedisEventNode
from arbiter import Minion

event_node = RedisEventNode(host="localhost", port=6379, password="", event_queue="tasks")
app = Minion(event_node, queue="default")
```
then you can declare tasks by decorating callable with `@app.task`
```python
@app.task(name="simple_add")
def adds(x, y):
    return x + y
```
Every task need to have a name, which it will be referred by when initiated from arbiter.
this is pretty much it to create first task

Now we need to create execution point
```python
if __name__ == "__main__":
    app.run(workers=3)
```
where `workers` is a quantity of worker slots to do the job(s)

Run created script. Minion is ready to accept work orders.

### Call created task from arbiter
Arbiter is job initiator, it maintain the state of all jobs it created and can retrieve results.

Each arbiter have it's own communication channel, so job results won't mess between two different arbiters

Declaring the arbiter
```python
from arbiter import RedisEventNode
from arbiter import Arbiter

event_node = RedisEventNode(host="localhost", port=6379, password="", event_queue="tasks")
arbiter = Arbiter(event_node)
```
to call the task and track it till it done (tasks are obviously async)
```python
task_keys = arbiter.apply("simple_add", tasks_count=1, task_args=[1, 2]) #  will return array of task ids

# while loop with returns results of each task once it done
for message in arbiter.wait_for_tasks(task_keys):
    print(message)
```
Alternatively you can get task result by calling
```python
arbiter.status(task_keys[0])
```
it will return `json` where `result` will be one of the keys
