# Getting Started

## 📦 Installation

```bash
pip install executor-engine
```

With dask support:

```bash
pip install "executor-engine[dask]"
```

## 🚀 Basic usage

`Engine` is the core object of executor-engine. It manages the job execution.
You can create an `Engine` object and submit jobs to it, then wait for the jobs to finish:

```python
from executor.engine import Engine, ProcessJob

engine = Engine()
engine.start()

def add(a, b):
    return a + b

job1 = ProcessJob(add, args=(1, 2))
job2 = ProcessJob(add, args=(job1.future, 4))  # job2 depends on job1
engine.submit(job1, job2)
engine.wait_job(job2)
print(job2.result())  # 7

engine.stop()
```

`Engine` object allow using the `with` statement to manage the engine's lifecycle.

```python
from executor.engine import Engine, LocalJob, ThreadJob, ProcessJob

def add(a, b):
    return a + b

with Engine() as engine:
    job1 = LocalJob(add, args=(1, 2))
    job2 = ThreadJob(add, args=(3, 4))
    job3 = ProcessJob(add, args=(5, 6))
    engine.submit(job1, job2, job3)
    engine.wait()  # wait all job finished
    print(job1.result())  # 3
    print(job2.result())  # 7
    print(job3.result())  # 11
```

Use with asyncio:

```python
from executor.engine import Engine, ProcessJob
import asyncio

engine = Engine()

def add(a, b):
    return a + b

async def main():
    job1 = ProcessJob(add, args=(1, 2))
    job2 = ProcessJob(add, args=(job1.future, 4))
    await engine.submit_async(job1, job2)
    await engine.join()
    print(job1.result())  # 3
    print(job2.result())  # 7

asyncio.run(main())
# or just `await main()` in jupyter environment
```

## 🧰 Extend job types

### SubprocessJob

`SubprocessJob` is a job type for executing shell commands.
`SubprocessJob` accept a shell command as its argument. It will execute the command in a subprocess:

```python
from executor.engine import Engine
from executor.engine.job.extend import SubprocessJob

job = SubprocessJob(
    "python -c 'print(1 + 2)'",
)

with Engine() as engine:
    engine.submit(job)
    engine.wait_job(job)
```


### WebappJob

`WebappJob` is a job type for launching a web application.
It can accept a function with `ip` and `port` as arguments:

```python
from executor.engine import Engine
from executor.engine.job.extend import WebAppJob
from http.server import HTTPServer, SimpleHTTPRequestHandler

def run_simple_httpd(ip: str, port: int):
    server_addr = (ip, port)
    httpd = HTTPServer(server_addr, SimpleHTTPRequestHandler)
    httpd.serve_forever()

with Engine() as engine:
    job = WebAppJob(run_simple_httpd, ip="127.0.0.1", port=8000)
    engine.submit(job)
    print("Open your browser and visit http://127.0.0.1:8000")
    engine.wait()
```

`WebappJob` can also accept a command template as its argument:

```python
from executor.engine import Engine
from executor.engine.job.extend import WebAppJob

with Engine() as engine:
    job = WebAppJob(
        "python -m http.server -b {ip} {port}",
        ip="127.0.0.1", port=8000)
    engine.submit(job)
    print("Open your browser and visit http://127.0.0.1:8000")
    engine.wait()
```

## 📝 Conditional job execution

After another job:

```python
from executor.engine import Engine, ProcessJob
from executor.engine.job.condition import AfterAnother

def add(a, b):
    print(f"add({a}, {b})")
    return a + b

with Engine() as engine:
    job1 = ProcessJob(add, args=(1, 2))
    job2 = ProcessJob(add, args=(3, 4), condition=AfterAnother(job_id=job1.id))
    engine.submit(job1, job2)
    # job2 will be executed after job1 finished
    engine.wait()
```

After other jobs:

```python
from executor.engine import Engine, ProcessJob
from executor.engine.job.condition import AfterOthers

def add(a, b):
    print(f"add({a}, {b})")
    return a + b

with Engine() as engine:
    job1 = ProcessJob(add, args=(1, 2))
    job2 = ProcessJob(add, args=(3, 4))
    job3 = ProcessJob(add, args=(5, 6), condition=AfterOthers(job_ids=[job1.id, job2.id]))
    engine.submit(job3, job2, job1)
    # job3 will be executed after job1 and job2 finished
    engine.wait()
```

After a time point:

```python
from executor.engine import Engine, ProcessJob
from executor.engine.job.condition import AfterTimepoint
from datetime import datetime, timedelta

def print_hello():
    print("Hello")

with Engine() as engine:
    now = datetime.now()
    after_5_seconds = now + timedelta(seconds=5)
    job = ProcessJob(
        print_hello,
        condition=AfterTimepoint(timepoint=after_5_seconds))
    # will print "Hello" after 5 seconds
    engine.submit(job)
    engine.wait()
```

### Condition combination

`AllSatisfied` is used to combine multiple conditions, all conditions must be satisfied to execute the job:

```python
from executor.engine import Engine, ThreadJob
from executor.engine.job.condition import AllSatisfied, AfterAnother

s = set()

job1 = ThreadJob(lambda: s.add(1))
job2 = ThreadJob(lambda: s.add(2))

def has_two_elements():
    assert len(s) == 2

job3 = ThreadJob(has_two_elements, condition=AllSatisfied(conditions=[
    AfterAnother(job_id=job1.id),
    AfterAnother(job_id=job2.id)
]))

with Engine() as engine:
    engine.submit(job3, job2, job1)
    engine.wait()
```

Similarly, `AnySatisfied` is used to combine multiple conditions, any condition is satisfied to execute the job:

```python
from executor.engine import Engine, ThreadJob
from executor.engine.job.condition import AnySatisfied, AfterAnother
import time

s = set()

def sleep_1s_and_add_1():
    time.sleep(1.0)
    s.add(1)

def has_one_element():
    # when job3 is executed, only job1 is finished
    assert len(s) == 1

with Engine() as engine:
    job1 = ThreadJob(lambda: s.add(1))
    job2 = ThreadJob(sleep_1s_and_add_1)
    job3 = ThreadJob(has_one_element, condition=AnySatisfied(conditions=[
        AfterAnother(job_id=job1.id),
        AfterAnother(job_id=job2.id)
    ]))
    engine.submit(job3, job2, job1)
    engine.wait()
```


### Custom condition

You can also define your own condition by inheriting `Condition` class:

```python
from executor.engine import Engine, ThreadJob
from executor.engine.job.condition import Condition
import random
from dataclasses import dataclass


@dataclass
class RandomCondition(Condition):
    probability: float = 0.5

    def satisfy(self, engine: "Engine") -> bool:
        p = random.random()
        print(f"p={p}")
        if p <= self.probability:
            return True
        else:
            return False


with Engine() as engine:
    job = ThreadJob(
        lambda: print("hi"),
        condition=RandomCondition(0.2),
        wait_time_delta=0.5)
    # job has a 20% chance to be executed at each 0.5 seconds
    engine.submit(job)
    engine.wait()
```
