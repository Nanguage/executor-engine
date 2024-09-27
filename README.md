<div align="center">
<h1> Executor Engine 🚀 </h1>

<p> Effortless, flexible, and powerful job execution engine. </p>

<p>
  <a href="https://github.com/Nanguage/executor-engine/actions/workflows/build_and_test.yml">
      <img src="https://github.com/Nanguage/executor-engine/actions/workflows/build_and_test.yml/badge.svg" alt="Build Status">
  </a>
  <a href="https://app.codecov.io/gh/Nanguage/executor-engine">
      <img src="https://codecov.io/gh/Nanguage/executor-engine/branch/master/graph/badge.svg" alt="codecov">
  </a>
  <a href="https://pypi.org/project/executor-engine/">
    <img src="https://img.shields.io/pypi/v/executor-engine.svg" alt="Install with PyPI" />
  </a>
  <a href="https://github.com/Nanguage/executor-engine/blob/master/LICENSE">
    <img src="https://img.shields.io/github/license/Nanguage/executor-engine" alt="MIT license" />
  </a>
  <a href="https://executor-engine.readthedocs.io/en/latest/">
  	<img src="https://readthedocs.org/projects/executor-engine/badge/?version=latest" alt="Documentation">
  </a>
</p>
</div>


Executor Engine 🚀 is a powerful and versatile package designed for managing and streamlining job execution across various platforms. With support for multiple job types, including `LocalJob`, `ThreadJob`, `ProcessJob`, `DaskJob`, and more, Executor Engine provides flexibility and adaptability for a wide range of tasks. The package also offers extensible job types, such as `SubprocessJob` and `WebappJob`, ensuring that your workflow can be easily customized to meet specific requirements.

By harnessing the capabilities of Executor Engine, users can effortlessly construct parallel workflows to optimize their processing pipeline. The engine facilitates conditional job execution, allowing for the configuration of conditions such as `AfterAnother`, `AfterTimepoint`, and more. This level of customization simplifies the creation of complex, parallel workflows and maximizes efficiency.


## ✨ Features

+ 📚 Support multiple job types
  * `LocalJob`, `ThreadJob`, `ProcessJob`, `DaskJob`
  * Extend job types: `SubprocessJob`, `WebappJob`
+ 🔧 Job management
  * Job dependency management.
  * Job status: Pending, Running, Done, Failed, Cancelled.
  * Limit the number of concurrent jobs.
  * Status management: Cancel, Re-run, ...
  * Auto retry on failure.
  * Serilization and deserialization.
+ ⏱️ Conditional job execution.
  * `AfterAnother`, `AfterOthers`: After another job or jobs done/failed/cancelled.
  * `AfterTimepoint`: After a time point.
  * Condition combination:
    - `AllSatisfied`: All conditions are met.
    - `AnySatisfied`: Any condition is met.
  * Allow user to define custom condition.
+ 🚀 The launcher API for create parallel workflow in an easy way.
+ ⚡ Provide async and sync API, fully compatible with asyncio.
+ 🎯 100% test coverage.


See our [documentation](https://executor-engine.readthedocs.io/en/latest/) for more details.


## 📦 Installation

```bash
pip install executor-engine
```

With dask support:

```bash
pip install "executor-engine[dask]"
```

## 💡 Examples

A simple example:

```python
from executor.engine import Engine, ProcessJob

def add(a, b):
    return a + b

with Engine() as engine:
    # job1 and job2 will be executed in parallel
    job1 = ProcessJob(add, args=(1, 2))
    job2 = ProcessJob(add, args=(3, 4))
    # job3 is dependent on job1 and job2
    job3 = ProcessJob(add, args=(job1.future, job2.future))
    engine.submit(job1, job2, job3)
    engine.wait_job(job3)  # wait for job3 done
    print(job3.result())  # 10

```

Async mode example:

```python
from executor.engine import Engine, ProcessJob
import asyncio

engine = Engine()

def add(a, b):
    return a + b

async def stream():
    for i in range(5):
      await asyncio.sleep(0.5)
      yield i

async def main():
    job1 = ProcessJob(add, args=(1, 2))
    job2 = ProcessJob(add, args=(job1.future, 4))
    await engine.submit_async(job1, job2)
    await engine.join()
    print(job1.result())  # 3
    print(job2.result())  # 7

    # generator
    job3 = ProcessJob(stream)
    # do not do engine.wait because the generator job's future is done only when StopIteration
    await engine.submit_async(job3)
    async for x in job3.result():
        print(x)

asyncio.run(main())
# or just `await main()` in jupyter environment
```


## 🔗 Related Projects

+ [executor-http](https://github.com/Nanguage/executor-http): HTTP server and client for executor engine.
+ [executor-view](https://github.com/Nanguage/executor-view): Web interface for executor HTTP server.
