<div align="center">
<h1> Executor engine </h1>

<p> Package for job execution management. </p>

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
</p>
</div>


**Work In Progress**

## Introduction

Executor engine is a package for job execution management. It supports multiple job types, such as `LocalJob`, `ThreadJob`, `ProcessJob`, `DaskJob` and so on. Each job type has its own advantages and disadvantages. You can choose the most suitable job type for your task. It also supports conditional job execution. You can set the condition for a job, such as `AfterAnother`, `AfterTimepoint` and so on. It will help you to create workflows in an easy way.

### Features

+ Support multiple job types:
  * LocalJob
  * ThreadJob
  * ProcessJob
  * DaskJob
  * Extend job types:
    - SubprocessJob
    - WebappJob
+ Job status management.
  * Cancel
  * Re-run
  * Auto retry on failure.
+ Support conditional job execution.
  * After another job.
  * After a time point.
  * Condition combination.
    - All conditions are met.
    - Any condition is met.
+ The task API for create workflow in an easy way.
+ Provide async and sync API.
+ 100% test coverage.

## Install

```bash
pip install executor-engine
```

## Examples

```python
from executor.engine import Engine, ProcessJob

engine = Engine()
engine.start()

def add(a, b):
    return a + b

job = ProcessJob(add, args=(1, 2))
future = engine.submit(job)
print(future.result())

engine.stop()
```

Use with asyncio:

```python
from executor.engine import Engine, ProcessJob

engine = Engine()

def add(a, b):
    return a + b

async def main():
    job1 = ProcessJob(add, args=(1, 2))
    job2 = ProcessJob(add, args=(3, 4))
    await engine.submit_async(job)
    await engine.join()

asyncio.run(main())
print(job1.result())
print(job2.result())
```


## TODO List

- [x] Task design.
- [x] Job retry.
- [x] Dask job.
- [x] Change engine's API to sync mode.
- [x] Logging system.
- [ ] Documentation.


## Related Projects

+ [executor-http](https://github.com/Nanguage/executor-http) - HTTP server and client for executor engine.
+ [executor-view](https://github.com/Nanguage/executor-view) - Web interface for executor HTTP server.
