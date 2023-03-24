# Job

::: executor.engine.job.base.Job
    handler: python
    options:
        show_root_heading: true

::: executor.engine.job.process.ProcessJob
    handler: python
    options:
        show_root_heading: true

::: executor.engine.job.thread.ThreadJob
    handler: python
    options:
        show_root_heading: true

::: executor.engine.job.local.LocalJob
    handler: python
    options:
        show_root_heading: true

::: executor.engine.job.dask.DaskJob
    handler: python
    options:
        show_root_heading: true
