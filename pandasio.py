# -*- coding: utf-8 -*-

import multiprocessing
import time
import numpy as np
import pandas as pd


class Worker(multiprocessing.Process):
    def __init__(self, job_queue):
        super().__init__()
        self.job_queue = job_queue
        
    def run(self):
        while True:
            next_task = self.job_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                self.job_queue.task_done()
                break
            print('{}: {}'.format(self.name, next_task))
            result = next_task()
            self.job_queue.task_done()
        return


class Task(object):
    def __init__(self, df, *args, **kwargs):
        self.df = df
        self.args = args
        self.kwargs = kwargs
        
    def __call__(self):
        self.df.to_sql(*self.args, **self.kwargs)


def to_sql(df, *args, **kwargs):
    """Write records stored in a DataFrame to a SQL database using
    multiple processes.

    """
    min_length = 1000
    initial_chunksize = 100

    # If df is too small, just writes it directly
    if len(df) <= min_length:
        df.to_sql(*args, **kwargs)
        return

    # Write a few rows to create the table
    df.iloc[:initial_chunksize, :].to_sql(*args, **kwargs)

    # Following processes can only append to table
    if kwargs['if_exists'] == 'replace':
        kwargs['if_exists'] = 'append'

    # Establish communication queues
    job_queue = multiprocessing.JoinableQueue()

    # Start consumers
    num_workers = multiprocessing.cpu_count()
    workers = [Worker(job_queue,) for _ in range(num_workers)]
    for w in workers:
        w.start()

    # Enqueue jobs
    for df_chunk in np.array_split(df, num_workers):
        job_queue.put(Task(df_chunk, *args, **kwargs))

    # Wait for all of the tasks to finish
    job_queue.join()

    for _ in range(num_workers):
        job_queue.put(None)
