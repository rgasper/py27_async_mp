'''
async_mp_cores.py
work and management functions for a bounded (in memory and cpu usage), logged, 
and fully asynchronous multiprocess ETL application
design prevents unbounded memory/process number growth that can occur in multiprocess ETL python applications
process creation and death overhead will dominate if work elements are small
Author: Raymond Gasper
'''
from logging import debug, info
from multiprocessing import Process
from time import sleep, time
from queue import Empty
from math import log as ln

def producer(num_elems, queue):
    ''' pushes stuff into the queue '''
    info('starting')
    for _ in range(num_elems):
        queue.put(range(1000))


def worker(in_queue, out_queue, poisonPill):
    ''' grabs input, does some work, and pushes results. ping-pongs the poisonPill '''
    i = in_queue.get()
    debug('doing work')
    j = [x**2 for x in i]
    out_queue.put(j)
    return


def consumer(out_queue, expected_num_results):
    ''' does something with the ETL results '''
    r_count = 0
    log_progress = False
    if expected_num_results > 999:
        log_progress = True
        has_logged = False
    while True:
        _ = out_queue.get()
        r_count += 1
        debug('result gotten')
        if log_progress:
            progress = int(float(r_count)/expected_num_results * 100)
            if progress % 10 == 0:
                if not has_logged:
                    info('pipeline {}% complete'.format(progress))
                    has_logged = True
            else:
                has_logged = False
        if r_count == expected_num_results:
            info('finished with expected number of result elements')
            return


def manager(in_queue, n_processes, poisonPill, worker, worker_args):
    ''' manages a set of concurrent processes defined by target and args 
    does not deliver or return any data 
    :params:
        in_queue: multiprocessing.Queue- input data for workers. manager observes queue status
        n_processes: int- number concurrent processes
        poisonPill: multiprocessing.Value- flag provided that indicates no further input data
        target: function- worker function. Will be daemonized, so can have no child processes
        worker_args: tuple- arguments for worker function'''
    # keep constant size objects, or pool can be a source of memory issues
    pool = {i:None for i in range(n_processes)}
    proc_time_tracker = {i:0 for i in range(n_processes)}
    n_procs, avg_duration = 0, 0.0
    empty_repeat_limit, repeats = int(ln(n_processes)*2)+1, 0
    info('managing')
    while True:
        sleep(max(0.1, 2*avg_duration/float(n_processes)))
        if bool(poisonPill.value):
            if in_queue.empty():
                repeats += 1
            if repeats > 0:
                debug('{} consecutive empty queue checks'.format(repeats))
            if repeats > empty_repeat_limit:
                break
        for i, p in pool.iteritems():
            if p is None:
                debug('starting a worker')
                new_p = Process(
                    target = worker,
                    args = worker_args,
                )
                new_p.daemon = True
                new_p.start()
                proc_time_tracker[i] = time()
                pool[i] = new_p
            elif not p.is_alive():
                worker_duration = time() - proc_time_tracker[i]
                debug('replacing a worker')
                new_p = Process(
                    target = worker,
                    args = worker_args,
                )
                new_p.daemon = True
                new_p.start()
                proc_time_tracker[i] = time()
                pool[i] = new_p
                # update duration statistic
                n_procs += 1
                diff = worker_duration - avg_duration
                avg_duration += float(diff)/n_procs

    info('all work consumed, shutting down')
    info('manager done, average worker element process time: {:.5f}s'.format(avg_duration))
    return


if __name__ == "__main__":
    raise NotImplementedError('use async_main.py')