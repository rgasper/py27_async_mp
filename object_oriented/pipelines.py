'''
classes that can be used as the base of ETL pipelines. Pipeline characteristics:
    - work and management functions for a bounded (in memory and cpu usage), logged, 
        and fully asynchronous multiprocess ETL application
    - design prevents unbounded memory/process number growth that can occur in 
        multiprocess ETL python applications
    - process management will dominate usage if work elements are small
    - attempts to implement multiprocess logging

Author: Raymond Gasper
'''

from logging import debug, info, exception
from multiprocessing import Process, Queue, Value
from multiprocessing_logging import install_mp_handler
from time import sleep, time
from queue import Empty
from math import log as ln
from inspect import isgeneratorfunction
from types import GeneratorType

def _producer(out_queue, total, producer_func, producer_config_args):
    ''' pushes stuff into the pipeline, dies when there is no more work'''
    info('starting')
    for i in producer_func(*producer_config_args):
        out_queue.put(i)
        with total.get_lock():
            total.value += 1


def _worker(in_queue, out_queue, worker_func, worker_config_args):
    ''' grabs input, does some work, pushes results, and dies '''
    i = in_queue.get()
    debug('working')
    r = worker_func(i, *worker_config_args)
    out_queue.put(r)


def _consumer(in_queue, total, consumer_func, consumer_config_args, flag):
    ''' does something with the pipeline results, like writing to storage '''
    info('started')
    while True:
        if bool(flag.value):
            break
        try:
            r = in_queue.get_nowait()
        except Empty:
            sleep(0.01)
            continue
        consumer_func(r, *consumer_config_args)
        with total.get_lock():
            total.value += 1
            debug('consumed {}, total results: {}'.format(r, total.value))
    info('all transformations completed, consuming remaining data')
    while True:
        try:
            r = in_queue.get_nowait()
            consumer_func(r, *consumer_config_args)
            with total.get_lock():
                total.value += 1
                debug('consumed {}, total results: {}'.format(r, total.value))
        except Empty:
            return


def manager(tag, in_queue, worker_func, worker_config_args, n_processes, flag):
    ''' manages a set of concurrent processes defined by worker_func and worker_config_args 
    does not deliver or return any data 
    :params:
        tag: printable - an identifier for this manager
        in_queue: multiprocessing.Queue- input data for workers. manager observes queue 
            status, but does not access any queued data directly
        worker_func: function- worker function. Will be daemonized.
        worker_config_args: tuple- arguments for worker function that aren't the input data
        n_processes: int- number concurrent processes
        flag: multiprocessing.Value- flag provided that indicates no further input data
    '''
    # keep constant size objects, or pool can be a source of memory issues
    pool = {i:None for i in range(n_processes)}
    proc_time_tracker = {i:0 for i in range(n_processes)}
    n_completed_procs, avg_duration = 0, 0.0
    empty_repeat_limit, repeats = int(ln(n_processes)*2)+1, 0
    info('mgr {}: started'.format(tag))
    while True:
        sleep(max(0.01, 2*avg_duration/float(n_processes)))
        if bool(flag.value): # no lock, as only reading
            if in_queue.empty():
                repeats += 1
            if repeats > 0:
                debug('mgr {}: {} consecutive empty queue checks'.format(
                    tag,
                    repeats
                ))
            if repeats > empty_repeat_limit:
                break
        for i, p in pool.iteritems():
            if p is None:
                debug('mgr {}: starting a worker'.format(tag))
                new_p = Process(
                    target = worker_func,
                    args = worker_config_args,
                )
                new_p.daemon = True
                new_p.start()
                proc_time_tracker[i] = time()
                pool[i] = new_p
            elif not p.is_alive():
                worker_duration = time() - proc_time_tracker[i]
                debug('mgr {}: replacing a worker'.format(tag))
                new_p = Process(
                    target = worker_func,
                    args = worker_config_args,
                )
                new_p.daemon = True
                new_p.start()
                proc_time_tracker[i] = time()
                pool[i] = new_p
                # update duration statistic
                n_completed_procs += 1
                diff = worker_duration - avg_duration
                avg_duration += float(diff)/n_completed_procs

    info('mgr {}: done, average worker element process time: {:.5f}s'.format(tag, avg_duration))
    return


class ConcurrentSingleElementPipeline:
    ''' Runs a concurrent asynchronous ETL pipeline with the provided functions.
    Currently only allows single producer, single consumer, but multiple serial pipe 
    functions. Worker Processes restricted handling one input element at a time.

    The funcs and matching args must be defined carefully!
    - producer function must be a generator function
    - pipe functions must take an input data object as their _first_ argument
    - consumer function must take an input data as its _first_ argument
    - In case of flawed data, pass nothing. class will check to see if number of produced
        elements at the start matches the number of consumed elements at the end

    In pipe_funcs, order matters!
    - first pipe_func receives input from producer
    - following pipe_funcs receives from previous, send to next
    - last pipe_func sends to consumer
    
    all functions (producer, pipe_funcs, and consumer) are responsible for their own input
    sanitation and error handling. All this does is manage process pools and data queues. 
    Pipe_funcs are daemonized and so cannot have their own child processes.

    see tests.py for simple examples of allowable functions
    '''
    # TODO: make class just return data as a generator if consumer is not provided
    # TODO: allow producer_func to instead be a producer_object (GeneratorType)
    def __init__(self, 
        producer_func, producer_config_args,
        pipe_funcs, pipe_funcs_config_args, pipe_n_procs,
        consumer_func, consumer_config_args,
        return_results = False):
        # enforce the contract.
        # check functions
        try:
            assert callable(producer_func)
            assert callable(consumer_func)
        except:
            raise AssertionError('must provide a callable function')
        try:
            assert isgeneratorfunction(producer_func)
        except:
            raise AssertionError('producer function must be a generator function')
        try:
            assert isinstance(pipe_funcs, tuple)
        except:
            raise AssertionError('must supply a tuple of callable functions')
        for pf in pipe_funcs:
            try:
                assert callable(pf)
            except:
                raise AssertionError('all elements inside of pipe_funcs must be callable functions')
        # check arguments
        try:
            assert isinstance(producer_config_args, tuple)
            assert isinstance(consumer_config_args, tuple)
        except:
            raise AssertionError('function arguments must be provided as a tuple')
        try:
            assert isinstance(pipe_funcs_config_args, tuple)            
            for pfa in pipe_funcs_config_args:
                assert isinstance(pfa, tuple)
        except:
            raise AssertionError('pipe function arguments must be provided as a tuple of tuples')
        # check procs
        try:
            assert isinstance(pipe_n_procs, tuple)
            for n in pipe_n_procs:
                assert isinstance(n, int)
        except:
            raise AssertionError('must provide a tuple of integers')
        # check agreement between corellated inputs
        try:
            assert len(pipe_funcs) == len(pipe_funcs_config_args) and len(pipe_funcs) == len(pipe_n_procs)
        except:
            raise AssertionError('must provide one tuple of arguments and a number of processes for each pipe function')
        try:
            assert len(pipe_funcs) != 0
        except:
            raise AssertionError('must provide work for the pipe to do')
        # contract satisfied
        self.producer_func = producer_func
        self.producer_config_args = producer_config_args
        self.pipe_funcs = pipe_funcs
        self.N = len(pipe_funcs) # called plenty so I tokenized it
        self.pipe_funcs_config_args = pipe_funcs_config_args
        self.pipe_n_procs = pipe_n_procs
        self.consumer_func = consumer_func
        self.consumer_config_args = consumer_config_args
        # 1 manager for each pipe func
        self._managers = [None for _ in range(self.N)]
        # 1 producer finished flag for each manager, 1 for the consumer
        self._flags = [Value('i',0) for _ in range(self.N+1)]
        # 1 out(in) queue per pipe_func, + 1 extra in(out)
        self._queues = [Queue() for _ in range(self.N+1)]
        self._total_produced = Value('i',0)
        self._total_consumed = Value('i',0)
        # pipe child process logs into main thread
        install_mp_handler()

    def run(self):
        ''' execute the pipeline '''
        struct_str = "Running pipeline with structure:\n"
        struct_str += "{} serial transformations, with {} queues\n\n".format(self.N, len(self._queues))
        struct_str += "Producer: {}\n".format(self.producer_func)
        for i in range(self.N):
            struct_str += "\tQueue {}: {}\n".format(i,self._queues[i])
            struct_str += "{} Workers: {}\n".format(self.pipe_n_procs[i], self.pipe_funcs[i])
        struct_str += "\tQueue {}: {}\n".format(self.N, self._queues[self.N])
        struct_str += "Consumer: {}\n".format(self.consumer_func)
        info(struct_str)
        # define processes
        self._producer = Process(
            target = _producer,
            args   = (
                self._queues[0],
                self._total_produced,
                self.producer_func,
                self.producer_config_args
            ),
        )
        self._consumer = Process(
            target = _consumer,
            args   = (
                self._queues[-1],
                self._total_consumed,
                self.consumer_func,
                self.consumer_config_args,
                self._flags[-1]
            ),
        )
        for i in range(self.N):
            self._managers[i] = Process(
                target = manager,
                args = (
                    i,
                    self._queues[i],
                    _worker,
                    (self._queues[i],self._queues[i+1],self.pipe_funcs[i], self.pipe_funcs_config_args[i]),
                    self.pipe_n_procs[i],
                    self._flags[i]
                ),
            )
        try:
            start_time = time()
            # start everyone
            self._producer.start()
            [self._managers[i].start() for i in range(self.N)]
            self._consumer.start()
            # join in order
            self._producer.join()
            info('producer completed')
            for i in range(self.N):
                with self._flags[i].get_lock():
                    self._flags[i].value = int(True)
                    info('flagged mgr {}'.format(i))
                self._managers[i].join()
            with self._flags[-1].get_lock():
                self._flags[-1].value = int(True)
                info('flagged consumer')
            self._consumer.join()
            # check for data loss
            if self._total_consumed.value != self._total_produced.value:
                n = self._total_produced.value - self._total_consumed.value
                p = 100 * (1 - float(self._total_consumed.value)/float(self._total_produced.value))
                Warning('Pipeline appears to have lost {n} data elements, about {p:2f}% of the total produced'.format(
                    n = n,
                    p = p,
                ))
            # congratulate ourselves on our success
            dur = time() - start_time
            try:
                rate = float(self._total_consumed.value)/float(dur)
            except ZeroDivisionError:
                rate = float('nan')
            info('pipeline finished. took {d:2f}s, throughput of approx. {r:2f} elements/second'.format(
                d = dur,
                r = rate,
            ))
        except KeyboardInterrupt:
            exception('user terminated pipeline')
            self._producer.terminate()
            [self._managers[i].terminate() for i in range(self.N)]
            self._consumer.terminate()
            raise
        finally:
            [q.close() for q in self._queues]






if __name__ == "__main__":
    raise NotImplementedError('use async_main.py')