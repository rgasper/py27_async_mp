'''
classes that can be used as the base of ETL pipelines. Pipeline characteristics:
    - work and management functions for a bounded (in memory and cpu usage), logged, 
        and fully asynchronous multiprocess ETL application
    - design prevents unbounded memory/process number growth that can occur in 
        multiprocess ETL python applications
    - process management will dominate usage if work elements are small
    - attempts to implement multiprocess logging (works in my tests, see run_pipe.py)

Author: Raymond Gasper
'''

from logging import debug, info, exception
from multiprocessing import Process, Manager
from multiprocessing_logging import install_mp_handler
from time import sleep, time
from queue import Empty
from math import log as ln
from inspect import isgeneratorfunction
# TODO from types import GeneratorType
# from random import randint # used to simulate data loss

def _producer(out_queue, total, producer_func, producer_config_args):
    ''' pushes stuff into the pipeline, dies when there is no more work 
    :params:
        out_queue - multiprocessing.Queue: where to put outgoing data
        total - multiprocessing.Value: track how many elements were generated
        producer_func - callable, generator: generates data
        producer_config_args - tuple: - any arguments required by producer_func'''
    info('starting')
    for i in producer_func(*producer_config_args):
        out_queue.put(i)
        total.value += 1


def _worker(in_queue, out_queue, worker_func, worker_config_args):
    ''' grabs input, does some work, pushes results, and dies
    in_queue - multiprocessing.Queue: where to get incoming data
    out_queue - multiprocessing.Queue: where to put outgoing data
    worker_func - callable: does something to the data
    worker_config_args - tuple: all arguments except first (input data) for worker'''
    i = in_queue.get()
    debug('working')
    r = worker_func(i, *worker_config_args)
    out_queue.put(r)


def _consumer(in_queue, total, consumer_func, consumer_config_args, flag):
    ''' does something with the pipeline results, like writing to storage     
    :params:
        in_queue - multiprocessing.Queue: where to get incoming data
        total - multiprocessing.Value: track how many elements were generated
        consumer_func - callable, generator: generates data
        consumer_config_args - tuple: - any arguments required by conusmer_func'''
    info('started')
    avg_wait = 0
    while not bool(flag.value):
        try:
            r = in_queue.get_nowait()
            wait_start = time()
        except Empty:
            sleep(0.01)
            continue
        # if randint(0,1):
        #     # simulate dropping data
        #     continue
        consumer_func(r, *consumer_config_args)
        total.value += 1
        wait = time() - wait_start
        diff = wait - avg_wait
        avg_wait += float(diff)/total.value
        # debug('consumed {}, total results: {}'.format(r, total.value))
        debug('total consumed: {}'.format(total.value))
    empties_limit, empties = 5, 0
    while empties < empties_limit:
        sleep(avg_wait/2)
        if in_queue.empty():
            empties += 1
            if empties > 1:
                debug('{} consecutive empty queue checks'.format(empties))
        try:
            r = in_queue.get(timeout=avg_wait/2)
            empties = 0
        except Empty:
            continue
        consumer_func(r, *consumer_config_args)
        total.value += 1
        # debug('consumed {}, total results: {}'.format(r, total.value))
        debug('total consumed: {}'.format(total.value))
    info('all data consumed')


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
    # NOTE: NEVER pull work element data into the manager. This could cause memory leaks.
    # all tracking data is statically sized (or the best we can do in python)- keeps memory use bounded
    pool = {i:None for i in range(n_processes)}
    proc_time_tracker = {i:0 for i in range(n_processes)}
    n_completed_procs, avg_duration = 0, 0.0
    empties_limit, empties = int(ln(n_processes)*2)+1, 0
    info('mgr {}: started'.format(tag))
    # quits if there appears to be no work to do
    while empties < empties_limit:
        # check if the workers have anything to do once every while
        # TODO this waits excessively long if there is only one process
        sleep(max(0.01, 2*avg_duration/float(n_processes)))
        if bool(flag.value): # no lock, only reading
            if in_queue.empty():
                empties += 1
                if empties > 1:
                    debug('mgr {}: {} consecutive empty queue checks'.format(
                        tag,
                        empties
                    ))
            else:
                empties = 0
        # start unused workers, or replace completed ones
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
    info('mgr {}: done, average worker element wait+work time: {:.5f}s'.format(tag, avg_duration))
    return


class ConcurrentSingleElementPipeline:
    ''' Runs a concurrent asynchronous ETL pipeline with the provided functions.
    Currently only allows single producer, single consumer, but multiple serial pipe 
    functions. Worker Processes restricted to handling one input element at a time.

    see run_pipe.py for simple example usage

    The funcs and matching args must be defined carefully!
    - producer function must be a generator function
    - pipe functions must take an input data object as their _first_ argument
    - consumer function must take an input data as its _first_ argument
    - In case of flawed data, pass nothing. class will check to see if number of produced
        elements at the start matches the number of consumed elements at the end

    When defining pipe_funcs, order matters!
    - first pipe_func receives input from producer
    - following pipe_funcs receives from previous, send to next
    - last pipe_func sends to consumer
    
    all functions (producer, pipe_funcs, and consumer) are responsible for their own input
    sanitation and error handling. All this does is manage process pools and data queues. 
    Pipe_funcs are daemonized and so cannot have their own child processes.

    If you want to use poisonPills to control consumer or worker behavior, just yield at the end of 
    the producer function.

    If you want data from the pipeline to pass into the main thread, DO NOT accumulate into a queue
    unless there is a main process thread async collecting. It will cause weird errors. 
    See run_pipe.py for a more detailed description.
    '''
    # TODO: allow producer_func to also be a producer_object (GeneratorType)
    #       complications:
    #           requires another _producer func
    #           requires ignoring producer_config_args
    # TODO: allow worker processes to grab multiple inputs before dying to reduce overhead
    #       complications (may just be able to ignore as defaults could be mostly safe?): 
    #           tuning how many are allowed (including picking a default)
    #           dealing with wildly inconsistent data element sizes
    #           on completion: rename class to ConcurrentPipeline
    # TODO: any way to allow just copying the full function args, not just "config" args?
    #           I am honestly unsure if this is possible
    # ?TODO?: implement backlog time estimates for each serial manager
    #           I am not sure it matters to anyone
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
        self.N = len(pipe_funcs) # used all over in here
        # setup handlers to send child process logs into main thread's logger
        install_mp_handler()
        self.producer_func = producer_func
        self.producer_config_args = producer_config_args
        self.pipe_funcs = pipe_funcs
        self.pipe_funcs_config_args = pipe_funcs_config_args
        self.pipe_n_procs = pipe_n_procs
        self.consumer_func = consumer_func
        self.consumer_config_args = consumer_config_args
        # using a manager server to make cleanup easy
        self._sync_server = Manager()
        # 1 manager for each pipe func
        self._managers = [None for _ in range(self.N)]
        # 1 producer finished flag for each manager, 1 for the consumer
        self._flags = [self._sync_server.Value('i',0) for _ in range(self.N+1)]
        # 1 out(in) queue per pipe_func, + 1 extra in(out)
        self._queues = [self._sync_server.Queue() for _ in range(self.N+1)]
        self._total_produced = self._sync_server.Value('i',0)
        self._total_consumed = self._sync_server.Value('i',0)

    def run(self):
        ''' execute the pipeline '''
        # let user know what they've asked for very clearly
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
            # start all child processes
            self._producer.start()
            [self._managers[i].start() for i in range(self.N)]
            self._consumer.start()
            # join in order
            self._producer.join()
            info('producer completed')
            for i in range(self.N):
                self._flags[i].value = int(True)
                info('flagged mgr {}'.format(i))
                self._managers[i].join()
            self._flags[-1].value = int(True)
            # some simple math to let the user know how long to wait if the consumer is backed up
            duration = time() - start_time
            elems_remaining = self._total_produced.value - self._total_consumed.value
            time_per_element = float(duration)/self._total_consumed.value
            info('flagged consumer. around {} elements left to consume, should take {} seconds'.format(
                elems_remaining, time_per_element*elems_remaining
            ))
            self._consumer.join()
            # check for data loss
            if self._total_consumed.value != self._total_produced.value:
                n_lost = self._total_produced.value - self._total_consumed.value
                pct_lost = 100 * (1 - float(self._total_consumed.value)/float(self._total_produced.value))
                try:
                    raise Warning('Pipeline appears to have lost {n} data elements, about {p:2f}% of the total produced'.format(n = n_lost, p = pct_lost))
                except:
                    exception('DATA LOSS WARNING:')
            # congratulate ourselves on our success
            dur = time() - start_time
            try:
                rate = float(self._total_consumed.value)/float(dur)
            except ZeroDivisionError:
                rate = float('nan')
            info('pipeline finished. took {d:2f}s, throughput of approximately {r:2f} elements/second'.format(
                d = dur,
                r = rate,
            ))
            return
        except KeyboardInterrupt:
            exception('user terminated pipeline')
            raise
        finally:
            info('cleaning up child processes')
            self._sync_server.shutdown()
            self._producer.terminate()
            [self._managers[i].terminate() for i in range(self.N)]
            self._consumer.terminate()


if __name__ == "__main__":
    raise NotImplementedError('use async_main.py')