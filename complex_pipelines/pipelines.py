'''
classes that can be used as the base of ETL pipelines. Pipeline characteristics:
    - work and management functions for a memory bounded, logged, 
        and fully asynchronous multiprocess ETL application
    - design prevents unbounded memory/process number growth that can occur in 
        multiprocess ETL python applications
    - process management will dominate usage if work elements are small
    - attempts to implement multiprocess logging

Author: Raymond Gasper

Usage Notes:
- Getting data out of the pipeline-
you have two options:
    - *concurrently* consume from an output queue in another thread/process. 
        WARNING! if you just accumulate into the queue and dont collect concurrently, 
            the pipeline will never finish (it will block on consumer.join())
        WARNING! if you do not use queue.task_done() the pipeline will never finish
    - write to an external data store (csv, database)

- Logging -
easiest and only currently verified way:
    - from logging import debug, info, warn, error, critical
    - put loglevel() calls in your work functions
    - use a dictConfig() at the very top of main .py

Memory stability testing:
I've tested upto 10K elements of 1000 integer long lists with 4 serial pools of 
25 concurrent workers, and have seen no evidence of memory load increasing during 
runtime. The main thread allocated a somewhat frightening 260 MB of memory (no work 
data, just child process info) but it stayed constant during runtime!
'''

# TODO: stop pipelines when child process raises an error
# TODO: somehow make worker time info more useful
#           only start tracking time once the queue has data?
# FIXME: add kwargs options to everything

from logging import debug, info, exception
from multiprocessing_logging import install_mp_handler
from multiprocessing import Process, Manager
from threading import Thread
from time import sleep, time
from queue import Empty
from math import log as ln
from inspect import isgeneratorfunction
# TODO from types import GeneratorType
# from random import randint # used to simulate data loss

def _producer(out_queue, total, producer_func, producer_config_args):
    ''' A generator that pushes stuff into the pipeline, 
    dies when there is no more work 
    :params:
        out_queue - multiprocessing.Queue: where to put outgoing data
        total - multiprocessing.Value: track how many elements were generated
        producer_func - callable, generator: generates data
        producer_config_args - tuple: - any arguments required by producer_func'''
    info('starting')
    for i in producer_func(*producer_config_args):
        out_queue.put(i)
        total.value += 1


def _worker(in_queue, out_queue, worker_func, worker_config_args, worker_get_limit):
    ''' grabs input, does some work, pushes results, and dies. Not intended to run
    on its own but to be created by a _manager
    :params:
        in_queue - multiprocessing.Queue: where to get incoming data
        out_queue - multiprocessing.Queue: where to put outgoing data
        worker_func - callable: does something to the data
        worker_config_args - tuple: all arguments except first (input data) for worker
        worker_get_limit: int- number of times worker gets and processes data before dying
    '''
    for _ in range(worker_get_limit):
        i = in_queue.get()
        debug('working')
        r = worker_func(i, *worker_config_args)
        in_queue.task_done()
        out_queue.put(r)


def _consumer(in_queue, total, consumer_func, consumer_config_args, flag):
    ''' does something with the pipeline results, like writing to storage    
    :params:
        in_queue - multiprocessing.Queue: where to get incoming data
        total - multiprocessing.Value: track how many elements were consumed
        consumer_func - callable: does the consuming work
        consumer_config_args - tuple: - all arguments except first (input data) for consumer
        flag: multiprocessing.Value- flag provided that indicates no further input data
    '''
    info('started')
    avg_wait = 0
    while True:
        sleep(max((0.01, avg_wait/5)))
        start = time()
        if bool(flag.value) and in_queue.empty():
            debug("consumer input queue is closed and empty")
            break
        try:
            r = in_queue.get_nowait()
            consumer_func(r, *consumer_config_args)
            in_queue.task_done()
            # debug('consumed {}, total results: {}'.format(r, total.value))
            debug('total consumed: {}'.format(total.value))
            total.value += 1
            wait = time() - start
            diff = wait - avg_wait
            avg_wait += float(diff)/total.value
        except Empty:
            continue
    info('completed')


def _io_thread(in_queue, connection, total, io_func, io_func_config_args, flag):
    ''' does something with the pipeline results, like writing to storage
    :params:
        in_queue - multiprocessing.Queue: where to get incoming data
        connection: shareable network IO connection- shared between the threads 
        total - multiprocessing.Value: track how many elements were consumed
        io_func - callable: pushes the data along the connection
        io_func_config_args - tuple: - all arguments except first two (input data, connection) for io func
        flag: multiprocessing.Value- flag provided that indicates no further input data
    '''
    info('started')
    avg_wait = 0
    while True:
        sleep(max((0.01, avg_wait/5)))
        start = time()
        if bool(flag.value) and in_queue.empty():
            debug("consumer input queue is closed and empty")
            break
        try:
            r = in_queue.get_nowait()
            io_func(r, connection, *io_func_config_args)
            in_queue.task_done()
            # debug('consumed {}, total results: {}'.format(r, total.value))
            debug('total consumed: {}'.format(total.value))
            total.value += 1
            wait = time() - start
            diff = wait - avg_wait
            avg_wait += float(diff)/total.value
        except Empty:
            continue
    info('completed')


def _proc_manager(
    tag, in_queue, worker_func, worker_args, n_processes, flag, worker_get_limit
    ):
    ''' process that manages a set of concurrent daemon child processes. Constantly restarts processes
    in order to clear up memory
    :params:
        tag: printable - an identifier for this manager
        in_queue: multiprocessing.Queue- input data for workers. manager observes queue 
            status, but does not access any queued data directly
        worker_func: callable- worker function. Daemonized to allow interrupts.
        worker_args: tuple- positional arguments for worker function
        n_processes: int- number concurrent processes
        flag: multiprocessing.Value- flag provided that indicates no further input data
        worker_get_limit: int- number of times workers get and process data before dying
    '''
    # NOTE: NEVER pull work element data into the manager. This could cause memory leaks.
    # all tracking data is statically sized (or the best we can do in python)- keeps memory use bounded
    pool = {i:None for i in range(n_processes)}
    proc_time_tracker = {i:0 for i in range(n_processes)}
    n_completed_procs, avg_duration = 0, 0.0
    info('mgr {}: started'.format(tag))
    while True:
        sleep(max(0.01, 2*avg_duration/float(n_processes)))
        if bool(flag.value) and in_queue.empty():
            debug("mgr {}: input queue is closed and empty".format(tag))
            break
        for i, p in pool.iteritems():
            if p is None:
                debug('mgr {}: starting a worker'.format(tag))
                new_p = Process(
                    target = worker_func,
                    args = worker_args,
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
                    args = worker_args,
                )
                new_p.daemon = True
                new_p.start()
                proc_time_tracker[i] = time()
                pool[i] = new_p
                # update duration statistic
                n_completed_procs += 1
                diff = worker_duration - avg_duration
                avg_duration += float(diff)/n_completed_procs
    info('mgr {}: done, average worker process wait+work time per element: {:.5f}s'.format(tag, avg_duration))
    return


def _io_thread_manager(
    tag, in_queue, connection, io_func, io_config_args, n_threads, flag, io_func_get_limit
    ):
    ''' process that manages a set of concurrent threads that perform io until the data is gone, 
    then dies
    :params:
        tag: printable - an identifier for this manager
        in_queue: multiprocessing.Queue- input data for io threads. manager observes queue 
            status, but does not access any queued data directly
        connection: a shareable network IO connection- shared between the threads 
            !NOTE! if connection was already opened in another process you will have issues
        io_func: callable- worker function. Daemonized to allow interrupts.
        io_config_args: tuple- arguments for io function that aren't the input data or the connection
        n_threads: int- number concurrent io threads
        flag: multiprocessing.Value- flag provided that indicates no further input data
        io_func_get_limit: int- number of times workers get and process data before dying
    '''
    # NOTE: NEVER pull work element data into the manager. This could cause memory leaks.
    # all tracking data is statically sized (or the best we can do in python)- keeps memory use bounded
    pool = {i:None for i in range(n_threads)}
    thread_time_tracker = {i:0 for i in range(n_threads)}
    n_completed_threads, avg_duration = 0, 0.0
    info('mgr {}: started'.format(tag))
    for i, t in pool.iteritems():
        if t is None:
            debug('mgr {}: starting a worker'.format(tag))
            new_t = Thread(
                target = io_func,
                args = (connection, io_config_args),
            )
            new_t.daemon = True
            new_t.start()
            thread_time_tracker[i] = time()
            pool[i] = new_t
    # silly logic to see if a thread is done but allow interrupts to come through
    while True:
        if all(t is None for _, t in pool.iteritems()):
            break
        sleep(max(0.01, 2*avg_duration/float(n_threads)))
        for i, t in pool.iteritems():
            t.join(max(0.01, avg_duration/float(n_threads)))
            if not t.is_alive():
                pool[i] = None
    info('io mgr {}: done, average io_thread wait+io time per element: {:.5f}s'.format(tag, avg_duration))
    return


class DatabasePipeline:
    ''' A concurrent asynchronous ETL pipeline that runs the provided functions. Specialized
    to handle database connections and IO needs. Currently only allows single producer, but 
    multiple serial pipe worker functions (in concurrent pools!) and parallel consumer 
    threads. Workers restricted to handling one input element at a time. Will warn user if 
    there appears to be lost data at end of execution.

    !NOTE! The functions and matching args must be defined carefully!
    - producer function must be a generator function
    - pipe functions must take an input data object as their _first_ argument
    - consumer function must take an input data as its _first_ argument

    When defining pipe_funcs, order matters!
    - first pipe_func receives input from producer
    - following pipe_funcs receives from previous, send to next
    - last pipe_func sends to consumer
    
    pipe functions are daemonized, so they cannot have children of their own

    all functions (producer, pipe_funcs, and consumer) are responsible for their own input
    sanitation and error handling. All this does is manage process pools and data queues. 
    Pipe_funcs are daemonized and so cannot have their own child processes. Do your best to
    not use poisonPills, it will be very hard to ensure data queue behavior that makes sense.
    '''
    # TODO: allow producer_func to also be a producer_object (GeneratorType)
    #       complications:
    #           requires another _producer func
    #           requires ignoring producer_config_args
    # TODO: any way to allow just copying the full function args, not just "config" args?
    #           I am honestly unsure if this is possible
    # TODO: implement returning results
    def __init__(
        self,
        functions, funcs_meta_tups, funcs_config_args, funcs_pool_sizes,
        n_repeats = 50):
        self.allowable_types = frozenset(
            'producer', # producer types
            'io_pool','cpu_pool', # worker types
            'io_consumer_pool','returning_consumer', # consumer types
        )
        self.N = len(functions) # used all over in the class
        # enforce the contract.
        try:
            assert hasattr(connection_class, 'connect')
            assert hasattr(connection_class, 'close')
        except:
            raise AssertionError('connection_class must have functions connect(self, credentials) and close(self)')
        try:
            assert isinstance(functions, tuple)
            assert isinstance(funcs_meta_tups, tuple)
            assert isinstance(funcs_config_args, tuple)
            assert isinstance(funcs_pool_sizes, tuple)
        except:
            raise AssertionError('all funcs information must be in tuples')
        try:
            assert all(
                (self.N==len(funcs_meta_tups)),
                (self.N==len(funcs_config_args)),
                (self.N==len(funcs_pool_sizes)),
            )
        except:
            raise AssertionError('length of all function definition tuples must be identical')
        for i in range(self.N):
            try:
                assert callable(functions[i])
            except:
                raise AssertionError('provided functions[{i}]: {f} is not callable'.format(
                    i=i,
                    f=functions[i],
                ))
            try:
                assert funcs_meta_tups[i][0] in self.allowable_types
            except:
                raise AssertionError('functions[{i}]: {f} must be typed as one of the allowable types: {a}'.format(
                    i=i,
                    f=functions[i],
                    a = self.allowable_types,
                ))
            try:
                assert isinstance(funcs_config_args[i], tuple)
            except:
                raise AssertionError('arguments for functions[{i}]: {f} must be a tuple'.format(
                    i=i,
                    f=functions[i],
                ))
            try:
                assert isinstance(funcs_pool_sizes[i]) and funcs_pool_sizes[i] > 0
            except:
                raise AssertionError('pool size for functions[{i}]: {f} must be a positive integer'.format(
                    i=i,
                    f=functions[i],
                ))
            if i == 0:
                try:
                    assert 'producer' in funcs_meta_tups[i]
                except:
                    raise AssertionError('first function must be a producer.')
            if 'producer' in funcs_meta_tups[i]:
                try:
                    assert isgeneratorfunction(functions[i])
                except:
                    raise AssertionError('producer function functions[{i}]: {f} must be a generator'.format(
                        i=i,
                        f=functions[i],
                    ))
        else:
            try:
                assert 'consumer' in funcs_meta_tups[i]
            except:
                raise AssertionError('final function functions[{i}]: {f} must be types as a consumer'.format(
                    i=i,
                    f=functions[i],
                ))
        # contract satisfied
        self.functions         = functions
        self.funcs_meta_tups       = funcs_meta_tups
        self.funcs_config_args = funcs_config_args
        self.funcs_pool_sizes  = funcs_pool_sizes
        self.n_repeats         = n_repeats
        # use a manager server to make cleanup easy
        self._sync_server = Manager()
        # 1 manager for each function - in case of producer, or returning consumer, is just a process
        self._managers = [None for _ in range(self.N)]
        # 1 out(in) queue per functions
        self._queues = [self._sync_server.Queue() for _ in range(self.N-1)]
        # 1 queue completed flag for each queue
        self._flags = [self._sync_server.Value('i',0) for _ in range(self.N-1)]
        # tracking data totals to alert for missing data
        self._total_produced = self._sync_server.Value('i',0)
        self._total_consumed = self._sync_server.Value('i',0)
        # setup handlers to send child process logs into main thread's logger
        install_mp_handler()

    def setup(self):
        ''' define the pipeline '''
        struct_str = "Running pipeline with structure:\n"
        struct_str += "{} serial transformations, with {} queues\n\n".format(self.N, len(self._queues))
        struct_str += "{} {}x{}\n".format(self.funcs_meta_tups[i], self.funcs_pool_sizes[-1], self.functions[-1])
        for i in range(1,self.N):
            struct_str += "\tQueue {}: {}\n".format(i,self._queues[i])
            struct_str += "{} {}x{}\n".format(self.funcs_meta_tups[i], self.funcs_pool_sizes[i], self.functions[i])
        info(struct_str)
        # define processes
        for i, f in enumerate(self.functions):
            f_type = self.funcs_meta_tups[i][0]
            f_pool_size = self.funcs_pool_sizes[i]
            f_args = self.funcs_config_args[i]
            if f_type == 'producer':
                self._managers[i] = Process(
                    target = _producer,
                    args   = (
                        self._queues[0],
                        self._total_produced,
                        f,
                        f_args
                    ),
                )
            if f_type == 'cpu_pool':
                self._managers[i] = Process(
                    target = _proc_manager,
                    args = (
                        i,
                        self._queues[i],
                        _worker,
                        (
                            self._queues[i],
                            self._queues[i+1],
                            self.pipe_funcs[i],
                            self.pipe_funcs_config_args[i],
                            self.n_repeats
                        ),
                        self.f_pool_size,
                        self._flags[i],
                        self.n_repeats
                    ),
                )
            if f_type == 'io_pool':
                f_conn_creds = self.funcs_meta_tups[i][0]
                self._managers[i] = Process(
                    target = _io_thread_manager,
                    args = (
                        i,
                        self._queues[i],
                        f_conn_creds,
                        _worker,
                        (
                            self._queues[i],
                            self._queues[i+1],
                            self.pipe_funcs[i],
                            self.pipe_funcs_config_args[i],
                            self.n_repeats
                        ),
                        self.pipe_n_procs[i],
                        self._flags[i],
                        self.n_repeats
                    ),
                )

    def run(self):
        ''' execute the pipeline '''
        # let user know what they've asked for very clearly
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
                self._queues[i].join()
                self._flags[i].value = int(True)
                info('flagged mgr {}'.format(i))
                self._managers[i].join()
            self._queues[-1].join()
            self._flags[-1].value = int(True)
            info('flagged consumer')
            self._consumer.join()
            # check for data loss, but catch the raise
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


class SimplePipeline:
    ''' A concurrent asynchronous ETL pipeline that runs the provided functions.
    Currently only allows single producer, single consumer, but multiple serial pipe 
    worker functions (in concurrent pools!). Workers restricted to handling one input 
    element at a time. Will warn user if there appears to be lost data at end of execution.

    !NOTE! The functions and matching args must be defined carefully!
    - producer function must be a generator function
    - pipe functions must take an input data object as their _first_ argument
    - consumer function must take an input data as its _first_ argument

    When defining pipe_funcs, order matters!
    - first pipe_func receives input from producer
    - following pipe_funcs receives from previous, send to next
    - last pipe_func sends to consumer
    
    pipe functions are daemonized, so they cannot have children of their own

    all functions (producer, pipe_funcs, and consumer) are responsible for their own input
    sanitation and error handling. All this does is manage process pools and data queues. 
    Pipe_funcs are daemonized and so cannot have their own child processes. Do your best to
    not use poisonPills, it will be very hard to ensure data queue behavior that makes sense.
    '''
    # TODO: allow producer_func to also be a producer_object (GeneratorType)
    #       complications:
    #           requires another _producer func
    #           requires ignoring producer_config_args
    # TODO: any way to allow just copying the full function args, not just "config" args?
    #           I am honestly unsure if this is possible
    # TODO: implement returning results
    def __init__(self, 
        producer_func, producer_config_args,
        pipe_funcs, pipe_funcs_config_args, pipe_n_procs,
        consumer_func, consumer_config_args,
        worker_get_limit=5):
        # enforce the contract.
        try:
            assert isinstance(worker_get_limit, int) and worker_get_limit > 1
        except:
            raise AssertionError('worker_get_limit must be an integer > 1')
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
        self.worker_get_limit = worker_get_limit
        # use a manager server to make cleanup easy
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
                target = _proc_manager,
                args = (
                    i,
                    self._queues[i],
                    _worker,
                    (
                        self._queues[i],
                        self._queues[i+1],
                        self.pipe_funcs[i],
                        self.pipe_funcs_config_args[i],
                        self.worker_get_limit
                    ),
                    self.pipe_n_procs[i],
                    self._flags[i],
                    self.worker_get_limit
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
                self._queues[i].join()
                self._flags[i].value = int(True)
                info('flagged mgr {}'.format(i))
                self._managers[i].join()
            self._queues[-1].join()
            self._flags[-1].value = int(True)
            info('flagged consumer')
            self._consumer.join()
            # check for data loss, but catch the raise
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


class SimpleCollectorPipeline:
    ''' See SimplePipeline. This is similar, but instead of allowing a user-defined
    consumer, this uses a data collection thread as the consumer and returns data
    into the main thread

    If pipeline produces too much data, this will obviously cause memory issues. Use
    SimplePipeline with a consumer writing to disk instead, then pick it up after
    '''
# TODO: allow producer_func to also be a producer_object (GeneratorType)
    #       complications:
    #           requires another _producer func
    #           requires ignoring producer_config_args
    # TODO: any way to allow just copying the full function args, not just "config" args?
    #           I am honestly unsure if this is possible
    # TODO: implement returning results
    def __init__(self, 
        producer_func, producer_config_args,
        pipe_funcs, pipe_funcs_config_args, pipe_n_procs,
        worker_get_limit=5):
        # enforce the contract.
        try:
            assert isinstance(worker_get_limit, int) and worker_get_limit > 1
        except:
            raise AssertionError('worker_get_limit must be an integer > 1')
        # check functions
        try:
            assert callable(producer_func)
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
        self.worker_get_limit = worker_get_limit
        # use a manager server to make cleanup easy
        self._sync_server = Manager()
        # 1 manager for each pipe func
        self._managers = [None for _ in range(self.N)]
        # 1 producer finished flag for each manager, 1 for the consumer
        self._flags = [self._sync_server.Value('i',0) for _ in range(self.N+1)]
        # 1 out(in) queue per pipe_func, + 1 extra in(out)
        self._queues = [self._sync_server.Queue() for _ in range(self.N+1)]
        self._total_produced = self._sync_server.Value('i',0)
        self._total_consumed = 0
        self._results = []

    def _consumer_thread(self):
        ''' a thread function that collects pipeline results into self._results '''
        info('started')
        avg_wait = 0
        while True:
            sleep(max((0.01, avg_wait/5)))
            start = time()
            if bool(self._flags[-1].value) and self._queues[-1].empty():
                debug("consumer input queue is closed and empty")
                break
            try:
                r = self._queues[-1].get_nowait()
                self._results += [r]
                self._queues[-1].task_done()
                # debug('consumed {}, total results: {}'.format(r, self._total_consumed))
                debug('total consumed: {}'.format(self._total_consumed))
                self._total_consumed += 1
                wait = time() - start
                diff = wait - avg_wait
                avg_wait += float(diff)/self._total_consumed
            except Empty:
                continue
        info('completed')
            
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
        struct_str += "Consumer: {}\n".format(self._consumer_thread)
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
        self._consumer = Thread(
            target = self._consumer_thread,
        )
        # consumer must be daemonized so it doesn't block raising for KeyboardInterrupt
        self._consumer.daemon = True
        for i in range(self.N):
            self._managers[i] = Process(
                target = _proc_manager,
                args = (
                    i,
                    self._queues[i],
                    _worker,
                    (
                        self._queues[i],
                        self._queues[i+1],
                        self.pipe_funcs[i],
                        self.pipe_funcs_config_args[i],
                        self.worker_get_limit
                    ),
                    self.pipe_n_procs[i],
                    self._flags[i],
                    self.worker_get_limit
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
                self._queues[i].join()
                self._flags[i].value = int(True)
                info('flagged mgr {}'.format(i))
                self._managers[i].join()
            self._queues[-1].join()
            self._flags[-1].value = int(True)
            info('flagged consumer')
            # silly logic to see if the thread is done but allow interrupts to come through
            while True:
                self._consumer.join(0.2)
                if not self._consumer.is_alive():
                    break
            # check for data loss, but catch the raise
            if self._total_consumed != self._total_produced.value:
                n_lost = self._total_produced.value - self._total_consumed
                pct_lost = 100 * (1 - float(self._total_consumed)/float(self._total_produced.value))
                try:
                    raise Warning('Pipeline appears to have lost {n} data elements, about {p:2f}% of the total produced'.format(n = n_lost, p = pct_lost))
                except:
                    exception('DATA LOSS WARNING:')
            # congratulate ourselves on our success
            dur = time() - start_time
            try:
                rate = float(self._total_consumed)/float(dur)
            except ZeroDivisionError:
                rate = float('nan')
            info('pipeline finished. took {d:2f}s, throughput of approximately {r:2f} elements/second'.format(
                d = dur,
                r = rate,
            ))
            return self._results
        except KeyboardInterrupt:
            exception('user terminated pipeline')
            raise
        finally:
            info('cleaning up child processes')
            self._sync_server.shutdown()
            self._producer.terminate()
            [self._managers[i].terminate() for i in range(self.N)]
            # self._consumer.terminate() # daemon Thread will die with owner Process

if __name__ == "__main__":
    raise NotImplementedError('use async_main.py')