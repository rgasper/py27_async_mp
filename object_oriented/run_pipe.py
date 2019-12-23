'''
test the pipeline configurable sizes of data and number of parallel workers
running validation of result accuracy
pipeline validates no data loss

logging works, memory is stable, CTRL-c works as fast as the workers can get to it,
and exit is clean - no child process left behind.
-nice-

Notes:
SO i've done some experimenting and if you want to get data out of the pipeline,
you have two options:
    - _constantly_ consume from an output queue in another thread/process. 
        If you let it accumulate, it will eventually block forever with no error
        even if you use put_nowait() or put(block=False) in the consumer. see WEIRD QUEUE ERROR
        (using Python 2.7 and Mac OS X)
    - write to an external data store (csv, database)

WEIRD QUEUE ERROR:
If you accumulate data into a Queue for collection into Main Thread, a weird error can occur!
The ConcurrentSingleElementPipeline._consumer will never successfully join. It gets to it's return
statement, will execute code after the last queue.put().
I'm 95% sure this is what is happening, and have tested thoroughly. 
It makes _absolutely_ no sense to me. Maybe need to use managers instead of multiprocessing.Queue

Memory usage:
I've tested upto 10K elements of 1000 integer long lists with 4 serial pools of 25 concurrent workers,
and have seen no evidence of memory load increasing during runtime. The main thread consumed a frightening
260 MB of memory but it stayed constant during runtime.
'''
from pipelines import ConcurrentSingleElementPipeline

from logging.config import dictConfig
from logging import debug, info, error
from random import randint
from time import sleep

log_config = {
    "version": 1,
    "disabled_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "[%(module)s:%(funcName)s:%(lineno)d] %(asctime)s - [%(levelname)s] - %(message)s",
        },
    },
    "handlers": {
        "stream": {
            "level"     : "NOTSET",
            "class"     : "logging.StreamHandler",
            "formatter" : "default",
        },
    },
    "loggers": {
        "": {
            "handlers" : ["stream"],
            "level"    : "DEBUG", # id recommend changing to info for many elements
        },
    },
}
dictConfig(log_config)

num_elements = 100
element_size = 10
num_serial_workers = 5
num_parallel_workers = 5

def my_producer(num_elements, element_size):
    for _ in range(num_elements):
        yield range(element_size)

# first (and in this case only) argument is input data
def my_worker(inp):
    sleep(randint(0,250)/100)
    return [i**2 for i in inp]

# first (and only) argument is input data
def my_consumer(inp, num_elements, element_size):
    ''' checks that our result is what we thought we'd get '''
    expected_val = range(element_size)
    for _ in range(num_serial_workers):
        expected_val = [i**2 for i in expected_val]
    try:
        assert inp == expected_val
    except:
        error('''data transformation looks like it failed:
        expected: {}
        actual:   {}'''.format(
            expected_val,
            inp
        ))

info('constructing pipeline')
etl = ConcurrentSingleElementPipeline(
    producer_func           = my_producer,
    producer_config_args    = (num_elements, element_size),
    consumer_func           = my_consumer,
    consumer_config_args    = (num_elements, element_size,),
    pipe_funcs              = tuple([my_worker]*num_serial_workers),
    pipe_funcs_config_args  = tuple([()]*num_serial_workers),
    pipe_n_procs            = tuple([num_parallel_workers]*num_serial_workers),
)
etl.run()
info('if my_consumer didnt throw an assertion error, it worked!')