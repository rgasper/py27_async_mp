'''
test the pipeline configurable sizes of data and number of parallel workers
running validation of result accuracy
pipeline validates no data loss

logging works, memory is stable, CTRL-c works as fast as the workers can get to it,
and exit is always clean - no child process left behind.
-nice-
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

num_elements = 55
element_size = 10
num_serial_workers = 3
num_parallel_workers = 3
worker_get_limit = 10

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
    worker_get_limit        = worker_get_limit
)
etl.run()
info('no major errors in main process, check logs to see if there was data loss or issues in child processes')