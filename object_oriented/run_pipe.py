'''
test the pipeline configurable sizes of data and number of parallel workers
running validation of result accuracy
pipeline validates no data loss

logging works, memory is stable, CTRL-c works as fast as the workers can get to it,
and exit is always clean - no child process left behind.
-nice-
'''
from pipelines import SimplePipeline, SimpleCollectorPipeline, SimpleAccumulatorPipeline

from logging.config import dictConfig
from logging import debug, info, error, exception
from random import randint
from time import sleep
from collections import defaultdict

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
            "level"    : "NOTSET", # id recommend changing to info for many elements
        },
    },
}
dictConfig(log_config)

num_elements = 25
element_size = 10
num_serial_workers = 3
num_parallel_workers = 4
worker_get_limit = 7

def my_producer(num_elements, element_size):
    for _ in range(num_elements):
        sleep(randint(0,100)/100.0)
        yield range(element_size)

# first (and in this case only) argument is input data
def my_worker(inp):
    sleep(randint(0,250)/100.0)
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

# first argument is input data, second argument is the accumulation so far
def my_accumulator(inp, acc):
    ''' builds a counts dictionary of elements it sees 
    inp is a list of hashables
    acc is a defaultdict(int)
    '''
    for element in inp:
        acc[element] += 1
    return acc


info('constructing pipeline')
etl = SimpleAccumulatorPipeline(
    producer_func           = my_producer,
    producer_config_args    = (num_elements, element_size),
    pipe_funcs              = tuple([my_worker]*num_serial_workers),
    pipe_funcs_config_args  = tuple([()]*num_serial_workers),
    pipe_n_procs            = tuple([num_parallel_workers]*num_serial_workers),
    accumulator_object      = defaultdict(int),
    accumulator_func        = my_accumulator,
    accumulator_config_args = tuple(),
    worker_get_limit        = worker_get_limit
)
acc = etl.run()
count_of_hits = acc[0]
for key, value in acc.iteritems():
    try:
        assert value == count_of_hits
    except AssertionError:
        exception('value {} at key {} didnt have a consistent match with {}'.format(
            value,
            key,
            count_of_hits
        ))
info('no major errors in main process, check logs to see if there was data loss or issues in child processes')


info('constructing pipeline')
etl = SimplePipeline(
    producer_func           = my_producer,
    producer_config_args    = (num_elements, element_size),
    consumer_func           = my_consumer,
    consumer_config_args    = (num_elements, element_size),
    pipe_funcs              = tuple([my_worker]*num_serial_workers),
    pipe_funcs_config_args  = tuple([()]*num_serial_workers),
    pipe_n_procs            = tuple([num_parallel_workers]*num_serial_workers),
    worker_get_limit        = worker_get_limit
)
etl.run()
info('no major errors in main process, check logs to see if there was data loss or issues in child processes')


info('constructing pipeline with multiple producers')
etl = SimplePipeline(
    producer_func           = (my_producer, my_producer,),
    producer_config_args    = ((num_elements, element_size), (num_elements, element_size),),
    consumer_func           = my_consumer,
    consumer_config_args    = (num_elements, element_size,),
    pipe_funcs              = tuple([my_worker]*num_serial_workers),
    pipe_funcs_config_args  = tuple([()]*num_serial_workers),
    pipe_n_procs            = tuple([num_parallel_workers]*num_serial_workers),
    worker_get_limit        = worker_get_limit
)
etl.run()
info('no major errors in main process, check logs to see if there was data loss or issues in child processes')


info('constructing collector pipeline')
etl = SimpleCollectorPipeline(
    producer_func           = my_producer,
    producer_config_args    = (num_elements, element_size),
    pipe_funcs              = tuple([my_worker]*num_serial_workers),
    pipe_funcs_config_args  = tuple([()]*num_serial_workers),
    pipe_n_procs            = tuple([num_parallel_workers]*num_serial_workers),
    worker_get_limit        = worker_get_limit
)
results = etl.run()
for result in results:
    my_consumer(result, num_elements, element_size)
info('no major errors in main process, check logs to see if there was data loss or issues in child processes')


info('constructing collector pipeline with multiple producers')
etl = SimpleCollectorPipeline(
    producer_func           = (my_producer, my_producer,),
    producer_config_args    = ((num_elements, element_size), (num_elements, element_size),),
    pipe_funcs              = tuple([my_worker]*num_serial_workers),
    pipe_funcs_config_args  = tuple([()]*num_serial_workers),
    pipe_n_procs            = tuple([num_parallel_workers]*num_serial_workers),
    worker_get_limit        = worker_get_limit
)
results = etl.run()
for result in results:
    my_consumer(result, num_elements, element_size)
info('no major errors in main process, check logs to see if there was data loss or issues in child processes')
