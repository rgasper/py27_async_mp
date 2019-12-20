from pipelines import ConcurrentSingleElementPipeline

from time import sleep
from queue import Empty
from multiprocessing import Queue
from logging.config import dictConfig
from logging import debug, info

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
            "level"    : "NOTSET",
        },
    },
}
dictConfig(log_config)

elem_size = 10
def my_producer(i):
    for _ in range(i):
        yield range(elem_size)

# first (and in this case only) argument is input data
def my_worker(inp_list):
    return [i**2 for i in inp_list]

# first argument is input data
def my_consumer(inp_list, out_queue):
    out_queue.put_nowait(inp_list)

def get_results(out_queue):
    results = []
    while True:
        try:
            val = out_queue.get_nowait()
            results.append(val)
        except Empty:
            return results

num_elements = 10
out_queue = Queue()

etl = ConcurrentSingleElementPipeline(
    producer_func           = my_producer,
    producer_config_args    = (num_elements,),
    consumer_func           = my_consumer,
    consumer_config_args    = (out_queue,),
    pipe_funcs              = (my_worker,),
    pipe_funcs_config_args  = ((),),
    pipe_n_procs            = (1,),
)

etl.run()

# the pipeline should warn us if data is lost, but double-check
results = get_results(out_queue)

assert len(results) == num_elements
expected_result = my_worker(range(elem_size))
for result in results:
    assert result == expected_result
info('it worked!')