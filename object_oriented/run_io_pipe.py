'''
test the pipeline configurable sizes of data and number of parallel workers
running validation of result accuracy
pipeline validates no data loss

logging works, memory is stable, CTRL-c works as fast as the workers can get to it,
and exit is always clean - no child process left behind.
-nice-
'''
from pipelines import MySQLPipeline

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

info('setting up definitions for pipeline testing')
num_elements = 25
element_size = 10
num_serial_workers = 3
num_parallel_workers = 4
worker_get_limit = 7
credentials = 'a fake database for {}'

class mock_connection:
    ''' a little tester object, looks like a mysql database
    connection if you squint real hard '''
    def __init__(self, element_size):
        self.status = False
        self.element_size = element_size
    def connect(self, credentials):
        debug('connecting to {}'.format(credentials))
        self.status = True
    def close(self):
        self.status = False
    def is_open(self):
        return self.status
    def execute(self, query):
        if not self.status:
            raise RuntimeError('connection is not open cannot execute')
        sleep(0.01)
        debug('"executed" a query: {}'.format(query))
        if 'select' in query.lower():
            return range(self.element_size)

# No special arguments for Producer
def io_producer(credentials, num_elements, element_size):
    mc = mock_connection(element_size)
    mc.connect(credentials)
    try:
        for _ in range(num_elements):
            yield mc.execute('select')
    except:
        raise
    finally:
        mc.close()

# For CPU Workers
# first (and in this case only) argument is input data
def cpu_worker(inp, element_size):
    sleep(randint(0,100)/100.0)
    return [i*j for i,j in zip(inp, range(element_size))]

# For IO Workers
# first argument is input data
# second argument is connection
# sleep much longer than cpu worker to simulate db query
def io_worker(inp, connection):
    assert connection.is_open()
    sleep(randint(0,500)/100.0)
    join = connection.execute('select')
    return [i*j for i,j in zip(inp, join)]

# For IO consumers
# first argument is input data
# second argument is connection
def io_consumer(inp, connection, element_size):
    ''' checks that our result is what we thought we'd get '''
    assert connection.is_open()
    expected_val = range(element_size)
    for _ in range(num_serial_workers):
        expected_val = [
            i*j for i,j in zip(expected_val, range(element_size))
        ]
    try:
        assert inp == expected_val
        connection.execute("INSERT {}".format(inp[0]))
    except:
        error('''data transformation looks like it failed:
        expected: {}
        actual:   {}'''.format(
            expected_val,
            inp
        ))

connection_class = mock_connection
producer_creds = credentials.format('a producer')
worker_creds = credentials.format('io workers')
consumer_creds = credentials.format('io consumers')
# build worker definition tuples
func_list = []
func_args_list = []
func_n_list = []
func_types_list = []
i = 0
# add producer
func_list.append(io_producer)
func_args_list.append((credentials, num_elements, element_size,))
func_n_list.append(1) # doesnt matter for producer, just has to be a number.
func_types_list.append(('producer',))
# add workers
while True:
    # cpu worker
    func_list.append(cpu_worker)
    func_args_list.append((element_size,))
    func_n_list.append(num_parallel_workers)
    func_types_list.append(('cpu_pool',))
    i += 1
    if not i < num_serial_workers:
        break
    # io worker - 5x longer wait, 5x more workers (threads tho!)
    func_list.append(io_worker)
    func_args_list.append(())
    func_n_list.append(5*num_parallel_workers)
    func_types_list.append(('io_pool', worker_creds))
    i += 1
    if not i < num_serial_workers:
        break
# add consumer
func_list.append(io_consumer)
func_args_list.append((element_size,))
func_n_list.append(5*num_parallel_workers)
func_types_list.append(('io_consumer_pool',))


info('constructing pipeline')
etl = MySQLPipeline(
    funcs              = tuple(func_list),
    funcs_types         = tuple(func_types_list),
    funcs_config_args   = tuple(func_args_list),
    funcs_pool_sizes    = tuple(func_n_list),
    n_repeats       = worker_get_limit,
)
etl.run()
info('no major errors in main process, check logs to see if there was data loss or issues in child processes')