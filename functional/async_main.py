'''
async_main.py
a simple test application to validate this multiprocess design
usage: $ python async_main.py num_elems num_procs
Author: Raymond Gasper
'''

import argparse
from threading import Thread
from multiprocessing import Process, Queue, Value
from multiprocessing_logging import install_mp_handler
from logging import info, debug, exception
from logging.config import dictConfig
from time import time

from async_mp_cores import producer, manager, worker, consumer

log_config = {
    "version": 1,
    "disabled_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "[%(module)s:%(funcName)s:%(lineno)d] %(asctime)s [PID:%(process)d] [THREAD:%(threadName)s] - [%(levelname)s] \n %(message)s",
        },
        "default": {
            "format": "[%(module)s:%(funcName)s:%(lineno)d] %(asctime)s - [%(levelname)s] - %(message)s",
        },
    },
    "handlers": {
        "daily": {
            # daily log is for high-level application health information
            "level"       : "INFO",
            "class"       : "logging.handlers.TimedRotatingFileHandler",
            "formatter"   : "default",
            "filename"    : 'log_timed',
            "when"        : "H",
            "backupCount" : 14,
        },
        "debug": {
            # debug logger is for high-volume stuff that's unimportant if the app is
            # working but so very clutch to see if the app breaks. 
            "level"       : "DEBUG",
            "class"       : "logging.handlers.RotatingFileHandler",
            "formatter"   : "verbose",
            "filename"    : 'log_rotating',
            "backupCount" : 1, # 0 induces handler to never rotate the file
            "maxBytes"     : 1024*1024*256 # 256 MB
        },
        "stream": {
            "level"     : "NOTSET",
            "class"     : "logging.StreamHandler",
            "formatter" : "default",
        },
    },
    "loggers": {
        # root | emptystr is a special name that allows any logger in the application to use it
        # if you use a loggger with a different name, imported libraries may not log properly
        "": {
            "handlers" : ["stream","debug","daily"],
            "level"    : "DEBUG", # send everything to handlers
        },
    },
}
dictConfig(log_config)
# install multiprocess log handlers on root logger by default
install_mp_handler() 

def pipeline(num_elems, num_procs):
    info('setting up')
    results_queue = Queue()
    input_queue   = Queue()
    poisonPill = Value('i',0)
    producer_p = Process(
        target = producer,
        args   = (
            args.num_elems,
            input_queue,
        ),
    )
    manager_p = Process(
        target = manager,
        args   = (
            input_queue,
            args.num_procs,
            poisonPill, 
            worker,
            (input_queue, results_queue, poisonPill),
        ),
    )
    consumer_p = Process(
        target = consumer,
        args   = (
            results_queue,
            args.num_elems,
        )
    )
    try:
        info('running pipeline')
        start_time = time()
        producer_p.start()
        manager_p.start()
        consumer_p.start()
        producer_p.join()
        info('asking main to stop')
        with poisonPill.get_lock():
            poisonPill.value = int(True)
        manager_p.join()
        consumer_p.join()
        info('pipeline finished')
        dur = time() - start_time
        info('pipeline took {d:2f}s to process {i} elements with {n} parallel processes'.format(
            d = dur,
            i = args.num_elems,
            n = args.num_procs
        ))
    except KeyboardInterrupt:
        exception('user terminated pipeline')
        producer_p.terminate()
        manager_p.terminate()
        consumer_p.terminate()
        raise

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('num_elems', type=int)
    parser.add_argument('num_procs', type=int)
    args = parser.parse_args()
    
    pipeline(args.num_elems, args.num_procs)
    