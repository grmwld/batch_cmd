#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import argparse
import logging
import multiprocessing
import Queue
import time
from multiworkers.multiworker import Controller, Worker


class QueueHandler(logging.Handler):
    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        self.queue.put(record)


class MyWorker(Worker):
    def __init__(self, *args, **kwargs):
        Worker.__init__(self, *args, **kwargs)

    def log(self, result):
        if result[1] == 0:
            self.global_params['logger'].info(result[0])
        else:
            self.global_params['logger'].error(result[0])

    def do(self, job):
        oc = os.system(job['cmd'])
        self.log((job['cmd'], oc))
        return {'oc': oc}


class MyController(Controller):
    def __init__(self, *args, **kwargs):
        Controller.__init__(self, *args, **kwargs)
        self.global_params['error_logs_queue'] = multiprocessing.Queue()
        self.error_logs = []
        logger = logging.getLogger(
            self.global_params['infile'].name.replace('/', '').strip('.')
        )
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler = logging.StreamHandler(self.global_params['logfile'])
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        console_handler = QueueHandler(self.global_params['error_logs_queue'])
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        self.global_params['logger'] = logger

    def update_error_logs(self):
        while True:
            try:
                self.error_logs.append(
                    str(self.global_params['error_logs_queue'].get_nowait())
                )
            except Queue.Empty:
                return self.error_logs

    def update_progress_message(self):
        msg = '\n\n'.join([
            Controller.update_progress_message(self).rstrip(),
            '\n'.join(self.update_error_logs())
        ])
        return msg

    def finish(self):
        output = ', '.join(map(str, [c['oc'] for c in self.results]))
        print >>self.global_params['outfile'], output


def main(args):
    c = MyController(
        jobs=({'cmd': l.strip()} for l in args.infile),
        global_params={
            'infile': args.infile,
            'outfile': args.outfile,
            'logfile': args.logfile
        },
        num_cpu=args.num_cpu,
        quiet=args.quiet,
        worker_class=MyWorker,
        debug=False
    )
    c.start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i', '--infile', dest='infile',
        type=argparse.FileType('r'),
        nargs='?',
        default=sys.stdin,
        help='Input file'
    )
    parser.add_argument(
        '-o', '--outfile', dest='outfile',
        type=argparse.FileType('w'),
        nargs='?',
        default=sys.stdout,
        help='Output file'
    )
    parser.add_argument(
        '-n', '--num_cpu', dest='num_cpu',
        type=int,
        default=multiprocessing.cpu_count(),
        help='Number of parallel jobs to run'
    )
    parser.add_argument(
        '-q', '--quiet', dest='quiet',
        action='store_true',
        default=False,
        help='No progress bar'
    )
    args = parser.parse_args()
    timestamp = time.strftime("%Y%m%d.%H%M%S", time.localtime())
    if args.infile.name == '<stdin>':
        default_logfile = '-'.join(['LOG', timestamp])
    else:
        default_logfile = '-'.join(['LOG', args.infile.name])
    parser.add_argument(
        '-l', '--logfile', dest='logfile',
        type=argparse.FileType('w'),
        default=default_logfile,
        help='File to use for logging'
    )
    main(parser.parse_args())
