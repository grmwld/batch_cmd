#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import argparse
import logging
import multiprocessing
import subprocess
import Queue
import time
from multiworkers.multiworker import Controller, Worker

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"

COLORS = {
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE,
    'CRITICAL': MAGENTA,
    'ERROR': RED,
    'RED': RED,
    'GREEN': GREEN,
    'YELLOW': YELLOW,
    'BLUE': BLUE,
    'MAGENTA': MAGENTA,
    'CYAN': CYAN,
    'WHITE': WHITE,
}

class ColoredConsoleFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        logging.Formatter.__init__(self, *args, **kwargs)

    def format(self, record):
        levelname = record.levelname
        color = COLOR_SEQ % (30 + COLORS[levelname])
        message = logging.Formatter.format(self, record)
        message = message.replace("$RESET", RESET_SEQ)\
                         .replace("$BOLD",  BOLD_SEQ)\
                         .replace("$COLOR", color)
        for k, v in COLORS.items():
            message = message.replace("$" + k, COLOR_SEQ % (v + 30))\
                             .replace("$BG" + k, COLOR_SEQ % (v + 40))\
                             .replace("$BG-" + k, COLOR_SEQ % (v + 40))
        return message + RESET_SEQ


class QueueHandler(logging.Handler):
    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        try:
            self.queue.put(self.format(record))
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


class MyWorker(Worker):
    def __init__(self, *args, **kwargs):
        Worker.__init__(self, *args, **kwargs)

    def log(self, result):
        if result['retcode'] == 0:
            self.global_params['logger'].info(result['cmd'])
        else:
            self.global_params['logger'].error(''.join([
                '$MAGENTAcommand$RESET: ' + result['cmd'] + '\n\t',
                ('$MAGENTAstdout:$RESET ' + result['stdout'] + '\n\t') if result['stdout'] else '',
                ('$MAGENTAstderr:$RESET ' + result['stderr'] + '\n\t') if result['stderr'] else ''
            ]))

    def do(self, job):
        proc = subprocess.Popen(
            job['cmd'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True
        )
        stdout_data, stderr_data = (o.strip() for o in proc.communicate())
        result = {
            'cmd': job['cmd'],
            'stdout': stdout_data,
            'stderr': stderr_data,
            'retcode': proc.returncode
        }
        self.log(result)
        return {'result': result}


class MyController(Controller):
    def __init__(self, *args, **kwargs):
        Controller.__init__(self, *args, **kwargs)
        self.global_params['error_logs_queue'] = multiprocessing.Queue()
        self.error_logs = []
        logger = logging.getLogger(
            self.global_params['infile'].name.replace('/', '').strip('.')
        )
        logger.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            "%(levelname)s - %(asctime)s - %(name)s - %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        colored_console_formatter = ColoredConsoleFormatter(
            "$COLOR%(levelname)s$RESET - %(asctime)s - $BOLD$CYAN%(name)s$RESET \n\t%(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        file_handler = logging.StreamHandler(self.global_params['logfile'])
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(file_formatter)
        console_handler = QueueHandler(self.global_params['error_logs_queue'])
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(colored_console_formatter)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        self.global_params['logger'] = logger

    def update_error_logs(self):
        while True:
            try:
                record = self.global_params['error_logs_queue'].get_nowait()
                self.error_logs.append(record)
            except Queue.Empty:
                return self.error_logs

    def update_progress_premessage(self):
        self.progress_premessage =  '\n' + '\n'.join(self.update_error_logs())
        return self.progress_premessage

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
