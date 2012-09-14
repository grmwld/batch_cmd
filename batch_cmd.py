#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import argparse
import logging
import multiprocessing
import time
from multiworkers.multiworker import Controller, Worker


class MyWorker(Worker):
    def __init__(self, *args, **kwargs):
        Worker.__init__(self, *args, **kwargs)

    def do(self, job):
        oc = os.system(job['cmd'])
        return {'oc': oc}


class MyController(Controller):
    def __init__(self, *args, **kwargs):
        Controller.__init__(self, *args, **kwargs)

    def finish(self):
        output = ', '.join(map(str, [c['oc'] for c in self.results]))
        print >>self.global_params['outfile'], output


def main(args):
    c = MyController(
        jobs=({'cmd': l.strip()} for l in args.infile),
        global_params={'outfile': args.outfile},
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
        default_logfile = '-'.join(['LOG', args.infile.name, timestamp])
    print default_logfile
    parser.add_argument(
        '-l', '--logfile', dest='logfile',
        type=argparse.FileType('w'),
        default=default_logfile,
        help='File to use for logging'
    )
    main(parser.parse_args())
