#!/bin/env python
#coding:utf-8

import sys
import os
import argparse
import logging


def ParseOptions(argv):
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--redis',
                    default='127.0.0.1:6379',
                    help='redis address, default')
    parser.add_argument('--key',
                    default='filebeat',
                    help='redis key to pop')
    parser.add_argument('--unix_sock',
                    default ="unix_sock",
                    help='unix socket path')

    opts = parser.parse_args(argv)
    return opts


class EventHandler(object):

    def __init__(self, opts):
        self.opts = opts
        self.conn = redis.Redis(opts.redis)
        self.key = opts.key
        self.sock = self.getsock(opts.unix_sock)

    def getSockFilelikeObj(self, unix_sock_path):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(unix_sock_path)
        output = sock.makefile('w')
        return output

    def parseEvent(self, evt):
        pass
    
    def run(self):
        while True:
            item = self.conn.blpop(self.key)
            try:
                event = self.parseEvent(item)
                self.sock.write(event + '\n')
            except:
                continue


def main():
    logging.basicConfig(format = '%(asctime)-15s %(message)s', level = logging.DEBUG)
    opts = ParseOptions(sys.argv[1:])

    logging.info("redis addr = %s", opts.redis,)
    logging.info("key = %s", opts.key)
    logging.info("unix_sock = %s", opts.unix_sock)

    handler = EventHandler(opts)
    handler.Run()
    


if __name__ == '__main__':
    main()
