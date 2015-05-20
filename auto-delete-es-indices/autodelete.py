#!/usr/bin/env python
# encoding:utf-8

'''
'''

__author__ = 'fenghao'

import sys
import argparse
import re
import os
import traceback
import logging.config
import datetime

# install with `pip install elasticsearch`
# for es 0.9x, install elasticsearch-py 0.4.x
import elasticsearch


def cfgLogging():
    logpath = os.path.join("log")

    if 'win' not in sys.platform:
        os.system("mkdir -p " + logpath)
    else:
        if not os.path.exists(logpath):
            os.makedirs(logpath)

    LOGGING = {
               'version': 1,
               'disable_existing_loggers': True,
               'formatters': {
                              'default': {'format': '[%(asctime)-25s] [%(relativeCreated)-15s] %(levelname)-10s pid:%(process)d %(message)s'},
                               # default': {
                               #           'format' : '%(asctime)s %(message)s',
                               #           'datefmt' : '%Y-%m-%d %H:%M:%S'
                               # }
                },
               'handlers': {
                            'console':{
                                       'level':'DEBUG',
                                       'class':'logging.StreamHandler',
                                       'formatter': 'default'
                            },
                            'file': {
                                     'level': 'DEBUG',
                                     'class': 'logging.handlers.RotatingFileHandler',
                                     'formatter': 'default',
                                     'filename' : os.path.join(logpath, 'runlog.log'),
                                     'maxBytes':    50 * 1024 * 1024,  # 10M
                                     'backupCount': 20,
                                     'encoding' : 'utf8',
                            }
                },
               'loggers' : {
                            '' : {
                                          'level' : 'DEBUG',
                                          'handlers' : ['console', 'file'],
                                          'propagate' : True
                            }
                }
    }
    logging.config.dictConfig(LOGGING)


def parseCmdOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-x', "--debug", help = "debug run, won't really do the delete", action = "store_true")
    parser.add_argument('-n', "--nday", help = "delete indices beyond which day, could specify -1, -2 and -n",
        type = int, default = -7, choices = xrange(-10, 1))

    cmdoptions = parser.parse_args()

    return cmdoptions


def getDeadLineDate(day = -7):
    dt_today = datetime.datetime.today()
    time_delta = datetime.timedelta(day)
    dead_line_day = dt_today + time_delta
    return dead_line_day


def run():
    logger = logging.getLogger('main')
    cmdoptions = parseCmdOptions()

    es_hosts = ['172.16.3.203', ]
    es = elasticsearch.Elasticsearch(es_hosts)
    dead_line_day = getDeadLineDate(cmdoptions.nday)
    date_format = '%Y.%m.%d'

    d = es.indices.get_alias('*stash*')
    logger.info("I see all names {}".format(d.keys()))
    
    for name in d:
        m = re.search(r"^(?:.*stash).(.*)$", name)
        if m:
            s_date = m.group(1)
            try:
                logdate = datetime.datetime.strptime(s_date, date_format)
                if dead_line_day > logdate:
                    ack = ""
                    if not cmdoptions.debug:
                        ack = es.indices.delete(name)
                    logger.info("deleting %s, ack={}".format(ack), name)
                else:
                    logger.info("keeping %s", name)
            except:
                ack = ""
                if not cmdoptions.debug:
                    ack = es.indices.delete(name)
                logger.info("deleting strange %s, ack={}".format(ack), name)


def main():
    try:
        cfgLogging()
        logger = logging.getLogger("main")
        run()
    except SystemExit:
        pass
    except:
        logger.info('----->>>>>>>>>> {}'.format(traceback.format_exc()))

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass


