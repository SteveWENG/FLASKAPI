# -*- coding: utf-8 -*-

import logging
from flask import g
from multiprocessing import Queue

class dbHandler(logging.Handler):
    def emit(self, record):
        dic = {'LogRecord': record}
        try:
            if g.get('Site'):
                dic['Site'] = g.get('Site')
            if g.get('User'):
                dic['User'] = g.get('User')

            g.LogQueue.put(dic)
        except:
            pass