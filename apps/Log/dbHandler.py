# -*- coding: utf-8 -*-

import logging
from flask import request, g

class dbHandler(logging.Handler):

    def emit(self, record):
        try:
            dic = {'LogRecord': record, 'data': request.json, 'method': request.full_path,
                   'UserIP': request.remote_addr}
            if g.get('Site'):
                dic['Site'] = g.get('Site')
            if g.get('User'):
                dic['User'] = g.get('User')

            g.LogQueue.put(dic)
        except:
            pass



