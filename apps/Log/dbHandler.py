# -*- coding: utf-8 -*-

import logging
from flask import request, g

class dbHandler(logging.Handler):

    def emit(self, record):
        dic = {'LogRecord': record}
        try:
            try:
                if request:
                    dic['data'] = request.json
                    dic['method'] = request.full_path
                    dic['UserIP'] = request.remote_addr
            except:
                pass
            if g.get('Site'):
                dic['Site'] = g.get('Site')
            if g.get('User'):
                dic['User'] = g.get('User')

            g.LogQueue.put(dic)
        except:
            pass



