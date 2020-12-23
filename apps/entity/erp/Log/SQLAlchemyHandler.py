# -*- coding: utf-8 -*-

import logging
import traceback

from ...erp import db
from ..Log import Log

class SQLAlchemyHandler(logging.Handler):
    def emit(self, record):
        trace = None
        exc = record.__dict__['exc_info']
        if exc:
            trace = traceback.format_exc(exc)

        '''
        path = request.path
        method = request.method
        ip = request.remote_addr
        '''
        log = Log(logger=record.__dict__['name'],
                  level=record.__dict__['levelname'],
                  trace=trace,
                  message=record.__dict__['msg']
                  )
        db.session.add(log)
        db.session.commit()