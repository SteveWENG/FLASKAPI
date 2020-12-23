# -*- coding: utf-8 -*-
from datetime import datetime

from ...erp import erp,db

class Log(erp):
    __tablename__ = 'tblLogs'

    logger = db.Column()
    level = db.Column()
    trace = db.Column()
    message = db.Column()
    path = db.Column()
    method = db.Column()
    ip = db.Column()

    def __init__(self, logger=None, level=None, trace=None, msg=None):
        self.logger = logger
        self.level = level
        self.trace = trace
        self.msg = msg

    def __unicode__(self):
        return self.__repr__()

    def __repr__(self):
        return "<Log: %s - %s>" % (datetime.datetime.now().strftime('%m/%d/%Y-%H:%M:%S'), self.messag[:50])