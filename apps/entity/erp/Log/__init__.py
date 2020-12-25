# -*- coding: utf-8 -*-
from datetime import datetime

from ...erp import erp,db

class Log(erp):
    __tablename__ = 'tblLogs'

    logger = db.Column()
    level = db.Column()
    trace = db.Column()
    message = db.Column()
    method = db.Column()
    data = db.Column()
    UserIP = db.Column()
    Site = db.Column()
    User = db.Column()

    def __unicode__(self):
        return self.__repr__()

    def __repr__(self):
        return "<Log: %s - %s>" % (datetime.datetime.now().strftime('%m/%d/%Y-%H:%M:%S'), self.message[:50])