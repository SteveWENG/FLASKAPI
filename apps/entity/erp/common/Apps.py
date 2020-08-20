# -*- coding: utf-8 -*-

from ...erp import erp, db

class Apps(erp):
    __tablename__ = 'tblApps'

    Guid = db.Column()
    PGuid = db.Column()
    #AppName = db.Column('AppNameEN')
    Action = db.Column()
    ClassName = db.Column()