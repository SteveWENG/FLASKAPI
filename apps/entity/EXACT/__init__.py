# -*- coding: utf-8 -*-

from .. import BaseModel, db, SaveDB

class EXACT(BaseModel):
    __abstract__  = True

    __bind_key__  = 'exact'
    #__table_args__ = {'schema': '120.dbo'}
    dbConfig = {
        'host': '192.168.0.98', 'port': '1433', 'database': '110',
        'user': 'sa', 'password':'gladis0083',
        #'charset':'utf8'
    }