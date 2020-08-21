# -*- coding: utf-8 -*-

from config import ExactConfig
from .. import BaseModel, db, SaveDB

class EXACT(BaseModel):
    __abstract__  = True

    __bind_key__  = 'exact'
    #__table_args__ = {'schema': '120.dbo'}
    dbconnect = ExactConfig.db