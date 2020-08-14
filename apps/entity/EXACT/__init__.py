# -*- coding: utf-8 -*-

from .. import BaseModel, db, SaveDB

class EXACT(BaseModel):
    __abstract__  = True

    __bind_key__  = 'exact'
    __table_args__ = {'schema': '120.dbo'}