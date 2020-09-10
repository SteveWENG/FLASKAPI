# -*- coding: utf-8 -*-

from ....entity import BaseModel,db

class BI_DM(BaseModel):
    __abstract__ = True
    __bind_key__ = 'BI_DM'