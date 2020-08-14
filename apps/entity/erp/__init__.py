# -*- coding: utf-8 -*-

from .. import BaseModel, db, SaveDB

class erp(BaseModel):
    __abstract__  = True

    __bind_key__  = 'salesorder'