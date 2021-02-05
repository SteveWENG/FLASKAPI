# -*- coding: utf-8 -*-

from flask import g

from .. import BaseModel, db, SaveDB

def CurrentUser():
    return g.get('User')

class erp(BaseModel):
    __abstract__  = True

    __bind_key__  = 'salesorder'