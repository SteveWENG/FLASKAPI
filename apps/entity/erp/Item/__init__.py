# -*- coding: utf-8 -*-

from ....entity import db

class BI_DM(db.Model):
    __abstract__ = True
    __bind_key__ = 'BI_DM'