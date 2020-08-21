# -*- coding: utf-8 -*-
from ....utils.functions import *
from ...erp import erp, db

class Company(erp):
    __tablename__ = 'Company'

    Company = db.Column()
    Status = db.Column(db.Boolean)

    @classmethod
    def show(cls):
        return [getStr(s.Company) for s in cls.query.filter(cls.Status==True).with_entities(cls.Company).all()]