# -*- coding: utf-8 -*-

from ....utils.functions import *
from ...erp import erp, db

class CCMast(erp):
    __tablename__ = 'CCMast'

    CostCenterCode = db.Column()
    Company = db.Column()
    DBName = db.Column()
    Status = db.Column(db.Boolean)

    @classmethod
    def show(cls, companys):
        if companys == None:
            return []

        filter = [cls.Status==True]
        company = getStr(companys)
        if len(companys) > 0:
            filter.append(cls.Company.in_(companys))
        tmp = cls.query.filter(*filter).with_entities(cls.CostCenterCode).distinct().all()
        return [getStr(l.CostCenterCode) for l in tmp]
