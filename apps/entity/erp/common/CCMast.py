# -*- coding: utf-8 -*-
from sqlalchemy import or_

from ....utils.functions import *
from ...erp import erp, db

class CCMast(erp):
    __tablename__ = 'CCMast'

    CostCenterCode = db.Column()
    SiteGuid = db.Column()
    Company = db.Column()
    DBName = db.Column()
    Status = db.Column(db.Boolean)

    @classmethod
    def show(cls, companys, costCenterCodes):
        if companys == None and not costCenterCodes:
            return []

        filter = []
        if companys:
            filter.append(cls.Company.in_(companys))
        if costCenterCodes:
            filter.append(cls.CostCenterCode.in_(costCenterCodes))
        if len(filter) == 2:
            filter = [or_(*filter)]
        filter.append(cls.Status==True)

        tmp = cls.query.filter(*filter).with_entities(cls.CostCenterCode, cls.SiteGuid).distinct().all()
        return [{k: getStr(getattr(l,k)) for k in l.keys()} for l in tmp]