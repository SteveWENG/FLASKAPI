# -*- coding: utf-8 -*-
from sqlalchemy import or_

from .LangMast import lang
from ....utils.functions import *
from ...erp import erp, db

class CostCenters(erp):
    __tablename__ = 'DM_D_ERP_COSTCENTER'

    CostCenterCode = db.Column()
    Division = db.Column()
    SiteGuid = db.Column()

    @classmethod
    def Sites(cls, divisions, costCenterCodes):
        if not divisions and not costCenterCodes:
            return []

        filter = []
        if divisions:
            filter.append(cls.DBName.in_(divisions))
        if costCenterCodes:
            filter.append(cls.CostCenterCode.in_(costCenterCodes))
        if len(filter) == 2:
            filter = [or_(*filter)]

        tmp = cls.query.filter(*filter).with_entities(cls.CostCenterCode, cls.SiteGuid).distinct().all()
        return getdict(tmp)

    @classmethod
    def Divisions(cls):
        return cls.query.with_entities(cls.Division).distinct().all()

    @classmethod
    def getDivision(cls, costCenterCode):
        qry = cls.query.filter(cls.CostCenterCode == costCenterCode).with_entities(cls.Division).first()
        if not qry:
            Error(lang('08FED174-9DF0-4F27-B5AE-B495295179B8') % costCenterCode)

        return qry[0]