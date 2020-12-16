# -*- coding: utf-8 -*-

from sqlalchemy import or_

from .LangMast import lang
from ....utils.functions import *
from ...erp import erp, db

class CostCenter(erp):
    __tablename__ = 'DM_D_ERP_COSTCENTER'

    CostCenterCode = db.Column()
    Division = db.Column()
    SiteGuid = db.Column()
    ExactWarehouse = db.Column()
    StartDate = db.Column('Load_Time',db.Date)

    @classmethod
    def Sites(cls, divisions, costCenterCodes):
        if divisions==None and costCenterCodes==None:
            return []

        filter = []
        if divisions:
            filter.append(cls.Division.in_(divisions))
        if costCenterCodes:
            filter.append(cls.CostCenterCode.in_(costCenterCodes))
        if len(filter) == 2:
            filter = [or_(*filter)]
        filter.append(cls.StartDate<=datetime.datetime.now())
        tmp = cls.query.filter(*filter).with_entities(cls.Division,cls.CostCenterCode, cls.SiteGuid,cls.ExactWarehouse)\
            .distinct().all()
        return getdict(tmp)

    @classmethod
    def Divisions(cls):
        return cls.query.filter(cls.StartDate<=datetime.date.today()).with_entities(cls.Division).distinct().all()

    @classmethod
    def getDivision(cls, costCenterCode):
        qry = cls.query.filter(cls.CostCenterCode == costCenterCode,cls.StartDate<=datetime.date.today())\
            .with_entities(cls.Division).first()
        if not qry:
            Error(lang('08FED174-9DF0-4F27-B5AE-B495295179B8') % costCenterCode)

        return qry[0]