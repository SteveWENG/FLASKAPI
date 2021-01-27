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
    ServiceCategory = db.Column()
    ExactWarehouse = db.Column()

    @classmethod
    def Sites(cls, divisions=None, costCenterCodes=None, serviceCategories=None):
        filter = []
        if divisions !=[] and costCenterCodes != [] and serviceCategories != []:
            if divisions:
                filter.append(cls.Division.in_(divisions))
            if costCenterCodes:
                filter.append(cls.CostCenterCode.in_(costCenterCodes))
            if serviceCategories:
                filter.append(cls.ServiceCategory.in_(serviceCategories))

        #if filter == [True]: filter = []
        if len(filter) > 1:
            filter = [or_(*filter)]
        #filter.append(cls.StartDate<=datetime.datetime.now())
        tmp = cls.query.filter(*filter).with_entities(cls.Division,cls.CostCenterCode, cls.SiteGuid,cls.ExactWarehouse)\
            .distinct().all()
        return getdict(tmp)

    @classmethod
    def Divisions(cls):
        return [l.Division for l in cls.query.with_entities(cls.Division).distinct().all()]

    @classmethod
    def GetDivision(cls, costCenterCode):
        qry = cls.query.filter(cls.CostCenterCode == costCenterCode)\
            .with_entities(cls.Division).first()
        if not qry:
            Error(lang('08FED174-9DF0-4F27-B5AE-B495295179B8') % costCenterCode)

        return qry[0]

    @classmethod
    def GetServiceCategory(cls,costCenterCode):
        return cls.query.filter(cls.CostCenterCode == costCenterCode).first().ServiceCategory