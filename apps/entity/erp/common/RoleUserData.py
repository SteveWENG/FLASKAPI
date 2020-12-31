# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import func, and_

from ....utils.functions import *
from .CostCenter import CostCenter
from .LangMast import lang
from .UserMast import UserMast
from ...erp import erp, db

class RoleUserData(erp):
    __tablename__ = 'tblRoleUserData'

    RoleGuid = db.Column()
    UserGuid = db.Column()
    Type = db.Column()
    Code = db.Column()

    @classmethod
    def list(cls):
        df = pd.read_sql(cls.query.join(UserMast, cls.UserGuid==UserMast.Guid)
                           .outerjoin(CostCenter,
                                      and_(func.lower(cls.Type)=='costcenter',
                                           cls.Code==CostCenter.CostCenterCode))\
            .filter(UserMast.Status==True)\
            .with_entities(cls.Id,cls.RoleGuid,cls.UserGuid,
                           UserMast.UserName,UserMast.FullName,
                           cls.Type,cls.Code,CostCenter.Division)\
            .order_by(cls.RoleGuid,UserMast.UserName).statement,cls.getBind())
        df.loc[df['Type'].str.lower().isin(['company','division']),'Divison'] = df['Code']
        df.loc[df['Type'].str.lower()=='costcenter', 'CostCenterCode'] = df['Code']
        return df.drop(['Code'],axis=1)

    @classmethod
    def save(cls,lines):
        with cls.merges(lines,True) as _:
            pass
        return lang('56CF8259-D808-4796-A077-11124C523F6F')