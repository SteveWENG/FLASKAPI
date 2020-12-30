# -*- coding: utf-8 -*-
from sqlalchemy import func, text
from sqlalchemy.orm.attributes import manager_of_class
import pandas as pd
from ....utils.functions import *
from .RoleUserData import RoleUserData
from .Apps import Apps
from ...erp import erp, db

class AppUserData(erp):
    __tablename__ = 'tblAppUserData'

    UserGuid = db.Column()
    RoleGuid = db.Column()
    RoleName = db.Column()
    AppGuid = db.Column()
    Type = db.Column()
    Code = db.Column()
    Status = db.Column(db.Boolean)

    @classmethod
    def apps(cls, userGuid, langCode):
        try:
            li = cls.__AppQuery(userGuid).with_entities(cls.AppGuid,Apps.PGuid, Apps.ReportGuid, Apps.Action,
                                                        Apps.AppName(langCode).label('AppName'))\
                .distinct().all()
            return [{'guid': l.AppGuid, 'name':l.AppName, 'pguid':getStr(l.PGuid),
                     'reportGuid':getStr(l.ReportGuid),'action':getStr(l.Action)}
                    for l in li]
        except Exception as e:
            raise e

    @classmethod
    def data(cls, userGuid, appGuid):
        try:
            guids = Apps.ParentApps(appGuid)
            qry = cls.__AppQuery(userGuid)
            qry = qry.filter(cls.AppGuid.in_([g.get('guid') for g in guids]))\
                .with_entities(cls.AppGuid, func.coalesce(cls.Type,RoleUserData.Type).label('Type'),
                               func.coalesce(cls.Code,RoleUserData.Code).label('Code'))\
                .distinct()

            dfdata = pd.read_sql(qry.statement, cls.getBind())
            if dfdata.empty:
                return dfdata

            dfdata['index'] = dfdata['AppGuid'].map(lambda x: [l.get('index') for l in guids if l.get('guid')==x][0])
            minindex = dfdata['index'].min()
            return dfdata[dfdata['index']==minindex]
        except Exception as e:
            raise e

    @classmethod
    def __AppQuery(cls, userGuid):
        try:
            return cls.query.join(Apps, cls.AppGuid == Apps.Guid) \
                .outerjoin(RoleUserData, cls.RoleGuid == RoleUserData.RoleGuid) \
                .filter(func.coalesce(cls.UserGuid, RoleUserData.UserGuid) == userGuid,
                        cls.Status==True, Apps.Status==True)
        except Exception as e:
            raise e

    @classmethod
    def RoleApps(cls):
        return cls.query.filter(func.coalesce(cls.RoleGuid,'') !='')\
            .with_entities(cls.RoleGuid,cls.RoleName,cls.AppGuid)\
            .order_by(cls.RoleGuid).all()