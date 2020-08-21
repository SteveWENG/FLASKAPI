# -*- coding: utf-8 -*-
from sqlalchemy import func
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
    AppGuid = db.Column()
    Type = db.Column()
    Code = db.Column()
    Status = db.Column(db.Boolean)

    @classmethod
    def apps(cls, userGuid, langCode):
        try:
            qry, clzApps = cls.__AppQuery(userGuid,langCode)
            li = qry.with_entities(cls.AppGuid,clzApps.PGuid, clzApps.AppName, clzApps.ReportGuid, clzApps.Action)\
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
            qry, clzApps = cls.__AppQuery(userGuid)
            qry = qry.filter(cls.AppGuid.in_([g.get('guid') for g in guids]))\
                .with_entities(cls.AppGuid, func.coalesce(cls.Type,RoleUserData.Type).label('Type'),
                               func.coalesce(cls.Code,RoleUserData.Code).label('Code'))\
                .distinct()

            dfdata = pd.read_sql(qry.statement, cls.getBind())
            if len(dfdata) == 1:
                return dfdata

            dfdata['index'] = dfdata['AppGuid'].map(lambda x: [l.get('index') for l in guids if l.get('guid')==x][0])
            minindex = dfdata['index'].min()
            return dfdata[dfdata['index']==minindex]
        except Exception as e:
            raise e

    @classmethod
    def __AppQuery(cls, userGuid, langCode=''):
        try:

            tmp = type('tmp', (Apps,), {})
            tmpfieldname = 'AppName'
            langCode = langCode.upper()
            if langCode in ('EN', 'ZH'):
                tmpfieldname += langCode
            setattr(tmp, 'AppName', db.Column(tmpfieldname))

            return cls.query.join(tmp, cls.AppGuid == tmp.Guid) \
                .outerjoin(RoleUserData, cls.RoleGuid == RoleUserData.RoleGuid) \
                .filter(func.coalesce(cls.UserGuid, RoleUserData.UserGuid) == userGuid,
                        cls.Status==True, tmp.Status==True), tmp
        except Exception as e:
            raise e