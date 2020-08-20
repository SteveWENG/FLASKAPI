# -*- coding: utf-8 -*-
from sqlalchemy import func

from ....utils.functions import *
from .RoleUserData import RoleUserData
from .Apps import Apps
from ...erp import erp, db

class AppUserData(erp):
    __tablename__ = 'tblAppUserData'

    UserGuid = db.Column()
    RoleGuid = db.Column()
    AppGuid = db.Column()
    Status = db.Column(db.Boolean)

    @classmethod
    def apps(cls, userGuid, langCode):
        try:
            tmp = type('tmp',(Apps,),{})
            tmpfieldname = 'AppName'
            langCode = langCode.upper()
            if langCode in ('EN','ZH'):
                tmpfieldname += langCode
            setattr(tmp, 'AppName', db.Column(tmpfieldname))

            li = cls.query.join(tmp,cls.AppGuid==tmp.Guid)\
                .outerjoin(RoleUserData, cls.RoleGuid==RoleUserData.RoleGuid)\
                .filter(func.coalesce(cls.UserGuid,RoleUserData.UserGuid)==userGuid)\
                .with_entities(cls.AppGuid,tmp.PGuid, tmp.AppName,tmp.Action).distinct().all()
            return [{'guid': l.AppGuid, 'name':l.AppName, 'pguid':getStr(l.PGuid), 'action':getStr(l.Action)}
                    for l in li]
        except Exception as e:
            raise