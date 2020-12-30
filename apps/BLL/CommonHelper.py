# -*- coding: utf-8 -*-
from copy import copy

from flask import jsonify
from pandas import merge

from ..entity.erp.common.AppHelp import AppHelp
from ..entity.erp.common.AppUserData import AppUserData
from ..entity.erp.common.Apps import Apps
from ..entity.erp.common.RoleUserData import RoleUserData
from ..utils.functions import *


class CommonHelper:

    @staticmethod
    def AppHelp(data):
        return AppHelp.list(data.get('appGuid',''))

    @staticmethod
    def AppRights(data):
        tmpApps = Apps.Struct()
        tmpRoleApps = AppUserData.RoleApps()
        tmpRoleUsers = RoleUserData.list()

        DataFrameSetNan(tmpRoleUsers)
        tmp = [{'RoleGuid':g1[0],'UserGuid':g1[1],'UserName':g1[2],'FullName':g1[3],
                'compSite':[[v for v in l.values()] for l in g2[['Division','Code','Id' ]].to_dict('record')]}
               for g1, g2 in tmpRoleUsers.groupby(['RoleGuid','UserGuid','UserName','FullName'])]

        return {'apps': tmpApps, 'RoleApps': getdict(tmpRoleApps), 'RoleUsers': tmp}
