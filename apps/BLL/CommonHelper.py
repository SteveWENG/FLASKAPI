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

        tmp = tmpRoleApps[['RoleGuid','RoleName']]
        tmp.drop_duplicates(inplace=True)

        tmpRoleUsers = merge(tmpRoleUsers,tmp, left_on="RoleGuid", right_on="RoleGuid")
        DataFrameSetNan(tmpRoleUsers)

        tmp = [{'RoleGuid':g1,'RoleName':g2.iloc[0]['RoleName'],'UserGuid':g2.iloc[0]['UserGuid'],
                'UserName':g2.iloc[0]['UserName'],'FullName':g2.iloc[0]['FullName'],
                'compSite':[[v for v in l.values()] for l in g2[['Division','Code','Id']].to_dict('record')]}
               for g1, g2 in tmpRoleUsers.groupby(['RoleGuid'])]

        return {'apps': tmpApps, 'RoleApps': getdict(tmpRoleApps), 'RoleUsers': tmp}
