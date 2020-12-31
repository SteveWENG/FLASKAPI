# -*- coding: utf-8 -*-
from copy import copy

from flask import jsonify
from pandas import merge

from ..entity.erp.common.AppHelp import AppHelp
from ..entity.erp.common.AppUserData import AppUserData
from ..entity.erp.common.Apps import Apps
from ..entity.erp.common.CostCenter import CostCenter
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
        tmpSites = pd.DataFrame(CostCenter.Sites())

        tmp = tmpRoleApps[['RoleGuid','RoleName']]
        tmp.drop_duplicates(inplace=True)

        tmpRoleUsers = merge(tmpRoleUsers,tmp, left_on="RoleGuid", right_on="RoleGuid")
        DataFrameSetNan(tmpRoleUsers)

        tmpRoleUsers = [{'RoleGuid':g1[0],'RoleName':g2.iloc[0]['RoleName'],'UserGuid':g2.iloc[0]['UserGuid'],
                'UserName':g1[1],'FullName':g2.iloc[0]['FullName'],
                'compSite':[[v for v in l.values()] for l in g2[['Division','CostCenterCode','Id']].to_dict('record')]}
               for g1, g2 in tmpRoleUsers
                            .sort_values(by=['RoleName', 'Division', 'CostCenterCode'])
                            .groupby(['RoleGuid','UserName'])]
        tmpSites = [{'value':g1,'children':getdict(g2[['CostCenterCode']].rename(columns={'CostCenterCode':'value'}))}
                    for g1,g2 in tmpSites.groupby(['Division'])]
        return {'apps': tmpApps, 'RoleApps': getdict(tmpRoleApps), 'RoleUsers': tmpRoleUsers, 'Sites':tmpSites}

    @staticmethod
    def SaveUserRole(data):
        tmp = data.get('data')
        return RoleUserData.save(tmp)