# -*- coding: utf-8 -*-
from pandas import merge

from ..entity.erp.common.AppUserData import AppUserData
from ..entity.erp.common.CCMast import CCMast
from ..entity.erp.common.Company import Company
from ..entity.erp.common.LangMast import lang
from ..entity.erp.common.UserMast import UserMast
from ..utils.functions import *


class UserHelper:
    @staticmethod
    def Rights(data):
        try:
            user = UserMast.login(data.get('userName',''),data.get('passWord',''))
            apps = AppUserData.apps(user.Guid, data.get('langCode',''))
            return {
                'userGuid': user.Guid,
                'account': user.UserName,
                'fullName': user.FullName,
                'employeeID': user.EmployeeId,
                'apps':apps
            }
        except Exception as e:
            raise e

    @staticmethod
    def AppData(data):
        try:
            dataType = data.get('dataType','').lower()
            li = AppUserData.data(data.get('userGuid',''),data.get('appGuid',''))
            if li.empty:
                Error(lang('0CD4331A-BCD2-468A-A18A-EE4EDA2FF0EE')) # No data

            li['Type'] = li['Type'].map(lambda x: getStr(x).lower())
            li['Code'] = li['Code'].map(lambda x: getStr(x))

            # 如有code为空，则是全部Company/Site
            types = li.loc[li['Code']=='','Type'].tolist()
            dic = {}
            dic['company'] = [] if 'company' in types else None
            dic['costcenter'] = [] if 'costcener' in types else None

            if len(types) > 0:
                li = li[~li['Type'].isin(types)]

            # 不是全部company或site
            keys = [k for k,v in dic.items() if v==None]
            for s in keys:
                tmp = set(li.loc[li['Type'] == s, 'Code'].tolist())
                if tmp and not dic[s]:
                    dic[s] = tmp

            if dataType == '' or dataType == 'costcenter':
                dic['costcenter'] = CCMast.ShowSites(dic.get('company'),dic.get('costcenter'))
            if (dataType == '' or dataType == 'company') and 'company' in types:
                dic['company'] = CCMast.ShowDB()

            return dic
        except Exception as e:
            raise e