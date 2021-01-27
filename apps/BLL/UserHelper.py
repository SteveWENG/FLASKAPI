# -*- coding: utf-8 -*-
from pandas import merge

from ..entity.erp.common.AppUserData import AppUserData
from ..entity.erp.common.Company import Company
from ..entity.erp.common.CCMast import CCMast
from ..entity.erp.common.CostCenter import CostCenter
from ..entity.erp.common.LangMast import lang
from ..entity.erp.common.UserMast import UserMast
from ..utils.functions import *


class UserHelper:
    @staticmethod
    def Rights(data):
        try:
            user = UserMast.login(data.get('userName',''),data.get('passWord',''))
            apps = AppUserData.apps(user.Guid)
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
    def ChangePassword(data):
        with RunApp():
            if data.get('userGuid','') == '' or data.get('password','') == '':
                Error(lang('A22AEB89-2067-4687-AEE6-D1016020CC52'))

            UserMast.ChangePassword(data.get('userGuid',''),data.get('password',''))
            return lang('2AA6641F-9ED0-4366-845A-21BEE84B4C42')

    @staticmethod
    def AppData(data):
        try:
            dataType = data.get('dataType','').lower().split(',')
            li = AppUserData.data(data.get('userGuid',''),data.get('appGuid',''))
            if li.empty:
                Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF')) # No data

            li['Type'] = li['Type'].map(lambda x: getStr(x).lower())
            li['Code'] = li['Code'].map(lambda x: getStr(x))

            # 如有code为空，则是全部Company/Site
            types = li.loc[li['Code']=='','Type'].tolist()
            dic = {k: [] if k in types else None for k in set(li['Type'])}
            '''
            dic['company'] = [] if 'company' in types else None
            dic['costcenter'] = [] if 'costcener' in types else None
            dic['supplier'] = [] if 'supplier' in types else None
            '''
            if types:
                li = li[~li['Type'].isin(types)]

            # 不是全部company或site
            keys = [k for k,v in dic.items() if v==None]
            for s in keys:
                tmp = set(li.loc[li['Type'] == s, 'Code'])
                if tmp and not dic[s]:
                    dic[s] = [x for x in tmp]

            if not dataType[0] or 'costcenter' in dataType:
                dic['costcenter'] = CostCenter.Sites(dic.get('company'),dic.get('costcenter'),dic.get('costcentercategory')) # CCMast.ShowSites(dic.get('company'),dic.get('costcenter')) # CCMast.ShowSites
            if (not dataType[0] or 'company' in dataType) and 'company' in types and dic.get('company')==[]: # 全部公司
                dic['company'] =  CostCenter.Division()# CCMast.ShowDB()

            return {k:v for k,v in dic.items() if v}
        except Exception as e:
            raise e