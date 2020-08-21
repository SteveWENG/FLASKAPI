# -*- coding: utf-8 -*-
from pandas import merge

from ..entity.erp.common.AppUserData import AppUserData
from ..entity.erp.common.CCMast import CCMast
from ..entity.erp.common.Company import Company
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
                Error('No data')

            li['Type'] = li['Type'].map(lambda x: getStr(x).lower())
            li['Code'] = li['Code'].map(lambda x: getStr(x))

            # 如有company的code为空，= 全部，则删除有具体值的company
            types = li.loc[li['Code']=='','Type'].tolist()
            companysforcostcenter = None
            #全部company/costcenter
            if len(types) > 0:
                li = li[~li['Type'].isin(types)]
                #全部costcenter, 因为是全部company/costcenter
                companysforcostcenter = []

            companys = li.loc[li['Type']=='company','Code'].tolist()
            costcenters = []

            if dataType == '' or dataType == 'costcenter':
                costcenters = li.loc[li['Type'] == 'costcenter', 'Code'].tolist()

                if companysforcostcenter == None and companys:
                    companysforcostcenter = companys
                costcenters = list(set(costcenters + CCMast.show(companysforcostcenter)))

            #全部company
            if (dataType == '' or dataType == 'company') and 'company' in types:
                companys = Company.show()

            dic = {}
            if len(companys)>0:
                dic['company'] = companys
            if len(costcenters)>0:
                dic['costcenter'] = costcenters

            return dic
        except Exception as e:
            raise e