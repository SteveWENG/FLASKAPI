# -*- coding: utf-8 -*-
from ..entity.erp.common.AppUserData import AppUserData
from ..entity.erp.common.UserMast import UserMast

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