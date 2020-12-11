# -*- coding: utf-8 -*-
from ..entity.erp.common.AppHelp import AppHelp

class CommonHelper:

    @staticmethod
    def AppHelp(data):
        return AppHelp.list(data.get('appGuid',''))