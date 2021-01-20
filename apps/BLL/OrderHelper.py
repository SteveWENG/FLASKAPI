# -*- coding: utf-8 -*-

from ..entity.erp.Order.OrderHead import OrderHead
from ..entity.erp.Recipe.MenuOrderHead import MenuOrderHead


class OrderHelper:

    @staticmethod
    def dates(data):
        return OrderHead().dates(data.get('costCenterCode',''),data.get('orderType',''))

    @staticmethod
    def updatepo(data):
        return OrderHead().updatepo(data.get('costCenterCode', ''),data.get('headGuid', ''),
                                  data.get('orderDate', ''),data.get('orderType', ''),
                                  data.get('orderSubType', ''),)

    @staticmethod
    def save(data):
        return OrderHead(data).save(data)

    @staticmethod
    def MenuOrder(data):
        return MenuOrderHead().list(data.get('costCenterCode', ''),
                                    data.get('startDate', ''),data.get('endDate', ''))

    @staticmethod
    def SaveMenuOrder(data):
        return MenuOrderHead().save(data)