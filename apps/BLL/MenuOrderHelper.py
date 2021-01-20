# -*- coding: utf-8 -*-

from ..entity.erp.Recipe.MenuOrderHead import MenuOrderHead


class MenuOrderHelper:

    @staticmethod
    def list(data):
        return MenuOrderHead().list(data.get('costCenterCode', ''),
                                    data.get('date', ''))

    @staticmethod
    def Save(data):
        return MenuOrderHead().save(data)