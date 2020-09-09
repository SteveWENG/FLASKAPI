# -*- coding: utf-8 -*-

from ..entity.erp.Order.OrderHead import OrderHead



class OrderHelper:

    @staticmethod
    def save(data):
        return OrderHead(data).save(data.get('orderLines'))