# -*- coding: utf-8 -*-

from ....utils.functions import Error
from .Stockin import Stockin
from ..Order.OrderLine import OrderLine

class POStockin(Stockin):
    type = 'PO'

    #1 检查对应的PO是否已入库
    #2 更新POlines中的剩余数量=0
    @classmethod
    def save_check_update(cls, data):
        try:
            if not data or len(data) == 0:
                Error('No PO lines to save!')

            orderLineGuids = [l.get('orderLineGUID') for l in data if l.get('orderLineGUID','') != '']
            if not orderLineGuids or len(orderLineGuids) < len(data):
                Error('Shortage of some info of PO lines!')
            '''
            if OrderLine.query.filter(OrderLine.Guid.in_(orderLineGuids), OrderLine.RemainQty != 0,
                                      OrderLine.DeleteTime == None) \
                .update({'RemainQty':0 },synchronize_session=False) != len(orderLineGuids) :
                Error('This PO has been received')
            '''
            return 'Successfully save PO receipt'
        except Exception as e:
            raise e