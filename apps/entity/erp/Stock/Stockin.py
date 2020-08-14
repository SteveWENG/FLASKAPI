# -*- coding: utf-8 -*-
from sqlalchemy import and_, or_, func

from ....utils.functions import *
from ..Stock import TransData

class Stockin(TransData):
    type = 'Stockin'

    @classmethod
    def save(cls, data):
        try:
            dic, itemcodes = cls.PrepareSave(data)
            if data.get('supplierCode','') != '':
                dic['supplierCode'] = data.get('supplierCode')

            #Purchase UOM => Stock UOM
            #去除无值的字段
            li = [{k:v for k,v in dict(dict(l,**{'qty':getNumber(l.get('purQty'))
                                                       * getNumber(l.get('purStkConversion')),
                                 'itemCost': round(getNumber(l.get('purPrice'))
                                                   / getNumber(l.get('purStkConversion')),6)})
                       if not l.get('qty') and getNumber(l.get('purStkConversion')) != 0 else l , **dic)
                .items() if getStr(v) != ''} for l in data.get('data')]

            with cls.adds(li) as session:
                #插入的记录数，和插入记录的min(Id)
                news = session.query(func.count(cls.Id), func.min(cls.Id))\
                    .filter(cls.TransGuid == dic.get('transGuid')).first()
                if news == None or news[0] != len(li):
                    Error('Error in save data')

                #判断本TransDate之前的Stock<0
                if session.query(cls).filter(cls.CostCenterCode == dic.get('costCenterCode'),
                                             cls.ItemCode.in_(itemcodes)) \
                        .filter(or_(cls.Qty < 0, and_(cls.Qty > 0, or_(cls.TransDate < getDate(dic.get('transDate')),
                                                                       and_(cls.TransDate == getDate(dic.get('transDate')),
                                                                            cls.Id < news[1]))))) \
                        .with_entities(cls.ItemCode).group_by(cls.ItemCode) \
                        .having(func.sum(cls.Qty) < 0).first() != None:
                    Error("Can't save PO receipt, because of some stockout after %s" % data.get('date'))

                if hasattr(cls, 'save_check_update'):
                    return cls.save_check_update(li)

                return 'Successfully save stockin'

        except Exception as e:
            raise e
