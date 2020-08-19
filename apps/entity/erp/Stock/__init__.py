# -*- coding: utf-8 -*-
from operator import attrgetter
import random
from sqlalchemy import func, select, and_, or_, case
import pandas as pd

from ....utils.functions import *
from ...erp import erp, db

class TransData(erp):
    __tablename__ = 'tblTransData'

    CostCenterCode = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column()
    UOM = db.Column()
    TransDate = db.Column(db.Date)
    OrderLineGuid = db.Column()
    SupplierCode = db.Column()
    BusinessType = db.Column()
    Remark = db.Column()
    TransGuid = db.Column()
    CreateUser = db.Column()
    StockQty = db.Column(db.Numeric)
    Qty = db.Column(db.Numeric)
    ItemCost = db.Column(db.Numeric)
    ItemPrice = db.Column(db.Numeric)

    def todict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns if getattr(self, c.name, None) != None}

    @classmethod
    def PrepareSave(cls, data):
        try:
            dic = {'costCenterCode': data.get('costCenterCode', ''), 'createUser': data.get('creater', ''),
                   'transDate': data.get('date', ''), 'transGuid': getGUID(), 'BusinessType': cls.type}

            itemcodes = set([l.get('itemCode') for l in data.get('data')])

            return dic, itemcodes
        except Exception as e:
            raise e

    #库存查询
    @classmethod
    def show(cls, costCenterCode, date=datetime.date.today(), itemcodes=None):
        filters = [cls.CostCenterCode==costCenterCode, cls.TransDate<=date]
        if itemcodes != None and len(itemcodes) > 0:
            filters.append(cls.ItemCode.in_(itemcodes))

        tmpsql = cls.query.filter(*filters, func.abs(func.round(func.coalesce(cls.Qty,0),6))>1/1000000)\
            .with_entities(cls.ItemCode,cls.TransDate.label('INDATE'),cls.ItemCost,
                           func.round(cls.Qty,6).label('InQty'),
                           func.max(case([(cls.Qty > 0, cls.Id)], else_=0)).over().label('MAXINID'),
                           func.max(case([(cls.Qty < 0, cls.Id)], else_=0)).over().label('MAXOUTID'),
                           func.sum(case([(cls.Qty>0, cls.Qty)],else_=0))
                           .over(partition_by=cls.ItemCode,order_by=[cls.TransDate, cls.Id]).label('EndQty'),
                           func.sum(case([(cls.Qty<0, cls.Qty)],else_=-1))
                           .over(partition_by=cls.ItemCode).label('OutQty')).subquery()
        tmpsql = select([tmpsql]).where(and_(tmpsql.c.InQty>0,func.round(tmpsql.c.EndQty+tmpsql.c.OutQty,6)>0))\
            .select_from(tmpsql)
        tmpList = pd.read_sql(tmpsql, cls.getBind())

        '''
        #出库总和
        tmpOut = cls.query.filter(*filters,cls.Qty < 0)\
            .with_entities(cls.ItemCode,func.max(cls.Id).label('OUTID'),func.sum(cls.Qty).label('OutQty'))\
            .group_by(cls.ItemCode).subquery()

        #入库明细
        tmpIn = cls.query.filter(*filters, cls.Qty > 0, cls.TransDate<=date,
                                 ~cls.query.filter(*filters, cls.Qty < 0, cls.TransDate>date).exists()) \
            .with_entities(cls.ItemCode, cls.TransDate.label('INDATE'),cls.Id, cls.Qty.label('InQty'),cls.ItemCost,
                          func.max(cls.Id).over().label('MAXINID'),
                          func.sum(cls.Qty).over(partition_by=cls.ItemCode,
                                                 order_by=[cls.TransDate, cls.Id]).label('RunningQty')).subquery()

        #RunningQty > OutQty
        tmpSelect = select([tmpIn, func.coalesce(tmpOut.c.OUTID,0).label('OUTID'),
                            (tmpIn.c.RunningQty + func.coalesce(tmpOut.c.OutQty,0)).label('EndQty')]) \
            .where((tmpIn.c.RunningQty + func.coalesce(tmpOut.c.OutQty,0))> 0) \
            .select_from(tmpIn.outerjoin(tmpOut, tmpIn.c.ItemCode==tmpOut.c.ItemCode))

        tmpList = db.session.execute(tmpSelect).fetchall()
        # 无库存
        if tmpList and len(tmpList) == 0:
            return None
            
        tmpList = pd.DataFrame([{k:v for k,v in l.items() if k != 'RunningQty'}
                                for l in sorted(tmpList, key=attrgetter('ItemCode','INDATE','Id'),
                                                reverse=False)])
        '''
        # 无库存
        if tmpList.empty:
            return None

        tmpList['EndQty'] = tmpList['EndQty'] + tmpList['OutQty']
        tmpList['StartQty'] = tmpList['EndQty'] - tmpList['InQty'] #tmpList.apply(lambda l: max(getNumber(l.get('EndQty')) - getNumber(l.get('Qty')), 0), axis=1)
        tmpList.loc[tmpList['StartQty']<0,'StartQty'] = 0
        tmpList.loc[tmpList['StartQty']==0, 'InQty'] = tmpList['EndQty']
        tmpList = tmpList.drop(['OutQty'],axis=1)
        return tmpList

    @classmethod
    def ZipStockList(cls, li):
        if li.empty:
            return li

        matchedFields = [s for s in li if s.lower() in ('itemcode','itemcost')]
        if len(li) == len(li.groupby(matchedFields)):
            return li

        qtyField = ''.join([s for s in li if s.lower()=='qty'])
        li = li.reset_index(drop=True)
        for x in range(len(li)-1, 0, -1):
            # 相同ItemCode/ItemCost
            if ''.join([s for s in matchedFields if li.loc[x,s] != li.loc[x-1,s]]) == '':
                li.loc[x-1,qtyField] = li.loc[x-1,qtyField] + li.loc[x,qtyField]
                li.loc[x,qtyField] = 0

        return li[li[qtyField] != 0].reset_index(drop=True) #.to_dict('records')

    @classmethod
    def UpdateOpenningStock(cls, li):
        try:
            if li.empty:
                return 'No openning stock'

            li['BusinessType'] = 'OpenningStock'
            li['TransGuid'] = getGUID()
            li = cls.ZipStockList(li).to_dict('records')
            with cls.adds(li) as _:
                pass
            # li.to_sql('TransData', db.get_engine(db.get_app(),cls.__bind_key__), if_exists='replace')

            return 'Sucessfully update your openning stock'
        except Exception as e:
            raise e