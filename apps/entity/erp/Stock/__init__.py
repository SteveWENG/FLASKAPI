# -*- coding: utf-8 -*-


from sqlalchemy import func, select, and_, or_, case, distinct
import pandas as pd

from ....utils.functions import *
from ...erp import erp, db
from ..common.LangMast import LangMast


class TransData(erp):
    __tablename__ = 'tblTransData'

    CostCenterCode = db.Column()
    OrderLineGuid = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column()
    IsServiceItem = db.Column(db.Boolean)
    UOM = db.Column()
    TransDate = db.Column(db.Date)
    SupplierCode = db.Column()
    BusinessType = db.Column()
    Remark = db.Column()
    TransGuid = db.Column()
    CreateUser = db.Column()
    StockQty = db.Column(db.Numeric)
    Qty = db.Column(db.Numeric)
    ItemCost = db.Column(db.Numeric)
    ItemPrice = db.Column(db.Numeric)

    _MAXTRANSID = None
    _StockItems = None

    def todict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns if getattr(self, c.name, None) != None}

    @classmethod
    def StockItemTrans(cls, data):
        return list(filter(lambda l:l.get('isServiceItem',False)==False, data))

    @classmethod
    def ServiceItemTrans(cls, data):
        return list(filter(lambda l: l.get('isServiceItem', False) == True, data))

    @classmethod
    def SummaryInfo(cls, data):
        return {'costCenterCode': data.get('costCenterCode', ''), 'createUser': data.get('creater', ''),
                'transDate': data.get('date', ''), 'transGuid': getGUID(), 'BusinessType': cls.type}

    @classmethod
    def save(cls, data):
        try:
            if not data.get('data'):
                Error(LangMast.getText('0CD4331A-BCD2-468A-A18A-EE4EDA2FF0EE')) # No data to save

            dic = cls.SummaryInfo(data)
            if data.get('supplierCode','') != '':
                dic['supplierCode'] = data.get('supplierCode')

            tmpStockItems = cls.StockItemTrans(data.get('data'))
            cls._StockItems = set([l.get('itemCode') for l in tmpStockItems])
            li = cls.SaveData(tmpStockItems,cls.ServiceItemTrans(data.get('data')), **dic)

            if not li:
                Error(LangMast.getText('0CD4331A-BCD2-468A-A18A-EE4EDA2FF0EE')) # No data to save

            li = [dict({k:v for k,v in l.items() if getStr(v) != ''}, **dic) for l in li]

            with cls.adds(li) as session:
                # 插入的记录数，和插入记录的min(Id)
                news = session.query(func.count(cls.Id), func.min(cls.Id)) \
                    .filter(cls.TransGuid == dic.get('transGuid')).first()
                if not news or news[0] != len(li):
                    Error(LangMast.getText('B03FCA74-D2B4-4504-842E-3A7FD649432F')) # Faild to save data

                if not cls._MAXTRANSID :
                    cls._MAXTRANSID = news[1]
                if hasattr(cls, 'save_check'):
                   return cls.save_check(li)

                return LangMast.getText('F7083ED1-26B3-4BD2-82FD-976C401D4CC0') # Successfully saved stock transactions
        except Exception as e:
            raise e

    @classmethod
    def PrepareSave(cls, data):
        try:
            dic = {'costCenterCode': data.get('costCenterCode', ''), 'createUser': data.get('creater', ''),
                   'transDate': data.get('date', ''), 'transGuid': getGUID(), 'BusinessType': cls.type}

            itemcodes = [] #set([l.get('itemCode') for l in stockItems])
            if len(itemcodes) == 0:
                itemcodes = None
            return dic, itemcodes
        except Exception as e:
            raise e

    #库存查询
    @classmethod
    def FIFO(cls, costCenterCode, date=datetime.date.today(), itemcodes=None):
        if itemcodes == None: # None 无；[] 全部
            Error(LangMast.getText('0CD4331A-BCD2-468A-A18A-EE4EDA2FF0EE')) # No data

        filters = [cls.CostCenterCode==costCenterCode, cls.TransDate<=date]
        if itemcodes != None and len(itemcodes) > 0:
            filters.append(cls.ItemCode.in_(itemcodes))

        tmpsql = cls.query.filter(*filters, func.abs(func.round(func.coalesce(cls.Qty,0),6))>1/1000000)\
            .with_entities(cls.ItemCode,cls.TransDate.label('INDATE'),cls.ItemCost,
                           func.round(cls.Qty,6).label('InQty'),
                           func.max(cls.Id).over().label('MAXID'),
                           # func.max(case([(cls.Qty > 0, cls.Id)], else_=0)).over().label('MAXINID'),
                           # func.max(case([(cls.Qty < 0, cls.Id)], else_=0)).over().label('MAXOUTID'),
                           func.sum(case([(cls.Qty>0, cls.Qty)],else_=0))
                           .over(partition_by=cls.ItemCode,order_by=[cls.TransDate, cls.Id]).label('EndQty'),
                           func.sum(case([(cls.Qty<0, cls.Qty)],else_=-1))
                           .over(partition_by=cls.ItemCode).label('OutQty')).subquery()
        tmpsql = select([tmpsql]).where(and_(tmpsql.c.InQty>0,func.round(tmpsql.c.EndQty+tmpsql.c.OutQty,6)>0))\
            .select_from(tmpsql)
        tmpList = pd.read_sql(tmpsql, cls.getBind())

        # 无库存
        if tmpList.empty:
            Error(LangMast.getText('2CF04A40-C498-406F-964E-36C0B17EC765')) # No stock

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

            now = datetime.date.today()
            li['TransDate'] = datetime.date(now.year,now.month,1) + datetime.timedelta(days=-1)

            li = cls.ZipStockList(li).to_dict('records')
            with cls.adds(li) as _:
                pass
            # li.to_sql('TransData', db.get_engine(db.get_app(),cls.__bind_key__), if_exists='replace')

            return 'Sucessfully update your openning stock'
        except Exception as e:
            raise e

    @classmethod
    def CheckOrderLine(cls, data):
        orderLineGuids = [l.get('orderLineGUID') for l in data if l.get('orderLineGUID', '') != '']
        if not orderLineGuids:
            Error(LangMast.getText('4EF57331-1C22-43DC-8878-81617171E034')) # Shortage of some info of order lines!

        tmp = cls.query.filter(cls.OrderLineGuid.in_(orderLineGuids)) \
            .with_entities(func.count(distinct(cls.OrderLineGuid)).label('GuidCount'),  # 全部save
                           func.count(cls.Id).label('TotalCount')).first()  # 重复save
        if not tmp or tmp.GuidCount < len(orderLineGuids):
            Error(LangMast.getText('B03FCA74-D2B4-4504-842E-3A7FD649432F')) # Can't save order lines!
        if tmp.GuidCount < tmp.TotalCount:
            Error(LangMast.getText('9D310041-7713-4C59-B1C0-BF4639B39552')) # Can't save because already saved this order!

        return orderLineGuids