# -*- coding: utf-8 -*-

from functools import reduce
from sqlalchemy import func, select, and_, or_, case, distinct
import pandas as pd
from pandas import merge

from ..Item import Item
from ..common.CostCenter import CostCenter
from ..common.Supplier import Supplier
from ....utils.functions import *
from ...erp import erp, db
from ..common.LangMast import lang

class TransData(erp):
    __tablename__ = 'tblTransData'

    CostCenterCode = db.Column()
    OrderLineGuid = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column()
    BatchGuid = db.Column(server_default='newid()')
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
    SysGuid = db.Column(server_default='newid()')
    DeleteTime = db.Column(server_default='getdate()')

    @classmethod
    def SummaryInfo(cls, data):
        dic = {'costCenterCode': data.get('costCenterCode', ''), 'createUser': data.get('creater', ''),
                'transGuid': getGUID(), 'BusinessType': cls.type}

        if data.get('supplierCode', '') != '':
            dic['supplierCode'] = data.get('supplierCode')

        if data.get('date', '') != '':
            dic['transDate'] = data.get('date')
        return dic

    @classmethod
    def save(cls, data):
        try:
            if not data.get('data'):
                Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF')) # No data to save

            dic = cls.SummaryInfo(data)
            if data.get('supplierCode','') != '':
                dic['supplierCode'] = data.get('supplierCode')

            itemcodes = set([l.get('itemCode') for l in data.get('data')])

            tmpd = pd.DataFrame(data.get('data'))
            tmpd = DataFrameSetNan(tmpd)

            #合并qty, purQty, xxxQty
            fqtys = {s:'sum' for s in tmpd if s.lower().endswith('qty')==True}
            fgroup = [s for s in tmpd if s.lower().endswith('qty') !=True]
            tmpd = tmpd.groupby(by=fgroup, as_index=False).agg(fqtys)

            # 存前数据处理
            li = cls.SaveData(tmpd, **dic, itemCodes=itemcodes)
            if li.empty:
                Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF')) # No data to save

            li = DataFrameSetNan(li)
            li = [dict({k:v for k,v in l.items() if getStr(v) != ''}, **dic) for l in li.to_dict('records')]

            with cls.adds([l for l in li if abs(round(l.get('qty',0),6)) > 0]) as session:
                # 存后数据检查
                if hasattr(cls, 'save_check'):
                    return cls.save_check(li,session=session, itemCodes=itemcodes,
                                          orderLineCreateTime=data.get('orderLineCreateTime',''))

            return lang('F7083ED1-26B3-4BD2-82FD-976C401D4CC0') # Successfully saved stock transactions
        except Exception as e:
            raise e

    @classmethod
    def __FiltersOfStock(cls,costCenterCode, date=datetime.date.today(), itemcodes=[]):
        if itemcodes == None: # None 无；[] 全部
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF')) # No data

        filters = [func.coalesce(cls.IsServiceItem,False)==False,cls.CostCenterCode==costCenterCode,
                   cls.DeleteTime == None,
                   func.abs(func.round(func.coalesce(cls.Qty,0),6))>1/1000000]
        if len(itemcodes) > 0:
            filters.append(cls.ItemCode.in_(itemcodes))

        # filters.append(or_(cls.TransDate<=date, func.round(func.coalesce(cls.Qty,0),6)<-1/1000000))
        # 期初和入库<=date,其它业务全部
        filters.append(or_(~cls.BusinessType.in_(['OpeningStock','POReceipt','Stockin']), cls.TransDate<=date))
        return filters

    @classmethod
    def ItemStock(cls,costCenterCode, date=datetime.date.today(), itemcodes=[]):
        return pd.read_sql(cls.query.filter(*cls.__FiltersOfStock(costCenterCode,date,itemcodes))
                           .with_entities(cls.ItemCode,
                                          func.round(func.sum(func.coalesce(cls.Qty,0)),6).label('stockQty'))
                           .group_by(cls.ItemCode)
                           .having(func.round(func.sum(func.coalesce(cls.Qty,0)),6) >1/1000000).statement,cls.getBind())

    #Batch cost
    @classmethod
    def ItemBatchCost(cls,costCenterCode, date=datetime.date.today(), itemcodes=[]):
        try:
            qry = cls.query.filter(*cls.__FiltersOfStock(costCenterCode,date,itemcodes))\
                .with_entities(cls.ItemCode,cls.BatchGuid,func.min(cls.Id).label('Id'),
                               func.round(func.sum(func.coalesce(cls.Qty,0)),6).label('Qty'),
                               func.min(case([(func.coalesce(cls.Qty, 0) > 0, cls.TransDate)],
                                             else_=None)).label('TransDate'),
                               func.max(case([(func.coalesce(cls.Qty, 0) > 0, cls.ItemCost)],
                                             else_=0)).label('ItemCost'))\
                .group_by(cls.ItemCode,cls.BatchGuid)\
                .having(func.round(func.sum(func.coalesce(cls.Qty,0)),6) >1/1000000)

            tmp = pd.read_sql(qry.statement, cls.getBind())
            if tmp.empty:
                return tmp

            tmp = tmp.sort_values(axis=0,by=['ItemCode','TransDate','Id'])
            tmp['EndQty'] = tmp.groupby('ItemCode')['Qty'].cumsum()
            return tmp
        except Exception as e:
            raise e

    @classmethod
    def UpdateOpenningStock(cls, li):
        try:
            if li.empty:
                return 'No openning stock'

            li['BusinessType'] = 'OpenningStock'
            li['TransGuid'] = getGUID()

            now = datetime.date.today()
            li['TransDate'] = datetime.date(now.year,now.month,1) + datetime.timedelta(days=-1)
            li = li.to_dict('records')
            with cls.adds(li) as _:
                pass
            # li.to_sql('TransData', db.get_engine(db.get_app(),cls.__bind_key__), if_exists='replace')

            return 'Sucessfully update your openning stock'
        except Exception as e:
            raise e

    # tblTransData中OrderLineGuid
    # 采购入库，一条采购行只能入一次
    # 收入，一条销售行可以多次出库
    @classmethod
    def CheckOrderLine(cls, data, **kw):
        orderLineGuids = [l.get('orderLineGuid') for l in data
                          if l.get('orderLineGuid', '') != '' and abs(round(l.get('qty',0),6))>0]
        if not orderLineGuids:
            if str(cls).find('DailyTicket.DailyTicket')  > 0:
                Error(lang('4EF57331-1C22-43DC-8878-81617171E034') % ''.join(orderLineGuids)) # Shortage of some info of order lines!
            else: # 采购入库0收货
                return

        # 检查tblTransData中OrderLineGuid重复
        filters = [cls.OrderLineGuid.in_(orderLineGuids),cls.DeleteTime==None]
        if str(cls).find('DailyTicket.DailyTicket')  > 0:  # 收入一天一个
            filters.append(cls.CostCenterCode == data[0].get('costCenterCode'))
            filters.append(cls.TransDate == data[0].get('transDate'))

        session = kw.get('session')
        tmp = session.query(cls).filter(*filters) \
            .with_entities(func.count(distinct(cls.OrderLineGuid)).label('GuidCount'),  # 全部save
                           func.count(cls.Id).label('TotalCount')).first()  # 重复save

        if tmp.GuidCount < tmp.TotalCount:
            Error(lang('9D310041-7713-4C59-B1C0-BF4639B39552')) # Can't save because already saved this order!


    # divisions: 120,110
    # costCenterCodes: 120SKF01,120CAS01
    # type detail, batch
    @classmethod
    def list(cls, data):
        filters = []
        divisions = getStr(data.get('divisions', ''))
        costCenterCodes = getStr(data.get('costCenterCodes', ''))
        supplierCode = data.get('supplierCode', '')
        if divisions:
            filters.append(CostCenter.Division.in_(divisions.split(',')))
        if costCenterCodes:
            filters.append(cls.CostCenterCode.in_(costCenterCodes.split(',')))
        if len(filters) == 2:
            filters = [or_(*filters)]

        if supplierCode:
            filters.append(cls.SupplierCode==supplierCode)
        filters.append(func.round(cls.Qty, 6) != 0)
        filters.append(cls.DeleteTime==None)

        openning = getStr(data.get('openning','false')).lower() == 'true'
        startDate = getStr(data.get('startDate',''))
        endDate = getStr(data.get('endDate',''))
        type = data.get('type','detail').lower()
        if type == 'batch':
            openning = True
        elif type == 'dailyporeceipt':
            openning = False
            filters.append(cls.BusinessType=='POReceipt')

        if not startDate: startDate = endDate if endDate else datetime.date.today()
        if not endDate: endDate = startDate

        df = pd.DataFrame([])
        df1 = cls._list(filters.copy(),startDate,'',openning,type)
        if not df1.empty: df = df.append(df1)
        df2 = cls._list(filters, startDate, endDate, openning, type)
        if not df2.empty: df = df.append(df2)

        if df1.empty and df2.empty:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))

        DataFrameSetNan(df)
        if type == 'batch':
            groupfields = [f for f in set(df.columns)
                           if ('qty' not in f.lower() and 'amt' not in f.lower() and 'supplier' not in f.lower()
                               and f.lower() not in ['remark'])]
            sumfields = {f :max if 'supplier' in f.lower() else sum for f in set(df.columns)
                         if ('qty' in f.lower() or 'amt' in f.lower() or 'supplier' in f.lower())}
            df = df.groupby(by=groupfields, as_index=False).agg(sumfields)
            for f in ['Qty','Amt']:
                df[f+'Closing'] = df[f+'Openning'] \
                                  +(df[f+'In'] if (f+'In') in df.columns else 0)\
                                  + (df[f+'Out'] if (f+'Out') in df.columns else 0)
            for f in [f for f in df.columns if ('qty' in f.lower() or 'amt' in f.lower())]:
                df[f] = round(df[f],6)

        return [{**{k: getVal(v) for k,v in l.items()
                 if k.lower() not in ['suppliercode','suppliername','remark'] and v},
                'SupplierCode':l.get('SupplierCode',''),'SupplierName':l.get('SupplierName',''),
                 'Remark':l.get('Remark','')}
                for l in df.to_dict('records')]

    # 期初：startDate, openning,type
    # 明细： startDate, endDate, openning, type
    @classmethod
    def _list(cls, filters, startDate, endDate='', openning=True, type='detail'):
        if not openning and not endDate and type in ['detail','dailyporeceipt']:
            return pd.DataFrame([])

        qry = cls.query.join(CostCenter, cls.CostCenterCode == CostCenter.CostCenterCode)

        fields = [CostCenter.Division, cls.CostCenterCode, Item.Category01,
                  Item.Category02, Item.Category03,Item.Category04,
                  cls.ItemCode, Item.ItemName.label('ItemName')]
        sumfields = []
        if not endDate: #现在查询期初
            sumfields = sumfields + [func.round(func.sum(cls.Qty), 6).label('Qty'),
                                     func.round(func.sum(cls.Qty * cls.ItemCost), 6).label('Amt')]
            if type == 'batch': # 现在查询期初而且batch
                sumfields = sumfields + [func.max(cls.SupplierCode).label('SupplierCode'),
                                         func.max(Supplier.SupplierName).label('SupplierName')]

        if openning or type in ['batch','dailyporeceipt']: # 需要期初或batch
            qry = qry.join(Item, cls.ItemCode == Item.ItemCode)
        else:
            qry = qry.outerjoin(Item, cls.ItemCode == Item.ItemCode)

        if type == 'dailyporeceipt':
            qry = qry.join(Supplier, and_(CostCenter.Division == Supplier.Division,
                                          cls.SupplierCode == Supplier.SupplierCode))
        elif type == 'batch' or endDate: # 现在查询batch，或明细
            qry = qry.outerjoin(Supplier, and_(CostCenter.Division == Supplier.Division,
                                               cls.SupplierCode == Supplier.SupplierCode))

        if endDate: # 现在查询明细
            filters = filters + [cls.TransDate>=startDate, cls.TransDate<=endDate]
            fields = fields + [cls.SupplierCode, Supplier.SupplierName.label('SupplierName'), cls.Qty,
                               func.coalesce(cls.ItemCost,cls.ItemPrice).label('Cost')]
            if type != 'batch':
                fields = fields + [cls.Remark, cls.TransDate,cls.BusinessType,cls.BatchGuid]
        else: # 现在查询期初
            filters.append(cls.TransDate<startDate)

        qry = qry.filter(*filters)
        if not endDate:
            qry = qry.group_by(*fields).having(func.round(func.sum(cls.Qty),6)!=0)

        qry = qry.with_entities(*fields,*sumfields)
        qry = pd.read_sql(qry.statement,cls.getBind())

        if qry.empty:
            return qry

        if endDate:
            qry['Amt'] = round(qry['Qty'] * qry['Cost'],6)
        else: #期初
            if type != 'batch':
                qry['TransDate'] = startDate
                qry['BusinessType'] = 'Openning'
            qry['Cost'] = round(qry['Amt'] / qry['Qty'],6)

        if type == 'batch':
            if endDate:
                qry.loc[qry['Qty'] > 0, 'QtyIn'] = qry['Qty']
                qry.loc[qry['Qty'] < 0, 'QtyOut'] = qry['Qty']
                qry.loc[qry['Qty'] > 0, 'AmtIn'] = qry['Amt']
                qry.loc[qry['Qty'] < 0, 'AmtOut'] = qry['Amt']
                qry.drop(['Qty','Amt'], axis=1, inplace=True)
            else: # 期初
                qry.rename(columns={'Qty': 'QtyOpenning', 'Amt': 'AmtOpenning'}, inplace=True)

        return qry