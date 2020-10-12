# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sqlalchemy import func,and_

from .OrderLine import OrderLine
from ..Item.ItemMast import ItemMast
from ..common.CCMast import CCMast
from ..common.LangMast import lang
from ....utils.functions import *
from ...erp import erp, db
from .OrderLine import OrderLine
from ....entity import SaveDB, dblog


class OrderHead(erp):
    __tablename__ = 'tblOrderHead'

    HeadGuid = db.Column()
    OrderNo = db.Column()
    OrderDate = db.Column(db.Date)
    CostCenterCode = db.Column()
    CreateUser = db.Column()
    AppGuid = db.Column()
    AppStatus = db.Column()
    FromType = db.Column()
    Active = db.Column(db.Boolean, default=True)

    lines = db.relationship('OrderLine', primaryjoin='OrderHead.HeadGuid == foreign(OrderLine.HeadGuid)',
                            lazy='joined')

    @dblog
    def save(self,data):
        try:
            dflines = pd.DataFrame(data.get('orderLines'))

            # 新增
            if getNumber(self.Id) < 1:
                self.HeadGuid = getGUID()
                self.Id = None
                dflines.drop(['ID'], axis=1, inplace=True)

            dflines['remainQty'] = dflines.apply(lambda x: (getNumber(x['adjQty']) if 'adjQty' in dflines.columns else 0)
                                                           +getNumber(x['qty']),  axis=1)
            dflines = dflines[(dflines['remainQty'] !=0)|(dflines['qty'] !=0)]
            self.lines = [OrderLine(getdict(l),True) for l in dflines.to_dict(orient='records')]

            with SaveDB() as session:
                # 已入库，不能修改
                if OrderLine.query.filter(OrderLine.HeadGuid==self.HeadGuid,
                                          OrderLine.Qty+func.coalesce(OrderLine.AdjQty,0)>0,
                                          OrderLine.RemainQty==0).first():
                    Error(lang('A88FDE80-74BF-4553-AB45-28F4751D74DB'))
                session.merge(self)

            return lang('A16AAA03-DCE8-4936-9D9E-FE23F9AE7378')
        except Exception as e:
            raise e

    @classmethod
    def list(cls, costCenterCode, date, supplierCode):
        # .join(CCMast, CCMast.CostCenterCode==costCenterCode) \
        # .join(ItemMast, and_(OrderLine.ItemCode == ItemMast.ItemCode,CCMast.DBName==ItemMast.Division)) \
        sql = cls.query.join(OrderLine, cls.HeadGuid == OrderLine.HeadGuid) \
            .filter(cls.CostCenterCode == costCenterCode, cls.OrderDate == date, cls.Active==True,
                    OrderLine.SupplierCode == supplierCode, OrderLine.RemainQty != 0,
                    func.lower(OrderLine.Status) != 'completed', OrderLine.DeleteTime == None) \
            .with_entities(OrderLine.Guid.label('orderLineGuid'), OrderLine.ItemCode.label('itemCode'),
                           #ItemMast.ItemName.label('itemName'), ItemMast.StockUnit.label('uom'),
                           OrderLine.PurchasePrice.label('purPrice'),
                           OrderLine.CreateTime,
                           OrderLine.PurStk_Conversion.label('purStk_Conversion'),
                           OrderLine.RemainQty.label('qty'), OrderLine.Remark.label('remark'))
        tmpdf = pd.read_sql(sql.statement, cls.getBind())
        tmpdf['CreateTime'] = tmpdf['CreateTime'].max().strftime('%Y-%m-%d %H:%M:%S.%f')
        # tmpdf.rename(columns={'CreateTime': 'orderLineCreateTime'}, inplace=True)

        # tmp = [{k:getVal(getattr(l,k)) for k in l.keys() if getattr(l,k)} for l in qry]
        return tmpdf    #.to_dict('records')