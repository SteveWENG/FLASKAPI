# -*- coding: utf-8 -*-

import pandas as pd
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
                dflines.drop([f for f in dflines if f.lower()=='id'], axis=1, inplace=True)

            dflines['remainQty'] = dflines['qty']
            self.lines = [OrderLine(l) for l in dflines.to_dict(orient='records')]

            with SaveDB() as session:
                session.merge(self)

            return lang('A16AAA03-DCE8-4936-9D9E-FE23F9AE7378')
        except Exception as e:
            raise e

    @classmethod
    def list(cls, costCenterCode, date, supplierCode):
        qry = cls.query.join(OrderLine, cls.HeadGuid == OrderLine.HeadGuid) \
            .join(CCMast, CCMast.CostCenterCode==costCenterCode) \
            .join(ItemMast, and_(OrderLine.ItemCode == ItemMast.ItemCode,CCMast.DBName==ItemMast.Division)) \
            .filter(cls.CostCenterCode == costCenterCode, cls.OrderDate == date, cls.Active==True,
                    OrderLine.SupplierCode == supplierCode, OrderLine.RemainQty != 0,
                    func.lower(OrderLine.Status) != 'completed', OrderLine.DeleteTime == None) \
            .with_entities(OrderLine.Guid.label('orderLineGuid'), OrderLine.ItemCode.label('itemCode'),
                           ItemMast.ItemName.label('itemName'), ItemMast.StockUnit.label('uom'),
                           OrderLine.PurchasePrice.label('purPrice'),
                           OrderLine.PurStk_Conversion.label('purStk_Conversion'),
                           OrderLine.RemainQty.label('qty'), OrderLine.Remark.label('remark')).all()


        # tmp = [{k:getVal(getattr(l,k)) for k in l.keys() if getattr(l,k)} for l in qry]
        return getdict(qry)