# -*- coding: utf-8 -*-
from sqlalchemy import func, select
import pandas as pd

from .Item import Items
from .ItemAssortment import ItemAssortment
from .grtbk import grtbk
from ..EXACT import db, EXACT
from .magaz import magaz

class gbkmut(EXACT):
    __tablename__ = 'gbkmut'

    Warehouse = db.Column()
    ItemCode = db.Column('artcode')
    TransDate = db.Column('datum', db.Date)
    Qty = db.Column('aantal',db.Numeric)

    COACode = db.Column('reknr')
    TransType = db.Column()
    ReminderCount = db.Column(db.Integer)

    @classmethod
    def ClosingStock(cls, costCenterCode):
        try:
            tmpsql = cls.query.join(grtbk, cls.COACode == grtbk.COACode) \
                .join(Items, cls.ItemCode == Items.Code) \
                .join(ItemAssortment, Items.Assortment == ItemAssortment.Assortment) \
                .join(magaz, gbkmut.Warehouse == magaz.Code) \
                .filter(magaz.Name == costCenterCode,
                        cls.COACode == func.coalesce(Items.GLAccountDistribution, ItemAssortment.GLStock),
                        cls.ReminderCount <= 99,
                        cls.TransType.in_(('N', 'C', 'P', 'X')),
                        grtbk.omzrek.in_(('G', 'K', 'N')))
            tmpOut= tmpsql.filter(func.coalesce(gbkmut.Qty, 0) < -1/1000000) \
                .with_entities(cls.ItemCode.label('ItemCode'), func.sum(cls.Qty).label('OutQty')).group_by(cls.ItemCode).subquery()
            tmpIn = tmpsql.filter(func.coalesce(gbkmut.Qty, 0) > -1/1000000) \
                .with_entities(cls.ItemCode.label('ItemCode'), cls.TransDate,
                               func.sum(cls.Qty).over(partition_by=cls.ItemCode,
                                                      order_by=[cls.TransDate, cls.Id]).label('RunningQty')).subquery()

            tmpSelect = select([tmpIn, func.coalesce(tmpOut.c.OutQty, 0).label('OutQty')]) \
                .where((tmpIn.c.RunningQty + func.coalesce(tmpOut.c.OutQty, 0)) > 1/1000000) \
                .select_from(tmpIn.outerjoin(tmpOut, tmpIn.c.ItemCode == tmpOut.c.ItemCode))

            tmpList = db.session.execute(tmpSelect).fetchall()
            if tmpList == None or len(tmpList) == 0:
                return None

            tmpList = pd.DataFrame([{k:v for k,v in l.items()} for l in tmpList])

            x = 1
        except Exception as e:
            raise e