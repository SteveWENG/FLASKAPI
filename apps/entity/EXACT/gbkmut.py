# -*- coding: utf-8 -*-
from sqlalchemy import func, select, case, and_
import pymssql
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
    Amt = db.Column('bdr_hfl',db.Numeric)

    COACode = db.Column('reknr')
    TransType = db.Column()
    ReminderCount = db.Column(db.Integer)

    @classmethod
    def ClosingStock(cls, dbName, costCenterCode):
        try:
            tmpsql = cls.query.join(grtbk, cls.COACode == grtbk.COACode) \
                .join(Items, cls.ItemCode == Items.Code) \
                .join(ItemAssortment, Items.Assortment == ItemAssortment.Assortment) \
                .join(magaz, gbkmut.Warehouse == magaz.Code) \
                .filter(magaz.Name == costCenterCode,
                        cls.COACode == func.coalesce(Items.GLAccountDistribution, ItemAssortment.GLStock),
                        cls.ReminderCount <= 99, cls.TransType.in_(('N', 'C', 'P', 'X')),
                        grtbk.omzrek.in_(('G', 'K', 'N')), func.abs(func.coalesce(gbkmut.Qty, 0)) > 1 / 1000000) \
                .with_entities(cls.ItemCode.label('ItemCode'), cls.TransDate.label('TransDate'),
                               cls.Id, cls.Qty.label('Qty'),
                               func.sum(case([(cls.Qty > 0, cls.Qty)], else_=0))
                               .over(partition_by=cls.ItemCode, order_by=[cls.TransDate,cls.Id])
                               .label('RunningQty'),
                               func.sum(case([(cls.Qty < 0, cls.Qty)], else_=0))
                               .over(partition_by=cls.ItemCode)
                               .label('OutQty'),
                               func.round(case([(cls.Qty > 0, func.coalesce(cls.Amt,0)/cls.Qty)], else_=0),6)
                               .label('ItemCost')).subquery()

            tmpsql = select([tmpsql])\
                .where(and_(func.round(tmpsql.c.Qty,6)>1/1000000,
                       func.round(tmpsql.c.RunningQty+tmpsql.c.OutQty,6)>1/1000000))\
                .select_from(tmpsql)

            dbconfig = cls.dbConfig
            dbconfig['database'] = str(dbName)
            with pymssql.connect(**dbconfig) as conn:  #host='192.168.0.98:1433', user='sa', password='gladis0083',database='120') as conn:
                tmpList = pd.read_sql(str(tmpsql.compile(compile_kwargs={'literal_binds':True})), conn)
                # db.get_engine(db.get_app(),cls.__bind_key__).connect())

            if tmpList.empty:
                return tmpList

            tmpList.loc[tmpList['Qty']>(tmpList['RunningQty']+tmpList['OutQty']),'Qty'] =\
                tmpList['RunningQty']+tmpList['OutQty']
            tmpList = tmpList.sort_values(by=['ItemCode', 'TransDate', 'Id'], axis=0).reset_index(drop=True)

            return tmpList
        except Exception as e:
            raise e