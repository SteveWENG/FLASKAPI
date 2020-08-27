# -*- coding: utf-8 -*-
from sqlalchemy import func, select, case, and_
import pymssql
import pandas as pd

from .Item import Item as ExactItem
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
    def ClosingStock(cls, dbsites):
        try:
            li = pd.DataFrame([])
            if dbsites == None or len(dbsites)==0:
                return li

            for l in dbsites:
                sites = l.get('sites',[])
                db = l.get('db','')
                if len(sites) == 0 or db == '':
                    continue

                tmpsql = cls.query.join(grtbk, cls.COACode == grtbk.COACode) \
                    .join(ExactItem, cls.ItemCode == ExactItem.Code) \
                    .join(ItemAssortment, ExactItem.Assortment == ItemAssortment.Assortment) \
                    .join(magaz, gbkmut.Warehouse == magaz.Code) \
                    .filter(magaz.Name.in_(sites),
                            cls.COACode == func.coalesce(ExactItem.GLAccountDistribution, ItemAssortment.GLStock),
                            cls.ReminderCount <= 99, cls.TransType.in_(('N', 'C', 'P', 'X')),
                            grtbk.omzrek.in_(('G', 'K', 'N')),
                            func.coalesce(gbkmut.Qty, 0) > 1 / 1000000) \
                    .with_entities(magaz.Name.label('CostCenterCode'), cls.ItemCode.label('ItemCode'),
                                   func.round(func.sum(func.coalesce(cls.Qty,0)),6).label('Qty'),
                                   func.round(func.sum(func.coalesce(cls.Amt,0)),6).label('Amt'))\
                    .group_by(magaz.Name,cls.ItemCode)\
                    .having(func.round(func.sum(func.coalesce(cls.Qty,0)),6) > 1/1000000)

                dbconfig = cls.dbconnect
                dbconfig['database'] = str(db)
                with pymssql.connect(**dbconfig) as conn:  #host='192.168.0.98:1433', user='sa', password='gladis0083',database='120') as conn:
                    li = li.append(pd.read_sql(str(tmpsql.statement.compile(compile_kwargs={'literal_binds':True})), conn))

            li['ItemCost'] = li['Amt'] / li['Qty']
            li = li.drop(['Amt'], axis=1)
            return li
        except Exception as e:
            raise e