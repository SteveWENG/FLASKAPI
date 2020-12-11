# -*- coding: utf-8 -*-
from pandas import merge
from sqlalchemy import func, select, case, and_
import pymssql
import pandas as pd

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
                warehouses = [x.get('warehouse') for x in l.get('sites',[])]
                db = l.get('db','')
                if len(warehouses) == 0 or db == '':
                    continue

                tmpsql = cls.query.join(grtbk, cls.COACode == grtbk.COACode) \
                    .filter(cls.Warehouse.in_(warehouses),
                            cls.TransType.in_(('N', 'C', 'P', 'X')),grtbk.omzrek=='G',
                            func.abs(func.coalesce(gbkmut.Qty, 0)) > 1 / 1000000) \
                    .with_entities(cls.Warehouse, cls.ItemCode.label('ItemCode'),
                                   func.round(func.sum(func.coalesce(cls.Qty,0)),6).label('Qty'),
                                   func.round(func.sum(func.coalesce(cls.Amt,0)),6).label('Amt'))\
                    .group_by(cls.Warehouse,cls.ItemCode)\
                    .having(func.round(func.sum(func.coalesce(cls.Qty,0)),6) > 1/1000000)

                dbconfig = cls.dbconnect
                dbconfig['database'] = str(db)
                with pymssql.connect(**dbconfig) as conn:  #host='192.168.0.98:1433', user='sa', password='gladis0083',database='120') as conn:
                    tmp = pd.read_sql(str(tmpsql.statement.compile(compile_kwargs={'literal_binds':True})), conn)

                if tmp.empty:
                    continue

                tmp1 = pd.DataFrame(l.get('sites'))
                tmp = merge(tmp,tmp1, left_on='Warehouse',right_on='warehouse')
                if tmp.empty: continue

                li = li.append(tmp)

            li['ItemCost'] = li['Amt'] / li['Qty']
            li.drop(['Amt','Warehouse','warehouse'], axis=1, inplace=True)
            return li
        except Exception as e:
            raise e