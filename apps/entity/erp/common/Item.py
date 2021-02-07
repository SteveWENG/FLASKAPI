# -*- coding: utf-8 -*-
from sqlalchemy.ext.hybrid import hybrid_property

from .LangMast import lang
from ...erp import erp, db
import pandas as pd

class Item1(erp):
    __tablename__ = 'DM_D_ERP_ITEM1'

    ItemCode = db.Column()
    #ItemName = db.Column('ItemName_ZH')
    StockUnit = db.Column('Stock_Unit')
    Category01 = db.Column()
    Category02 = db.Column()
    Category03 = db.Column()
    Category04 = db.Column()

    @hybrid_property
    def ItemName(self):
        return self.LangColumn('ItemName_')

    @classmethod
    def list(cls):
        sql = cls.query.with_entities(cls.ItemCode, cls.ItemName.label('ItemName'), cls.StockUnit).distinct()
        return pd.read_sql(sql.statement, cls.getBind())