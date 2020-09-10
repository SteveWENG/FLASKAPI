# -*- coding: utf-8 -*-

import pandas as pd
from ..Item import BI_DM, db

class ItemMast(BI_DM):
    __tablename__= 'DM_D_DIGIMENU_STDPURCHASEPRICE'

    Division = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column()
    StockUnit = db.Column('Stock_Unit')

    @classmethod
    def list(cls, divsion):
        sql = cls.query.filter(cls.Division==divsion)
        return pd.read_sql(sql.statement,cls.getBind())