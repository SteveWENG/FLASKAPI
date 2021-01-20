# -*- coding: utf-8 -*-

import pandas as pd
from ...erp import erp, db
from ..common.CCMast import CCMast

class ItemMast(erp):
    __tablename__= 'DM_D_DIGIMENU_STDPURCHASEPRICE'

    Division = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column()
    Stock_Unit = db.Column('Stock_Unit')

    @classmethod
    def list(cls, costCenterCode):
        sql = cls.query.filter(cls.Division==CCMast.GetDivision(costCenterCode))\
            .with_entities(cls.ItemCode,cls.ItemName,cls.Stock_Unit).distinct()
        return pd.read_sql(sql.statement,cls.getBind())