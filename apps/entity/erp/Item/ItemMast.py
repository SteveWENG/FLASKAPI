# -*- coding: utf-8 -*-

import pandas as pd
from ..Item import BI_DM, db
from ..common.CostCenters import CostCenters

class ItemMast(BI_DM):
    __tablename__= 'DM_D_DIGIMENU_STDPURCHASEPRICE'

    Division = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column()
    Stock_Unit = db.Column('Stock_Unit')

    @classmethod
    def list(cls, costCenterCode):
        sql = cls.query.filter(cls.Division==CostCenters.etDivision(costCenterCode))\
            .with_entities(cls.ItemCode,cls.ItemName,cls.Stock_Unit).distinct()
        return pd.read_sql(sql.statement,cls.getBind())