# -*- coding: utf-8 -*-

from sqlalchemy.ext.hybrid import hybrid_property

from ...erp import erp, db

class DivisionItem(erp):
    __tablename__= 'DM_D_DIGIMENU_STDPURCHASEPRICE'

    Division = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column()
    BOMUnit = db.Column('BOM_Unit')
    PurPrice = db.Column()

    @hybrid_property
    def PurBOMConversion(self):
        return self.getColumn('StkBOM_Conversion')