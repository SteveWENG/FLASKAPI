# -*- coding: utf-8 -*-
from datetime import date

from ..Item import BI_DM, db

class PriceList(BI_DM):
    __tablename__ = 'DM_D_ERP_PurchaseAgreement_NEW'

    Company = db.Column()
    ItemCode = db.Column()
    ItemNameZH = db.Column('ItemName_ZH')
    ItemNameEN = db.Column('ItemName_EN')
    SupplierCode = db.Column('crdnr')
    SupplierName = db.Column('Supplier_Name')
    PurUnit = db.Column('Pur_Unit')
    StockUnit = db.Column('Stock_Unit')
    PurStkConversion = db.Column('PurStk_Conversion', db.Numeric)
    ValidFrom = db.Column(db.Date)
    ValidTo = db.Column(db.Date)

    __mapper_args__ = {
        'primary_key': {Company, ItemCode, SupplierCode, PurUnit, ValidFrom, ValidTo}
    }

    @classmethod
    def list(cls, company, itemCodes, *supplierCodes):
        filters = [ cls.Company == company, cls.ItemCode.in_(itemCodes),
                  cls.ValidFrom <= date.today(), cls.ValidTo >= date.today()]
        if len(supplierCodes) > 0:
            filters.append(cls.SupplierCode.in_(supplierCodes))

        return cls.query.filter(*filters).all()