# -*- coding: utf-8 -*-

from ...erp import erp, db

class Supplier(erp):
    __tablename__ = 'DM_D_ERP_SUPPLIER'

    Division = db.Column()
    SupplierCode = db.Column('Supplier_Code')
    SupplierName = db.Column('Supplier_Name')