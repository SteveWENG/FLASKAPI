# -*- coding: utf-8 -*-

from ...erp import erp, db

class CCMast(erp):
    __tablename__ = 'CCMast'

    CostCenterCode = db.Column()
    Company = db.Column()