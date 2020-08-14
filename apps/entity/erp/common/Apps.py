# -*- coding: utf-8 -*-

from ...erp import erp, db

class Apps(erp):
    __tablename__ = 'tblApps'

    Guid = db.Column()
    ClassName = db.Column()