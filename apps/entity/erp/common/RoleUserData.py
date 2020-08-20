# -*- coding: utf-8 -*-

from ...erp import erp, db

class RoleUserData(erp):
    __tablename__ = 'tblRoleUserData'

    RoleGuid = db.Column()
    UserGuid = db.Column()
    Type = db.Column()
    Code = db.Column()