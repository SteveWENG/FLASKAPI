# -*- coding: utf-8 -*-

from ...erp import erp,db


class RoleApps(erp):
    __tablename__ = 'tblRoleApps'

    RoleGuid = db.Column()
    RoleName = db.Column()
    AppGuid = db.Column()
    Status = db.Column()