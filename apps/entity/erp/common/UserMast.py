# -*- coding: utf-8 -*-

from ....utils.functions import *
from ...erp import erp, db

class UserMast(erp):
    __tablename__ = 'UserMast'

    Guid = db.Column()
    UserName = db.Column()
    Password = db.Column()
    FullName = db.Column()
    Status = db.Column()
    EmployeeId = db.Column()

    @classmethod
    def login(cls, userName, password):
        tmp = cls.query.filter(cls.UserName==getStr(userName),
                                cls.Password==getStr(password),
                                cls.Status==True).first()
        if not tmp:
            Error('Error login')

        return tmp