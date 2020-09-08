# -*- coding: utf-8 -*-

from ldap3 import Server, Connection, ALL,NTLM

from config import AD
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
        filter = [cls.UserName==getStr(userName),cls.Status==True]
        if cls.adlogin(userName,password) == False:
            filter.append(cls.Password==getStr(password))

        tmp = cls.query.filter(*filter).first()
        if not tmp:
            Error('Error login')

        return tmp

    @classmethod
    def adlogin(cls, userName, password):
        try:
            server = Server(AD.server, get_info=ALL)
            conn = Connection(server, user= '%s\\%s' % (AD.domain,userName), password=password,
                              auto_bind=True,authentication='NTLM')
            return True
        except:
            return False

