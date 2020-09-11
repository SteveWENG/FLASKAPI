# -*- coding: utf-8 -*-

from .OrderLine import OrderLine,db



class OrderLineF(OrderLine):

    Guid = db.Column()
    Status = db.Column()
    DeleteTime = db.Column(db.DateTime)