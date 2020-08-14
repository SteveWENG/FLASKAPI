# -*- coding: utf-8 -*-

from ..EXACT import db, EXACT

class Items(EXACT):
    __tablename__ = 'items'

    Code = db.Column('itemcode')
    Assortment = db.Column()
    GLAccountDistribution = db.Column()