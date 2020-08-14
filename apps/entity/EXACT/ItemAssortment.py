# -*- coding: utf-8 -*-

from ..EXACT import db, EXACT

class ItemAssortment(EXACT):
    __tablename__ = 'ItemAssortment'

    Assortment = db.Column()
    GLStock = db.Column()