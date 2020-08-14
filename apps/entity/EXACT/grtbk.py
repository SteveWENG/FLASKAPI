# -*- coding: utf-8 -*-

from ..EXACT import db, EXACT

class grtbk(EXACT):
    __tablename__ = 'grtbk'

    COACode = db.Column('reknr')
    omzrek = db.Column()
