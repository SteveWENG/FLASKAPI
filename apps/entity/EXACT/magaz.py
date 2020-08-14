# -*- coding: utf-8 -*-

from apps.entity.EXACT import db, EXACT

class magaz(EXACT):
    __tablename__ = 'magaz'

    Code = db.Column('magcode')
    Name = db.Column('naam')