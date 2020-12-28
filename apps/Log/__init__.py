# -*- coding: utf-8 -*-
from copy import copy
from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import attributes, properties, sessionmaker, scoped_session

import apps
from apps.utils.functions import *
from config import Config

engine = create_engine(Config.SQLALCHEMY_DATABASE_URI,echo=False,logging_name='adenlog')
Base = declarative_base(engine)

class Log(Base):
    __tablename__ = 'tblLogs'

    Id = Column(Integer, primary_key=True)
    Guid = Column()
    logger = Column()
    level = Column()
    message = Column()
    method = Column()
    data = Column()
    UserIP = Column()
    Site = Column()
    User = Column()

    def __init__(self, d=None):
        self.Guid = apps.get_value('Log.Guid')
        if not d:
            return

        fields = dir(self)

        for k, v in d.items():
            tmpkey = ''.join([f for f in fields if f.lower() == k.lower()])  # 不分大小写，找出对应的属性
            if not tmpkey or not v or (tmpkey.lower() == 'id' and getNumber(v) < 1):  # 无值和 Id
                continue

            # Date和Numeric，数据要转换
            prop = getattr(type(self), tmpkey)
            if isinstance(prop, attributes.InstrumentedAttribute):
                prop = prop.prop
                if isinstance(prop, properties.ColumnProperty):
                    t = prop.columns[0].type
                    if isinstance(t, Numeric):
                        v = getNumber(v)
                    elif isinstance(t, Date):
                        v = getDateTime(v)

            setattr(self, tmpkey, self._wrap(v))

    def _wrap(self, value):
        if isinstance(value, (tuple, list, set, frozenset)):
            return type(value)([self._wrap(v) for v in value])

        return self(value) if isinstance(value, dict) else value

    def __unicode__(self):
        return self.__repr__()

    def __repr__(self):
        return "<Log: %s - %s>" % (datetime.datetime.now().strftime('%m/%d/%Y-%H:%M:%S'), self.message[:50])

    def save(self):
        SessionFactory = sessionmaker(bind=engine)
        session = scoped_session(SessionFactory)
        try:
            session.add(self)
            session.commit()
            return self.Id
        except:
            session.rollback()
        finally:
            session.close()

