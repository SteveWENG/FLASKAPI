# -*- coding: utf-8 -*-

from datetime import datetime

from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import  sessionmaker, scoped_session

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

    def __init__(self, guid,logger,level,method,UserIP,Site,User): # d=None):
        self.Guid = guid
        self.logger = logger
        self.level = level
        self.method = method
        self.UserIP = UserIP
        if Site: self.Site = Site
        if User: self.User = User

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

