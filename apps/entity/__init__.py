from flask import g
from flask_sqlalchemy import SQLAlchemy
from contextlib import contextmanager
import functools
from sqlalchemy import inspect
from sqlalchemy.orm import attributes, properties
from sqlalchemy.orm.base import manager_of_class
import pandas as pd

from ..utils.functions import *

db = SQLAlchemy()

class BaseModel(db.Model):
    __abstract__ = True

    Id = db.Column(db.Integer, primary_key = True)

    # dict to BaseModel
    # d is a dict
    # keepEmpty: 是否保留‘’和0
    def __init__(self, d=None, keepEmpty=False):
        if not d:
            return

        fields = dir(self) #所有属性
        # tablename = self.__tablename__ if self.__tablename__ else self.__class__.__name__.lower()
        # table = db.Model.metadata.tables[tablename]
        hid = None
        for k, v in d.items():
            tmpkey = ''.join([f for f in fields if f.lower() == k.lower()]) #不分大小写，找出对应的属性
            if tmpkey=='' or v==None or (not keepEmpty and  getStr(v)=='') \
                    or (tmpkey.lower()=='id' and getNumber(v)<1):  #无值和 Id
                continue

            if not v: v = None
            if tmpkey.lower() == 'id': hid = getNumber(v)
            #Date和Numeric，数据要转换
            prop = getattr(type(self),tmpkey)
            if not isinstance(prop, attributes.InstrumentedAttribute):
                continue

            '''
            prop = prop.prop
            if not isinstance(prop, properties.ColumnProperty):
                continue

            t = prop.columns[0].type
            if isinstance(t,db.Numeric):
                v = getNumber(v)
            elif isinstance(t,db.Date):
                v = getDateTime(v)
            '''
            tv = self.__wrap(prop,v)
            if tv:
                setattr(self, tmpkey, tv)
        '''
        # CreatedUser & ChangedUser
        for user in [user for user in fields if user.lower().endswith('user') and user.lower().startswith('c')]:
            if user.lower().startswith('change') or \
                    (getNumber(self.Id)==0 and user.lower().startswith('create')):
                setattr(self,user,g.get('User'))
        '''

    def __wrap(self,prop,value,HId=None):
        if not value:
            return None

        prop = prop.prop
        if isinstance(prop,properties.RelationshipProperty):
            if isinstance(value, (tuple, list, set, frozenset)):
                return [prop.mapper.class_({k:v for k,v in val.items()})# if HId or k.lower() != 'id'})
                                            for val in value]

        if isinstance(prop, properties.ColumnProperty):
            t = prop.columns[0].type
            if isinstance(t, db.Numeric):
                value = getNumber(value)
            elif isinstance(t, db.Date):
                value = getDateTime(value)

            return self(value) if isinstance(value, dict) else value

        if isinstance(value, (tuple, list, set, frozenset)):
            return type(value)([self.__wrap(v) for v in value])

        return None

    def __wrap_del(self, value):
        if isinstance(value, (tuple, list, set, frozenset)):
            return type(value)([self._wrap(v) for v in value])
        
        return self(value) if isinstance(value, dict) else value

    def show(self):
        return '<tr>%s</tr>' %(''.join (list('<td>%s : %s</td>' %(key,val) for key,val in vars(self).items() if key.find('_',0)<0)))

    '''
    def __repr__(self):
        state = inspect(self)
        attrs = " ".join([f"{attr.key}={attr.loaded_value!r}"
                          for attr in state.attrs])
        return f"<User {attrs}>"
    '''

    @classmethod
    def LangColumn(cls,field):
        def _col(lang):
            column = '_' + field + lang.upper()
            if column in dir(cls):
                return getattr(cls, column)

            setattr(cls, column, db.Column(column[1:]))
            return getattr(cls, column)

        try:
            return _col(g.get('LangCode'))
        except:
            return _col('EN')

    @classmethod
    def getColumn(cls, field):
        def _col(lang=None):
            column = '_' + field
            if lang: column += lang.upper()

            if column in dir(cls):
                return getattr(cls, column)

            setattr(cls, column, db.Column(column[1:]))
            return getattr(cls, column)

        return _col()

    def to_dict(self):
        ret = {}
        for k in self.__dict__:
            if k.startswith('_'): continue

            v = getattr(self, k)

            if isinstance(v,list):
                ret[k] = [l.to_dict() for l in v]
                continue

            if isinstance(v,BaseModel):
                ret.update(v.to_dict())
                continue

            ret[k] = getVal(v)

        return ret
        '''
        return {c: getattr(self, c) if not isinstance(getattr(self, c),list)
                                    else [l.to_dict() for l in getattr(self, c)]
                for c in self.__dict__ if not c.startswith('_')} #.name self.__table__.columns}
        '''

    @classmethod
    def getBind(cls):
        return db.get_engine(db.get_app(), cls.__bind_key__)

    @classmethod
    @contextmanager
    def adds(cls, lines):
        try:
            tmp = [l if isinstance(l,cls) else cls(l) for l in lines]
            with SaveDB() as session:
                session.add_all(tmp)
                yield session
        except Exception as e:
            raise e

    @classmethod
    @contextmanager
    def merges(cls, lines, keepEmpty=False):
        try:
            with SaveDB() as session:
                for l in lines:
                    session.merge(cls(l,keepEmpty))
                yield session
        except Exception as e:
            raise e

    def updates(cls, data, limit):
        try:
            tmplist = [data]
            if isinstance(data, list):
                tmplist = data
            
            #转换成对象。有效的Id，及其它字段; 
            tmplist = [l for l in tmplist if l.get('Id',-1) > 0 and len(l.keys()) > 1]  

            with SaveDB() as session: 
                for line in data:
                    if session.query(cls).filter(cls.Id == id).update(line) < 1 :
                        raise RuntimeError('No updata. (%s)' %(str(line)) )
                    
        except Exception as e:
            raise e

@contextmanager
def SaveDB(session = db.session):
    try:
        yield session
        session.commit()
        #pass
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def merges(data):
    try:
        tmplist = [data]
        if isinstance(data, list):
            tmplist = data

        with SaveDB() as session:
            for line in tmplist:
                session.merge(line)
            pass

    except Exception as e:
        raise e
