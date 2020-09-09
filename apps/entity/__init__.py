from flask_sqlalchemy import SQLAlchemy
from contextlib import contextmanager
import functools
from sqlalchemy import inspect

from ..utils.functions import *

db = SQLAlchemy()

class BaseModel(db.Model):
    __abstract__ = True

    Id = db.Column(db.Integer, primary_key = True)

    # dict to BaseModel
    # d is a dict
    def __init__(self, d=None):
        if not d:
            return

        fields = dir(self) #所有属性

        for k, v in d.items():
            tmpkey = ''.join([f for f in fields if f.lower() == k.lower()]) #不分大小写，找出对应的属性
            if tmpkey == '' or getStr(v) == '' or (tmpkey.lower()=='id' and getNumber(v)==0):  #无值和 Id
                continue

            setattr(self, tmpkey, self._wrap(v))

    def _wrap(self, value):
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

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def getBind(cls):
        return db.get_engine(db.get_app(), cls.__bind_key__)

    @classmethod
    @contextmanager
    def adds(cls, lines):
        try:
            tmp = [cls(l) for l in lines]
            with SaveDB() as session:
                session.add_all(tmp)
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

class DataLogs(BaseModel):
    __bind_key__ = 'salesorder'
    __tablename__ = 'tblDataLogs'

    ClassName = db.Column()
    FuncName = db.Column()
    Site = db.Column()
    TableName = db.Column()
    Data = db.Column()
    Errors = db.Column()
    CreatedUser = db.Column()

def dblog(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        dic = {}
        try:
            return func(*args, **kw)
        except Exception as e:
            dic['Errors'] = str(e)
            raise e
        finally:

            dic['FuncName'] = func.__qualname__

            vals = []
            if len(args) > 0 :
                # [str(v) for v in args if  "<class '%s" % func.__module__ in str(type(v))]
                clzs = [v for v in args if  "<class '%s" % func.__module__ in str(type(v))]
                if clzs:
                    dic['ClassName'] = str(clzs[0])[1:str(clzs[0]).index(' (transient ')]
                    dic['TableName'] = str(clzs[0].__table__)

                #去除参数中的self值
                vals = vals + [v for v in args if  "<class '%s" % func.__module__ not in str(type(v))]

            if len(kw) > 0:
                vals = vals + [kw]

            for l in vals:
                if isinstance(l, dict) and (l.get('creater','') != '' or l.get('costCenterCode','') != ''):
                    dic['CreatedUser'] = l.get('creater')
                    dic['Site'] = l.get('costCenterCode')
                    break
            if vals and len(vals) == 1:
                vals = vals[0]

            dic['Data'] = str(vals)

            try:
                with DataLogs.adds([dic]) as _:
                    pass
            except Exception as e:
                raise e
    return wrapper

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
