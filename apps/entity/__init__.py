from flask_sqlalchemy import SQLAlchemy
from contextlib import contextmanager
import functools
from sqlalchemy import inspect

db = SQLAlchemy()

class BaseModel(db.Model):
    __abstract__ = True

    Id = db.Column(db.Integer, primary_key = True)

    # dict to BaseModel
    # d is a dict
    def __init__(self, d):
        fields = dir(self) #所有属性

        for k, v in d.items():
            tmpkey = ''.join([f for f in fields if f.lower() == k.lower()]) #不分大小写，找出对应的属性
            if tmpkey == '':
                continue

            setattr(self, tmpkey, self._wrap(v))

    def _wrap(self, value):
        if isinstance(value, (tuple, list, set, frozenset)): 
            return type(value)([self._wrap(v) for v in value])
        
        return self(value) if isinstance(value, dict) else value

    def show(self):
        return '<tr>%s</tr>' %(''.join (list('<td>%s : %s</td>' %(key,val) for key,val in vars(self).items() if key.find('_',0)<0)))

    def __repr__(self):
        state = inspect(self)
        attrs = " ".join([f"{attr.key}={attr.loaded_value!r}"
                          for attr in state.attrs])
        return f"<User {attrs}>"

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

    @classmethod
    def merges(cls, data):
        try:
            tmplist = [data]
            if isinstance(data, list):
                tmplist = data
            
            #转换成对象。有效的Id，及其它字段; 
            tmplist = [cls(l) for l in tmplist if l.get('Id',-1) > 0 and len(l.keys()) > 1]  

            with SaveDB() as session:
                for line in tmplist:
                    session.merge(line)
                    pass

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

class DataLogger(BaseModel):
    __bind_key__ = 'salesorder'
    __tablename__ = 'tblDataLogger'

    FuncName = db.Column()
    DBName = db.Column()
    TableName = db.Column()
    Values = db.Column()
    Errors = db.Column()
    CreatedUser = db.Column()

def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        errmsg = ''
        try:
            return func(*args, **kw)
        except Exception as e:
            errmsg = str(e)
            raise e
        finally:
            dic = {}
            dic['FuncName'] = func.__name__

            vals = []
            if len(args) > 0 :
                #去除参数中的self值
                vals.append(str([v for v in args if  "<class '%s" % func.__module__ not in str(v)]))

            if len(kw) > 0:
                dic.update({k[3:]:v for k, v in kw.items() if k[3:] in ['CreatedUser', 'DBName', 'TableName']})

                vals.append(str({k:v for k, v in kw.items() if k[3:] not in ['CreatedUser', 'DBName', 'TableName']}))

            if errmsg != '':
                dic['Errors'] = errmsg

            if len(vals) == 0 and errmsg == '':
                return

            dic['Values'] = '\n'.join(vals)
            try:
                ##with DataLogger.adds([dic]) as session:
                    pass
            except Exception as e:
                pass
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

