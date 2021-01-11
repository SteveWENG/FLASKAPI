# -*- coding: utf-8 -*-

import queue,re,threading,traceback

from . import Log
from ..utils.functions import *

class LogToDB(threading.Thread):
    def __init__(self, que):
        threading.Thread.__init__(self)
        self.daemon = False
        self._queue = que
        self._guid = getGUID()

    def run(self):
        while True:
            values = self._queue.get()
            if isinstance(values,dict) and values.get('LogRecord'):
                self._save(values)

    def _save(self,dic):
        logger = dic['LogRecord'].__dict__['name'] + ' ' + dic['LogRecord'].__dict__['module']
        level = dic['LogRecord'].__dict__['levelname']
        if not dic.get('data') and level == 'INFO': return

        def checkSQL_Vals(sql, vals):
            if not sql or not vals: return False
            try:
                if isinstance(sql, str):
                    sql = re.split(r'(?:\%\(|\)s)', sql)

                if isinstance(vals, str):
                    vals = eval(vals)
                if isinstance(vals, dict) and set(vals.keys()) <= set(sql):
                    return True
            except:
                pass

            return False

        if level != 'INFO' or '_log' not in dir(self) \
                or 'sqlalchemy.engine' not in logger or \
                self._log.logger != logger or self._log.level != level:
            self._log = Log(self._guid,logger,level,dic.get('method',''),
                            dic.get('UserIP',''),dic.get('Site',''),dic.get('User',''))

        self._log.data = getStr(self._log.data) + ('\n\n' if self._log.data else '')

        if 'sqlalchemy.engine' in dic['LogRecord'].__dict__['name']:
            if checkSQL_Vals(self._log.data,dic['LogRecord'].__dict__['message']):
                self._log.data = self._log.data % {
                    k: "\'" + getVal(v) + "\'" if (isinstance(v, str) or isinstance(v, datetime.datetime)) else v
                    for k, v in eval(dic['LogRecord'].__dict__['message']).items()}
            else:
                self._log.data += '[%s]\n' % getDateTime(datetime.datetime.now()) + dic['LogRecord'].__dict__['message']

            # 不记录
            # SELECT ... WHERE [tblDataControlConfig].[Type] = %(Type_1)s AND coalesce([tblDataControlConfig].[Val1], %(coalesce_1)s) LIKE %(coalesce_2)s AND coalesce([tblDataControlConfig].[StartDate], %(coalesce_3)s) <= %(coalesce_4)s AND coalesce([tblDataControlConfig].[EndDate], %(coalesce_5)s) >= %(coalesce_6)s) AND [INFORMATION_SCHEMA].[COLUMNS].[TABLE_SCHEMA] = CAST(%(table_schema_1)s AS NVARCHAR(max))
            # {'Type_1': 'StockReport', 'coalesce_1': '', 'coalesce_2': '%batch%', 'coalesce_3': '2000-1-1', 'coalesce_4': datetime.datetime(2020, 12, 25, 0, 0), 'coalesce_5': '2222-12-31', 'coalesce_6': datetime.datetime(2020, 12, 25, 0, 0), 'table_schema_1': 'dbo'}
            # ROLLBACK
            if 'FROM [INFORMATION_SCHEMA].[' in self._log.data:
                return
        else:
            self._log.message = getStr(self._log.message) + ('\n' if self._log.message else '') \
                               + dic['LogRecord'].__dict__['message']
            self._log.data += str(dic['data'])

        if level == 'ERROR':
            self._log.message = getStr(self._log.message) + ('\n' if self._log.message else '') + traceback.format_exc()

        self._log.Id = self._log.save()