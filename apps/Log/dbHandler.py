# -*- coding: utf-8 -*-
import _thread
import logging
import re
import traceback
from flask import request, g
import datetime

import apps
from apps.Log import Log
from apps.utils.functions import *


class dbHandler(logging.Handler):
    def emit(self, record):
        logger = record.__dict__['name'] + ' ' + record.__dict__['module']
        level = record.__dict__['levelname']
        if not request.json and level == 'INFO': return

        # 不记录
        # SELECT ... WHERE [tblDataControlConfig].[Type] = %(Type_1)s AND coalesce([tblDataControlConfig].[Val1], %(coalesce_1)s) LIKE %(coalesce_2)s AND coalesce([tblDataControlConfig].[StartDate], %(coalesce_3)s) <= %(coalesce_4)s AND coalesce([tblDataControlConfig].[EndDate], %(coalesce_5)s) >= %(coalesce_6)s) AND [INFORMATION_SCHEMA].[COLUMNS].[TABLE_SCHEMA] = CAST(%(table_schema_1)s AS NVARCHAR(max))
        # {'Type_1': 'StockReport', 'coalesce_1': '', 'coalesce_2': '%batch%', 'coalesce_3': '2000-1-1', 'coalesce_4': datetime.datetime(2020, 12, 25, 0, 0), 'coalesce_5': '2222-12-31', 'coalesce_6': datetime.datetime(2020, 12, 25, 0, 0), 'table_schema_1': 'dbo'}
        # ROLLBACK
        if 'sqlalchemy.' in record.__dict__['name']:
            if ('UnwantedLog' not in dir(self) or not self.UnwantedLog)\
                    and 'FROM [INFORMATION_SCHEMA].[' in record.__dict__['message']:
                self.UnwantedLog = re.split(r'(?:\%\(|\)s)', record.__dict__['message'])
                return

            # 发出 [INFORMATION_SCHEMA]后，再发出值和ROLLBACK
            if 'UnwantedLog' in dir(self) and self.UnwantedLog:
                if record.__dict__['message'] == 'ROLLBACK':
                    self.UnwantedLog = None
                    return

                # 前SQL命令的赋值
                try:
                    vals = eval(record.__dict__['message'])
                    if isinstance(vals, dict) and set(vals.keys()) <= set(self.UnwantedLog):
                        return
                except:
                    pass

        self.UnwantedLog = None
        guid = g.LogGuid

        if level != 'INFO' or \
                'log' not in dir(self) or self.log.Guid !=guid \
                or 'sqlalchemy.engine' not in logger or \
                self.log.logger != logger or self.log.level != level:
            self.log = Log(guid)
            self.log.logger = logger
            self.log.level = level
            self.log.method = request.full_path
            self.log.UserIP = request.remote_addr

            if level == 'ERROR':
                self.log.message = getStr(self.log.message) + ('\n' if self.log.message else '') + traceback.format_exc()

        self.log.data = getStr(self.log.data) + ('\n' if self.log.data else '')

        if 'sqlalchemy.' in record.__dict__['name']:
            try:
                vals = eval(record.__dict__['message'])
                if not isinstance(vals, dict) or set(vals.keys()) > set(re.split(r'(?:\%\(|\)s)', self.log.data)):
                    Error('')

                self.log.data = self.log.data % {k:"\'"+getVal(v)+"\'"
                                                if (isinstance(v,str) or isinstance(v,datetime.datetime)) else v
                                                 for k,v in vals.items()}
            except:
                self.log.data += record.__dict__['message']
        else:
            self.log.message = getStr(self.log.message) + ('\n' if self.log.message else '')\
                               + record.__dict__['message']
            self.log.data += str(request.json)

        if g.get('Site'):
            self.log.Site = g.get('Site')
        if g.get('User'):
            self.log.User = g.get('User')

        # _thread.start_new_thread(self.log.save, ())
        self.log.Id = self.log.save()
