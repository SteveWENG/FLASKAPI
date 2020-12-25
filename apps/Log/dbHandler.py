# -*- coding: utf-8 -*-

import logging
import traceback

from flask import request

from apps.Log import Log


class dbHandler(logging.Handler):
    def emit(self, record):
        self.setLevel(20)
        dic = {}
        dic['logger'] = record.__dict__['name'] + ' '+ record.__dict__['module']
        dic['level'] = record.__dict__['levelname']
        if not request.json and dic['level']=='INFO': return

        if dic.get('level') == 'ERROR':
            dic['trace'] = traceback.format_exc()

        dic['method'] = request.full_path
        dic['UserIP'] = request.remote_addr

        if 'sqlalchemy.engine' in record.__dict__['name']:
            dic['data'] = record.__dict__['message']
        else:
            dic['message'] = record.__dict__['message']
            dic['data'] = str(request.json)

        site = {k.lower(): v for k, v in request.json.items()
                if k.lower() in ['costcentercode', 'division', 'company', 'creater']}
        if site:
            dic['Site'] = site.get('costcentercode', site.get('division', site.get('company')))
            if site.get('creater'):
                dic['User'] = site.get('creater')
        log = Log(dic)
        log.save()
