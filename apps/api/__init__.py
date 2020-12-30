
from flask import g, jsonify
from flask_restful import request

from ..utils.functions import *

def webapi(func):
    #try:
        dic = request.get_json()
        if dic == None or not isinstance(dic, dict):
            Error('No parameter')

        tmpdic = {k.lower():v for k,v in dic.items()}
        if tmpdic.get('langcode'):
            g.LangCode = tmpdic.get('langcode','').upper()
        if tmpdic.get('creater'):
            g.User = tmpdic.get('creater')

        tmp = tmpdic.get('costcentercode',tmpdic.get('division',tmpdic.get('company')))
        if tmp:
            g.Site = tmp

        return jsonify({'status': 200, 'data': func(dic)}),200
    #except Exception as e:

        # abort(jsonify({'status': 500, 'error': e.args if len(e.args) else e.description}))
        # return {'status': 500, 'error': e.args if len(e.args) else e.description}