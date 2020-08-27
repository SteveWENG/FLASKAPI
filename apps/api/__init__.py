from flask import g
from flask_restful import request

from ..utils.functions import ErrorExit,Err

def webapi(func):
    try:
        dic = request.get_json()
        if dic == None or not isinstance(dic, dict):
            Error('No parameter')

        if dic.get('langCode',''):
            g.LangCode = dic.get('langCode','').upper()
        return {'status': 200, 'data': func(dic)}
    except Exception as e:
        ErrorExit(e.args if len(e.args) else e.description)