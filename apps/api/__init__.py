from flask_restful import request

from ..utils.functions import *

def webapi(func):
    try:
        dic = request.get_json()
        if dic == None or not isinstance(dic, dict):
            raise RuntimeError('No parameter') 

        return {'status': 200, 'data': func(dic)}
    except Exception as e:
        ErrorExit(e.args if len(e.args) else e.description)