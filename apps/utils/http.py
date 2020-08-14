import requests
from .functions import *


def httpget(url):
    if getStr(url) == '':
        raise RuntimeError('No url')

    try:
        res = requests.get(url)
        res.encoding = 'utf-8'
        return res.text

    except Exception as e:
        raise e
