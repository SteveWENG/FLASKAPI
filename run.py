# -*- coding: utf-8 -*-

import logging
import os
import traceback
from logging import handlers

from flask import request, jsonify
from time import strftime
from apps import create_app

app = create_app()

@app.route('/')
def show():
    return 'hello world', 200

@app.after_request
def after_request(response):
    """ Logging after every request. """
    # This avoids the duplication of registry in the log,
    # since that 500 is already logged via @app.errorhandler.

    if response.status_code == 200:
        app.logger.info('\n\t%s %s %s %s %s',
                         request.remote_addr,
                         request.method,
                         request.scheme,
                         request.full_path,
                         response.status)

    return response

@app.errorhandler(Exception)
def exception(e):
    """ Logging after every Exception. """

    app.logger.error('\n\t%s %s %s %s\n\t%s\n\t%s',
                 request.remote_addr,
                 request.method,
                 request.scheme,
                 request.full_path,e.args if len(e.args) else e.description,
                 traceback.format_exc())

    return jsonify({'status': 500, 'error': e.args if len(e.args) else e.description}), 500

def _logging(**kwargs):
    log_path = './logs'
    filename = 'aden'

    level = kwargs.pop('level', None)
    # filename = kwargs.pop('filename', None)
    datefmt = kwargs.pop('datefmt', None)
    format = kwargs.pop('format', None)
    if level is None:
        level = logging.DEBUG
    if filename is None:
        filename = 'default.log'
    if datefmt is None:
        datefmt = '%Y-%m-%d %H:%M:%S'
    if format is None:
        format = '%(asctime)s [%(module)s] %(levelname)s [%(lineno)d] %(message)s'

    log = logging.getLogger(filename)
    format_str = logging.Formatter(format, datefmt)

    def namer(filename):
        return filename.split('default.')[1]

    if not os.path.exists(log_path):
        os.makedirs(log_path, exist_ok=True)

    th = handlers.TimedRotatingFileHandler(filename=os.path.join(log_path,filename),
                                           when='D', backupCount=3, encoding='utf-8')
    # th.namer = namer
    th.suffix = "%Y-%m-%d.log"
    th.setFormatter(format_str)
    th.setLevel(logging.NOTSET)

    app.logger.addHandler(th)
    app.logger.setLevel(logging.NOTSET)

_logging()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=18000, debug=True)