# -*- coding: utf-8 -*-

from flask import jsonify, g
# from apps import create_app
import apps
from apps.utils.functions import getGUID

app = apps.create_app()

@app.route('/')
def show():
    return 'hello world', 200

@app.before_request
def before_request(*args, **kwargs):
    g.LogGuid = getGUID()


@app.after_request
def after_request(response):
    """ Logging after every request. """
    # This avoids the duplication of registry in the log,
    # since that 500 is already logged via @app.errorhandler.

    if response.status_code == 200:
        app.logger.info('Completed')

    return response

@app.errorhandler(Exception)
def exception(e):
    """ Logging after every Exception. """
    app.logger.error(e.args if len(e.args) else e.description)

    return jsonify({'status': 500, 'error': e.args if len(e.args) else e.description}), 500

def _logging():
    #  '%(asctime)s %(levelname)s [%(module)s - %(funcName)s]\n\t%(pathname)s\n\t%(message)s [%(lineno)d]

    # log = logging.getLogger(filename)
    pass

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=18000, debug=True)