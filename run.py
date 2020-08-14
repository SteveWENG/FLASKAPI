# -*- coding: utf-8 -*-

from apps import create_app

app = create_app()

@app.route('/')
def show():
    return 'hello world'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=18000, debug=True)