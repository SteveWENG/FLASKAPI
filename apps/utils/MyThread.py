# -*- coding: utf-8 -*-

import threading
from functools import wraps

from flask import g, current_app


class MyThread(threading.Thread):
    def __init__(self,func, args):
        threading.Thread.__init__(self)
        self._app = current_app._get_current_object()
        self._logqueue = g.LogQueue
        self._func = func
        self._args = args
        self.start()

    def run(self):
        with self._app.app_context():
            g.LogQueue = self._logqueue
            self._result = self._func(*self._args)

    def get(self):
        try:
            self.join()
            return self._result
        except:
            return None


