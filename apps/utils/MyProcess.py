# -*- coding: utf-8 -*-

from multiprocessing.dummy import Process
from multiprocessing import Queue

from flask import current_app, g

from ..Log.LogToDB import LogToDB

class MyProcess(Process):
    def __init__(self, func, *args):
        Process.__init__(self)
        self.daemon = True
        self._app = current_app._get_current_object()
        self._func = func
        self._args = args
        self._LogGuid = g.LogGuid

        self.start()

    def run(self):
        with self._app.app_context():
            if 'LogQueue' not in dir(g):
                g.LogQueue = Queue()
                LogToDB(g.LogQueue,self._LogGuid)
            self._result = self._func(*self._args)

    def get(self):
        try:
            self.join()
            return self._result
        except:
            return None