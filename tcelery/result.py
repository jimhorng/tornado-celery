from __future__ import absolute_import
from __future__ import with_statement

from functools import partial

import celery


class AsyncResult(celery.result.AsyncResult):
    def __init__(self, task_id, status=None, traceback=None,
                 result=None, producer=None, **kwargs):
        super(AsyncResult, self).__init__(task_id)
        self._status = status
        self._traceback = traceback
        self._result = result
        self._producer = producer

    @property
    def status(self):
        return self._status or super(AsyncResult, self).status
    state = status

    @property
    def traceback(self):
        if self._result is not None:
            return self._traceback
        else:
            return super(AsyncResult, self).traceback

    @property
    def result(self):
        return self._result or super(AsyncResult, self).result
    
    def get(self, callback=None):
        self._producer.fail_if_backend_not_supported()
        self._producer.consumer.wait_for(self.task_id,
                                         partial(self.on_result, callback),
                                         self._producer.prepare_expires(type=int))

    def on_result(self, callback, reply):
        reply = self._producer.decode(reply)
        self._status = reply.get('status')
        self._traceback = reply.get('traceback')
        self._result = reply.get('result')
        if callback:
            callback(self._result)
    
    def _get_task_meta(self):
        self._producer.fail_if_backend_not_supported()
        return super(AsyncResult, self)._get_task_meta()