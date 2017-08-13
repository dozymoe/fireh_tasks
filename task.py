"""Base Task and interface definition.

Task produces chain of Jobs and stores data in between Jobs.
"""
import logging


class TaskStorage(object):
    """ Part of Task that can be serialized.
    """
    OurIdentity = None
    OriginIdentity = None
    PeerIdentity = None

    Data = None
    Validity = 0
    ErrorMessages = None

    def __init__(self, our_id):
        self.OurIdentity = self.OriginIdentity = self.PeerIdentity = our_id
        self.Data = {}
        self.ErrorMessages = {}


    def set_status_failed(self, validity=1):
        """ Determine that the task ended up as a failure.

        It was a legitimate error so retrying the Task was not needed.
        """
        self.Validity = validity


    def set_status_error(self, code, message, validity=-1, field_name='_'):
        """ Determine that something went wrong while processing the Task.

        Retrying the Task may yield better result.
        """
        self.Data['status'] = code
        self.Validity = validity

        if field_name not in self.ErrorMessages:
            self.ErrorMessages[field_name] = []
        self.ErrorMessages[field_name].append(message)


class ITask(object):
    """ Interface for Task.
    """

    def get_storage(self):
        """ Retrieve the public data.
        """
        raise NotImplementedError()


    def create_job(self):
        """ Return a Job.

        A Job when finished will create other Jobs.
        """
        raise NotImplementedError()


    def finish(self):
        """ Drop the Task out of queue.
        """
        raise NotImplementedError()


    def is_finished(self):
        """ Check if the Task was completed.
        """
        raise NotImplementedError()


    def on_job_created(self, callback):
        """ Add event listener for when the Task created a Job.
        """
        raise NotImplementedError()


    def trigger_job_created(self, job):
        """ Public method to trigger all registered callback.
        """
        raise NotImplementedError()


    def on_finished(self, callback):
        """ Add event listener for when the Task was finished.
        """
        raise NotImplementedError()


class TaskBase(ITask):
    """ Base class that implementes ITask.
    """
    task_data = None

    logger = None

    _is_finished = False

    _job_created_callbacks = None
    _finished_callbacks = None

    def __init__(self, task_data):

        self.task_data = task_data
        self.logger = logging.getLogger(type(self).__name__)

        self._job_created_callbacks = []
        self._finished_callbacks = []


    def get_storage(self):
        return self.task_data


    def is_finished(self):
        return self._is_finished


    def finish(self):
        self._is_finished = True
        for callback in self._finished_callbacks:
            callback(self)


    def on_job_created(self, callback):
        self._job_created_callbacks.append(callback)


    def trigger_job_created(self, job):
        for callback in self._job_created_callbacks:
            callback(self, job)


    def on_finished(self, callback):
        self._finished_callbacks.append(callback)


    def create_job(self):
        raise NotImplementedError()
