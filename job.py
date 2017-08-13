""" Base Job and interface definition.

Stores serializable data of Job to be queued.
"""
from datetime import datetime, timedelta
import logging
from uuid import uuid4

from pytz import utc

from .message_storage import MessageStorage


class JobStorage(object):
    """ Part of Job that can be serialized.
    """
    Payload = None
    Reply = None

    Data = None
    Validity = 0
    ErrorMessages = None

    def __init__(self):
        self.Payload = MessageStorage()
        self.Data = {}
        self.ErrorMessages = {}


    def set_status_failed(self, validity=1):
        """ Determine that the Job ended up as a failure.

        It was a legitimate error so retrying the Job was not needed.
        """
        self.Validity = validity


    def set_status_error(self, code, message, validity=-1, field_name='_'):
        """ Determine that something went wrong while processing the Job.

        Retrying the Job may yield better result.
        """
        self.Data['status'] = code
        self.Validity = validity

        if field_name not in self.ErrorMessages:
            self.ErrorMessages[field_name] = []
        self.ErrorMessages[field_name].append(message)


class IJob(object):
    """ Interface for Job.
    """

    def get_storage(self):
        """ Retrieve the public data.
        """
        raise NotImplementedError()


    def validate_deliverable(self, worker_id):
        """ Validate the reply received from Worker.
        """
        raise NotImplementedError()


    def on_job_success(self):
        """ Add event listener for when the Job was successful.
        """
        raise NotImplementedError()


    def on_job_failed(self):
        """ Add event listener for when the Job has failed.
        """
        raise NotImplementedError()


    def on_job_error(self):
        """ Add event listener for when the Job has failed for unexpected
        error.
        """
        raise NotImplementedError()


    def create_next_job(self):
        """ After seeing the Worker's reply, produce next Job in the sequence.
        """
        raise NotImplementedError()


    def finish(self):
        """ Drop the Job out of queue.
        """
        raise NotImplementedError()


    def is_finished(self):
        """ Check if the Job was completed.
        """
        raise NotImplementedError()


    def assign_to_worker(self, worker):
        """ Assign the Job exclusive to a Worker.
        """
        raise NotImplementedError()


    def is_assigned(self):
        """ Has a Worker taken this Job?
        """
        raise NotImplementedError()


    def get_id(self):
        """ Get the identity.
        """
        raise NotImplementedError()


    def get_name(self):
        """ Get the Job's name.

        Could be anything, usually the class name, used by Workers to determine
        if they should take the Job.
        """
        raise NotImplementedError()


    def get_task(self):
        """ Get the Task associated with the Job.
        """
        raise NotImplementedError()


    def reset(self):
        """ Prepare Job's data for another try at the queue.
        """
        raise NotImplementedError()


    def on_assigned(self, callback):
        """ Add event listener for when the Job assigned to a Worker.
        """
        raise NotImplementedError()


    def trigger_assigned(self, worker):
        """ Public method to trigger all registered callbacks.
        """
        raise NotImplementedError()


    def on_performed(self, callback):
        """ Add even listener for when the Job was being processed.
        """
        raise NotImplementedError()


    def trigger_performed(self, worker):
        """ Public method to trigger all registered callbacks.
        """
        raise NotImplementedError()


    def on_delivered(self, callback):
        """ Add event listener for when the Worker has sent their reply.
        """
        raise NotImplementedError()


    def trigger_delivered(self, message, worker):
        """ Public method to trigger all registered callbacks.
        """
        raise NotImplementedError()


class JobBase(IJob):
    """ Base class that implements IJob.
    """
    job_data = None
    task = None
    worker = None

    logger = None

    _id = None
    _is_finished = False
    _started_at = None
    _finished_at = None
    _assigned_until = datetime.min

    _assigned_callbacks = None
    _performed_callbacks = None
    _delivered_callbacks = None

    def __init__(self, task):

        self.task = task
        self.job_data = JobStorage()
        self._id = uuid4().hex
        self.job_data.Payload.OurCookies['jobId'] = self._id

        self.logger = logging.getLogger(type(self).__name__)

        self._assigned_callbacks = []
        self._performed_callbacks = []
        self._delivered_callbacks = []

        self._started_at = datetime.utcnow().replace(tzinfo=utc)


    def get_storage(self):
        return self.job_data


    def validate_deliverable(self, worker_id):
        # validity < 0: invalid response from worker, retry later
        # validity > 0: failure? send reply to client anyway
        # validity = 0: success! send reply to client, alter database, etc..

        if self.worker is None or worker_id != self.worker.get_id():
            self.job_data.set_status_error('INVALID_RESPONSE',
                    "Worker's identity doesn't match job's worker.")

        return self.job_data.Validity


    def on_job_success(self):
        pass


    def on_job_failed(self):
        task_data = self.task.get_storage()
        task_data.set_status_failed(self.job_data.Validity)


    def on_job_error(self):
        task_data = self.task.get_storage()
        status_code = self.job_data.Data.get('status', 'UNEXPECTED_ERROR'),
        for field_name in self.job_data.ErrorMessages:
            for msg in self.job_data.ErrorMessages[field_name]:
                task_data.set_status_error(status_code, msg,
                        self.job_data.Validity, field_name)

        log_msg = []
        for messages in task_data.ErrorMessages.values():
            log_msg.extend(messages)

        if log_msg:
            self.logger.error('\n'.join(log_msg))


    def create_next_job(self):
        return None


    def finish(self):
        self._is_finished = True
        self._finished_at = datetime.utcnow().replace(tzinfo=utc)
        self.worker.unassign()


    def reset(self):
        self.job_data.Validity = 0
        self.job_data.Reply = None
        if self.worker is not None:
            self.worker.unassign()
            self.worker = None

        self._assigned_until = datetime.min


    def is_finished(self):
        return self._is_finished


    def assign_to_worker(self, worker):
        self.worker = worker
        self._assigned_until = datetime.utcnow().replace(tzinfo=utc) +\
                timedelta(seconds=900)


    def is_assigned(self):
        return datetime.utcnow().replace(tzinfo=utc) < self._assigned_until


    def get_id(self):
        return self._id


    def get_name(self):
        return type(self).__name__


    def get_task(self):
        return self.task


    def on_assigned(self, callback):
        self._assigned_callbacks.append(callback)


    def trigger_assigned(self, worker):
        for callback in self._assigned_callbacks:
            callback(self, worker)


    def on_performed(self, callback):
        self._performed_callbacks.append(callback)


    def trigger_performed(self, worker):
        for callback in self._performed_callbacks:
            callback(self, worker)


    def on_delivered(self, callback):
        self._delivered_callbacks.append(callback)


    def trigger_delivered(self, message, worker):
        for callback in self._delivered_callbacks:
            callback(self, message, worker)
