""" Base Worker and interface definiton.

While Job is something that stores data, can be serialized, Worker is the one
doing the implementation.
"""
import logging
from uuid import uuid4

class IWorker(object):
    """ Interface for Worker.
    """

    def is_available(self):
        """ Check if worker was idle.
        """
        raise NotImplementedError()


    def assign_job(self, job):
        """ Remove idle status.
        """
        raise NotImplementedError()


    def initialize(self):
        """ Late initialization.

        For example if the instance was made too early, and the data it
        depended on was not available.
        """
        raise NotImplementedError()


    async def perform_job(self):
        """ Actually doing the work.
        """
        raise NotImplementedError()


    def get_id(self):
        """ Get identity.
        """
        raise NotImplementedError()


    def unassign(self):
        """ Set status to idle.
        """
        raise NotImplementedError()


class WorkerBase(IWorker):
    """ Base class that implement IWorker.

    You don't need to override assign_job(), just set the Job.get_name()
    values to ACCEPTED_JOBS, to filter what jobs this worker is willing to
    accept.
    """
    ACCEPTED_JOBS = []

    logger = None

    _id = None
    job = None

    def __init__(self):

        self.logger = logging.getLogger(type(self).__name__)
        self._id = uuid4().hex


    def is_available(self):
        return self.job is not None


    def assign_job(self, job):
        if self.job is not None:
            return False
        elif self.ACCEPTED_JOBS and\
                job.get_name() not in self.ACCEPTED_JOBS:

            return False

        self.job = job
        job.assign_to_worker(self)
        return True


    def unassign(self):
        self.job = None


    def get_id(self):
        return self._id


    def initialize(self):
        pass


    def perform_job(self):
        return NotImplementedError()
