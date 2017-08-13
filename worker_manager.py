""" Base Worker Manager and interface definition.

Manages jobs and workers.
"""
import logging
from threading import Lock


class IWorkerManager(object):
    """ Interface for Worker Manager.
    """

    def add_job(self, job):
        """ Add new item to job queue.
        """
        raise NotImplementedError()


    async def find_job(self):
        """ Try to assign queued jobs to idle workers.

        Long running.
        """
        raise NotImplementedError()


    def get_job_from_worker_reply(self, message):
        """ Find queued job from worker's response.

        Useful in asynchronous system, where the replies was not received in
        orderly manner.
        You received a message from remote worker, but you don't know the job
        or the local worker, the message was meant for.
        """
        raise NotImplementedError()


    def add_worker(self, worker):
        """ Add new item to registered worker.
        """
        raise NotImplementedError()


    def get_by_id(self, identity):
        """ Get worker by its id.
        """
        raise NotImplementedError()


class WorkerManager(IWorkerManager):
    """ Generic class that implements IWorkerManager.
    """
    jobs = None
    workers = None

    logger = None

    _jobs_lock = None
    _workers_lock = None

    def __init__(self):

        self.logger = logging.getLogger(type(self).__name__)
        self.jobs = []
        self._jobs_lock = Lock()
        self.workers = []
        self._workers_lock = Lock()


    def add_job(self, job):
        with self._jobs_lock:
            self.jobs.append(job)


    async def find_job(self):
        job_assigned = False

        with self._jobs_lock:
            remove_jobs = []
            insert_jobs = []

            for job in self.jobs:
                if job.is_finished():
                    next_job = job.create_next_job()
                    if next_job is None:
                        job.get_task().finish()
                    else:
                        insert_jobs.append(next_job)
                    remove_jobs.append(job)
                    continue
                elif job.is_assigned():
                    continue

                with self._workers_lock:
                    for worker in self.workers:
                        if worker.is_available() and worker.assign_job(job):
                            job_assigned = True
                            job.trigger_assigned(worker)

                            await worker.perform_job()
                            job.trigger_performed(worker)

            for job in remove_jobs:
                self.jobs.remove(job)

            for job in insert_jobs:
                self.jobs.append(job)

        return job_assigned


    def get_job_from_worker_reply(self, message):
        job_id = message.OurCookies.get('jobId')
        if job_id is None:
            return None

        with self._jobs_lock:
            for job in self.jobs:
                if job.get_id() == job_id:
                    return job


    def add_worker(self, worker):
        with self._workers_lock:
            self.workers.append(worker)


    def get_by_id(self, identity):
        with self._workers_lock:
            for worker in self.workers:
                if identity == worker.get_id():
                    return worker
