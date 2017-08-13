""" Base Task Producer and interface definition.

Produces tasks of a set number of tasks.
"""
import logging


class ITaskProducer(object):
    """ Interface for Task Producer.
    """

    def create_tasks(self, count, our_id, extra_data):
        """ Return list of Tasks or empty list, no more than n items.
        """
        raise NotImplementedError()


class TaskProducerBase(ITaskProducer):
    """ Base class that implements ITaskProducer.
    """
    logger = None

    def __init__(self):
        self.logger = logging.getLogger(type(self).__name__)


    def create_tasks(self, count, our_id, extra_data):
        raise NotImplementedError()
