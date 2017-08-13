""" Generic Message Storage with Headers and Body.
"""
from copy import copy
import json


class MessageStorage(object):
    """ Store data about json message protocol.
    """
    ApiVersion = None

    Headers = None # possibly None
    Body = None # possibly None
    Attachments = None # possibly None

    Cookies = None # possibly None
    OurCookies = None # possibly None

    def __init__(self, initialize=True):
        if initialize:
            self.Headers = {}
            self.Body = {}
            self.Attachments = {}
            self.OurCookies = {}


    def merge_attachments_to_body(self):
        """ Attachments is binary blob data that was once part of Body.

        We want to send them through the wire as binary, not json, so we don't
        have to encode the binary with base64, as one of multipart message.
        """
        if self.Attachments is None:
            return

        if self.Body is None:
            self.Body = {}

        for raw_field_name in self.Attachments:
            if not raw_field_name:
                continue

            field = self.Body
            field_names = raw_field_name.split('.')

            for ii in range(len(field_names) - 1):
                if not field_names[ii] in field:
                    field[field_names[ii]] = {}

                field = field[field_names[ii]]

            field[field_names[-1]] = self.Attachments[raw_field_name]

        self.Attachments = None


    def get_headers(self, our_id):
        """ Compile Headers part of the Message.

        It was separated to class attributes for easier viewing.
        """
        result = copy(self.Headers)

        result['version'] = self.ApiVersion

        if self.Cookies and self.OurCookies:
            if self.Cookies:
                cookies = copy(self.Cookies)
            else:
                cookies = {}

            cookies[our_id] = copy(self.OurCookies)
            result['cookies'] = cookies

        if self.Attachments:
            result['attachments'] = list(self.Attachments.keys())

        return result


    def to_multipart_message(self, our_id):
        """ Export Headers, Body, and Attachments and contiguous multipart
        message.
        """
        headers = self.get_headers(our_id)

        yield json.dumps(headers).encode('utf-8')
        yield json.dumps(self.Body).encode('utf-8')

        for item in self.Attachments.values():
            yield item


    @classmethod
    def from_multipart_message(cls, message, our_id):
        """ Create a Message from a multipart message.
        """
        msg = cls(initialize=False)

        msg.Headers = json.loads(message.pop(0).decode('utf-8'))
        msg.Body = json.loads(message.pop(0).decode('utf-8'))

        attachments = msg.Headers.pop('attachments', None)
        if attachments is not None:
            for key in attachments:
                msg.Attachments[key] = message.pop(0)

        msg.ApiVersion = msg.Headers['version']
        msg.Cookies = msg.Headers.pop('Cookies', None)

        if msg.Cookies:
            msg.OurCookies = msg.Cookies.pop(our_id, {})
        else:
            msg.OurCookies = {}

        return msg
