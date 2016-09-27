import logging

from twisted.internet import defer
from twisted.protocols.basic import FileSender
from twisted.python.failure import Failure
from zope.interface import implements

from lbrynet.core.Offer import Offer, Negotiate
from lbrynet.core.Strategy import get_default_strategy
from lbrynet.interfaces import IQueryHandlerFactory, IQueryHandler, IBlobSender


log = logging.getLogger(__name__)


class BlobRequestHandlerFactory(object):
    implements(IQueryHandlerFactory)

    def __init__(self, blob_manager, blob_tracker, wallet, payment_rate_manager):
        self.blob_tracker = blob_tracker
        self.blob_manager = blob_manager
        self.wallet = wallet
        self.payment_rate_manager = payment_rate_manager

    ######### IQueryHandlerFactory #########

    def build_query_handler(self):
        q_h = BlobRequestHandler(self.blob_manager, self.blob_tracker, self.wallet, self.payment_rate_manager)
        return q_h

    def get_primary_query_identifier(self):
        return 'requested_blob'

    def get_description(self):
        return "Blob Uploader - uploads blobs"


class BlobRequestHandler(object):
    implements(IQueryHandler, IBlobSender)

    def __init__(self, blob_manager, blob_tracker, wallet, payment_rate_manager):
        self.blob_manager = blob_manager
        self.blob_tracker = blob_tracker
        self.payment_rate_manager = payment_rate_manager
        self.wallet = wallet
        self.query_identifiers = ['blob_data_payment_rate', 'requested_blob', 'requested_blobs']
        self.peer = None
        self.blob_data_payment_rate = None
        self.read_handle = None
        self.currently_uploading = None
        self.file_sender = None
        self.blob_bytes_uploaded = 0
        self.strategy = get_default_strategy(self.blob_tracker)
        self._blobs_requested = []

    ######### IQueryHandler #########

    def register_with_request_handler(self, request_handler, peer):
        self.peer = peer
        request_handler.register_query_handler(self, self.query_identifiers)
        request_handler.register_blob_sender(self)

    def handle_queries(self, queries):
        response = defer.succeed({})

        if self.query_identifiers[2] in queries:
            self._blobs_requested = queries[self.query_identifiers[2]]
            response.addCallback(lambda r: self._reply_to_availability(r, self._blobs_requested))

        if self.query_identifiers[0] in queries:
            offer = Offer(queries[self.query_identifiers[0]])
            response.addCallback(lambda r: self.reply_to_offer(offer, r))

        if self.query_identifiers[1] in queries:
            incoming = queries[self.query_identifiers[1]]
            log.info("Request download: %s", str(incoming))
            response.addCallback(lambda r: self._reply_to_send_request({}, incoming))

        return response

    ######### IBlobSender #########

    def send_blob_if_requested(self, consumer):
        if self.currently_uploading is not None:
            return self.send_file(consumer)
        return defer.succeed(True)

    def cancel_send(self, err):
        if self.currently_uploading is not None:
            self.currently_uploading.close_read_handle(self.read_handle)
        self.read_handle = None
        self.currently_uploading = None
        return err

    ######### internal #########

    def _add_to_response(self, response, to_add):

        return response

    def _reply_to_availability(self, request, blobs):
        d = self._get_available_blobs(blobs)

        def set_available(available_blobs):
            log.debug("available blobs: %s", str(available_blobs))
            request.update({'available_blobs': available_blobs})
            return request

        d.addCallback(set_available)
        return d

    def open_blob_for_reading(self, blob, response):
        response_fields = {}
        if blob.is_validated():
            read_handle = blob.open_for_reading()
            if read_handle is not None:
                self.currently_uploading = blob
                self.read_handle = read_handle
                log.info("Sending %s to client", str(blob))
                response_fields['blob_hash'] = blob.blob_hash
                response_fields['length'] = blob.length
                response['incoming_blob'] = response_fields
                log.info(response)
                return response, blob
        log.warning("We can not send %s", str(blob))
        response['error'] = "BLOB_UNAVAILABLE"
        return response, blob

    def record_transaction(self, response, blob, rate):
        d = self.blob_manager.add_blob_to_upload_history(str(blob), self.peer.host, rate)
        d.addCallback(lambda _: response)
        log.info(response)
        return d

    def _reply_to_send_request(self, response, incoming):
        response_fields = {}
        response['incoming_blob'] = response_fields
        rate = self.blob_data_payment_rate

        if self.blob_data_payment_rate is None:
            log.warning("Rate not set yet")
            response['error'] = "RATE_UNSET"
            return defer.succeed(response)
        else:
            d = self.blob_manager.get_blob(incoming, True)
            d.addCallback(lambda blob: self.open_blob_for_reading(blob, response))
            d.addCallback(lambda (r, blob): self.record_transaction(r, blob, rate))
            return d

    def reply_to_offer(self, offer, request):
        blobs = request.get("available_blobs", [])
        log.info("Offered rate %f/mb for %i blobs", offer.rate, len(blobs))
        reply = self.strategy.respond_to_offer(offer, self.peer, blobs)
        if reply.accepted:
            self.blob_data_payment_rate = reply.rate
        r = Negotiate.make_dict_from_offer(reply)
        request.update(r)
        return request

    def _get_available_blobs(self, requested_blobs):
        d = self.blob_manager.completed_blobs(requested_blobs)
        return d

    def send_file(self, consumer):

        def _send_file():
            inner_d = start_transfer()
            # TODO: if the transfer fails, check if it's because the connection was cut off.
            # TODO: if so, perhaps bill the client
            inner_d.addCallback(lambda _: set_expected_payment())
            inner_d.addBoth(set_not_uploading)
            return inner_d

        def count_bytes(data):
            self.blob_bytes_uploaded += len(data)
            self.peer.update_stats('blob_bytes_uploaded', len(data))
            return data

        def start_transfer():
            self.file_sender = FileSender()
            log.info("Starting the file upload")
            assert self.read_handle is not None, "self.read_handle was None when trying to start the transfer"
            d = self.file_sender.beginFileTransfer(self.read_handle, consumer, count_bytes)
            return d

        def set_expected_payment():
            log.debug("Setting expected payment")
            if self.blob_bytes_uploaded != 0 and self.blob_data_payment_rate is not None:
                # TODO: explain why 2**20
                self.wallet.add_expected_payment(self.peer,
                                                 self.currently_uploading.length * 1.0 *
                                                 self.blob_data_payment_rate / 2**20)
                self.blob_bytes_uploaded = 0
            self.peer.update_stats('blobs_uploaded', 1)
            return None

        def set_not_uploading(reason=None):
            if self.currently_uploading is not None:
                self.currently_uploading.close_read_handle(self.read_handle)
                self.read_handle = None
                self.currently_uploading = None
            self.file_sender = None
            if reason is not None and isinstance(reason, Failure):
                log.warning("Upload has failed. Reason: %s", reason.getErrorMessage())

        return _send_file()
