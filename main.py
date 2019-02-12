import logging
import argparse

from thrift import Thrift
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport

from messenger import TransferService
from core import data
from core import message

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

class TransferServiceHandler(object):
    def __init__(self):
        pass

    def writeMessageBegin(self, _v):
        return None

    def writeMessage(self, _v):
        return None

    def writeMessageEnd(self, _v):
        return None

    def writeBool(self, _v):
        return None

    def writeI16(self, _v):
        return None

    def writeI32(self, _v):
        return None

    def writeI64(self, _v):
        return None

    def writeDouble(self, _v):
        return None

    def writeString(self, _v):
        logging.info("** Received [%s]", _v)

        msgParsing = message.TransferMessage()
        msgParsing.parseMessage(_v)

        dataIO = data.DataIO('192.168.0.74', 6379, 1, 'brique0901#$')
        return_data = dataIO.read_data(msgParsing.msg_redis_key, msgParsing.msg_redis_field, msgParsing.msg_redis_prop,
                                       msgParsing.msg_data_type, msgParsing.msg_lang_src, msgParsing.msg_lang_dest)

        return return_data

    def readMessageBegin(self, _receiver_id):
        return None

    def readMessage(self, _receiver_id):
        return None

    def readMessageEnd(self, _receiver_id):
        return None

    def readBool(self):
        return None

    def readI16(self):
        return None

    def readI32(self):
        return None

    def readI64(self):
        return None

    def readDouble(self):
        return None

    def readString(self):
        return None

    def writeId(self, _id):
        return None

    def readId(self, _id):
        return None

def main():
    try:
        transfer_service_handler = TransferServiceHandler()
        processor = TransferService.Processor(transfer_service_handler)

        transport = TSocket.TServerSocket(port=16001)
        transport_factory = TTransport.TBufferedTransportFactory()
        protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()

        server = TServer.TSimpleServer(processor, transport, transport_factory, protocol_factory)
        server.serve()
    except Thrift.TException as ex:
        logging.error("ERR: %s", ex.message)

if __name__ == '__main__':
    main()