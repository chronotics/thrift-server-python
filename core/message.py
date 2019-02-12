import json
from core import constant

class TransferMessage(object):
    def __init__(self):
        self.msg_callback = None
        self.msg_request_id = None
        self.msg_task_id = None
        self.msg_redis_key = None
        self.msg_redis_field = None
        self.msg_redis_prop = None
        self.msg_data_type = None
        self.msg_lang_src = None
        self.msg_lang_dest = None

    def parseMessage(self, message):
        json_message = json.loads(message)

        if json_message is not None:
            self.msg_callback = json_message[constant.MSG_CALLBACK]
            self.msg_request_id = json_message[constant.MSG_REQUEST_ID]
            self.msg_task_id = json_message[constant.MSG_TASK_ID]
            self.msg_redis_key = json_message[constant.MSG_REDIS_KEY]
            self.msg_redis_field = json_message[constant.MSG_REDIS_FIELD]
            self.msg_redis_prop = json_message[constant.MSG_REDIS_PROP]
            self.msg_data_type = json_message[constant.MSG_DATA_TYPE]
            self.msg_lang_src = json_message[constant.MSG_LANG_SRC]
            self.msg_lang_dest = json_message[constant.MSG_LANG_DEST]