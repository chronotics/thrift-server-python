import pyredis
import pickle
import logging
import numpy
import re
import rpy2.robjects as robjects

from rpy2.robjects import pandas2ri

pandas2ri.activate()


class DataIO(object):
    def __init__(self, redis_host, redis_port, redis_db, redis_pass):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_pass = redis_pass

    def read_data(self, redis_key, field_name, prop_name, data_type, lang_src, lang_dest):
        return_data = None

        try:
            if lang_src is 'python':
                redis_conn = pyredis.Client(host=self.redis_host, port=self.redis_port,
                                            database=self.redis_db, password=self.redis_pass)

                prev_output = redis_conn.hget(redis_key, field_name)
                if prev_output is not None:
                    prev_output = pickle.loads(prev_output)

                    if prop_name in prev_output:
                        return_data = prev_output[prop_name]
            elif lang_src is 'r':
                r_string = 'library(rredis);redisConnect("192.168.0.74", 6379, "brique0901#$");redisSelect(1);'
                r_string += 'prev_output <- redisHGet("' + redis_key + '", "' + field_name + ");prev_output;"

                robjects.r(r_string)
                return_data = robjects.r('prev_output[["' + prop_name + '"]]')

                if 'single' in data_type:
                    return_data = return_data[0]
        except Exception as ex:
            return_data = None
            logging.error("ERR: %s", ex.__str__())

        if return_data is not None:
            return_data = self.convert_data_to_str(prop_name, return_data, lang_dest, data_type)

        return return_data

    def convert_data_to_str(self, prop_name, original_data, lang_dest, data_type):
        return_data = None

        try:
            if 'single' in data_type:
                if lang_dest == 'java':
                    if 'int' in data_type:
                        return_data = 'Integer ' + prop_name + ' = ' + str(int(original_data)) + ';'
                    elif 'double' in data_type:
                        return_data = 'Double ' + prop_name + ' = ' + str(float(original_data)) + ';'
                    elif 'string' in data_type:
                        return_data = 'String ' + prop_name + ' = ' + str(original_data) + ';'
                    elif 'boolean' in data_type:
                        return_data = 'Boolean ' + prop_name + ' = ' + str(bool(original_data)) + ';'
                    elif 'date' in data_type:
                        return_data = 'Long ' + prop_name + ' = ' + str(int(original_data)) + 'L;'
                elif lang_dest == 'c++':
                    if 'int' in data_type:
                        return_data = 'int ' + prop_name + ' = ' + str(int(original_data)) + ';'
                    elif 'double' in data_type:
                        return_data = 'double ' + prop_name + ' = ' + str(float(original_data)) + ';'
                    elif 'string' in data_type:
                        return_data = 'wchar_t ' + prop_name + ' = ' + str(original_data) + ';'
                    elif 'boolean' in data_type:
                        return_data = 'bool ' + prop_name + ' = ' + str(bool(original_data)) + ';'
                    elif 'date' in data_type:
                        return_data = 'signed long ' + prop_name + ' = ' + str(int(original_data)) + ';'
            elif 'container' in data_type:
                if type(original_data) is not numpy.ndarray:
                    original_data = numpy.asarray(original_data)

                if original_data is not None:
                    data_shape = numpy.shape(original_data)
                    data_type = original_data.dtype.name

                    original_data_str = numpy.array2string(original_data)

                    return_data = re.sub('(\\n+)(\s+)', ',', original_data_str)
                    return_data = re.sub('\s+', ',', return_data)
                    return_data = re.sub('\[', '{', return_data)
                    return_data = re.sub('\]', '}', return_data)

                    if lang_dest == 'java':
                        if 'int' in data_type:
                            return_data = 'int ' + '[]' * len(data_shape) + ' = ' + return_data
                        elif 'float' in data_type:
                            return_data = 'float ' + '[]' * len(data_shape) + ' = ' + return_data
                        elif 'bool' in data_type:
                            return_data = 'boolean ' + '[]' * len(data_shape) + ' = ' + return_data
                        elif 'str' in data_type:
                            return_data = 'string ' + '[]' * len(data_shape) + ' = ' + return_data
                    elif lang_dest == 'c++':
                        if 'int' in data_type:
                            return_data = 'int ' + '[]' * len(data_shape) + ' = ' + return_data
                        elif 'float' in data_type:
                            return_data = 'float ' + '[]' * len(data_shape) + ' = ' + return_data
                        elif 'bool' in data_type:
                            return_data = 'bool ' + '[]' * len(data_shape) + ' = ' + return_data
                        elif 'str' in data_type:
                            return_data = 'wchar_t ' + '[]' * len(data_shape) + ' = ' + return_data

        except Exception as ex:
            return_data = None
            logging.error("ERR: %s", ex.__str__())

        return return_data