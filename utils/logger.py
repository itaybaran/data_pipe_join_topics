import logging
import time
import os
import uuid
import json
from kafka_producer import ConfluentProducer
from dotenv import load_dotenv, find_dotenv

class Logger:
    def __init__(self):
        self.batch_id = str(uuid.uuid4())
        self.log_level=os.getenv("LOG_LEVEL")
        self.logger = logging.getLogger()
        self.log_information = {'search_fields': {'function_name': 'name', 'batch_id': 'batchId'},
                                'attribute_data': {'msg': 'message'}}
        self.log_debug = {'search_fields': {'function_name': 'name', 'batch_id': 'batchId'},
                                'attribute_data': {'msg': 'message'}}
        self.log_error = {'search_fields': {'function_name': 'name', 'batch_id': 'batchId'},
                          'attribute_data': {'error_code': "--", 'error_type': 'type', 'err_msg': 'message'}}
        self.kafka_client_auth = {"bootstrap.servers": "kafka:29092","security.protocol": "PLAINTEXT"}
        self.audit_poducer = ConfluentProducer(os.getenv("AUDIT_TOPIC"),os.getenv("PRODUCER_BATCH_SIZE"),self.kafka_client_auth)
        self.error_poducer = ConfluentProducer(os.getenv("ERROR_TOPIC"),os.getenv("PRODUCER_BATCH_SIZE"),self.kafka_client_auth)
        self.set_logger()


    def set_logger(self):
        try:
            if len(self.logger.handlers) ==0:
                self.logger.setLevel(self.log_level)
                # create console handler and set level to debug
                ch = logging.StreamHandler()
                ch.setLevel(self.log_level)

                # create formatter
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

                # add formatter to ch
                ch.setFormatter(formatter)
                # add formatter to ch

                # add handlers to logger
                self.logger.addHandler(ch)
        except:
            print("Unable to create Logger")

    def insert_error_to_log(self,function_name, errorCode,errorType,errMsg):
        self.log_error['search_fields']['batch_id'] = self.batch_id
        self.log_error['search_fields']['function_name'] = function_name
        self.log_error['attribute_data']['error_code'] = errorCode
        self.log_error['attribute_data']['error_type'] = errorType
        self.log_error['attribute_data']['err_msg'] = errMsg
        self.logger.error(str(self.log_error))
        self.error_poducer.send(self.log_error)

    def insert_info_to_log(self, function_name, msg):
        self.log_information['search_fields']['batch_id'] = self.batch_id
        self.log_information['search_fields']['function_name'] = function_name
        self.log_information['attribute_data']['msg'] = msg
        self.logger.info((self.log_information))
        if self.logger.level <= logging.INFO:
            self.audit_poducer.send(self.log_information)


    def insert_debug_to_log(self, function_name, msg):
        self.log_debug['search_fields']['batch_id'] = self.batch_id
        self.log_debug['search_fields']['function_name'] = function_name
        self.log_debug['attribute_data']['msg'] = msg
        self.logger.debug(str(self.log_debug))
        if self.logger.level <= logging.INFO:
            self.audit_poducer.send(self.log_debug)

    def format_json(self, log_dict):
         # .encode('utf8')
         return json.dumps(log_dict, sort_keys=False, ensure_ascii=False)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)
    def debug(self, msg):
        self.logger.debug(msg)
    def get_logger(self):
        return self.logger
    
    def get_error_code(self,error_type):
        default_code = "4011"
        codes =  [{"AssertionError":4112},{"ValidatorError":4111},
                  {"DataExtractorError":4121},{"HL7ParserError":4131},
                  {"FilterError":4141},{"OperatorError":4151},
                  {"ExplodeError":4161},{"EnrichError":4171}]
        for element in codes:
            for key in element:
                if key == error_type:
                    return str(element[error_type]) 
        return default_code

