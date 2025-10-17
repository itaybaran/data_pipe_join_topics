import json
import datetime
import copy
import pytz

class JsonHandler:
    def __init__(self, Config, Logger):
        self.config = Config
        self.logger = Logger


    def format_json(self, log_dict):
        # .encode('utf8')
        return json.dumps(log_dict, sort_keys=False, ensure_ascii=False)

    def get_element_data(self,jsonObj,elementPathF,typeElement = "string"):
        try:
            elementPath = copy.deepcopy(elementPathF)
            #if elementPathF[0] == "ns2:SendingFacility":
                #a=1
            nextElement = jsonObj
            while elementPath:
                currentPath = elementPath.pop(0)
                if nextElement != None and currentPath in nextElement:
                    nextElement = nextElement[currentPath]
                else:
                    nextElement =  'לא קיים'

            if nextElement ==  'לא קיים':
                return nextElement
            if nextElement ==  None:
                nextElement = 'לא קיים'
                return nextElement
            if nextElement ==  "":
                nextElement = 'לא קיים'
                return nextElement

            if typeElement == "element":
                return nextElement
            elif typeElement == "string":
                return nextElement
            elif typeElement == "int":
                return int(nextElement)
            elif typeElement == "float":
                return float(nextElement)
            elif typeElement == "datetime":
                return datetime.datetime.strptime(nextElement, '%Y-%m-%d %H:%M:%S')
            elif typeElement == "datetimeIso":
                return datetime.datetime.strptime(nextElement, '%Y-%m-%dT%H:%M:%S').isoformat()
            elif typeElement == "utc":
                naive_dt = datetime.datetime.strptime(nextElement, '%Y-%m-%d %H:%M:%S') if type(nextElement) is str else nextElement
                milliseconds_since_epoch = int(naive_dt.timestamp() * 1000)
                utcElement = { "$date": milliseconds_since_epoch }
                return  utcElement
            elif typeElement == "offset":
                timezone_str = "Asia/Jerusalem"  # Timezone for Israel
                naive_dt = datetime.datetime.strptime(nextElement, '%Y-%m-%d %H:%M:%S') if type(nextElement) is str else nextElement
                timezone = pytz.timezone(timezone_str)
                aware_dt = timezone.localize(naive_dt)
                offset_minutes = - aware_dt.utcoffset().total_seconds() / 60
                return int(offset_minutes)

        except Exception as e:
            if e.args[0] == "Fail":
                raise Exception("Fail")
            else:
                ErrorType = type(e).__name__
                errorCode = "--" if e.args.__len__() == 1 else e.args[0]
                self.logger.insert_error_to_log("get_element_data", errorCode, ErrorType, str(e))
                raise Exception(e)

    def remove_empyy_elements(self,data):
        try:
            for key, value in list(data.items()):
                if value == 'לא קיים' or value == 'לא קיים לא קיים' or value == None:
                    del data[key]
                elif isinstance(value, dict):
                    self.remove_empyy_elements(value)
                elif isinstance(value, list):
                    for jsonList in value:
                        self.remove_empyy_elements(jsonList)
            return data

        except Exception as e:
            if e.args[0] == "Fail":
                raise Exception("Fail")
            else:
                ErrorType = type(e).__name__
                errorCode = "--" if e.args.__len__() == 1 else e.args[0]
                self.logger.insert_error_to_log("remove_empyy_elements", errorCode, ErrorType, str(e))
                raise Exception(e)
    def remove_empyy_dict_elements(self,data):
        try:
            for key, value in list(data.items()):
                if (isinstance(value, dict) or isinstance(value, list)) and value.__len__() == 0 :
                    del data[key]
                elif isinstance(value, dict):
                    self.remove_empyy_dict_elements(value)
                    if (isinstance(value, dict) or isinstance(value, list)) and value.__len__() == 0:
                        del data[key]
                elif isinstance(value, list):
                    for jsonList in value:
                        self.remove_empyy_dict_elements(jsonList)
                        if (isinstance(value, dict) or isinstance(value, list)) and value.__len__() == 0:
                            del data[key]
            return data

        except Exception as e:
            if e.args[0] == "Fail":
                raise Exception("Fail")
            else:
                ErrorType = type(e).__name__
                errorCode = "--" if e.args.__len__() == 1 else e.args[0]
                self.logger.insert_error_to_log("remove_empyy_dict_elements", errorCode, ErrorType, str(e))
                raise Exception(e)

    def get_offset_from_date(self,dtDate):
        try:
            timezone_str = "Asia/Jerusalem"  # Timezone for Israel
            timezone = pytz.timezone(timezone_str)
            aware_dt = timezone.localize(dtDate)
            offset_minutes = - aware_dt.utcoffset().total_seconds() / 60
            return int(offset_minutes)

        except Exception as e:
            if e.args[0] == "Fail":
                raise Exception("Fail")
            else:
                ErrorType = type(e).__name__
                errorCode = "--" if e.args.__len__() == 1 else e.args[0]
                self.logger.insert_error_to_log("get_offset_from_date", errorCode, ErrorType, str(e))
                raise Exception(e)

    def get_uts_date_from_date(self,dtDate):
        try:
            intUtsDate =  int(dtDate.timestamp() * 1000)
            return intUtsDate

        except Exception as e:
            if e.args[0] == "Fail":
                raise Exception("Fail")
            else:
                ErrorType = type(e).__name__
                errorCode = "--" if e.args.__len__() == 1 else e.args[0]
                self.logger.insert_error_to_log("get_uts_date_from_date", errorCode, ErrorType, str(e))
                raise Exception(e)
    def get_date_from_utc_date(self,offsetDate,utcDate):
        try:
            dtDate = datetime.datetime.fromtimestamp(utcDate / 1000) -  datetime.timedelta(minutes=offsetDate)
            return dtDate

        except Exception as e:
            if e.args[0] == "Fail":
                raise Exception("Fail")
            else:
                ErrorType = type(e).__name__
                errorCode = "--" if e.args.__len__() == 1 else e.args[0]
                self.logger.insert_error_to_log("get_date_from_utc_date", errorCode, ErrorType, str(e))
                raise Exception(e)
