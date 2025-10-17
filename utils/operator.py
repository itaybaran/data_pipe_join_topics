import datetime
import copy
from flink_steps.flink_step import FlinkStep, StepError


class OperatorError(StepError):
    def __init__(self, description):
        super().__init__()
        self.msg = description


class Operator(FlinkStep):
    @classmethod
    def validate_token(self,token,message):
        try:
            field = token[0]
            rule = str(token[1]).lower()
            is_constant = token[3]
            if is_constant:value = token[2]
            else: value = message[field]
            res = self.validate_operator(message[field],value,rule)
            assert res[0], res[1]
        except AssertionError as e:
            raise AssertionError(e)
        except Exception as e:
            raise OperatorError("OperatorError Error {}".format(str(e)))

    @classmethod
    def check_operator(self,token,message):
        try:
            field = token[0]
            rule = str(token[1]).lower()
            is_constant = token[3]
            if is_constant:value = token[2]
            else: value = message[field]
            res = self.validate_operator(message[field],value,rule)
            return res[0]
        except Exception as e:
            raise OperatorError("OperatorError Error {}".format(str(e)))

    @classmethod 
    def validate_operator(self,left,right,operator):
        try:
            if operator == 'eq':
                return (left == right,"field {} must be equal to {}".format(left,right)) 
            elif operator == 'neq':
                return  (left != right,"field {}  must not be equal to {}".format(left,right))
            elif operator == 'gte':
                return  (left >= right,"field {} must be greater than or equal to {}".format(left,right))
            elif operator == 'gt':
                return  (left > right, "field {} must be greater than {}".format(left,right))
            elif operator == 'lte':
                return  (left <= right,"field {} must be less than or equal to {}".format(left,right))
            elif operator == 'lt':
                return  (left < right,"field {} must be less than {}".format(left,right))
            elif operator == 'nnull':
                return (not left is None,"field {} must not be null {}".format(left,right))
            elif operator == 'fex':
                return (isinstance( left, str),"field {} must be a part of the message".format(left))
            else:
                raise validatorError("validate_operator Error Operator is not valid")
        except AssertionError as e:
            raise AssertionError(e)
        except Exception as e:
            raise OperatorError("validate_operator Error {}".format(str(e)))
        
    