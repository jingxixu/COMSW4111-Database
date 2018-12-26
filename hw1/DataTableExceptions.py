

class DataTableException(Exception):

    def __init__(self, code=None, message=None):
        self.code = code
        self.message = message

    def __str__(self):
        result = ""
        result += "DataTableException: code: {:<3}, message: {}".format(self.code, self.message)
        return result