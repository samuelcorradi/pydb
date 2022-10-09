class ConnectionFail(Exception):
    
    message = "Fail to connect to the source."

    def __init__(self, handler_error:str=None):
        super().__init__(ConnectionFail.message)
        self.source_error = handler_error

    def __str__(self)->str:
        return ConnectionFail.message if not self.source_error else "{}: '{}'".format(ConnectionFail.message[0:-1], self.source_error)