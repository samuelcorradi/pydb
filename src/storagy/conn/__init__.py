from abc import ABC, abstractmethod

class Conn(ABC):
    """
    """

    def __init__(self):
        self._handler = None
        self.connect()

    def __del__(self):
        print('Objeto de conexão da classe {} destruído.'.format(self.__class__))
        self.disconnect()

    def connect(self):
        if not self._handler:
            self._handler = self._connect()

    def disconnect(self):
        if self._handler:
            self._disconnect()

    def get_handler(self):
        return self._handler

    def open(self):
        pass

    def close(self):
        pass

    @abstractmethod
    def _connect(self):
        pass

    @abstractmethod
    def _disconnect(self):
        pass