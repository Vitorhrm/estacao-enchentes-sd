"""Objeto remoto usado pelo terminal e pela demonstração."""
from xmlrpc.client import ServerProxy
from uuid import uuid4

class FloodStation:
    def __init__(self, port=18080):
        """Prepara o proxy XML-RPC do middleware local."""
        self.remote = ServerProxy(f"http://127.0.0.1:{port}/RPC2")

    def status(self):
        """Consulta a telemetria e o último estado confirmado pelo atuador."""
        return self.remote.status()

    def set_mode(self, mode, request_id=None):
        """Solicita um modo e usa o mesmo ID quando o cliente pede uma repetição."""
        return self.remote.set_mode(mode, request_id or uuid4().hex)
