"""Objeto remoto usado pelo terminal e pela demonstração."""
from xmlrpc.client import ServerProxy
from uuid import uuid4

class FloodStation:
    def __init__(self, port=18080):
        self.remote = ServerProxy(f"http://127.0.0.1:{port}/RPC2")

    def status(self):
        return self.remote.status()

    def set_mode(self, mode, request_id=None):
        return self.remote.set_mode(mode, request_id or uuid4().hex)
