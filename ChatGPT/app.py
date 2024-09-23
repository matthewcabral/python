import os
import uvicorn
import sys
from api.outbound.ApiController import ApiController

class App:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(App, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):  # Ensure __init__ is only called once
            self._initialized = True
            sys.dont_write_bytecode = True
            self.api = ApiController()
            self.run()

    def run(self):
        port = int(os.environ.get('PORT', 8000))
        host = '0.0.0.0' if "DYNO" in os.environ else 'localhost'
        uvicorn.run(self.api.app, host=host, port=port)

if __name__ == "__main__":
    app = App()