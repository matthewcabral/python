import os
import uvicorn

from utils.UtilsController import *
from apis.outbound.ApiController import ApiController

class App:
    def __init__(self):
        sys.dont_write_bytecode = True
        load_dotenv()
        self.api = ApiController()
        self.run()

    def run(self):
        port = int(os.environ.get('PORT', 8000))
        host = '0.0.0.0' if "DYNO" in os.environ else 'localhost'
        uvicorn.run(self.api.app, host=host, port=port)

if __name__ == "__main__":
    app = App()