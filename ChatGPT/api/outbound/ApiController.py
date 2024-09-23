from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from utils.UtilsController import *
from .routers.AiRouter import AiRouter

class ApiController:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ApiController, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):  # Ensure __init__ is only called once
            self._initialized = True

            self.app = FastAPI()
            self.app.add_middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
            )
            self.setup_routes()

    def setup_routes(self):
        chatGPT_router = AiRouter()
        self.app.include_router(chatGPT_router.router, prefix="/chatGPT", tags=["Chat"])

        @self.app.get('/')
        def status():
            return { "status": "online." }, 200
