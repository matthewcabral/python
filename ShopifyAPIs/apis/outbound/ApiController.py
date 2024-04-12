from fastapi import FastAPI
from order.OrderController import *
from utils.UtilsController import *
from utils.LogsController import *
from product.ProductController import *
from system.SysPrefController import *
from .routers.order import OrderRouter
from .routers.product import ProductRouter

class ApiController:
    def __init__(self):
        self.sys = SysPrefController()

        self.app = FastAPI()
        self.setup_routes()

    def setup_routes(self):
        order_router = OrderRouter()
        product_router = ProductRouter()
        self.app.include_router(order_router.router, prefix="/order", tags=["Order"])
        self.app.include_router(product_router.router, prefix="/product", tags=["Product"])

        @self.app.get('/')
        def status():
            return { "status": "online." }, 200
