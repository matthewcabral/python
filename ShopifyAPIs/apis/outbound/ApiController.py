from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from order.OrderController import *
from utils.UtilsController import *
from utils.LogsController import *
from product.ProductController import *
from product.PhoneCasesController import *
from system.SysPrefController import *
from .routers.order import OrderRouter
from .routers.product import ProductRouter
from .routers.phone_case import PhoneCaseRouter

from .routers.customs import CustomsRouter
from .routers.fonts import FontsRouter
from .routers.currency import CurrencyRouter
from .routers.vendors import VendorsRouter
from .routers.locations import LocationsRouter


class ApiController:
    def __init__(self):
        self.sys = SysPrefController()

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
        order_router = OrderRouter()
        product_router = ProductRouter()
        phone_case_router = PhoneCaseRouter()
        customs_router = CustomsRouter()
        fonts_router = FontsRouter()
        currency_router = CurrencyRouter()
        vendors_router = VendorsRouter()
        locations_router = LocationsRouter()

        self.app.include_router(order_router.router, prefix="/order", tags=["Order"])
        self.app.include_router(product_router.router, prefix="/product", tags=["Product"])
        self.app.include_router(phone_case_router.router, prefix="/phone_case", tags=["Phone Cases"])
        self.app.include_router(customs_router.router, prefix="/customs", tags=["Customs"])
        self.app.include_router(fonts_router.router, prefix="/fonts", tags=["Fonts"])
        self.app.include_router(currency_router.router, prefix="/currency", tags=["Currency"])
        self.app.include_router(vendors_router.router, prefix="/vendors", tags=["Vendors"])
        self.app.include_router(locations_router.router, prefix="/locations", tags=["Locations"])

        @self.app.get('/')
        def status():
            return { "status": "online." }, 200
