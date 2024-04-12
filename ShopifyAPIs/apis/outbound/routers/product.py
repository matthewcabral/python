from product.ProductController import *
from utils.UtilsController import *
from utils.LogsController import *

class ProductRouter:
    def __init__(self):
        self.prod = ProductController()
        self.utils = UtilsController()
        self.log = LogsController()
        self.router = APIRouter()
        self.setup_routes()

    def setup_routes(self):
        @self.router.post('/insert')
        async def product_insert(product_data: dict, response: Response):
            if product_data is None:
                raise HTTPException(status_code=400, detail='Empty JSON.')
            else:
                try:
                    is_upserted_success, response_description, status_code = self.prod.webhook_save_product(product_json=product_data, function="insert")
                    
                    if not is_upserted_success:
                        raise HTTPException(status_code=status_code, detail=response_description)
                    else:
                        return { "is_upserted_success": is_upserted_success, "description": response_description }
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

        @self.router.put('/update')
        async def product_update(product_data: dict, response: Response):
            if product_data is None:
                raise HTTPException(status_code=400, detail='Empty JSON.')
            else:
                try:
                    is_upserted_success, response_description, status_code = self.prod.webhook_save_product(product_json=product_data, function="update")
                    
                    if not is_upserted_success:
                        raise HTTPException(status_code=status_code, detail=response_description)
                    else:
                        return { "is_upserted_success": is_upserted_success, "description": response_description }
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))
