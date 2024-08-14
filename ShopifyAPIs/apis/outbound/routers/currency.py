from system.CurrencyController import *
from utils.UtilsController import *
from utils.LogsController import *

class CurrencyRouter:
    def __init__(self):
        self.currency = CurrencyController()
        self.utils = UtilsController()
        self.log = LogsController()
        self.router = APIRouter()
        self.setup_routes()

    def setup_routes(self):
        @self.router.get('/get_rates', status_code=200)
        async def get_rates(response: Response):
            try:
                return_flag, status_code, response_json = self.currency.get_rates()
                response.status_code = status_code
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_json)
                else:
                    return response_json
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))
