from order.OrderController import *
from utils.UtilsController import *
from utils.LogsController import *
from mq.MQController import *

class OrderRouter:
    def __init__(self):
        self.order = OrderController()
        self.utils = UtilsController()
        self.log = LogsController()
        self.router = APIRouter()
        self.mq = MQController()
        self.setup_routes()

    def setup_routes(self):
        @self.router.post('/insert', status_code=200)
        async def order_create(request_data: dict, response: Response):
            try:
                if request_data is None:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Empty JSON.')
                else:
                    is_upserted_success, response_description, status_code = await self.mq.queue_message(virtual_host="orders", queue_name="order", message_to_queue=json.dumps(request_data).encode("utf-8"))
                    response.status_code = status_code
                    # is_upserted_success, response_description, status_code = self.order.webhook_save_order(request_data)

                    if not is_upserted_success:
                        raise HTTPException(status_code=status_code, detail=response_description)
                    else:
                        return { "is_upserted_success": is_upserted_success, "description": response_description }
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post('/update', status_code=200)
        async def order_update(request_data: dict, response: Response):
            try:
                if request_data is None:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Empty JSON.')
                else:
                    is_upserted_success, response_description, status_code = await self.mq.queue_message(virtual_host="orders", queue_name="order", message_to_queue=json.dumps(request_data).encode("utf-8"))
                    response.status_code = status_code
                    # is_upserted_success, response_description, status_code = self.order.webhook_save_order(request_data)

                    if not is_upserted_success:
                        raise HTTPException(status_code=status_code, detail=response_description)
                    else:
                        return { "is_upserted_success": is_upserted_success, "description": response_description }
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post('/edit', status_code=200)
        async def order_edit(request_data: dict, response: Response):
            try:
                if request_data is None:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Empty JSON.')
                else:
                    # is_upserted_success, response_description, status_code = self.order.webhook_edit_order(request_data)
                    is_upserted_success, response_description, status_code = await self.order.webhook_order_edit(request_data)
                    response.status_code = status_code

                    if not is_upserted_success:
                        raise HTTPException(status_code=status_code, detail=response_description)
                    else:
                        return { "is_upserted_success": is_upserted_success, "description": response_description }
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get('/get_from_queue', status_code=200)
        async def order_get(response: Response):
            try:
                return_flag, response_json, response_description, status_code = await self.order.get_order_from_mq()
                response.status_code = status_code

                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_description)
                else:
                    return response_json
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get('/get_specific_order', status_code=200)
        async def order_get_specific(response: Response, order_id: str = None, order_name: str = None):
            try:
                return_flag, response_json, response_description, status_code = self.order.get_specific_order_from_db(order_id=order_id, order_name=order_name)
                response.status_code = status_code

                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_description)
                else:
                    return response_json
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))