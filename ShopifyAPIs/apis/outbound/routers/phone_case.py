from product.PhoneCasesController import *
from order.OrderController import *
from utils.UtilsController import *
from utils.LogsController import *
from utils.PrintingController import *

class PhoneCaseRouter:
    def __init__(self):
        self.order = OrderController()
        self.phone_case = PhoneCasesController()
        self.utils = UtilsController()
        self.log = LogsController()
        self.router = APIRouter()
        self.printing = PrintingController()
        self.setup_routes()

    def setup_routes(self):

        @self.router.get('/get_all', status_code=200)
        async def get_all_phone_cases(response: Response):
            try:
                return_flag, response_json, response_description, status_code = self.phone_case.api_get_all_phone_cases()
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_description)
                else:
                    response_json = json.loads(response_json)
                    return response_json
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @self.router.get('/get/{phone_case_id}', status_code=200)
        async def get_specific_phone_case(phone_case_id: str, response: Response):
            try:
                return_flag, response_json, response_description, status_code = self.phone_case.api_get_specific_phone_case(phone_case_id)
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_description)
                else:
                    response_json = json.loads(response_json)
                    return response_json
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @self.router.post('/insert', status_code=200)
        async def insert_phone_case(phone_case_data: dict, response: Response):
            if phone_case_data is None:
                raise HTTPException(status_code=400, detail='Empty JSON.')
            else:
                try:
                    is_upserted_success, response_description, status_code = self.phone_case.api_save_phone_case(phone_case_json=phone_case_data, function="insert")

                    if not is_upserted_success:
                        raise HTTPException(status_code=status_code, detail=response_description)
                    else:
                        return { "is_upserted_success": is_upserted_success, "description": response_description }
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

        @self.router.put('/update', status_code=200)
        async def update_phone_case(phone_case_data: dict, response: Response):
            if phone_case_data is None:
                raise HTTPException(status_code=400, detail='Empty JSON.')
            else:
                try:
                    is_upserted_success, response_description, status_code = self.phone_case.api_save_phone_case(phone_case_json=phone_case_data, function="update")

                    if not is_upserted_success:
                        raise HTTPException(status_code=status_code, detail=response_description)
                    else:
                        return { "is_upserted_success": is_upserted_success, "description": response_description }
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

        @self.router.delete('/delete/{phone_case_id}', status_code=200)
        async def delete_phone_case(phone_case_id: str, response: Response, variant_name: str = None):
            try:
                is_deleted_success, total_deleted, response_description, status_code = self.phone_case.api_delete_phone_case(phone_case_id, variant_name)
                response.status_code = status_code
                if not is_deleted_success:
                    raise HTTPException(status_code=status_code, detail=response_description)
                else:
                    return { "Deleted Flag": is_deleted_success, "Total deleted": total_deleted, "Error Message": response_description }
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @self.router.get('/get_print_history', status_code=200)
        async def get_phone_cases_printing_history(response: Response, limit: str = None, since_id: str = None):
            try:
                if limit is None:
                    limit = "50"
                return_flag, status_code, return_message = self.printing.get_all_printing_history(limit=limit, since_id=since_id)
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=return_message)
                else:
                    response_json = self.utils.convert_json_to_object(self.utils.convert_object_to_json(return_message))
                    return response_json
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @self.router.post('/print', status_code=200)
        async def print_phone_cases(background_tasks: BackgroundTasks, quantity: int = 50, country_code: str = None):
            try:
                if country_code is None:
                    country_code = "ALL"

                background_tasks.add_task(self.order.process_phone_case_orders, quantity, country_code)

                return {
                    "status": "processing",
                    "message": "Processing your request. Please wait."
                }
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        # Function to get the last printed batch
        @self.router.get('/get_last_batch_info', status_code=200)
        async def get_last_batch_info(response: Response, country_code: str = None):
            try:
                return_flag, status_code, response_json  = self.phone_case.get_last_phone_case_batch_info(country_code=country_code)
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_json)
                else:
                    return response_json
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @self.router.get('/get_total_available_to_print', status_code=200)
        async def get_total_available_to_print(response: Response):
            try:
                return_flag, status_code, response_json  = self.phone_case.get_total_available_to_print()
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_json)
                else:
                    return response_json
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))
