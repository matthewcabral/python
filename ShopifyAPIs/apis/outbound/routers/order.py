from order.OrderController import *
from order.BillingAddressController import *
from utils.UtilsController import *
from utils.LogsController import *
from mq.MQController import *

class OrderRouter:
    def __init__(self):
        self.order = OrderController()
        self.billing_address = BillingAddressController()
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

        @self.router.post('/billing_address/insert', status_code=200)
        async def order_billing_address_create(request_data: dict, response: Response):
            result_flag = False
            return_code = 200
            result_string = "Success"
            row_count = 0
            try:
                result_flag, return_code, row_count, result_string = self.billing_address.add_billing_address(address_json=request_data)

                response.status_code = return_code
                if not result_flag:
                    raise HTTPException(status_code=return_code, detail=result_string)
                else:
                    return {
                        "code": return_code,
                        "total": row_count,
                        "message": result_string
                    }
            except Exception as e:
                print(f"[ERROR] Failed add Billing Address. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.put('/billing_address/update/{order_id}', status_code=200)
        async def order_billing_address_edit(order_id, request_data: dict, response: Response):
            result_flag = False
            return_code = 200
            result_string = "Success"
            row_count = 0
            try:
                if order_id is None or order_id == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail="Order ID is required.")

                result_flag, return_code, row_count, result_string = self.billing_address.edit_billing_address(order_id=order_id, address_json=request_data)

                response.status_code = return_code
                if not result_flag:
                    raise HTTPException(status_code=return_code, detail=result_string)
                else:
                    return {
                        "code": return_code,
                        "total": row_count,
                        "message": result_string
                    }
            except Exception as e:
                print(f"[ERROR] Failed to update Billing Address. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.delete('/billing_address/delete/{order_id}', status_code=200)
        async def delete_order_billing_address(order_id, response: Response):
            try:
                if order_id is None or order_id == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail="Order ID is required.")

                result_flag, return_code, row_count, result_string = self.billing_address.remove_billing_address(order_id=order_id)

                response.status_code = return_code
                if not result_flag:
                    raise HTTPException(status_code=return_code, detail=result_string)
                else:
                    return {
                        "code": return_code,
                        "total": row_count,
                        "message": result_string
                    }
            except Exception as e:
                print(f"[ERROR] Failed to delete Billing Address. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get('/billing_address/get_all', status_code=200)
        async def get_all_billing_addresses(response: Response):
            try:
                return_flag, status_code, response_json = self.billing_address.get_all_billing_addresses_api()
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

        @self.router.get('/billing_address/get_specific/{order_id}', status_code=200)
        async def get_specific_billing_address(order_id, response: Response):
            try:
                if order_id is None or order_id == "":
                    raise HTTPException(status_code=400, detail="Order ID is required.")

                return_flag, status_code, response_json = self.billing_address.get_specific_billing_address_api(order_id=order_id)
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

        @self.router.post('/fulfillment/upload_orders', status_code=200)
        async def orders_to_fulfill(file: UploadFile):
            try:
                file_base_path = self.utils.get_base_directory()
                file_path = "/files/csv/"
                file_final_path = file_base_path + file_path
                file_content = await file.read()
                file_name = file.filename
                file_name = file_name.format(str(file_name).replace(" ", "_").replace(":", "_"))

                if self.utils.upload_file(file_final_path + file_name, file_content):
                    print(f"File '{file_name}' uploaded successfully")
                    return {
                        "message": f"File '{file_name}' uploaded successfully",
                        "file_path": file_path,
                        "file_name": file_name
                    }
                else:
                    print(f"Failed to upload file '{file_name}'")
                    raise HTTPException(status_code=500, detail="Failed to upload file")
            except Exception as e:
                print(f"Failed to upload file '{file_name}'. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post('/fulfillment/fulfill', status_code=200)
        async def fulfill_orders(request_data: dict, response: Response, background_tasks: BackgroundTasks):
            return_code = 200
            result_string = "Success"
            email_to = None
            try:
                if request_data is None:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Empty JSON.')
                if "file_path" not in request_data or "file_name" not in request_data:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='File path and file name are required.')
                if request_data.get("file_path") is None or request_data.get("file_path") == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='File path is required.')
                if request_data.get("file_name") is None or request_data.get("file_name") == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='File name is required.')
                if "email_to" in request_data:
                    email_to = request_data.get("email_to")
                else:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Email is required.')
                if email_to is None or email_to == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Email is required.')

                file_path = self.utils.get_base_directory() + request_data.get("file_path")
                file_name = request_data.get("file_name")
                background_tasks.add_task(self.order.fulfill_orders, file_path=file_path, file_name=file_name, email_to=email_to)
                response.status_code = 200
                return {
                    "message": result_string
                }
            except Exception as e:
                print(f"[ERROR] Failed Processing Fulfillments. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
            
        @self.router.post('/upload_file', status_code=200)
        async def upload_csv_file(file: UploadFile):
            try:
                # Validade file type:
                result_flag, file_type, file_path, result_message = self.utils.validade_file_type(file)
                
                if not result_flag:
                    raise Exception(result_message)
                
                # Extract file extension
                file_extension = f".{file_type.lower()}"

                # Validade CSV file:
                if file_extension != ".csv":
                    raise Exception(f"The uploaded file is a {file_type}. It should be a CSV file.")

                file_content = await file.read()

                if self.utils.upload_file(file_path + file.filename, file_content):
                    print(f"[INFO] File '{file.filename}' uploaded successfully")
                    return {
                        "message": f"File '{file.filename}' uploaded successfully",
                        "file_name": file.filename,
                        "file_path": file_path
                    }
                else:
                    print(f"[ERROR] Failed to upload file '{file.filename}'")
                    raise HTTPException(status_code=500, detail="Failed to upload file")
            except Exception as e:
                print(f"[ERROR] Failed to upload file '{file.filename}'. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.router.put('/update_tags', status_code=200)
        async def update_tags(response: Response, request_data: dict, background_tasks: BackgroundTasks):
            """
            update_tags(response: Response, request_data: dict)

            Arguments:
                request_data (JSON):
                        {
                            "file_path": file_path
                            "file_name": file_name
                            "tags": tags
                        }
                response (JSON)
            """
            result_flag = False
            return_code = 200
            result_string = "Success"
            try:
                if request_data is None:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Empty JSON.')
                if "file_path" not in request_data or "file_name" not in request_data:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='File path and file name are required.')
                if request_data.get("file_path") is None or request_data.get("file_path") == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='File path is required.')
                if request_data.get("file_name") is None or request_data.get("file_name") == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='File name is required.')
                if "tags" in request_data:
                    new_tags_string = request_data.get("tags")
                else:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Tags are required.')
                if new_tags_string is None or new_tags_string == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Tags are required.')

                file_path = request_data.get("file_path")
                file_name = request_data.get("file_name")
                background_tasks.add_task(self.order.add_tags_to_orders, file_path=file_path,file_name=file_name, new_tags_string=new_tags_string)
                response.status_code = 200
                return {
                    "message": result_string
                }
            except Exception as e:
                print(f"[ERROR] Failed Adding Order Tags. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        # New Orders Dashboard API routes
        @self.router.get('/get_orders_data', status_code=200)
        async def get_orders_data_from_view_management(response: Response):
            pass
