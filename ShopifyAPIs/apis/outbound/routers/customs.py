from product.CustomsProductController import *
from utils.UtilsController import *
from utils.LogsController import *

class CustomsRouter:
    def __init__(self):
        self.customs = CustomsProductController()
        self.utils = UtilsController()
        self.log = LogsController()
        self.router = APIRouter()
        self.setup_routes()

    def setup_routes(self):
        @self.router.post('/upload_sheet', status_code=200)
        async def upload_excel_file(file: UploadFile, background_tasks: BackgroundTasks):
            try:
                file_path = "files/customs/sheets/"
                file_content = await file.read()

                if self.utils.upload_file(file_path + file.filename, file_content):
                    print(f"File '{file.filename}' uploaded successfully")
                    background_tasks.add_task(self.customs.process_file_from_vendor, file_path=file_path, file_name=file.filename)
                    return {"message": f"File '{file.filename}' uploaded successfully"}
                else:
                    print(f"Failed to upload file '{file.filename}'")
                    raise HTTPException(status_code=500, detail="Failed to upload file")
            except Exception as e:
                print(f"Failed to upload file '{file.filename}'. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post('/upload_font', status_code=200)
        async def upload_font_file(file: UploadFile, background_tasks: BackgroundTasks):
            try:
                file_path = "files/fonts/"
                file_content = await file.read()

                if self.utils.upload_file(file_path + file.filename, file_content):
                    print(f"File '{file.filename}' uploaded successfully")
                    background_tasks.add_task(self.customs.process_file_from_vendor, file_path=file_path, file_name=file.filename)
                    return {"message": f"File '{file.filename}' uploaded successfully"}
                else:
                    print(f"Failed to upload file '{file.filename}'")
                    raise HTTPException(status_code=500, detail="Failed to upload file")
            except Exception as e:
                print(f"Failed to upload file '{file.filename}'. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post('/mark_as_received', status_code=200)
        async def mark_as_received(request_data: dict, response: Response):
            try:
                if request_data is None:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Empty JSON.')
                else:
                    status_code, is_updated_success, rowcount, response_description = self.customs.mark_customs_as_received(json_data=request_data)
                    response.status_code = status_code

                    if not is_updated_success:
                        raise HTTPException(status_code=status_code, detail=response_description)
                    else:
                        return { "Tracking info updated": is_updated_success, "Total Orders Updated": rowcount, "Description": response_description }
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post('/export', status_code=200)
        async def export_customs_orders_info(response: Response, background_tasks: BackgroundTasks, email_to=None, since_id=None, order_id=None, order_number=None, fields=None, product_name=None, product_variant=None, sorority_flag=None, rush_flag=None, created_at_min=None, created_at_max=None, font=None, prod_sku=None, vendor_name=None, sent_to_vendor_flag=None, sent_to_vendor_date_min=None, sent_to_vendor_date_max=None, country=None, country_code=None, location_name=None, sent_back_from_vendor_flag=None, sent_back_from_vendor_date_min=None, sent_back_from_vendor_date_max=None, received_flag=None, received_back_date_min=None, received_back_date_max=None, tracking_number=None, is_fulfilled_flag=None, fulfilled_date_min=None, fulfilled_date_max=None, is_cancelled_flag=None, fulfillment_status=None, limit="all", order_by="desc"):
            try:
                if email_to is None or email_to == "":
                    print(f"[ERROR] Email To is required.")
                    response_json = {
                        "message": "Email To is required."
                    }
                    raise HTTPException(status_code=400, detail=response_json)

                background_tasks.add_task(
                    self.customs.export_list_of_custom_orders,
                    email_to=email_to,
                    since_id=since_id,
                    order_id=order_id,
                    order_number=order_number,
                    fields=fields,
                    product_name=product_name,
                    product_variant=product_variant,
                    is_sorority_flag=sorority_flag,
                    is_rush_flag=rush_flag,
                    created_at_min=created_at_min,
                    created_at_max=created_at_max,
                    font=font,
                    prod_sku=prod_sku,
                    vendor_name=vendor_name,
                    sent_to_vendor_flag=sent_to_vendor_flag,
                    sent_to_vendor_date_min=sent_to_vendor_date_min,
                    sent_to_vendor_date_max=sent_to_vendor_date_max,
                    country=country,
                    country_code=country_code,
                    location_name=location_name,
                    sent_back_from_vendor_flag=sent_back_from_vendor_flag,
                    sent_back_from_vendor_date_min=sent_back_from_vendor_date_min,
                    sent_back_from_vendor_date_max=sent_back_from_vendor_date_max,
                    received_flag=received_flag,
                    received_back_date_min=received_back_date_min,
                    received_back_date_max=received_back_date_max,
                    tracking_number=tracking_number,
                    is_fulfilled_flag=is_fulfilled_flag,
                    fulfilled_date_min=fulfilled_date_min,
                    fulfilled_date_max=fulfilled_date_max,
                    is_cancelled_flag=is_cancelled_flag,
                    fulfillment_status=fulfillment_status,
                    limit=limit,
                    order_by=order_by
                )
                response.status_code = 200
                return {
                    "code": 200,
                    "message": "Success",
                    "Description": "Generating File"
                }
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get('/get_all_orders', status_code=200)
        async def get_customs_orders_info(response: Response, since_id=None, order_id=None, order_number=None, fields=None, product_name=None, product_variant=None, sorority_flag=None, rush_flag=None, created_at_min=None, created_at_max=None, font=None, prod_sku=None, vendor_name=None, sent_to_vendor_flag=None, sent_to_vendor_date_min=None, sent_to_vendor_date_max=None, country=None, country_code=None, location_name=None, sent_back_from_vendor_flag=None, sent_back_from_vendor_date_min=None, sent_back_from_vendor_date_max=None, received_flag=None, received_back_date_min=None, received_back_date_max=None, tracking_number=None, is_fulfilled_flag=None, fulfilled_date_min=None, fulfilled_date_max=None, is_cancelled_flag=None, fulfillment_status=None, limit="50", order_by="desc"):
            try:
                return_flag, response_json, status_code = self.customs.get_list_of_custom_orders (
                    since_id=since_id,
                    order_id=order_id,
                    order_number=order_number,
                    fields=fields,
                    product_name=product_name,
                    product_variant=product_variant,
                    is_sorority_flag=sorority_flag,
                    is_rush_flag=rush_flag,
                    created_at_min=created_at_min,
                    created_at_max=created_at_max,
                    font=font,
                    prod_sku=prod_sku,
                    vendor_name=vendor_name,
                    sent_to_vendor_flag=sent_to_vendor_flag,
                    sent_to_vendor_date_min=sent_to_vendor_date_min,
                    sent_to_vendor_date_max=sent_to_vendor_date_max,
                    country=country,
                    country_code=country_code,
                    location_name=location_name,
                    sent_back_from_vendor_flag=sent_back_from_vendor_flag,
                    sent_back_from_vendor_date_min=sent_back_from_vendor_date_min,
                    sent_back_from_vendor_date_max=sent_back_from_vendor_date_max,
                    received_flag=received_flag,
                    received_back_date_min=received_back_date_min,
                    received_back_date_max=received_back_date_max,
                    tracking_number=tracking_number,
                    is_fulfilled_flag=is_fulfilled_flag,
                    fulfilled_date_min=fulfilled_date_min,
                    fulfilled_date_max=fulfilled_date_max,
                    is_cancelled_flag=is_cancelled_flag,
                    fulfillment_status=fulfillment_status,
                    limit=limit,
                    order_by=order_by
                )
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_json)
                else:
                    return { "orders": response_json }
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get('/get_order/{order_id}', status_code=200)
        async def get_customs_orders_info(response: Response, order_id, fields=None):
            try:
                return_flag, response_json, status_code = self.customs.get_specific_custom_order (
                    order_id=order_id,
                    fields=fields
                )
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_json)
                else:
                    return response_json
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get('/get_reports', status_code=200)
        async def get_reports(response: Response):
            try:
                return_flag, status_code, response_json = self.customs.get_customs_analytics ()
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_json)
                else:
                    return response_json
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get('/font/get_all', status_code=200)
        async def get_all_custom_font(response: Response):
            try:
                return_flag, status_code, response_json = self.customs.get_all_custom_prod_and_var()
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_json)
                else:
                    return response_json
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post('/font/insert', status_code=200)
        async def insert_custom_font(custom_font_json: dict, response: Response):
            if custom_font_json is None:
                raise HTTPException(status_code=400, detail='Empty JSON.')
            else:
                try:
                    is_upserted_success, status_code, response_description = self.customs.add_custom_font(custom_font_json=custom_font_json)

                    if not is_upserted_success:
                        raise HTTPException(status_code=status_code, detail=response_description)
                    else:
                        return { "is_inserted_success": is_upserted_success, "description": response_description }
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

        @self.router.put('/font/update/{product_id}', status_code=200)
        async def update_custom_font(product_id: str, custom_font_json: dict, response: Response):
            if custom_font_json is None:
                raise HTTPException(status_code=400, detail='Empty JSON.')
            else:
                try:
                    is_upserted_success, status_code, response_description = self.customs.upd_custom_font(product_id=product_id, custom_font_json=custom_font_json)

                    if not is_upserted_success:
                        raise HTTPException(status_code=status_code, detail=response_description)
                    else:
                        return { "is_updated_success": is_upserted_success, "description": response_description }
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

        @self.router.delete('/font/delete/{product_id}', status_code=200)
        async def delete_custom_font(product_id: str, response: Response, variant_name: str = None):
            try:
                is_deleted_success, status_code, total_deleted, total_variant_deleted, response_description = self.customs.delete_custom_font(product_id=product_id)
                response.status_code = status_code
                if not is_deleted_success:
                    raise HTTPException(status_code=status_code, detail=response_description)
                else:
                    return {
                        "Deleted Flag": is_deleted_success,
                        "Total deleted": total_deleted,
                        "Total Variant deleted": total_variant_deleted,
                        "Error Message": response_description
                    }
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @self.router.delete('/font/delete/{product_id}/variant/{title}', status_code=200)
        async def delete_custom_font_variant(product_id: str, title: str, response: Response, variant_name: str = None):
            try:
                is_deleted_success, total_deleted, response_description, status_code = self.customs.delete_custom_variant_font(product_id=product_id, title=title)
                response.status_code = status_code
                if not is_deleted_success:
                    raise HTTPException(status_code=status_code, detail=response_description)
                else:
                    return { "Deleted Flag": is_deleted_success, "Total deleted": total_deleted, "Error Message": response_description }
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @self.router.get('/get_filters', status_code=200)
        async def get_filters(response: Response):
            try:
                return_flag, status_code, response_json = self.customs.get_customs_filters()
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_json)
                else:
                    return response_json
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))
