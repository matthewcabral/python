from vendors.VendorsController import *
from utils.UtilsController import *
from utils.LogsController import *

class VendorsRouter:
    def __init__(self):
        self.vendors = VendorsController()
        self.utils = UtilsController()
        self.log = LogsController()
        self.router = APIRouter()
        self.setup_routes()

    def setup_routes(self):
        @self.router.get('/get_all', status_code=200)
        async def get_all(response: Response):
            try:
                return_flag, status_code, response_json = self.vendors.get_all_vendors_api()
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

        @self.router.get('/get_specific_vendor/{vendor_name}', status_code=200)
        async def get_specific(vendor_name, response: Response):
            try:
                if vendor_name is None or vendor_name == "":
                    raise HTTPException(status_code=400, detail="Vendor name is required.")

                return_flag, status_code, response_json = self.vendors.get_specific_vendor_api(vendor_name)
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

        @self.router.post('/insert', status_code=200)
        async def insert_vendor(response: Response, request_data: dict):
            result_flag = False
            return_code = 200
            result_string = "Success"
            row_count = 0
            try:
                result_flag, return_code, row_count, result_string = self.vendors.add_vendor(vendor_json=request_data)

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
                print(f"[ERROR] Failed Insert Vendor. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.put('/update/{vendor_id}', status_code=200)
        async def update_vendor(vendor_id, response: Response, request_data: dict):
            result_flag = False
            return_code = 200
            result_string = "Success"
            row_count = 0
            try:
                if vendor_id is None or vendor_id == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail="Vendor ID is required.")

                result_flag, return_code, row_count, result_string = self.vendors.edit_vendor(row_id=vendor_id, vendor_json=request_data)

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
                print(f"[ERROR] Failed to update Vendor. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.delete('/delete/{vendor_id}', status_code=200)
        async def delete_vendor(vendor_id, response: Response):
            try:
                if vendor_id is None or vendor_id == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail="Vendor ID is required.")

                result_flag, return_code, row_count, result_string = self.vendors.delete_vendor(row_id=vendor_id)

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
                print(f"[ERROR] Failed to delete Vendor. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))