from locations.LocationsController import *
from utils.UtilsController import *
from utils.LogsController import *

class LocationsRouter:
    def __init__(self):
        self.locations = LocationsController()
        self.utils = UtilsController()
        self.log = LogsController()
        self.router = APIRouter()
        self.setup_routes()

    def setup_routes(self):
        @self.router.get('/get_all', status_code=200)
        async def get_all(response: Response):
            try:
                return_flag, status_code, response_json = self.locations.get_all_locations_api()
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

        @self.router.get('/get_all_active', status_code=200)
        async def get_all_active(response: Response):
            try:
                return_flag, status_code, response_json = self.locations.get_all_active_locations_api()
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

        @self.router.get('/get_specific_location/{code}', status_code=200)
        async def get_specific(code, response: Response):
            try:
                if code is None or code == "":
                    raise HTTPException(status_code=400, detail="Location Code is required.")

                return_flag, status_code, response_json = self.locations.get_specific_location_api(code)
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
        async def insert_location(response: Response, request_data: dict):
            result_flag = False
            return_code = 200
            result_string = "Success"
            row_count = 0
            try:
                result_flag, return_code, row_count, result_string = self.locations.add_location(location_json=request_data)

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
                print(f"[ERROR] Failed Insert Location. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.put('/update/{id}', status_code=200)
        async def update_location(id, response: Response, request_data: dict):
            result_flag = False
            return_code = 200
            result_string = "Success"
            row_count = 0
            try:
                if id is None or id == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail="Location Code is required.")

                result_flag, return_code, row_count, result_string = self.locations.edit_location(id=id, location_json=request_data)

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
                print(f"[ERROR] Failed to update Location. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.delete('/delete/{id}', status_code=200)
        async def delete_location(id, response: Response):
            try:
                if id is None or id == "":
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail="Location ID is required.")

                result_flag, return_code, row_count, result_string = self.locations.delete_location(id=id)

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
                print(f"[ERROR] Failed to delete Location. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))