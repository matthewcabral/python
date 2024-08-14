from system.FontsController import *
from utils.UtilsController import *
from utils.LogsController import *

class FontsRouter:
    def __init__(self):
        self.fonts = FontsController()
        self.utils = UtilsController()
        self.log = LogsController()
        self.router = APIRouter()
        self.setup_routes()

    def setup_routes(self):
        @self.router.post('/upload_file', status_code=200)
        async def upload_font_file(file: UploadFile):
            try:
                file_path = self.utils.get_base_directory() + "/files/fonts/"
                file_content = await file.read()

                if self.utils.upload_file(file_path + file.filename, file_content):
                    print(f"[INFO] File '{file.filename}' uploaded successfully")
                    # result_flag, result_file_name, result_url, result_error = self.fonts.upload_font_to_s3(file_path + file.filename, file.filename)
                    # background_tasks.add_task(self.customs.process_file_from_lee, file_path=file_path, file_name=file.filename)
                    return {
                        "message": f"File '{file.filename}' uploaded successfully",
                        "file name": file.filename,
                        "file path": file_path
                    }
                else:
                    print(f"[ERROR] Failed to upload file '{file.filename}'")
                    raise HTTPException(status_code=500, detail="Failed to upload file")
            except Exception as e:
                print(f"[ERROR] Failed to upload file '{file.filename}'. Error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post('/insert', status_code=200)
        async def insert_font(response: Response, request_data: dict):
            result_flag = False
            return_code = 200
            result_string = "Success"
            try:
                result_flag, return_code, result_string = self.fonts.upsert_font(font_id=None, font_json=request_data, function="insert")

                response.status_code = return_code
                if not result_flag:
                    raise HTTPException(status_code=return_code, detail=result_string)
                else:
                    return {
                        "code": return_code,
                        "message": result_string
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.put('/update/{font_id}', status_code=200)
        async def update_font(font_id: str, response: Response, request_data: dict):
            result_flag = False
            return_code = 200
            result_string = "Success"
            try:
                if not font_id or font_id == "":
                    raise HTTPException(status_code=400, detail="Font ID is required")

                result_flag, return_code, result_string = self.fonts.upsert_font(font_id=font_id, font_json=request_data, function="update")

                response.status_code = return_code
                if not result_flag:
                    raise HTTPException(status_code=return_code, detail=result_string)
                else:
                    return {
                        "code": return_code,
                        "message": result_string
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.delete('/delete/{font_id}', status_code=200)
        async def delete_font(font_id: str, response: Response):
            try:
                is_deleted_success, status_code, total_deleted, response_description = self.fonts.delete_font(font_id)
                response.status_code = status_code
                if not is_deleted_success:
                    raise HTTPException(status_code=status_code, detail=response_description)
                else:
                    return { "Deleted Flag": is_deleted_success, "Total deleted": total_deleted, "Message": response_description }
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @self.router.get('/get_all', status_code=200)
        async def get_all_fonts(response: Response):
            try:
                return_flag, status_code, response_json = self.fonts.get_all_fonts()
                if not return_flag:
                    raise HTTPException(status_code=status_code, detail=response_json)
                else:
                    # response_json = json.loads(response_json)
                    return response_json
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))
