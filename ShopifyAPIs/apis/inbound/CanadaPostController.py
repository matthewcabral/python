from utils.UtilsController import *
from utils.LogsController import *

class CanadaPostController:

    def __init__(self):
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "CanadaPostController"

    def get_module_name(self):
        return self.module_name

    def post_canadapost_Get_Tracking_Summary(self, tracking_number):
        response_description = "Success"

        if tracking_number is None or tracking_number == []:
            return False, self.utils.get_error_message(400), "[ERROR] No TrackingNumber provided.", 400
        else:
            endpoint = f'https://soa-gw.canadapost.ca/vis/track/pin/{tracking_number}/summary'
            headers = {
                'Content-Type': 'application/json',
                'Authorization': self.utils.get_CANADA_POST_AUTHORIZATION(),
                'Accept-language': 'en-CA'
            }

            try:
                res = requests.get(url=endpoint, headers=headers)
                summary = res.content
                res.raise_for_status()
            except Exception as e:
                try:
                    code = res.status_code
                except:
                    code = 500

                error_message = str(e)
                additional_details = f'[INFO] endpoint: {endpoint}'
                additional_details += f'\n[INFO] headers: {headers}'
                additional_details += f'\n[INFO] res: {res}'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_canadapost_Get_Tracking_Summary", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))

                response_description = self.utils.get_error_message(code)
                error = res.content
                summary = None
                
                if response_description == False:
                    response_description = str(e)

                return False, error, f"[ERROR] Could not get order status. ({response_description})", code
            finally:
                del endpoint
                del headers

            return True, summary, response_description, res.status_code

    def post_canadapost_Get_Tracking_Details(self, tracking_number):
        response_description = "Success"

        if tracking_number is None or tracking_number == []:
            return False, self.utils.get_error_message(400), "[ERROR] No TrackingNumber provided.", 400
        else:
            endpoint = f'https://soa-gw.canadapost.ca/vis/track/pin/{tracking_number}/detail'
            headers = {
                'Content-Type': 'application/json',
                'Authorization': self.utils.get_CANADA_POST_AUTHORIZATION(),
                'Accept-language': 'en-CA'
            }

            try:
                res = requests.get(url=endpoint, headers=headers)
                detail = res.content
                res.raise_for_status()
            except Exception as e:
                try:
                    code = res.status_code
                except:
                    code = 500

                error_message = str(e)
                additional_details = f'[INFO] endpoint: {endpoint}'
                additional_details += f'\n[INFO] headers: {headers}'
                additional_details += f'\n[INFO] res: {res}'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_canadapost_Get_Tracking_Details", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))

                response_description = self.utils.get_error_message(code)
                error = res.content
                detail = None

                if response_description == False:
                    response_description = str(e)

                return False, error, f"[ERROR] Could not get order status. ({response_description})", code
            finally:
                del endpoint
                del headers

            return True, detail, response_description, res.status_code