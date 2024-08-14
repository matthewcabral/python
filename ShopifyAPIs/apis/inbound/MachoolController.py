from utils.UtilsController import *
from utils.LogsController import *

class MachoolController:

    def __init__(self):
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "MachoolController"

    def get_module_name(self):
        return self.module_name

    # Machool APIs
    def post_machool_rates(self, recipientAddress):
        response_description = "Success"

        if recipientAddress is None or recipientAddress == {}:
            return False, self.utils.get_error_message(400), "[ERROR] No recipient address provided.", 400
        else:
            endpoint = "https://api.machool.com/external/api/v1/rates"
            headers = {
                'accept': 'application/json',
                'x-key-id': self.utils.get_MACHOOL_PUB_KEY(),
                'x-api-key': self.utils.get_MACHOOL_PRIV_KEY(),
                'Content-Type': 'application/json',
            }
            recipientAddress = {
                "line1": recipientAddress.get("address1", ""),
                "line2": recipientAddress.get("address2", ""),
                "city": recipientAddress.get("city", ""),
                "province": recipientAddress.get("province_code", ""),
                "postalCode": recipientAddress.get("zip", ""),
                "country": recipientAddress.get("country_code", ""),
            }
            request_body = {
                "senderAddress":{
                    "name": "COMPANY NAME",
                    "company": "COMPANY NAME",
                    "line1": "XXXXXXXXXXXXXX",
                    "city": "XXXXXXXXXXXXXX",
                    "province": "XXXXXXXXXXXXXX",
                    "postalCode": "XXXXXXXXXXXXXX",
                    "country": "XXXXXXXXXXXXXX",
                    "phone": "XXXXXXXXXXXXXX",
                    "email": "XXXXXXXXXXXXXX@company_name.com",
                },
                "recipientAddress": recipientAddress,
                "measurementSystem": "metric",
                "parcels": [{
                    "quantity": 1,
                    "weight": 0.05,
                    "length": 15.24,
                    "width": 22.86,
                    "height": 1.02
                }],
                "providers": ["canadapost", "uniuni", "rivo"],
            }
            json_payload = json.dumps(request_body)

            try:
                res = requests.post(url=endpoint, headers=headers, data=json_payload)
                rates = res.json().get('rates')
            except Exception as e:
                try:
                    code = res.status_code
                except:
                    code = 500

                error_message = str(e)
                additional_details = f'[INFO] request_body: {request_body}'
                additional_details += f'\n[INFO] endpoint: {endpoint}'
                additional_details += f'\n[INFO] headers: {headers}'
                additional_details += f'\n[INFO] res: {res}'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_machool_rates", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))

                response_description = self.utils.get_error_message(code)
                rates = None

                if response_description == False:
                    response_description = str(e)

                return False, response_description, f"[ERROR] Could not get rates. ({response_description})", code
            finally:
                del endpoint
                del headers
                del request_body
                del json_payload

            return True, rates, response_description, res.status_code

    # fazer o do create shipments