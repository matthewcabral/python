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

    def post_canadapost_close_manifests(self):
        # Variables
        res = None
        manifest_link = None
        logs_response = None
        response_description = "Success"
        status_code = 200
        error_message = None
        additional_details = None
        today = self.utils.get_current_date()
        yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')

        url = "https://soa-gw.canadapost.ca/rs/4015337/4015337/manifest"

        headers = {
            'Accept': 'application/vnd.cpc.manifest-v8+xml',
            'Content-Type': 'application/vnd.cpc.manifest-v8+xml',
            'Accept-language': 'en-CA',
            'Authorization': self.utils.get_CANADA_POST_AUTHORIZATION(),
            'Cookie': 'XXXXXXXXXXXXXX'
        }

        group_ids = f"""
        <group-id>{yesterday}</group-id>
        <group-id>{today}</group-id>
        """

        payload = f"""
        <transmit-set xmlns="http://www.canadapost.ca/ws/manifest-v8">
            <group-ids>
                {group_ids}
            </group-ids>
            <requested-shipping-point>XXXXXXXXXXXXXX</requested-shipping-point>
            <cpc-pickup-indicator>true</cpc-pickup-indicator>
            <detailed-manifests>true</detailed-manifests>
            <method-of-payment>Account</method-of-payment>
            <manifest-address>
                <manifest-company>COMPANY NAME</manifest-company>
                <manifest-name>CANADA POST MANIFESTING</manifest-name>
                <phone-number>XXXXXXXXXXXXXX</phone-number>
                <address-details>
                    <address-line-XXXXXXXXXXXXXX</address-line-1>
                    <city>XXXXXXXXXXXXXX</city>
                    <prov-state>XXXXXXXXXXXXXX</prov-state>
                    <postal-zip-code>XXXXXXXXXXXXXX</postal-zip-code>
                </address-details>
            </manifest-address>
        </transmit-set>"""

        try:
            res = requests.request("POST", url, headers=headers, data=payload)
            manifest_link = res.text.split('href="')[1].split('"')[0]
            logs_response = res.text
            res.raise_for_status()
            status_code = res.status_code

            return True, manifest_link, logs_response, response_description, status_code
        except Exception as e:
            try:
                status_code = res.status_code
            except:
                status_code = 500

            error_message = str(e)
            additional_details = f'[INFO] url: {url}'
            additional_details += f'\n[INFO] headers: {headers}'
            additional_details += f'\n[INFO] payload: {payload}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_canadapost_close_manifests", error_code=status_code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))

            response_description = self.utils.get_error_message(status_code)
            error = res.content

            if response_description == False:
                response_description = str(e)

            return False, error, f"[ERROR] Could not close manifests. ({response_description})", status_code
        finally:
            print("[INFO] Cleaning up variables.")
            try:
                del res
                del manifest_link
                del logs_response
                del response_description
                del status_code
                del error_message
                del additional_details
                del today
                del yesterday
                del url
                del headers
                del group_ids
                del payload
            except:
                pass

    def get_canadapost_artifact_link(self, manifest_url):
        # Variables
        res = None
        response_description = "Success"
        status_code = 200
        error_message = None
        artifact_link = None
        logs_response = None
        additional_details = None
        error = None

        headers = {
            'Accept': 'application/vnd.cpc.manifest-v8+xml',
            'Content-Type': 'application/vnd.cpc.manifest-v8+xml',
            'Accept-language': 'en-CA',
            'Authorization': self.utils.get_CANADA_POST_AUTHORIZATION(),
            'Cookie': 'XXXXXXXXXXXXXX'
        }

        try:
            res = requests.request("GET", manifest_url, headers=headers)
            artifact_link = res.text.split('rel="artifact"')[1].split('href="')[1].split('"')[0]
            logs_response = res.text
            print(f'[INFO] - Artifact link: {artifact_link}')
            return True, artifact_link, res, logs_response, response_description, status_code, error_message
        except Exception as e:
            try:
                status_code = res.status_code
            except:
                status_code = 500

            error_message = str(e)
            error = res.content

            additional_details = f'[INFO] url: {manifest_url}'
            additional_details += f'\n[INFO] headers: {headers}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_canadapost_close_manifests", error_code=status_code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))
            print(f'[ERROR] - Error getting artifact link: {e}')

            response_description = self.utils.get_error_message(status_code)
            if response_description == False:
                response_description = str(e)

            return False, artifact_link, error, logs_response, response_description, status_code, error_message
        finally:
            print("[INFO] Cleaning up variables.")
            try:
                del res
                del response_description
                del status_code
                del error_message
                del artifact_link
                del logs_response
                del additional_details
                del error
                del headers
            except:
                pass

    def get_canadapost_download_manifest_pdf(self, artifact_link):
        # Variables
        res = None
        status_code = 200
        error_message = None
        response_description = "Success"
        file = f'{self.utils.get_base_directory()}/files/pdfs/manifests/{self.utils.get_current_date()}.pdf'
        headers = {
            'Accept': 'application/pdf',
            'Accept-language': 'en-CA',
            'Authorization': self.utils.get_CANADA_POST_AUTHORIZATION(),
            'Cookie': 'XXXXXXXXXXXXXX'
        }

        try:
            res = requests.request("GET", artifact_link, headers=headers)
            with open(file, 'wb') as f:
                f.write(res.content)
            response_description = f"Manifest pdf downloaded successfully"
            return True, res, response_description, error_message, status_code
        except Exception as e:
            try:
                status_code = res.status_code
            except:
                status_code = 500

            error_message = str(e)
            response_description = res.content

            return False, res, response_description, error_message, status_code
        finally:
            print("[INFO] Cleaning up variables.")
            try:
                del res
                del status_code
                del error_message
                del file
                del headers
            except:
                pass

    def get_canadapost_pdf_label(self, order_id, label_url):
        # Variables
        res = None
        status_code = 200
        error_message = None
        response_description = "Success"
        file = f'{self.utils.get_base_directory()}/files/pdfs/labels/{order_id}.pdf'
        headers = {
            'Authorization': self.utils.get_CANADA_POST_AUTHORIZATION(),
            'Cookie': 'XXXXXXXXXXXXXX'
        }

        try:
            res = requests.request("GET", label_url, headers=headers)
            with open(file, 'wb') as f:
                f.write(res.content)
            response_description = f"Label pdf downloaded successfully"
            return True, res, file, response_description, error_message, status_code
        except Exception as e:
            try:
                status_code = res.status_code
            except:
                status_code = 500

            error_message = str(e)
            response_description = res.content

            return False, res, None, response_description, error_message, status_code
        finally:
            print("[INFO] Cleaning up variables.")
            try:
                del res
                del status_code
                del error_message
                del file
                del headers
            except:
                pass

    def post_canadapost_create_shipment(self, order, destination, customs):
        # Variables
        order_id = order.get('order_id')
        order_number = order.get('order_name')
        today = self.utils.get_current_date()
        response_description = "Success"
        status_code = 200
        error_message = None
        endpoint = "https://soa-gw.canadapost.ca/rs/4015337/4015337/shipment"
        headers = {
            'Accept': 'application/vnd.cpc.shipment-v8+xml',
            'Content-Type': 'application/vnd.cpc.shipment-v8+xml',
            'Accept-language': 'en-CA',
            'Authorization': self.utils.get_CANADA_POST_AUTHORIZATION(),
            'Cookie': 'XXXXXXXXXXXXXX'
        }
        payload = f"""
            <?xml version=\"1.0\" encoding=\"UTF-8\"?>
            <shipment xmlns=\"http://www.canadapost.ca/ws/shipment-v8\">
                <group-id>{today}</group-id>
                <requested-shipping-point>XXXXXXXXXXXXXX</requested-shipping-point>
                <cpc-pickup-indicator>true</cpc-pickup-indicator>
                <delivery-spec>
                    <service-code>INT.TP</service-code>
                    <sender>
                        <name>XXXXXXXXXXXXXX</name>
                        <company>COMPANY NAME</company>
                        <contact-phone>XXXXXXXXXXXXXX</contact-phone>
                        <address-details>
                            <address-line-1>XXXXXXXXXXXXXX</address-line-1>
                            <city>XXXXXXXXXXXXXX</city>
                            <prov-state>XXXXXXXXXXXXXX</prov-state>
                            <country-code>XXXXXXXXXXXXXX</country-code>
                            <postal-zip-code>XXXXXXXXXXXXXX</postal-zip-code>
                        </address-details>
                    </sender>
                    {destination}
                    <parcel-characteristics>
                        <weight>0.03</weight>
                        <dimensions>
                            <length>1</length>
                            <width>15</width>
                            <height>21</height>
                        </dimensions>
                    </parcel-characteristics>
                    <print-preferences>
                        <output-format>4x6</output-format>
                        <encoding>PDF</encoding>
                    </print-preferences>
                    <preferences>
                        <show-packing-instructions>true</show-packing-instructions>
                    </preferences>
                    <references>
                        <cost-centre>{order_number}</cost-centre>
                        <customer-ref-1>{order_number}</customer-ref-1>
                    </references>
                    {customs}
                    <settlement-info>
                        <paid-by-customer>XXXXXXXXXXXXXX</paid-by-customer>
                        <contract-id>XXXXXXXXXXXXXX</contract-id>
                        <intended-method-of-payment>Account</intended-method-of-payment>
                    </settlement-info>
                    <options>
                        <option>
                            <option-code>XXXXXXXXXXXXXX</option-code>
                        </option>
                    </options>
                </delivery-spec>
            </shipment>
        """
        payload = payload.replace('&', 'and')
        
        try:
            res = requests.request("POST", endpoint, headers=headers, data=payload.encode('utf-8'))
            res.raise_for_status()
            status_code = res.status_code

            return True, res, response_description, status_code
        except Exception as e:
            try:
                status_code = res.status_code
            except:
                status_code = 500

            error_message = str(e)
            response_description = res.content

            return False, res, response_description, status_code
        finally:
            print("[INFO] Cleaning up variables.")
            try:
                del order_id
                del order_number
                del today
                del response_description
                del status_code
                del error_message
                del endpoint
                del headers
                del payload
            except:
                pass


