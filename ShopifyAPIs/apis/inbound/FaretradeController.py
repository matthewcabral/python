from utils.UtilsController import *
from utils.LogsController import *

class FaretradeController:

    def __init__(self):
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "FaretradeController"

    def get_module_name(self):
        return self.module_name

    def post_faretrade_Rate_V1(self, carrier_id, shipping_address, currency_code):
        currencyCode = "CAD" if currency_code is None or "" else currency_code
        carrierId = 68 if carrier_id is None or "" else carrier_id

        response_description = "Success"

        if shipping_address is None or shipping_address == {}:
            return False, self.utils.get_error_message(400), "[ERROR] No recipient address provided.", 400
        else:
            endpoint = "https://atlasapi.2ship.com/api/Rate_V1"
            headers = {
                'Content-Type': 'application/json',
            }
            sender = {
                "Id": "Default CA",
                "Country": "XX",
                "State": "XX",
                "City": "XXX",
                "PostalCode": "XXX XXX",
                "Address1": "XX XXXXX XXXX.",
                "CompanyName": "XXXXX XXXXXX",
                "IsResidential": False
            }
            recipient = {
                "line1": shipping_address.get("address1", ""),
                "line2": shipping_address.get("address2", ""),
                "city": shipping_address.get("city", ""),
                "province": shipping_address.get("province_code", ""),
                "postalCode": shipping_address.get("zip", ""),
                "country": shipping_address.get("country_code", ""),
            }
            request_body = {
                "WS_Key": self.utils.get_FARETRADE_API_KEY(),
                "CarrierId": carrierId,
                "Sender": sender,
                "Recipient": recipient,
                "Packages": [
                    {
                        "Weight": 0.05
                    }
                ],
                "RateCurrencySelect": currencyCode
            }

            try:
                res = requests.post(url=endpoint, headers=headers, json=request_body)
                services = res.json()[0].get('Services')
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
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_faretrade_Rate_V1", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))

                response_description = self.utils.get_error_message(code)
                error = res.json()
                services = None
                
                if response_description == False:
                    response_description = str(e)

                return False, error, f"[ERROR] Could not get services. ({response_description})", code
            finally:
                del endpoint
                del headers
                del request_body

            return True, services, response_description, res.status_code

    def post_faretrade_Tracking_V1(self, order_number, tracking_number, shipment_id, find_by, start_date, end_date):
        response_description = "Success"
        findByOptions = {
            "ByTrackingNumber": 0,
            "ByOrderNumber": 1,
            "ByShipmentID": 2
        }
        findBy = 1

        if find_by is None or "":
            return False, self.utils.get_error_message(400), "[ERROR] No FindBy provided.", 400
        else:
            if find_by not in findByOptions:
                return False, self.utils.get_error_message(404), "[ERROR] Invalid FindBy provided.", 404
            else:
                findBy = findByOptions[find_by]
        
                if find_by == "ByOrderNumber":
                    if order_number is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No OrderNumber provided.", 400
                elif find_by == "ByTrackingNumber":
                    if tracking_number is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No TrackingNumber provided.", 400
                else:
                    if shipment_id is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No ShipmentId provided.", 400

        if start_date is None or "":
            return False, self.utils.get_error_message(400), "[ERROR] No StartDate provided.", 400

        if end_date is None or "":
            return False, self.utils.get_error_message(400), "[ERROR] No EndDate provided.", 400

        endpoint = 'https://atlasapi.2ship.com/api/Tracking_V1'
        headers = {'Content-Type': 'application/json'}
        request_body = {
            "WS_Key": self.utils.get_FARETRADE_API_KEY(),
            "FindBy": find_by,
            "StartDate": start_date,
            "EndDate": end_date
        }

        if order_number is not None or "":
            request_body["OrderNumber"] = order_number

        if tracking_number is not None or "":
            request_body["TrackingNumber"] = tracking_number

        if shipment_id is not None or "":
            request_body["ShipmentId"] = shipment_id

        try:
            res = requests.post(url=endpoint, headers=headers, json=request_body)
            trackingResponse = res.json()
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
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_faretrade_Tracking_V1", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))

            response_description = self.utils.get_error_message(code)
            error = res.json()
            trackingResponse = None

            if response_description == False:
                response_description = str(e)

            return False, error, f"[ERROR] Could not get tracking. ({response_description})", code
        finally:
            del findByOptions
            del findBy
            del endpoint
            del headers
            del request_body

        return True, trackingResponse, response_description, res.status_code

    def post_faretrade_GetOrderStatus(self, order_number_array):
        response_description = "Success"

        if order_number_array is None or order_number_array == []:
            return False, self.utils.get_error_message(400), "[ERROR] No OrderNumberArray provided.", 400
        else:
            endpoint = 'https://atlasapi.2ship.com/api/GetOrdersStatus'
            headers = {'Content-Type': 'application/json'}
            request_body = {
                "WS_Key": self.utils.get_FARETRADE_API_KEY(),
                "OrderNumbers": order_number_array
            }

            try:
                res = requests.post(url=endpoint, headers=headers, json=request_body)
                orderStatusResponse = res.json()
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
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_faretrade_GetOrderStatus", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))

                response_description = self.utils.get_error_message(code)
                error = res.json()
                orderStatusResponse = None

                if response_description == False:
                    response_description = str(e)

                return False, error, f"[ERROR] Could not get order status. ({response_description})", code
            finally:
                del endpoint
                del headers
                del request_body

            return True, orderStatusResponse, response_description, res.status_code

    def post_faretrade_GetShipments_V1(self, type, order_number, shipment_ids, tracking_number, carrier_id, date_start, date_end, reference, po_number, retrieveBase64StringDocuments):
        response_description = "Success"

        if type is None or "":
            return False, self.utils.get_error_message(400), "[ERROR] No Type provided.", 400
        else:
            typeOptions = {
                "ByIds": 0,
                "ByDate": 1,
                "ByCarrierId": 2,
                "ByTrackingNumber": 3,
                "ByReference": 4,
                "ByPONumber": 5,
                "ByOrderNumber": 6,
                "ByDateAndAggregation": 7,
                "ByDateAndCarrierAndAggregation": 8,
                "ByTransactionDate": 9,
                "ByLabelGenerationDate": 10,
                "ByStatusChangedDate": 11,
                "ByStatusChangedDate2": 12,
            }

            if type not in typeOptions:
                return False, self.utils.get_error_message(404), "[ERROR] Invalid Type provided.", 404
            else:
                type = typeOptions[type]

                if type == "ByIds":
                    if shipment_ids is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No ShipmentIds provided.", 400
                if type == "ByTrackingNumber":
                    if tracking_number is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No TrackingNumber provided.", 400
                if type == "ByCarrierId":
                    if carrier_id is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No CarrierId provided.", 400
                if type == "ByDate" or type == "ByDateAndAggregation" or type == "ByTransactionDate" or type == "ByLabelGenerationDate" or type == "ByStatusChangedDate" or type == "ByStatusChangedDate2":
                    if date_start is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No DateStart provided", 400
                    if date_end is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No DateEnd provided.", 400
                if type == "ByReference":
                    if reference is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No Reference provided.", 400
                if type == "ByPONumber":
                    if po_number is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No PONumber provided.", 400
                if type == "ByOrderNumber":
                    if order_number is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No OrderNumber provided.", 400
                if type == "ByDateAndCarrierAndAggregation":
                    if date_start is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No DateStart provided.", 400
                    if date_end is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No DateEnd provided.", 400
                    if carrier_id is None or "":
                        return False, self.utils.get_error_message(400), "[ERROR] No CarrierId provided.", 400

        retrieveBase64StringDocuments = True if retrieveBase64StringDocuments is None or "" else retrieveBase64StringDocuments

        endpoint = 'https://atlasapi.2ship.com/api/GetShipments_V1'
        headers = {'Content-Type': 'application/json'}
        request_body = {
            "WS_Key": self.utils.get_FARETRADE_API_KEY(),
            "Type": type,
            "RetrieveBase64StringDocuments": retrieveBase64StringDocuments
        }

        if shipment_ids is not None or "":
            request_body["ShipmentIds"] = shipment_ids
        if date_start is not None or "":
            request_body["DateStart"] = date_start
        if date_end is not None or "":
            request_body["DateEnd"] = date_end
        if carrier_id is not None or "":
            request_body["CarrierId"] = carrier_id
        if tracking_number is not None or "":
            request_body["TrackingNumber"] = tracking_number
        if reference is not None or "":
            request_body["Reference"] = reference
        if po_number is not None or "":
            request_body["PONumber"] = po_number
        if order_number is not None or "":
            request_body["OrderNumber"] = order_number

        try:
            res = requests.post(url=endpoint, headers=headers, json=request_body)
            shipmentsResponse = res.json()
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
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_faretrade_GetShipments_V1", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))

            response_description = self.utils.get_error_message(code)
            error = res.json()
            shipmentsResponse = None

            if response_description == False:
                response_description = str(e)

            return False, error, f"[ERROR] Could not get shipments. ({response_description})", code
        finally:
            del endpoint
            del headers
            del request_body

        return True, shipmentsResponse, response_description, res.status_code

    # NOT TESTED
    def post_faretrade_Ship_V1(self, order_number, shipping_address, service_group_id, service_group_name, commodities_body):
        response_description = "Success"

        if order_number is None or order_number == "":
            return False, self.utils.get_error_message(400), "[ERROR] No order provided.", 400
        elif shipping_address is None or shipping_address == {}:
            return False, self.utils.get_error_message(400), "[ERROR] No recipient provided.", 400
        else:
            endpoint = 'https://atlasapi.2ship.com/api/Ship_V1'
            headers = {'Content-Type': 'application/json'}
            sender = {
                "Id": "Default CA",
                "CompanyName": "XXXX XXXXXX",
                "Country": "XX",
                "State": "XX",
                "City": "XXXXXX-XXXXXX",
                "PostalCode": "XXX XXX",
                "Address1": "XX XXXXXXX XXXXXX.",
                "IsResidential": False
            }
            recipient = {
                "PersonName": shipping_address.get("name"),
                "Country": shipping_address.get("country_code"),
                "State": shipping_address.get("province_code"),
                "City": shipping_address.get("city"),
                "PostalCode": shipping_address.get("zip"),
                "Address1": shipping_address.get("address1"),
                "Address2": shipping_address.get("address2"),
                "Telephone": shipping_address.get("phone"),
                "IsResidential": True
            }
            shipByServiceGroupOptions = {
                "ServiceGroupID": service_group_id,
                "ServiceGroupName": service_group_name,
                "GetServiceByGroupType": 0
            }
            contents = {
                "Commodities": commodities_body
            }
            request_body = {
                "WS_Key": self.utils.get_FARETRADE_API_KEY(),
                "OrderNumber": order_number,
                "ShipmentReference": order_number,
                "Sender": sender,
                "Recipient": recipient,
                "Contents": contents,
                "Packages": [
                    {
                        "Weight": 0.05
                    }
                ],
                "RetrieveBase64StringDocuments": True,
                "ShipByServiceGroup": True,
                "ShipByServiceGroupOptions": shipByServiceGroupOptions
            }

            try:
                res = requests.post(url=endpoint, headers=headers, json=request_body)
                faretradeResponse = res.json()
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
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_faretrade_Ship_V1", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))

                response_description = self.utils.get_error_message(code)
                error = res.json()
                faretradeResponse = None

                if response_description == False:
                    response_description = str(e)

                return False, error, f"[ERROR] Could not ship. ({response_description})", code
            finally:
                del endpoint
                del headers
                del request_body

            return True, faretradeResponse, response_description, res.status_code

    # NOT TESTED
    def post_faretrade_Close_V1(self, carrier_id, location_id, customer_reference, shipping_point_id, container_id):
        response_description = "Success"

        if carrier_id is None or "":
            return False, self.utils.get_error_message(400), "[ERROR] No CarrierId provided.", 400
        if location_id is None or "":
            return False, self.utils.get_error_message(400), "[ERROR] No LocationId provided.", 400

        endpoint = 'https://atlasapi.2ship.com/api/Close_V1'
        headers = {'Content-Type': 'application/json'}
        request_body = {
            "WS_Key": self.utils.get_FARETRADE_API_KEY(),
            "CarrierId": carrier_id,
            "LocationId": location_id
        }

        if customer_reference is not None or "":
            request_body["CustomerReference"] = customer_reference
        if shipping_point_id is not None or "":
            request_body["ShippingPointId"] = shipping_point_id
        if container_id is not None or "":
            request_body["ContainerID"] = container_id

        try:
            res = requests.post(url=endpoint, headers=headers, json=request_body)
            closeResponse = res.json()
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
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_faretrade_Close_V1", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(5))

            response_description = self.utils.get_error_message(code)
            error = res.json()
            closeResponse = None

            if response_description == False:
                response_description = str(e)
                
            return False, error, f"[ERROR] Could not close. ({response_description})", code
        finally:
            del endpoint
            del headers
            del request_body

        return True, closeResponse, response_description, res.status_code
