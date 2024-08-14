from database.DataController import DataController
from utils.UtilsController import *
from utils.LogsController import *

class FulfillmentsController(DataController):
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "OrderFulfillmentController"

    def get_module_name(self):
        return self.module_name

    class Fulfillment:
        def __init__(self):
            self.id = None
            self.name = None
            self.status = None
            self.receipt = None
            self.service = None
            self.order_id = None
            self.created_at = None
            self.updated_at = None
            self.location_id = None
            self.tracking_url = None
            self.tracking_urls = None
            self.origin_address = None
            self.shipment_status = None
            self.tracking_number = None
            self.tracking_numbers = None
            self.tracking_company = None
            self.admin_graphql_api_id = None

        def get_id(self):
            return self.id

        def set_id(self, id):
            self.id = id

        def get_name(self):
            return self.name

        def set_name(self, name):
            self.name = name

        def get_status(self):
            return self.status

        def set_status(self, status):
            self.status = status

        def get_receipt(self):
            return self.receipt

        def set_receipt(self, receipt):
            self.receipt = receipt

        def get_service(self):
            return self.service

        def set_service(self, service):
            self.service = service

        def get_order_id(self):
            return self.order_id

        def set_order_id(self, order_id):
            self.order_id = order_id

        def get_created_at(self):
            return self.created_at

        def set_created_at(self, created_at):
            self.created_at = created_at

        def get_updated_at(self):
            return self.updated_at

        def set_updated_at(self, updated_at):
            self.updated_at = updated_at

        def get_location_id(self):
            return self.location_id

        def set_location_id(self, location_id):
            self.location_id = location_id

        def get_tracking_url(self):
            return self.tracking_url

        def set_tracking_url(self, tracking_url):
            self.tracking_url = tracking_url

        def get_tracking_urls(self):
            return self.tracking_urls

        def set_tracking_urls(self, tracking_urls):
            self.tracking_urls = tracking_urls

        def get_origin_address(self):
            return self.origin_address

        def set_origin_address(self, origin_address):
            self.origin_address = origin_address

        def get_shipment_status(self):
            return self.shipment_status

        def set_shipment_status(self, shipment_status):
            self.shipment_status = shipment_status

        def get_tracking_number(self):
            return self.tracking_number

        def set_tracking_number(self, tracking_number):
            self.tracking_number = tracking_number

        def get_tracking_numbers(self):
            return self.tracking_numbers

        def set_tracking_numbers(self, tracking_numbers):
            self.tracking_numbers = tracking_numbers

        def get_tracking_company(self):
            return self.tracking_company

        def set_tracking_company(self, tracking_company):
            self.tracking_company = tracking_company

        def get_admin_graphql_api_id(self):
            return self.admin_graphql_api_id

        def set_admin_graphql_api_id(self, admin_graphql_api_id):
            self.admin_graphql_api_id = admin_graphql_api_id

    class FulfillmentLineItem:
        def __init__(self):
            self.fulfillment_id = None
            self.order_id = None
            self.line_item_id = None
            self.product_id = None
            self.variant_id = None
            self.fulfillment_service = None
            self.fulfillment_status = None

        def get_fulfillment_id(self):
            return self.fulfillment_id

        def set_fulfillment_id(self, fulfillment_id):
            self.fulfillment_id = fulfillment_id

        def get_order_id(self):
            return self.order_id

        def set_order_id(self, order_id):
            self.order_id = order_id

        def get_line_item_id(self):
            return self.line_item_id

        def set_line_item_id(self, line_item_id):
            self.line_item_id = line_item_id

        def get_product_id(self):
            return self.product_id

        def set_product_id(self, product_id):
            self.product_id = product_id

        def get_variant_id(self):
            return self.variant_id

        def set_variant_id(self, variant_id):
            self.variant_id = variant_id

        def get_fulfillment_service(self):
            return self.fulfillment_service

        def set_fulfillment_service(self, fulfillment_service):
            self.fulfillment_service = fulfillment_service

        def get_fulfillment_status(self):
            return self.fulfillment_status

        def set_fulfillment_status(self, fulfillment_status):
            self.fulfillment_status = fulfillment_status

    # Database Functions
    def get_all_order_fulfillments(self):
        # print(f"\n[INFO] BEGIN - Getting all order fulfillments")
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        fulfillments = []
        row = None
        fulfillment = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_FULFILLMENTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    fulfillment = {
                        "id": row.get('ID'),
                        "name": row.get('NAME'),
                        "status": row.get('STATUS'),
                        "receipt": row.get('RECEIPT'),
                        "service": row.get('SERVICE'),
                        "order_id": row.get('ORDER_ID'),
                        "created_at": row.get('CREATED_AT'),
                        "updated_at": row.get('UPDATED_AT'),
                        "location_id": row.get('LOCATION_ID'),
                        "tracking_url": row.get('TRACKING_URL'),
                        "tracking_urls": row.get('TRACKING_URLS'),
                        "origin_address": row.get('ORIGIN_ADDRESS'),
                        "shipment_status": row.get('SHIPMENT_STATUS'),
                        "tracking_number": row.get('TRACKING_NUMBER'),
                        "tracking_numbers": row.get('TRACKING_NUMBERS'),
                        "admin_graphql_api_id": row.get('ADMIN_GRAPHQL_API_ID')
                    }
                    fulfillments.append(fulfillment)

            return result_flag, fulfillments, result_query
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all order fulfillments")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, fulfillments, row, fulfillment
            except:
                pass

    def get_all_fulfillment_line_items(self):
        # print(f"\n[INFO] BEGIN - Getting all fulfillment line items")
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        line_items = []
        row = None
        line_item = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_FULFILLMENT_LINE_ITEMS(), columns, condition)

            if result_flag:
                for row in result_query:
                    line_item = {
                        "line_item_id": row.get('LINE_ITEM_ID'),
                        "fulfillment_id": row.get('FULFILLMENT_ID'),
                        "order_id": row.get('ORDER_ID'),
                        "product_id": row.get('PRODUCT_ID'),
                        "variant_id": row.get('VARIANT_ID')
                    }
                    line_items.append(line_item)

            return result_flag, line_items, result_query
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all fulfillment line items")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, line_items, row, line_item
            except:
                pass

    def get_specific_order_fulfillment(self, id, order_id):
        # print(f"\n[INFO] BEGIN - Getting specific order fulfillment")
        columns = ["*"]
        condition = "1=1"
        condition += f"\nAND ID = '{id}'" if id is not None and id != "" else ""
        condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
        result_flag = False
        result_query = None
        fulfillment = {}
        fulfillments = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_FULFILLMENTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    fulfillment = {
                        "id": row.get('ID'),
                        "name": row.get('NAME'),
                        "status": row.get('STATUS'),
                        "receipt": row.get('RECEIPT'),
                        "service": row.get('SERVICE'),
                        "order_id": row.get('ORDER_ID'),
                        "created_at": row.get('CREATED_AT'),
                        "updated_at": row.get('UPDATED_AT'),
                        "location_id": row.get('LOCATION_ID'),
                        "tracking_url": row.get('TRACKING_URL'),
                        "tracking_urls": row.get('TRACKING_URLS'),
                        "origin_address": row.get('ORIGIN_ADDRESS'),
                        "shipment_status": row.get('SHIPMENT_STATUS'),
                        "tracking_number": row.get('TRACKING_NUMBER'),
                        "tracking_numbers": row.get('TRACKING_NUMBERS'),
                        "admin_graphql_api_id": row.get('ADMIN_GRAPHQL_API_ID')
                    }
                    fulfillments.append(fulfillment)

            return result_flag, fulfillments, result_query
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, None, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific order fulfillment")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, fulfillment, fulfillments
            except:
                pass

    def get_specific_fulfillment_line_item(self, fulfillment_id, order_id, line_item_id):
        # print(f"\n[INFO] BEGIN - Getting specific fulfillment line item")
        columns = ["*"]
        condition = "1=1"
        condition += f"\nAND FULFILLMENT_ID = '{fulfillment_id}'" if fulfillment_id is not None and fulfillment_id != "" else ""
        condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
        condition += f"\nAND LINE_ITEM_ID = '{line_item_id}'" if line_item_id is not None and line_item_id != "" else ""
        result_flag = False
        result_query = None
        line_item = {}
        line_items = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_FULFILLMENT_LINE_ITEMS(), columns, condition)

            if result_flag:
                for row in result_query:
                    line_item = {
                        "line_item_id": row.get('LINE_ITEM_ID'),
                        "fulfillment_id": row.get('FULFILLMENT_ID'),
                        "order_id": row.get('ORDER_ID'),
                        "product_id": row.get('PRODUCT_ID'),
                        "variant_id": row.get('VARIANT_ID')
                    }
                    line_items.append(line_item)

            return result_flag, line_items, result_query
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, None, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific fulfillment line item")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, line_item, line_items
            except:
                pass

    def get_specific_fulfillment_and_line_items(self, id, order_id):
        result_flag_fulfillment = False
        fulfillments = []
        result_query_fulfillment = None
        result_flag_line_items = False
        line_items = []
        result_query_line_items = None
        final_fulfillment = []

        try:
            result_flag_fulfillment, fulfillments, result_query_fulfillment = self.get_specific_order_fulfillment(id=id, order_id=order_id)
            if result_flag_fulfillment:
                for fulfillment in fulfillments:
                    result_flag_line_items, line_items, result_query_line_items = self.get_specific_fulfillment_line_item(fulfillment_id=id, order_id=order_id, line_item_id=None)
                    if result_flag_line_items:
                        fulfillment['line_items'] = []
                        for item in line_items:
                            fulfillment['line_items'].append(item)
                    else:
                        fulfillment['line_items'] = []
                    final_fulfillment.append(fulfillment)
            else:
                fulfillments = []

            return result_flag_fulfillment, final_fulfillment, "Success"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, None, str(e)
        finally:
            try:
                del result_flag_fulfillment, fulfillments, result_query_fulfillment, result_flag_line_items, line_items, result_query_line_items, final_fulfillment
            except:
                pass

    def verify_order_fulfillment_exists(self, id, order_id):
        # print(f"\n[INFO] BEGIN - Verifying Discount Code exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND ID = '{id}'" if id is not None and id != "" else ""
        condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_FULFILLMENTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    if row.get('TOTAL') > 0:
                        return True
                    else:
                        return False
            else:
                return None
        except Exception as e:
            print(f"[ERROR] {e}")
            return None
        finally:
            # print(f"[INFO] END - Finished verifying Discount Code exists")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    def verify_fulfillment_line_item_exists(self, fulfillment_id, order_id, line_item_id):
        # print(f"\n[INFO] BEGIN - Verifying Discount Code exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND LINE_ITEM_ID = '{line_item_id}'" if line_item_id is not None and line_item_id != "" else ""
        condition += f"\nAND FULFILLMENT_ID = '{fulfillment_id}'" if fulfillment_id is not None and fulfillment_id != "" else ""
        condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_FULFILLMENT_LINE_ITEMS(), columns, condition)

            if result_flag:
                for row in result_query:
                    if row.get('TOTAL') > 0:
                        return True
                    else:
                        return False
            else:
                return None
        except Exception as e:
            print(f"[ERROR] {e}")
            return None
        finally:
            # print(f"[INFO] END - Finished verifying Discount Code exists")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    def insert_order_fulfillment(self, fulfillment: Fulfillment):
        # print(f"\n[INFO] BEGIN - Inserting order fulfillment")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        id = None
        order_id = None
        keep_trying = True
        count_try = 0
        maximum_insert_try = 100
        condition = ""

        try:
            self.utils.clear_columns_values_arrays()
            id = fulfillment.get_id()
            order_id = fulfillment.get_order_id()
            self.utils.validate_columns_values("ID", id)
            self.utils.validate_columns_values("NAME", fulfillment.get_name())
            self.utils.validate_columns_values("STATUS", fulfillment.get_status())
            self.utils.validate_columns_values("RECEIPT", fulfillment.get_receipt())
            self.utils.validate_columns_values("SERVICE", fulfillment.get_service())
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("CREATED_AT", fulfillment.get_created_at())
            self.utils.validate_columns_values("UPDATED_AT", fulfillment.get_updated_at())
            self.utils.validate_columns_values("LOCATION_ID", fulfillment.get_location_id())
            self.utils.validate_columns_values("TRACKING_URL", fulfillment.get_tracking_url())
            self.utils.validate_columns_values("TRACKING_URLS", fulfillment.get_tracking_urls())
            self.utils.validate_columns_values("ORIGIN_ADDRESS", fulfillment.get_origin_address())
            self.utils.validate_columns_values("SHIPMENT_STATUS", fulfillment.get_shipment_status())
            self.utils.validate_columns_values("TRACKING_NUMBER", fulfillment.get_tracking_number())
            self.utils.validate_columns_values("TRACKING_NUMBERS", fulfillment.get_tracking_numbers())
            self.utils.validate_columns_values("TRACKING_COMPANY", fulfillment.get_tracking_company())
            self.utils.validate_columns_values("ADMIN_GRAPHQL_API_ID", fulfillment.get_admin_graphql_api_id())

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_FULFILLMENTS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Fulfillment Inserted...\t\t\tOrder: {order_id}\tFulfillment: {id}")
            else:
                if "1062" in result_string or "Duplicate entry" in result_string:
                    if "PRIMARY" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            print(f"[INFO] Trying to insert Fulfillment again...\tOrder: {order_id}\tFulfillment: {id}.Try: {count_try}")
                            super().generate_next_id()
                            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_FULFILLMENTS(), self.utils.get_columns_array(), self.utils.get_values_array())

                            if result_flag == True:
                                print(f"[INFO] Fulfillment Inserted...\t\t\tOrder: {order_id}\tFulfillment: {id}")
                                keep_trying = False
                            else:
                                if "ID" in result_string:
                                    print(f"[INFO] Updating Fulfillment...\t\tOrder: {order_id}\tFulfillment: {id}")
                                    condition = "1=1"
                                    condition += f"\nAND ID = '{id}'"
                                    condition += f"\nAND ORDER_ID = '{order_id}'"
                                    result_flag, rowcount, result_string = super().update_record(super().get_tbl_FULFILLMENTS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

                                    if result_flag == True:
                                        print(f"[INFO] Fulfillment Updated...\t\tOrder: {order_id}\tFulfillment: {id}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Fulfillment NOT Inserted/Maximum tries...\tOrder: {order_id}\tFulfillment: {id}.Try: {count_try}")
                                            keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Fulfillment NOT Inserted/Maximum tries...\tOrder: {order_id}\tFulfillment: {id}.Try: {count_try}")
                                        keep_trying = False
                    else:
                        print(f"[INFO] Updating Fulfillment...\t\tOrder: {order_id}\tFulfillment: {id}")
                        condition = "1=1"
                        condition += f"\nAND ID = '{id}'"
                        condition += f"\nAND ORDER_ID = '{order_id}'"
                        result_flag, rowcount, result_string = super().update_record(super().get_tbl_FULFILLMENTS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)
                else:
                    print(f"[ERROR] Error inserting Fulfillment...\tOrder: {order_id}\tFulfillment: {id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished inserting order fulfillment")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount, order_id, id
            except:
                pass

    def insert_fulfillment_line_item(self, line_item: FulfillmentLineItem):
        # print(f"\n[INFO] BEGIN - Inserting Fulfillment Line Item")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        fulfillment_id = None
        order_id = None
        line_item_id = None
        keep_trying = True
        count_try = 0
        maximum_insert_try = 100
        condition = ""

        try:
            fulfillment_id = line_item.get_fulfillment_id()
            order_id = line_item.get_order_id()
            line_item_id = line_item.get_line_item_id()
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("LINE_ITEM_ID", line_item_id)
            self.utils.validate_columns_values("FULFILLMENT_ID", fulfillment_id)
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("PRODUCT_ID", line_item.get_product_id())
            self.utils.validate_columns_values("VARIANT_ID", line_item.get_variant_id())
            self.utils.validate_columns_values("FULFILLMENT_SERVICE", line_item.get_fulfillment_service())
            self.utils.validate_columns_values("FULFILLMENT_STATUS", line_item.get_fulfillment_status())

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_FULFILLMENT_LINE_ITEMS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Fulfillment Item Inserted...\t\tOrder: {order_id}\tItem: {line_item_id}")
            else:
                if "1062" in result_string or "Duplicate entry" in result_string:
                    if "PRIMARY" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            print(f"[INFO] Trying to insert Fulfill Item again...\tOrder: {order_id}\tItem: {line_item_id}.Try: {count_try}")
                            super().generate_next_id()
                            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_FULFILLMENT_LINE_ITEMS(), self.utils.get_columns_array(), self.utils.get_values_array())

                            if result_flag == True:
                                print(f"[INFO] Fulfillment Item Inserted...\t\tOrder: {order_id}\tItem: {line_item_id}")
                                keep_trying = False
                            else:
                                if "ID" in result_string:
                                    print(f"[INFO] Updating Fulfillment...\t\tOrder: {order_id}\tItem: {line_item_id}")
                                    condition = f"1=1"
                                    condition += f"\nAND FULFILLMENT_ID = '{fulfillment_id}'"
                                    condition += f"\nAND ORDER_ID = '{order_id}'"
                                    condition += f"\nAND LINE_ITEM_ID = '{line_item_id}'"
                                    result_flag, rowcount, result_string = super().update_record(super().get_tbl_FULFILLMENT_LINE_ITEMS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

                                    if result_flag == True:
                                        print(f"[INFO] Fulfillment Item Updated...\t\tOrder: {order_id}\tItem: {line_item_id}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Fulfillment Item NOT Inserted/Maximum tries...\tOrder: {order_id}\tItem: {line_item_id}.Try: {count_try}")
                                            keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Fulfillment Item NOT Inserted/Maximum tries...\tOrder: {order_id}\tItem: {line_item_id}.Try: {count_try}")
                                        keep_trying = False
                    else:
                        print(f"[INFO] Updating Fulfillment Item...\t\tOrder: {order_id}\tItem: {line_item_id}")
                        condition = f"1=1"
                        condition += f"\nAND FULFILLMENT_ID = '{fulfillment_id}'"
                        condition += f"\nAND ORDER_ID = '{order_id}'"
                        condition += f"\nAND LINE_ITEM_ID = '{line_item_id}'"
                        result_flag, rowcount, result_string = super().update_record(super().get_tbl_FULFILLMENT_LINE_ITEMS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)
                else:
                    print(f"[ERROR] Error inserting Fulfillment Item...\tOrder: {order_id}\tItem: {line_item_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished inserting Fulfillment Line Item")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount, fulfillment_id, order_id, line_item_id, keep_trying, count_try, maximum_insert_try, condition
            except:
                pass

    def update_order_fulfillment(self, fulfillment: Fulfillment):
        # print(f"\n[INFO] BEGIN - Updating order fulfillment")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        id = None
        order_id = None

        try:
            id = fulfillment.get_id()
            order_id = fulfillment.get_order_id()
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ID", id)
            self.utils.validate_columns_values("NAME", fulfillment.get_name())
            self.utils.validate_columns_values("STATUS", fulfillment.get_status())
            self.utils.validate_columns_values("RECEIPT", fulfillment.get_receipt())
            self.utils.validate_columns_values("SERVICE", fulfillment.get_service())
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("CREATED_AT", fulfillment.get_created_at())
            self.utils.validate_columns_values("UPDATED_AT", fulfillment.get_updated_at())
            self.utils.validate_columns_values("LOCATION_ID", fulfillment.get_location_id())
            self.utils.validate_columns_values("TRACKING_URL", fulfillment.get_tracking_url())
            self.utils.validate_columns_values("TRACKING_URLS", fulfillment.get_tracking_urls())
            self.utils.validate_columns_values("ORIGIN_ADDRESS", fulfillment.get_origin_address())
            self.utils.validate_columns_values("SHIPMENT_STATUS", fulfillment.get_shipment_status())
            self.utils.validate_columns_values("TRACKING_NUMBER", fulfillment.get_tracking_number())
            self.utils.validate_columns_values("TRACKING_NUMBERS", fulfillment.get_tracking_numbers())
            self.utils.validate_columns_values("TRACKING_COMPANY", fulfillment.get_tracking_company())
            self.utils.validate_columns_values("ADMIN_GRAPHQL_API_ID", fulfillment.get_admin_graphql_api_id())

            condition = f"ID = '{id}'"
            condition += f"\nAND ORDER_ID = '{order_id}'"

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_FULFILLMENTS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Fulfillment Updated...\t\t\tOrder: {order_id}\tFulfillment: {id}")
            else:
                print(f"[ERROR] Error updating Fulfillment...\tOrder: {order_id}\tFulfillment: {id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished updating order fulfillment")
            # print("[INFO] Cleaning up variables")
            try:
                del condition, result_flag, result_string, rowcount
            except:
                pass

    def update_fulfillment_line_item(self, line_item: FulfillmentLineItem):
        # print(f"\n[INFO] BEGIN - Updating Fulfillment Line Item")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        fulfillment_id = None
        order_id = None
        line_item_id = None

        try:
            fulfillment_id = line_item.get_fulfillment_id()
            order_id = line_item.get_order_id()
            line_item_id = line_item.get_line_item_id()
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("FULFILLMENT_ID", fulfillment_id)
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("LINE_ITEM_ID", line_item_id)
            self.utils.validate_columns_values("PRODUCT_ID", line_item.get_product_id())
            self.utils.validate_columns_values("VARIANT_ID", line_item.get_variant_id())
            self.utils.validate_columns_values("FULFILLMENT_SERVICE", line_item.get_fulfillment_service())
            self.utils.validate_columns_values("FULFILLMENT_STATUS", line_item.get_fulfillment_status())

            condition = f"1=1"
            condition += f"\nAND FULFILLMENT_ID = '{fulfillment_id}'"
            condition += f"\nAND ORDER_ID = '{order_id}'"
            condition += f"\nAND LINE_ITEM_ID = '{line_item_id}'"

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_FULFILLMENT_LINE_ITEMS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Fulfillment Item Updated...\t\tOrder: {order_id}\tItem: {line_item_id}")
            else:
                print(f"[ERROR] Error updating Fulfillment Item...\tOrder: {order_id}\tItem: {line_item_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished updating Fulfillment Line Item")
            # print("[INFO] Cleaning up variables")
            try:
                del condition, result_flag, result_string, rowcount, fulfillment_id, order_id, line_item_id
            except:
                pass

    def delete_order_fulfillment(self, id, order_id):
        # print(f"\n[INFO] BEGIN - Deleting order fulfillment")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        condition = "1=1"

        try:
            condition += f"\nAND ID = '{id}'" if id is not None and id != "" else ""
            condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
            result_flag, rowcount, return_string = super().delete_record(super().get_tbl_FULFILLMENTS(), condition)

            if result_flag:
                print(f"[INFO] Fulfillment Deleted...\t\t\tOrder: {order_id}\tFulfillment: {id}")
            else:
                print(f"[ERROR] Error deleting Fulfillment...\tOrder: {order_id}\tFulfillment: {id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished deleting order fulfillment")
            # print("[INFO] Cleaning up variables")
            try:
                del condition, result_flag, result_string, rowcount
            except:
                pass

    def upsert_order_fulfillment(self, fulfillment: Fulfillment):
        # print(f"\n[INFO] BEGIN - Upserting Order Fulfillment")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        fulfillment_exists = False
        id = None
        order_id = None

        try:
            id = fulfillment.get_id()
            order_id = fulfillment.get_order_id()

            fulfillment_exists = self.verify_order_fulfillment_exists(id, order_id)

            if fulfillment_exists:
                result_flag, rowcount, result_string = self.update_order_fulfillment(fulfillment)
            else:
                result_flag, rowcount, result_string = self.insert_order_fulfillment(fulfillment)

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Order Fulfillment")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, fulfillment_exists, id, order_id
            except:
                pass

    def upsert_fulfillment_line_item(self, line_item: FulfillmentLineItem):
        # print(f"\n[INFO] BEGIN - Upserting Fulfillment Line Item")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        fulfillment_exists = False
        fulfillment_id = None
        order_id = None
        line_item_id = None

        try:
            line_item_id = line_item.get_line_item_id()
            fulfillment_id = line_item.get_fulfillment_id()
            order_id = line_item.get_order_id()

            fulfillment_exists = self.verify_fulfillment_line_item_exists(fulfillment_id, order_id, line_item_id)

            if fulfillment_exists:
                result_flag, rowcount, result_string = self.update_fulfillment_line_item(line_item)
            else:
                result_flag, rowcount, result_string = self.insert_fulfillment_line_item(line_item)

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Fulfillment Line Item")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, fulfillment_exists, fulfillment_id, order_id, line_item_id
            except:
                pass

    # API Functions
    def upsert_order_fulfillment_api(self, fulfillment_json):
        # print(f"\n[INFO] BEGIN - Upserting Order Fulfillment")
        fulfillment = self.Fulfillment()
        result_flag = False
        final_result_flag = True
        rowcount = 0
        result_string = "Success"
        fulfillment_exists = False
        line_items = None
        count_fullfillments = 0
        total_success_fullfillments = 0
        count_line_items = 0
        total_success_line_items = 0
        order_id = None
        fulfillment_id = None

        try:
            for fulfill in fulfillment_json:
                count_fullfillments += 1
                order_id = fulfill.get('order_id')
                fulfillment_id = fulfill.get('id')
                fulfillment.set_id(fulfillment_id)
                fulfillment.set_name(self.utils.replace_special_chars(fulfill.get('name')))
                fulfillment.set_status(self.utils.replace_special_chars(fulfill.get('status')))
                fulfillment.set_receipt(self.utils.replace_special_chars(self.utils.convert_object_to_json(fulfill.get('receipt'))))
                fulfillment.set_service(self.utils.replace_special_chars(fulfill.get('service')))
                fulfillment.set_order_id(order_id)
                fulfillment.set_created_at(self.utils.replace_special_chars(fulfill.get('created_at')))
                fulfillment.set_updated_at(self.utils.replace_special_chars(fulfill.get('updated_at')))
                fulfillment.set_location_id(self.utils.replace_special_chars(fulfill.get('location_id')))
                fulfillment.set_tracking_url(self.utils.replace_special_chars(fulfill.get('tracking_url')))
                fulfillment.set_tracking_urls(self.utils.replace_special_chars(self.utils.convert_object_to_json(fulfill.get('tracking_urls'))))
                fulfillment.set_origin_address(self.utils.replace_special_chars(self.utils.convert_object_to_json(fulfill.get('origin_address'))))
                fulfillment.set_shipment_status(self.utils.replace_special_chars(fulfill.get('shipment_status')))
                fulfillment.set_tracking_number(self.utils.replace_special_chars(fulfill.get('tracking_number')))
                fulfillment.set_tracking_numbers(self.utils.replace_special_chars(self.utils.convert_object_to_json(fulfill.get('tracking_numbers'))))
                fulfillment.set_tracking_company(self.utils.replace_special_chars(fulfill.get('tracking_company')))
                fulfillment.set_admin_graphql_api_id(self.utils.replace_special_chars(fulfill.get('admin_graphql_api_id')))
                line_items = fulfill.get('line_items')

                result_flag, rowcount, result_string = self.upsert_order_fulfillment(fulfillment)
                if result_flag:
                    total_success_fullfillments += 1
                    for item in line_items:
                        count_line_items += 1
                        line_item = self.FulfillmentLineItem()
                        line_item.set_line_item_id(item.get('id'))
                        line_item.set_fulfillment_id(fulfillment_id)
                        line_item.set_order_id(order_id)
                        line_item.set_product_id(item.get('product_id'))
                        line_item.set_variant_id(item.get('variant_id'))
                        line_item.set_fulfillment_service(item.get('fulfillment_service'))
                        line_item.set_fulfillment_status(item.get('fulfillment_status'))

                        result_flag, rowcount, result_string = self.upsert_fulfillment_line_item(line_item)
                        if result_flag:
                            total_success_line_items += 1
                        else:
                            final_result_flag = False
                else:
                    final_result_flag = False

            if final_result_flag:
                # print(f"[INFO] A total of {total_success_fullfillments} Fulfillments and {total_success_line_items} Line Items was/were upserted.")
                return final_result_flag, 200, total_success_fullfillments, f"Success. {total_success_fullfillments} Fulfillments and {total_success_line_items} Line Items upserted."
            else:
                return final_result_flag, 500, 0, "Error upserting Fulfillments and Line Items."
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Order Fulfillment")
            # print("[INFO] Cleaning up variables")
            try:
                del fulfillment, result_flag, final_result_flag, rowcount, result_string, fulfillment_exists, line_items, count_fullfillments, total_success_fullfillments, count_line_items, total_success_line_items
            except:
                pass

    # def add_order_fulfillment(self, fulfillment_json):
    #     # print(f"\n[INFO] BEGIN - Adding Order Fulfillment")
    #     fulfillment = self.OrderFulfillment()
    #     result_flag = False
    #     rowcount = 0
    #     result_string = "Success"
    #     fulfillment_exists = False

    #     try:
    #         fulfillment_exists = self.verify_order_fulfillment_exists(order_id)
    #         if fulfillment_exists:
    #             return False, 409, 0, "Order Fulfillment already exists"
    #         else:
    #             fulfillment.order_id = order_id
    #             fulfillment.tracking_number = tracking_number
    #             fulfillment.carrier = carrier
    #             fulfillment.status = status
    #             fulfillment.created_at = created_at
    #             fulfillment.updated_at = updated_at

    #             result_flag, rowcount, result_string = self.insert_order_fulfillment(order_id, fulfillment)
    #             if result_flag:
    #                 return result_flag, 200, rowcount, result_string
    #             else:
    #                 return result_flag, 500, rowcount, result_string
    #     except Exception as e:
    #         print(f"[ERROR] {e}")
    #         return False, 500, 0, str(e)
    #     finally:
    #         # print(f"[INFO] END - Finished adding Order Fulfillment")
    #         # print("[INFO] Cleaning up variables")
    #         try:
    #             del fulfillment, fulfillment_exists, result_flag, rowcount, result_string, order_id, tracking_number, carrier, status, created_at, updated_at
    #         except:
    #             pass

    # def edit_order_fulfillment(self, order_id, fulfillment_json):
    #     # print(f"\n[INFO] BEGIN - Editing Order Fulfillment")
    #     fulfillment = self.OrderFulfillment()
    #     result_flag = False
    #     rowcount = 0
    #     result_string = "Success"
    #     fulfillment_exists = False

    #     if order_id is None or order_id == "":
    #         return False, 400, 0, "Order ID is required."

    #     tracking_number = fulfillment_json.get('tracking_number')
    #     carrier = fulfillment_json.get('carrier')
    #     status = fulfillment_json.get('status')
    #     created_at = fulfillment_json.get('created_at')
    #     updated_at = fulfillment_json.get('updated_at')

    #     try:
    #         fulfillment_exists = self.verify_order_fulfillment_exists(order_id)
    #         if not fulfillment_exists:
    #             return False, 404, 0, "Order Fulfillment not found!"
    #         else:
    #             fulfillment.order_id = order_id
    #             fulfillment.tracking_number = tracking_number
    #             fulfillment.carrier = carrier
    #             fulfillment.status = status
    #             fulfillment.created_at = created_at
    #             fulfillment.updated_at = updated_at

    #             result_flag, rowcount, result_string = self.update_order_fulfillment(order_id, fulfillment)
    #             if result_flag:
    #                 return result_flag, 200, rowcount, result_string
    #             else:
    #                 return result_flag, 500, rowcount, result_string
    #     except Exception as e:
    #         print(f"[ERROR] {e}")
    #         return False, 500, 0, str(e)
    #     finally:
    #         # print(f"[INFO] END - Finished editing Order Fulfillment")
    #         # print("[INFO] Cleaning up variables")
    #         try:
    #             del fulfillment, fulfillment_exists, result_flag, rowcount, result_string, order_id, tracking_number, carrier, status, created_at, updated_at
    #         except:
    #             pass

    # def remove_order_fulfillment(self, order_id):
    #     # print(f"\n[INFO] BEGIN - Removing Order Fulfillment")
    #     result_flag = False
    #     rowcount = 0
    #     result_string = "Success"
    #     fulfillment_exists = False

    #     if order_id is None or order_id == "":
    #         return False, 400, 0, "Order ID is required."

    #     try:
    #         fulfillment_exists = self.verify_order_fulfillment_exists(order_id)
    #         if not fulfillment_exists:
    #             return False, 404, 0, "Order Fulfillment not found!"
    #         else:
    #             result_flag, rowcount, result_string = self.delete_order_fulfillment(order_id)
    #             if result_flag:
    #                 return result_flag, 200, rowcount, result_string
    #             else:
    #                 return result_flag, 500, rowcount, result_string
    #     except Exception as e:
    #         print(f"[ERROR] {e}")
    #         return False, 500, 0, str(e)
    #     finally:
    #         # print(f"[INFO] END - Finished removing Order Fulfillment")
    #         # print("[INFO] Cleaning up variables")
    #         try:
    #             del fulfillment_exists, result_flag, rowcount, result_string
    #         except:
    #             pass

    def get_all_order_fulfillments_api(self):
        # print(f"\n[INFO] BEGIN - Getting all order fulfillments")
        result_flag = False
        result = None

        try:
            result_flag, result = self.get_all_order_fulfillments()
            if result_flag:
                return result_flag, 200, result
            else:
                return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all order fulfillments")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass

    def get_specific_order_fulfillment_api(self, order_id):
        # print(f"\n[INFO] BEGIN - Getting specific order fulfillment")
        result_flag = False
        result = None
        fulfillment_exists = False

        try:
            fulfillment_exists = self.verify_order_fulfillment_exists(order_id)
            if not fulfillment_exists:
                return False, 404, "Order Fulfillment not found!"
            else:
                result_flag, result = self.get_specific_order_fulfillment(order_id)
                if result_flag:
                    return result_flag, 200, result
                else:
                    return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific order fulfillment")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass
