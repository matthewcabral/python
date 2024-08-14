from database.DataController import *
from utils.UtilsController import *
from order.OrderController import *

class OrderProcessingController(DataController):
    def __init__(self):
        super().__init__()
        self.order = OrderController()
        self.utils = UtilsController()
        self.NUMBER_OF_ORDERS = self.utils.load_env_variable("NUMBER_OF_ORDERS")

    # Getters
    def get_number_of_orders(self):
        return self.NUMBER_OF_ORDERS

    # Functions
    def process_orders(self):
        first_order_id = None
        last_order_id = None
        range_inserted_flag = False
        order_ids = []
        order_info = []

        try:
            print(f"\n[INFO] BEGIN - Processing orders...")
            first_order_id = self.get_last_processed_order_id()

            if first_order_id is not None:
                order_ids = self.get_next_order_ids_range(first_order_id)

                if len(order_ids) > 0:
                    last_order_id = order_ids[-1]
                    range_inserted_flag = self.insert_new_order_range(first_order_id, last_order_id)

                    if range_inserted_flag:
                        # print(f"[INFO] Processing orders: {order_ids}")
                        order_info = self.get_orders_info(order_ids)
                        print(f"[INFO] Order info: \n{order_info}")
                    else:
                        print(f"[ERROR] process_orders: Unable to insert new order range")
                else:
                    print(f"[ERROR] process_orders: Unable to get next order ids range")
            else:
                print(f"[ERROR] process_orders: Unable to get last order id")
        except Exception as e:
            print(f"[ERROR] process_orders: {str(e)}")
        finally:
            print(f"[INFO] END - Processing orders...")
            print(f"[INFO] Clearing variables...")
            try:
                del first_order_id
                del last_order_id
                del range_inserted_flag
                del order_ids
                del order_info
            except:
                pass

    def get_last_processed_order_id(self):
        print(f"\n[INFO] BEGIN - Getting last processed order id...")
        result_query = None
        row = None
        order_ids = []
        last_order_id = None

        try:
            columns = ["MAX(LAST_ID) AS LAST_ID"]
            result_flag, result_query = super().query_record(super().get_tbl_ORDER_RANGE(), columns, None)

            if result_flag:
                for row in result_query:
                    last_order_id = row.get("LAST_ID")
                    return last_order_id
            else:
                return None
        except Exception as e:
            print(f"[ERROR] get_order_ids_for_processing: {str(e)}")
            return None
        finally:
            print(f"[INFO] END - Got last processed order id: {last_order_id}")
            print(f"[INFO] Clearing variables...")
            try:
                del columns
                del condition
                del result_query
                del result_flag
                del row
                del order_ids
                del last_order_id
            except:
                pass

    def get_next_order_ids_range(self, first_id):
        print(f"\n[INFO] BEGIN - Getting next order ids...")
        columns = ["ID"]
        condition = "1=1"
        result_flag = False
        result_query = None
        row = None
        range_ids = []
        two_hours_ago = self.utils.get_x_hours_ago(hours=0)
        number_of_orders = self.get_number_of_orders()

        try:
            condition += f"\nAND ID > '{first_id}'"
            condition += f"\nAND CREATED_AT < DATE_FORMAT('{two_hours_ago}', '%Y-%m-%d %H:%i:%s')"
            condition += f"\nAND (FULFILLMENT_STATUS IS NULL OR FULFILLMENT_STATUS = 'partial')"
            condition += f"\nAND TOTAL_PRICE > 0"
            condition += f"\nAND TAGS NOT LIKE '%XXXXXXX%'"
            condition += f"\nAND COUNTRY_CODE = 'US'"
            condition += f"\nORDER BY ID ASC"
            condition += f"\nLIMIT {number_of_orders}"

            print(f"[INFO] Querying orders...")
            result_flag, result_query = super().query_record(super().get_tbl_ORDER(), columns, condition)

            if result_flag:
                for row in result_query:
                    print(f"[INFO] Appending Order ID: {row.get('ID')}")
                    range_ids.append(row.get("ID"))
                return range_ids
            else:
                return None
        except Exception as e:
            print(f"[ERROR] get_order_ids_for_processing: {str(e)}")
            return None
        finally:
            print(f"[INFO] END - Got a total of {len(range_ids)} order ids")
            print(f"[INFO] Clearing variables...")
            try:
                del columns
                del condition
                del result_query
                del result_flag
                del row
                del range_ids
                del two_hours_ago
                del number_of_orders
            except:
                pass

    def insert_new_order_range(self, first_order_id, last_order_id):
        print(f"\n[INFO] BEGIN - Inserting new order range...")
        result_flag = False
        rowcount = 0
        result_query = None
        columns = ["FIRST_ID" , "LAST_ID"]
        values = [first_order_id, last_order_id]
        try:
            print(f"[INFO] Inserting new order range. First Id: {first_order_id} - Last Id: {last_order_id}")
            result_flag, rowcount, result_query = super().insert_record(super().get_tbl_ORDER_RANGE(), columns, values)

            if not result_flag:
                print(f"[ERROR] Error while inserting new order range. Error: {str(result_query)}")

            return result_flag
        except Exception as e:
            print(f"[ERROR] Error while inserting new order range. Error: {str(e)}")
        finally:
            print(f"[INFO] END - New order range inserted. Total of inserted rows: {rowcount}")
            print(f"[INFO] Clearing variables...")
            try:
                del columns
                del values
                del result_flag
                del result_query
            except:
                pass

    def get_orders_info(self, order_ids):
        print(f"\n[INFO] BEGIN - Getting order info...")
        order_result_flag = False
        order_result_query = None
        item_result_flag = False
        item_result_query = None
        order_columns = ["ID", "NAME", "CREATED", "CREATED_AT", "EMAIL", "TAGS", "COUNTRY_CODE", "SHIPPING_ADDRESS", "SHIPPING_LINES", "PRESENTMENT_CURRENCY", "FULFILLMENT_STATUS", "CONCAT(JSON_UNQUOTE(JSON_EXTRACT(CUSTOMER, '$.first_name')), \" \", JSON_UNQUOTE(JSON_EXTRACT(CUSTOMER, '$.last_name'))) AS CUSTOMER"]
        line_item_columns = ["*"]
        order_condition = "1=1"
        item_condition = None
        row_order = None
        row_item = None
        order_info = []
        items = []
        order_id = None
        total_orders_array = len(order_ids)
        total_orders_result = 0
        total_valid_orders = 0
        counter = 0

        try:
            print(f"[INFO] Querying for orders info...")
            order_condition += f"\nAND ID IN ({', '.join([f'\n\t\'{id}\'' for id in order_ids])}\n)"
            order_condition += f"\nORDER BY ID ASC"
            order_result_flag, order_result_query = super().query_record(super().get_tbl_ORDER(), order_columns, order_condition)

            if order_result_flag:
                total_orders_result = len(order_result_query)
                print(f"[INFO] Got a total of {total_orders_result} orders")
                for row_order in order_result_query:
                    order_id = row_order.get("ID")
                    counter += 1
                    print(f"[INFO] Getting line items for Order ID: {order_id} - {counter} of {total_orders_result}")
                    item_condition = "1=1"
                    item_condition += f"\nAND ORDER_ID = '{order_id}'"
                    item_result_flag, item_result_query = super().query_record(super().get_tbl_ORDER_LINE_ITEM(), line_item_columns, item_condition)

                    if item_result_flag:
                        order_data = {
                            "order_id": order_id,
                            "order_name": row_order.get("NAME"),
                            "created_at": f"{row_order.get("CREATED_AT")}",
                            "customer_name": row_order.get("CUSTOMER"),
                            "customer_email": row_order.get("EMAIL"),
                            "tags": row_order.get("TAGS"),
                            "country_code": row_order.get("COUNTRY_CODE"),
                            "shipping_address": self.utils.convert_json_to_object(row_order.get("SHIPPING_ADDRESS")),
                            "shipping_lines": self.utils.convert_json_to_object(row_order.get("SHIPPING_LINES")),
                            "presentment_currency": row_order.get("PRESENTMENT_CURRENCY"),
                            "fulfillment_status": row_order.get("FULFILLMENT_STATUS"),
                            "line_items": []
                        }
                        items = []

                        for row_item in item_result_query:
                            item_data = {
                                "id": row_item.get("ID"),
                                "order_id": row_item.get("ORDER_ID"),
                                "product_id": row_item.get("PRODUCT_ID"),
                                "name": row_item.get("NAME"),
                                "title": row_item.get("TITLE"),
                                "sku": row_item.get("SKU"),
                                "variant_id": row_item.get("VARIANT_ID"),
                                "variant_title": row_item.get("VARIANT_TITLE"),
                                "fulfillable_quantity": row_item.get("FULFILLABLE_QUANTITY")
                            }
                            items.append(item_data)
                        order_data['line_items'] = items
                        order_info.append(order_data)
                        total_valid_orders += 1
                    else:
                        continue
                return order_info
            else:
                return None
        except Exception as e:
            print(f"[ERROR] Error while getting orders info. Error: {str(e)}")
            return None
        finally:
            print(f"[INFO] END - Got a total of {total_valid_orders} valid orders out of {total_orders_array}")
            print(f"[INFO] Clearing variables...")
            try:
                del order_result_query
                del order_result_flag
                del item_result_flag
                del item_result_query
                del order_columns
                del line_item_columns
                del order_condition
                del item_condition
                del row_order
                del row_item
                del order_info
                del items
                del order_id
                del total_orders_array
                del total_valid_orders
                del counter
                del total_orders_result
            except:
                pass
