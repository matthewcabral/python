from database.DataController import DataController
from utils.UtilsController import *
from utils.LogsController import *

class RefundsController(DataController):
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "OrderRefundController"

    def get_module_name(self):
        return self.module_name

    class Refund:
        def __init__(self):
            self.id = None
            self.order_id = None
            self.admin_graphql_api_id = None
            self.created_at = None
            self.note = None
            self.processed_at = None
            self.restock = None
            self.total_duties_set = None
            self.user_id = None
            self.order_adjustments = None
            self.transactions = None
            self.duties = None

        def get_id(self):
            return self.id

        def set_id(self, id):
            self.id = id

        def get_order_id(self):
            return self.order_id

        def set_order_id(self, order_id):
            self.order_id = order_id

        def get_admin_graphql_api_id(self):
            return self.admin_graphql_api_id

        def set_admin_graphql_api_id(self, admin_graphql_api_id):
            self.admin_graphql_api_id = admin_graphql_api_id

        def get_created_at(self):
            return self.created_at

        def set_created_at(self, created_at):
            self.created_at = created_at

        def get_note(self):
            return self.note

        def set_note(self, note):
            self.note = note

        def get_processed_at(self):
            return self.processed_at

        def set_processed_at(self, processed_at):
            self.processed_at = processed_at

        def get_restock(self):
            return self.restock

        def set_restock(self, restock):
            self.restock = restock

        def get_total_duties_set(self):
            return self.total_duties_set

        def set_total_duties_set(self, total_duties_set):
            self.total_duties_set = total_duties_set

        def get_user_id(self):
            return self.user_id

        def set_user_id(self, user_id):
            self.user_id = user_id

        def get_order_adjustments(self):
            return self.order_adjustments

        def set_order_adjustments(self, order_adjustments):
            self.order_adjustments = order_adjustments

        def get_transactions(self):
            return self.transactions

        def set_transactions(self, transactions):
            self.transactions = transactions

        def get_duties(self):
            return self.duties

        def set_duties(self, duties):
            self.duties = duties

    class RefundLineItem:
        def __init__(self):
            self.id = None
            self.refund_id = None
            self.order_id = None
            self.line_item_id = None
            self.location_id = None
            self.product_id = None
            self.quantity = None
            self.restock_type = None
            self.subtotal = None
            self.subtotal_set = None
            self.total_tax = None
            self.total_tax_set = None

        def get_id(self):
            return self.id

        def set_id(self, id):
            self.id = id

        def get_refund_id(self):
            return self.refund_id

        def set_refund_id(self, refund_id):
            self.refund_id = refund_id

        def get_order_id(self):
            return self.order_id

        def set_order_id(self, order_id):
            self.order_id = order_id

        def get_line_item_id(self):
            return self.line_item_id

        def set_line_item_id(self, line_item_id):
            self.line_item_id = line_item_id

        def get_location_id(self):
            return self.location_id

        def set_location_id(self, location_id):
            self.location_id = location_id

        def get_product_id(self):
            return self.product_id

        def set_product_id(self, product_id):
            self.product_id = product_id

        def get_quantity(self):
            return self.quantity

        def set_quantity(self, quantity):
            self.quantity = quantity

        def get_restock_type(self):
            return self.restock_type

        def set_restock_type(self, restock_type):
            self.restock_type = restock_type

        def get_subtotal(self):
            return self.subtotal

        def set_subtotal(self, subtotal):
            self.subtotal = subtotal

        def get_subtotal_set(self):
            return self.subtotal_set

        def set_subtotal_set(self, subtotal_set):
            self.subtotal_set = subtotal_set

        def get_total_tax(self):
            return self.total_tax

        def set_total_tax(self, total_tax):
            self.total_tax = total_tax

        def get_total_tax_set(self):
            return self.total_tax_set

        def set_total_tax_set(self, total_tax_set):
            self.total_tax_set = total_tax_set

    # Database Functions
    def get_all_refunds(self):
        # print(f"\n[INFO] BEGIN - Getting all refunds")
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        refunds = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REFUNDS(), columns, condition)

            if result_flag:
                for row in result_query:
                    refund = {
                        "id": row.get('ID'),
                        "order_id": row.get('ORDER_ID'),
                        "admin_graphql_api_id": row.get('ADMIN_GRAPHQL_API_ID'),
                        "created_at": row.get('CREATED_AT'),
                        "note": row.get('NOTE'),
                        "processed_at": row.get('PROCESSED_AT'),
                        "restock": row.get('RESTOCK'),
                        "total_duties_set": row.get('TOTAL_DUTIES_SET'),
                        "user_id": row.get('USER_ID'),
                        "order_adjustments": row.get('ORDER_ADJUSTMENTS'),
                        "transactions": row.get('TRANSACTIONS'),
                        "duties": row.get('DUTIES')
                    }
                    refunds.append(refund)
                return True, refunds
            else:
                return False, "Error getting refunds"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all refunds")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, refunds
            except:
                pass

    def get_all_refund_line_items(self):
        # print(f"\n[INFO] BEGIN - Getting all refund line items")
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        refund_line_items = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REFUNDS_LINE_ITEMS(), columns, condition)

            if result_flag:
                for row in result_query:
                    refund_line_item = {
                        "id": row.get('ID'),
                        "refund_id": row.get('REFUND_ID'),
                        "order_id": row.get('ORDER_ID'),
                        "line_item_id": row.get('LINE_ITEM_ID'),
                        "location_id": row.get('LOCATION_ID'),
                        "product_id": row.get('PRODUCT_ID'),
                        "quantity": row.get('QUANTITY'),
                        "restock_type": row.get('RESTOCK_TYPE'),
                        "subtotal": row.get('SUBTOTAL'),
                        "subtotal_set": row.get('SUBTOTAL_SET'),
                        "total_tax": row.get('TOTAL_TAX'),
                        "total_tax_set": row.get('TOTAL_TAX_SET')
                    }
                    refund_line_items.append(refund_line_item)
                return True, refund_line_items
            else:
                return False, "Error getting refund line items"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all refund line items")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, refund_line_items
            except:
                pass

    def get_specific_refund(self, id, order_id):
        # print(f"\n[INFO] BEGIN - Getting specific refund")
        columns = ["*"]
        condition = "1=1"
        condition += f"\nAND ID = '{id}'" if id is not None and id != "" else ""
        condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
        result_flag = False
        result_query = None
        refund = {}
        refunds = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REFUNDS(), columns, condition)

            if result_flag:
                for row in result_query:
                    refund = {
                        "id": row.get('ID'),
                        "order_id": row.get('ORDER_ID'),
                        "admin_graphql_api_id": row.get('ADMIN_GRAPHQL_API_ID'),
                        "created_at": row.get('CREATED_AT'),
                        "note": row.get('NOTE'),
                        "processed_at": row.get('PROCESSED_AT'),
                        "restock": row.get('RESTOCK'),
                        "total_duties_set": row.get('TOTAL_DUTIES_SET'),
                        "user_id": row.get('USER_ID'),
                        "order_adjustments": row.get('ORDER_ADJUSTMENTS'),
                        "transactions": row.get('TRANSACTIONS'),
                        "duties": row.get('DUTIES')
                    }
                    refunds.append(refund)

            return result_flag, refunds, result_query
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific refund")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, refund, refunds
            except:
                pass

    def get_specific_refund_line_item(self, id, refund_id, order_id):
        # print(f"\n[INFO] BEGIN - Getting specific refund line item")
        columns = ["*"]
        condition = "1=1"
        condition += f"\nAND ID = '{id}'" if id is not None and id != "" else ""
        condition += f"\nAND REFUND_ID = '{refund_id}'" if refund_id is not None and refund_id != "" else ""
        condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
        result_flag = False
        result_query = None
        refund_line_item = {}
        refund_line_items = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REFUNDS_LINE_ITEMS(), columns, condition)

            if result_flag:
                for row in result_query:
                    refund_line_item = {
                        "id": row.get('ID'),
                        "refund_id": row.get('REFUND_ID'),
                        "order_id": row.get('ORDER_ID'),
                        "line_item_id": row.get('LINE_ITEM_ID'),
                        "location_id": row.get('LOCATION_ID'),
                        "product_id": row.get('PRODUCT_ID'),
                        "quantity": row.get('QUANTITY'),
                        "restock_type": row.get('RESTOCK_TYPE'),
                        "subtotal": row.get('SUBTOTAL'),
                        "subtotal_set": row.get('SUBTOTAL_SET'),
                        "total_tax": row.get('TOTAL_TAX'),
                        "total_tax_set": row.get('TOTAL_TAX_SET')
                    }
                    refund_line_items.append(refund_line_item)

            return result_flag, refund_line_items, result_query
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific refund line item")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, refund_line_item, refund_line_items
            except:
                pass

    def get_specific_refund_and_line_items(self, id, order_id):
        result_flag_refund = False
        refunds = []
        result_query_refund = None
        result_flag_line_items = False
        line_items = []
        result_query_line_items = None
        final_refund = []

        try:
            result_flag_refund, refunds, result_query_refund = self.get_specific_refund(id, order_id)
            if result_flag_refund:
                for refund in refunds:
                    result_flag_line_items, line_items, result_query_line_items = self.get_specific_refund_line_item(id, order_id, None)
                    if result_flag_line_items:
                        refund['line_items'] = []
                        for item in line_items:
                            refund['line_items'].append(item)
                    else:
                        refund['line_items'] = []
                    final_refund.append(refund)
            else:
                refunds = []

            return result_flag_refund, final_refund, "Success"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            try:
                del result_flag_refund, refunds, result_query_refund, result_flag_line_items, line_items, result_query_line_items, final_refund
            except:
                pass

    def verify_refund_exists(self, id, order_id):
        # print(f"\n[INFO] BEGIN - Verifying Refund exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND ID = '{id}'" if id is not None and id != "" else ""
        condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REFUNDS(), columns, condition)

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
            # print(f"[INFO] END - Finished verifying Refund exists")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    def verify_refund_line_item_exists(self, id, refund_id, order_id, line_item_id):
        # print(f"\n[INFO] BEGIN - Verifying Refund Line Item exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND ID = '{id}'" if id is not None and id != "" else ""
        condition += f"\nAND REFUND_ID = '{refund_id}'" if refund_id is not None and refund_id != "" else ""
        condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
        condition += f"\nAND LINE_ITEM_ID = '{line_item_id}'" if line_item_id is not None and line_item_id != "" else ""
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REFUNDS_LINE_ITEMS(), columns, condition)

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
            # print(f"[INFO] END - Finished verifying Refund Line Item exists")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    def insert_refund(self, refund: Refund):
        # print(f"\n[INFO] BEGIN - Inserting Refund")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        refund_id = None
        order_id = None
        keep_trying = True
        count_try = 0
        maximum_insert_try = 100
        condition = ""

        try:
            refund_id = refund.get_id()
            order_id = refund.get_order_id()
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ID", refund_id)
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("ADMIN_GRAPHQL_API_ID", refund.get_admin_graphql_api_id())
            self.utils.validate_columns_values("CREATED_AT", refund.get_created_at())
            self.utils.validate_columns_values("NOTE", refund.get_note())
            self.utils.validate_columns_values("PROCESSED_AT", refund.get_processed_at())
            self.utils.validate_columns_values("RESTOCK", refund.get_restock())
            self.utils.validate_columns_values("TOTAL_DUTIES_SET", refund.get_total_duties_set())
            self.utils.validate_columns_values("USER_ID", refund.get_user_id())
            self.utils.validate_columns_values("ORDER_ADJUSTMENTS", refund.get_order_adjustments())
            self.utils.validate_columns_values("TRANSACTIONS", refund.get_transactions())
            self.utils.validate_columns_values("DUTIES", refund.get_duties())

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_REFUNDS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Refund Inserted...\t\t\tOrder: {order_id}\tRefund: {refund_id}")
            else:
                if "1062" in result_string or "Duplicate entry" in result_string:
                    if "PRIMARY" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            print(f"[INFO] Trying to insert Refund again...\tOrder: {order_id}\tRefund: {refund_id}.Try: {count_try}")
                            super().generate_next_id()
                            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_REFUNDS(), self.utils.get_columns_array(), self.utils.get_values_array())

                            if result_flag == True:
                                print(f"[INFO] Refund Inserted...\t\tOrder: {order_id}\tRefund: {refund_id}")
                                keep_trying = False
                            else:
                                if "ID" in result_string:
                                    print(f"[INFO] Updating Refund...\t\tOrder: {order_id}\tRefund: {refund_id}")
                                    condition = f"1=1"
                                    condition += f"\nAND ID = '{refund_id}'"
                                    condition += f"\nAND ORDER_ID = '{order_id}'"
                                    result_flag, rowcount, result_string = super().update_record(super().get_tbl_REFUNDS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

                                    if result_flag == True:
                                        print(f"[INFO] Refund Updated...\t\tOrder: {order_id}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Refund NOT Inserted/Maximum tries...\tOrder: {order_id}\tRefund: {refund_id}.Try: {count_try}")
                                            keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Refund NOT Inserted/Maximum tries...\tOrder: {order_id}\tRefund: {refund_id}.Try: {count_try}")
                                        keep_trying = False
                    else:
                        print(f"[INFO] Updating Refund...\t\tOrder: {order_id}\tRefund: {refund_id}")
                        condition = f"1=1"
                        condition += f"\nAND ID = '{refund_id}'"
                        condition += f"\nAND ORDER_ID = '{order_id}'"
                        result_flag, rowcount, result_string = super().update_record(super().get_tbl_REFUNDS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)
                else:
                    print(f"[ERROR] Error inserting Refund...\t\tOrder: {order_id}\tRefund: {refund_id}")
            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished inserting Refund")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount, refund_id, order_id, keep_trying, count_try, maximum_insert_try, condition
            except:
                pass

    def insert_refund_line_item(self, refund_line_item: RefundLineItem):
        # print(f"\n[INFO] BEGIN - Inserting Refund Line Item")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        id = None
        refund_id = None
        order_id = None
        line_item_id = None
        keep_trying = True
        count_try = 0
        maximum_insert_try = 100
        condition = ""

        try:
            id = refund_line_item.get_id()
            refund_id = refund_line_item.get_refund_id()
            order_id = refund_line_item.get_order_id()
            line_item_id = refund_line_item.get_line_item_id()
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ID", id)
            self.utils.validate_columns_values("REFUND_ID", refund_id)
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("LINE_ITEM_ID", line_item_id)
            self.utils.validate_columns_values("LOCATION_ID", refund_line_item.get_location_id())
            self.utils.validate_columns_values("PRODUCT_ID", refund_line_item.get_product_id())
            self.utils.validate_columns_values("QUANTITY", refund_line_item.get_quantity())
            self.utils.validate_columns_values("RESTOCK_TYPE", refund_line_item.get_restock_type())
            self.utils.validate_columns_values("SUBTOTAL", refund_line_item.get_subtotal())
            self.utils.validate_columns_values("SUBTOTAL_SET", refund_line_item.get_subtotal_set())
            self.utils.validate_columns_values("TOTAL_TAX", refund_line_item.get_total_tax())
            self.utils.validate_columns_values("TOTAL_TAX_SET", refund_line_item.get_total_tax_set())

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_REFUNDS_LINE_ITEMS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Refund Line Item Inserted...\t\tOrder: {order_id}\tItem: {line_item_id}")
            else:
                if "1062" in result_string or "Duplicate entry" in result_string:
                    if "PRIMARY" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            print(f"[INFO] Trying to insert Refund Line Item again...\tOrder: {order_id}\tItem: {line_item_id}.Try: {count_try}")
                            super().generate_next_id()
                            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_REFUND_LINE_ITEMS(), self.utils.get_columns_array(), self.utils.get_values_array())

                            if result_flag == True:
                                print(f"[INFO] Refund Line Item Inserted...\t\t\tOrder: {order_id}\tItem: {line_item_id}")
                                keep_trying = False
                            else:
                                if "ID" in result_string:
                                    print(f"[INFO] Updating Refund Line Item...\t\tOrder: {order_id}\tItem: {line_item_id}")
                                    condition = f"1=1"
                                    condition += f"\nAND ID = '{id}'"
                                    condition += f"\nAND REFUND_ID = '{refund_id}'" if refund_id is not None and refund_id != "" else ""
                                    condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
                                    condition += f"\nAND LINE_ITEM_ID = '{line_item_id}'" if line_item_id is not None and line_item_id != "" else ""
                                    result_flag, rowcount, result_string = super().update_record(super().get_tbl_REFUND_LINE_ITEMS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

                                    if result_flag == True:
                                        print(f"[INFO] Refund Line Item Updated...\t\tOrder: {order_id}\tItem: {line_item_id}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Refund Line Item NOT Inserted/Maximum tries...\tOrder: {order_id}\tItem: {line_item_id}.Try: {count_try}")
                                            keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Refund Line Item NOT Inserted/Maximum tries...\tOrder: {order_id}\tItem: {line_item_id}.Try: {count_try}")
                                        keep_trying = False
                        else:
                            print(f"[INFO] Updating Refund Line Item...\t\tOrder: {order_id}\tItem: {line_item_id}")
                            condition = f"1=1"
                            condition += f"\nAND ID = '{id}'"
                            condition += f"\nAND REFUND_ID = '{refund_id}'" if refund_id is not None and refund_id != "" else ""
                            condition += f"\nAND ORDER_ID = '{order_id}'" if order_id is not None and order_id != "" else ""
                            condition += f"\nAND LINE_ITEM_ID = '{line_item_id}'" if line_item_id is not None and line_item_id != "" else ""
                            result_flag, rowcount, result_string = super().update_record(super().get_tbl_REFUND_LINE_ITEMS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)
                    else:
                        print(f"[ERROR] Error inserting Refund Line Item...\tOrder: {order_id}\tItem: {line_item_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished inserting Refund Line Item")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount, id, refund_id, order_id, line_item_id, keep_trying, count_try, maximum_insert_try, condition
            except:
                pass

    def update_refund(self, refund: Refund):
        # print(f"\n[INFO] BEGIN - Updating Refund")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        order_id = None
        refund_id = None

        try:
            refund_id = refund.get_id()
            order_id = refund.get_order_id()
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ID", refund_id)
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("ADMIN_GRAPHQL_API_ID", refund.get_admin_graphql_api_id())
            self.utils.validate_columns_values("CREATED_AT", refund.get_created_at())
            self.utils.validate_columns_values("NOTE", refund.get_note())
            self.utils.validate_columns_values("PROCESSED_AT", refund.get_processed_at())
            self.utils.validate_columns_values("RESTOCK", refund.get_restock())
            self.utils.validate_columns_values("TOTAL_DUTIES_SET", refund.get_total_duties_set())
            self.utils.validate_columns_values("USER_ID", refund.get_user_id())
            self.utils.validate_columns_values("ORDER_ADJUSTMENTS", refund.get_order_adjustments())
            self.utils.validate_columns_values("TRANSACTIONS", refund.get_transactions())
            self.utils.validate_columns_values("DUTIES", refund.get_duties())

            condition = f"ID = '{refund_id}'"
            condition += f"\nAND ORDER_ID = '{order_id}'"

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_REFUNDS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Refund Updated...\t\t\tOrder: {order_id}\tRefund: {refund_id}")
            else:
                print(f"[ERROR] Error updating Refund...\t\tOrder: {order_id}\tRefund: {refund_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished updating Refund")
            # print("[INFO] Cleaning up variables")
            try:
                del condition, result_flag, result_string, rowcount, order_id, refund_id
            except:
                pass

    def update_refund_line_item(self, line_item: RefundLineItem):
        # print(f"\n[INFO] BEGIN - Updating Refund Line Item")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        id = None
        refund_id = None
        order_id = None
        line_item_id = None

        try:
            id = line_item.get_id()
            refund_id = line_item.get_refund_id()
            order_id = line_item.get_order_id()
            line_item_id = line_item.get_line_item_id()
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ID", id)
            self.utils.validate_columns_values("REFUND_ID", refund_id)
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("LINE_ITEM_ID", line_item_id)
            self.utils.validate_columns_values("LOCATION_ID", line_item.get_location_id())
            self.utils.validate_columns_values("PRODUCT_ID", line_item.get_product_id())
            self.utils.validate_columns_values("QUANTITY", line_item.get_quantity())
            self.utils.validate_columns_values("RESTOCK_TYPE", line_item.get_restock_type())
            self.utils.validate_columns_values("SUBTOTAL", line_item.get_subtotal())
            self.utils.validate_columns_values("SUBTOTAL_SET", line_item.get_subtotal_set())
            self.utils.validate_columns_values("TOTAL_TAX", line_item.get_total_tax())
            self.utils.validate_columns_values("TOTAL_TAX_SET", line_item.get_total_tax_set())

            condition = f"1=1"
            condition += f"\nAND ID = '{id}'"
            condition += f"\nAND REFUND_ID = '{refund_id}'"
            condition += f"\nAND ORDER_ID = '{order_id}'"
            condition += f"\nAND LINE_ITEM_ID = '{line_item_id}'"

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_REFUNDS_LINE_ITEMS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Refund Line Item Updated...\t\tOrder: {order_id}\tItem: {line_item_id}")
            else:
                print(f"[ERROR] Error updating Refund Line Item...\tOrder: {order_id}\tItem: {line_item_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished updating Refund Line Item")
            # print("[INFO] Cleaning up variables")
            try:
                del condition, result_flag, result_string, rowcount, id, refund_id, order_id, line_item_id
            except:
                pass

    def upsert_refund(self, refund: Refund):
        # print(f"\n[INFO] BEGIN - Upserting Refund")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        refund_exists = False
        id = None
        order_id = None

        try:
            id = refund.get_id()
            order_id = refund.get_order_id()

            refund_exists = self.verify_refund_exists(id, order_id)

            if refund_exists:
                result_flag, rowcount, result_string = self.update_refund(refund)
            else:
                result_flag, rowcount, result_string = self.insert_refund(refund)

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Refund")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, refund_exists, id, order_id
            except:
                pass

    def upsert_refund_line_item(self, line_item: RefundLineItem):
        # print(f"\n[INFO] BEGIN - Upserting Refund Line Item")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        refund_line_item_exists = False
        id = None
        refund_id = None
        order_id = None
        line_item_id = None

        try:
            id = line_item.get_id()
            line_item_id = line_item.get_line_item_id()
            refund_id = line_item.get_refund_id()
            order_id = line_item.get_order_id()

            refund_line_item_exists = self.verify_refund_line_item_exists(id, refund_id, order_id, line_item_id)

            if refund_line_item_exists:
                result_flag, rowcount, result_string = self.update_refund_line_item(line_item)
            else:
                result_flag, rowcount, result_string = self.insert_refund_line_item(line_item)

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Refund Line Item")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, refund_line_item_exists, id, refund_id, order_id, line_item_id
            except:
                pass

    # API Functions
    def upsert_refund_api(self, refund_json):
        # print(f"\n[INFO] BEGIN - Upserting Refund")
        refund = self.Refund()
        result_flag = False
        final_result_flag = True
        rowcount = 0
        result_string = "Success"
        refund_exists = False
        refund_line_items = None
        line_items = None
        count_refunds = 0
        total_success_refunds = 0
        count_line_items = 0
        total_success_line_items = 0
        order_id = None
        refund_id = None
        product_id = None

        try:
            for refund_data in refund_json:
                count_refunds += 1
                refund_id = refund_data.get('id')
                order_id = refund_data.get('order_id')
                refund.set_id(refund_id)
                refund.set_order_id(order_id)
                refund.set_admin_graphql_api_id(self.utils.replace_special_chars(refund_data.get('admin_graphql_api_id')))
                refund.set_created_at(self.utils.replace_special_chars(refund_data.get('created_at')))
                refund.set_note(self.utils.replace_special_chars(refund_data.get('note')))
                refund.set_processed_at(self.utils.replace_special_chars(refund_data.get('processed_at')))
                refund.set_restock(self.utils.replace_special_chars(refund_data.get('restock')))
                refund.set_total_duties_set(self.utils.replace_special_chars(self.utils.convert_object_to_json(refund_data.get('total_duties_set', {}))))
                refund.set_user_id(self.utils.replace_special_chars(refund_data.get('user_id')))
                refund.set_order_adjustments(self.utils.replace_special_chars(self.utils.convert_object_to_json(refund_data.get('order_adjustments', []))))
                refund.set_transactions(self.utils.replace_special_chars(self.utils.convert_object_to_json(refund_data.get('transactions', []))))
                refund.set_duties(self.utils.replace_special_chars(self.utils.convert_object_to_json(refund_data.get('duties', []))))
                refund_line_items = refund_data.get('refund_line_items')

                result_flag, rowcount, result_string = self.upsert_refund(refund)

                if result_flag:
                    total_success_refunds += 1
                    for item in refund_line_items:
                        product_id = item.get('line_item').get('product_id', None)
                        count_line_items += 1
                        line_item = self.RefundLineItem()
                        line_item.set_id(item.get('id'))
                        line_item.set_refund_id(refund_id)
                        line_item.set_order_id(order_id)
                        line_item.set_line_item_id(item.get('line_item_id'))
                        line_item.set_product_id(product_id)
                        line_item.set_quantity(item.get('quantity'))
                        line_item.set_restock_type(item.get('restock_type'))
                        line_item.set_subtotal(item.get('subtotal'))
                        line_item.set_subtotal_set(self.utils.replace_special_chars(self.utils.convert_object_to_json(item.get('subtotal_set', {}))))
                        line_item.set_total_tax(item.get('total_tax'))
                        line_item.set_total_tax(item.get('total_tax'))
                        line_item.set_total_tax_set(self.utils.replace_special_chars(self.utils.convert_object_to_json(item.get('total', {}))))

                        result_flag, rowcount, result_string = self.upsert_refund_line_item(line_item)
                        if result_flag:
                            total_success_line_items += 1
                        else:
                            final_result_flag = False
                else:
                    final_result_flag = False

            if final_result_flag:
                # print(f"[INFO] A total of {total_success_refunds} Refunds and {total_success_line_items} Line Items was/were upserted.")
                return final_result_flag, 200, total_success_refunds, f"Success. {total_success_refunds} Refunds and {total_success_line_items} Line Items upserted."
            else:
                return final_result_flag, 500, 0, "Error upserting Refunds and Line Items."
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Refund")
            # print("[INFO] Cleaning up variables")
            try:
                del refund, result_flag, final_result_flag, rowcount, result_string, refund_exists, refund_line_items, line_items, count_refunds, total_success_refunds, count_line_items, total_success_line_items, product_id, order_id, refund_id
            except:
                pass

    def get_all_refunds_api(self):
        # print(f"\n[INFO] BEGIN - Getting all refunds")
        result_flag = False
        result = None

        try:
            result_flag, result = self.get_all_refunds()
            if result_flag:
                return result_flag, 200, result
            else:
                return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all refunds")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass

    def get_specific_refund_api(self, order_id):
        # print(f"\n[INFO] BEGIN - Getting specific refund")
        result_flag = False
        result = None
        refund_exists = False

        try:
            refund_exists = self.verify_refund_exists(order_id)
            if not refund_exists:
                return False, 404, "Refund not found!"
            else:
                result_flag, result = self.get_specific_refund(order_id)
                if result_flag:
                    return result_flag, 200, result
                else:
                    return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific refund")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass

