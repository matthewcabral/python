from database.DataController import DataController
from utils.UtilsController import *
from utils.LogsController import *

class DiscountCodeController(DataController):
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "DiscountCodeController"

    def get_module_name(self):
        return self.module_name

    class DiscountCode:
        def __init__(self):
            self.row_id = None
            self.order_id = None
            self.code = None
            self.type = None
            self.amount = None

        def get_row_id(self):
            return self.row_id

        def get_order_id(self):
            return self.order_id

        def set_order_id(self, order_id):
            self.order_id = order_id

        def get_code(self):
            return self.code

        def set_code(self, code):
            self.code = code

        def get_type(self):
            return self.type

        def set_type(self, type):
            self.type = type

        def get_amount(self):
            return self.amount

        def set_amount(self, amount):
            self.amount = amount

    # Database Functions
    def get_all_discount_codes(self):
        # print(f"\n[INFO] BEGIN - Getting all discount codes")
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        discount_codes = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_DISCOUNT_CODES(), columns, condition)

            if result_flag:
                for row in result_query:
                    discount_code = {
                        "row_id": row.get('ROW_ID'),
                        "order_id": row.get('ORDER_ID'),
                        "code": row.get('CODE'),
                        "type": row.get('TYPE'),
                        "amount": row.get('AMOUNT')
                    }
                    discount_codes.append(discount_code)
                return True, discount_codes
            else:
                return False, "Error getting discount codes"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all discount codes")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, discount_codes
            except:
                pass

    def get_specific_discount_code(self, order_id, code):
        # print(f"\n[INFO] BEGIN - Getting specific discount code")
        columns = ["*"]
        condition = f"ORDER_ID = '{order_id}'"
        condition += f"\nAND CODE = '{code}'"
        result_flag = False
        result_query = None
        discount_code = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_DISCOUNT_CODES(), columns, condition)

            if result_flag:
                for row in result_query:
                    discount_code = {
                        "row_id": row.get('ROW_ID'),
                        "order_id": row.get('ORDER_ID'),
                        "code": row.get('CODE'),
                        "type": row.get('TYPE'),
                        "amount": row.get('AMOUNT')
                    }
                return True, discount_code
            else:
                return False, "Error getting discount code"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific discount code")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, discount_code
            except:
                pass

    def insert_discount_code(self, order_id, discount_code:DiscountCode):
        # print(f"\n[INFO] BEGIN - Inserting Discount Code")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        keep_trying = True
        count_try = 0
        maximum_insert_try = 100
        code = None
        condition = ""

        try:
            code = discount_code.get_code()
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ORDER_ID", self.utils.replace_special_chars(order_id))
            self.utils.validate_columns_values("CODE", self.utils.replace_special_chars(code))
            self.utils.validate_columns_values("TYPE", self.utils.replace_special_chars(discount_code.get_type()))
            self.utils.validate_columns_values("AMOUNT", self.utils.replace_special_chars(discount_code.get_amount()))

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_DISCOUNT_CODES(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Discount Code Inserted...\t\tOrder: {order_id}")
            else:
                if "1062" in result_string or "Duplicate entry" in result_string:
                    if "PRIMARY" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            print(f"[INFO] Trying to insert Discount Codes again...\tOrder: {order_id}.Try: {count_try}")
                            super().generate_next_id()
                            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_DISCOUNT_CODES(), self.utils.get_columns_array(), self.utils.get_values_array())

                            if result_flag == True:
                                print(f"[INFO] Discount Codes Inserted...\t\t\tOrder: {order_id}")
                                keep_trying = False
                            else:
                                if "ID" in result_string:
                                    print(f"[INFO] Updating Discount Codes...\t\tOrder: {order_id}")
                                    condition = "1=1"
                                    condition += f"\nAND ORDER_ID = '{order_id}'"
                                    condition += f"\nAND CODE = '{code}'"
                                    result_flag, rowcount, result_string = super().update_record(super().get_tbl_DISCOUNT_CODES(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

                                    if result_flag == True:
                                        print(f"[INFO] Discount Codes Updated...\t\tOrder: {order_id}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Discount Codes NOT Inserted/Maximum tries...\tOrder: {order_id}.Try: {count_try}")
                                            keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Discount Codes NOT Inserted/Maximum tries...\tOrder: {order_id}.Try: {count_try}")
                                        keep_trying = False
                    else:
                        print(f"[INFO] Updating Discount Codes...\t\tOrder: {order_id}")
                        condition = "1=1"
                        condition += f"\nAND ORDER_ID = '{order_id}'"
                        condition += f"\nAND CODE = '{code}'"
                        result_flag, rowcount, result_string = super().update_record(super().get_tbl_DISCOUNT_CODES(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)
                else:
                    print(f"[ERROR] Error inserting Discount Code...\tOrder: {order_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            # print(f"[INFO] END - Finished inserting Discount Code")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount, keep_trying, count_try, maximum_insert_try, code, condition
            except:
                pass

    def update_discount_code(self, order_id, discount_code:DiscountCode):
        # print(f"\n[INFO] BEGIN - Updating Discount Code")
        result_flag = False
        result_string = "Success"
        condition = f"ORDER_ID = '{order_id}'"
        condition += f"\nAND CODE = '{discount_code.get_code()}'"
        rowcount = 0

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("CODE", self.utils.replace_special_chars(discount_code.get_code()))
            self.utils.validate_columns_values("TYPE", self.utils.replace_special_chars(discount_code.get_type()))
            self.utils.validate_columns_values("AMOUNT", self.utils.replace_special_chars(discount_code.get_amount()))

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_DISCOUNT_CODES(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Discount Code Updated...\t\t\tOrder: {order_id}")
            else:
                print(f"[ERROR] Error updating Discount Code...\t\tOrder: {order_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            # print(f"[INFO] END - Finished updating Discount Code")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, condition
            except:
                pass

    def delete_discount_code(self, order_id, code):
        # print(f"\n[INFO] BEGIN - Deleting Discount Code")
        result_flag = False
        result_string = "Success"
        condition = f"ORDER_ID = '{order_id}'"
        condition += f"\nAND CODE = '{code}'"
        rowcount = 0

        try:
            result_flag, rowcount, result_string = super().delete_record(super().get_tbl_DISCOUNT_CODES(), condition)
            if result_flag:
                print(f"[INFO] Discount Code Deleted...\t\tOrder: {order_id}")
            else:
                print(f"[ERROR] Error deleting Discount Code...\tOrder: {order_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            # print(f"[INFO] END - Finished deleting Discount Code")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, condition
            except:
                pass

    def verify_discount_code_exists(self, order_id, code):
        # print(f"\n[INFO] BEGIN - Verifying Discount Code exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = f"ORDER_ID = '{order_id}'"
        condition += f"\nAND CODE = '{code}'"
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_DISCOUNT_CODES(), columns, condition)

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

    def upsert_discount_code(self, order_id, discount_code:DiscountCode):
        # print(f"\n[INFO] BEGIN - Upserting Discount Code")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_code_exists = False

        try:
            code = discount_code.get_code()
            discount_code_exists = self.verify_discount_code_exists(order_id, code)
            if discount_code_exists:
                result_flag, rowcount, result_string = self.update_discount_code(order_id, discount_code)
            else:
                result_flag, rowcount, result_string = self.insert_discount_code(order_id, discount_code)

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Discount Code")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, discount_code_exists
            except:
                pass

    # API Functions
    def upsert_discount_code_api(self, order_id, discount_code_json):
        discount_code = self.DiscountCode()
        result_flag = False
        result_all_codes_flag = True
        rowcount = 0
        result_string = "Success"
        discount_code_exists = False

        if order_id is None or order_id == "":
            return False, 400, 0, "Order ID is required."

        try:
            for codes in discount_code_json:
                code = codes.get('code')
                type = codes.get('type')
                amount = codes.get('amount')

                discount_code.order_id = order_id
                discount_code.code = code
                discount_code.type = type
                discount_code.amount = amount

                result_flag, rowcount, result_string = self.upsert_discount_code(order_id, discount_code)

                if not result_flag:
                    result_all_codes_flag = False

            if result_all_codes_flag:
                return result_all_codes_flag, 200, rowcount, result_string
            else:
                return result_all_codes_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Discount Code")
            # print("[INFO] Cleaning up variables")
            try:
                del discount_code, result_flag, rowcount, result_string, created_by, last_upd_by, modification_num, code, type, amount, result_all_codes_flag
            except:
                pass

    def add_discount_code(self, order_id, discount_code_json):
        discount_code = self.DiscountCode()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_code_exists = False

        
        code = discount_code_json.get('code')
        type = discount_code_json.get('type')
        amount = discount_code_json.get('amount')

        try:
            discount_code_exists = self.verify_discount_code_exists(order_id, code)
            if discount_code_exists:
                return False, 409, 0, "Discount Code already exists"
            else:
                discount_code.created_by = created_by
                discount_code.last_upd_by = last_upd_by
                discount_code.modification_num = modification_num
                discount_code.order_id = order_id
                discount_code.code = code
                discount_code.type = type
                discount_code.amount = amount

                result_flag, rowcount, result_string = self.insert_discount_code(order_id, discount_code)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished adding Discount Code")
            # print("[INFO] Cleaning up variables")
            try:
                del discount_code, result_flag, rowcount, result_string, created_by, last_upd_by, modification_num, order_id, code, type, amount
            except:
                pass

    def edit_discount_code(self, order_id, discount_code_json):
        discount_code = self.DiscountCode()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_code_exists = False

        code = discount_code_json.get('code')
        type = discount_code_json.get('type')
        amount = discount_code_json.get('amount')

        if order_id is None or order_id == "":
            return False, 400, 0, "Order ID is required."

        try:
            discount_code_exists = self.verify_discount_code_exists(order_id, code)
            if not discount_code_exists:
                return False, 404, 0, "Discount Code not found!"
            else:
                discount_code.created_by = created_by
                discount_code.last_upd_by = last_upd_by
                discount_code.modification_num = modification_num
                discount_code.order_id = order_id
                discount_code.code = code
                discount_code.type = type
                discount_code.amount = amount

                result_flag, rowcount, result_string = self.update_discount_code(order_id, discount_code)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished editing Discount Code")
            # print("[INFO] Cleaning up variables")
            try:
                del discount_code, result_flag, rowcount, result_string, created_by, last_upd_by, modification_num, order_id, code, type, amount
            except:
                pass

    def remove_discount_code(self, order_id, code):
        # print(f"\n[INFO] BEGIN - Removing Discount Code")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_code_exists = False

        if order_id is None or order_id == "":
            return False, 400, 0, "Order ID is required."

        try:
            discount_code_exists = self.verify_discount_code_exists(order_id, code)
            if not discount_code_exists:
                return False, 404, 0, "Discount Code not found!"
            else:
                result_flag, rowcount, result_string = self.delete_discount_code(order_id, code)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished removing Discount Code")
            # print("[INFO] Cleaning up variables")
            try:
                del discount_code_exists, result_flag, rowcount, result_string
            except:
                pass

    def get_all_discount_codes_api(self):
        # print(f"\n[INFO] BEGIN - Getting all discount codes")
        result_flag = False
        result = None

        try:
            result_flag, result = self.get_all_discount_codes()
            if result_flag:
                return result_flag, 200, result
            else:
                return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all discount codes")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass

    def get_specific_discount_code_api(self, order_id, code):
        # print(f"\n[INFO] BEGIN - Getting specific discount code")
        result_flag = False
        result = None
        discount_code_exists = False

        try:
            discount_code_exists = self.verify_discount_code_exists(order_id, code)
            if not discount_code_exists:
                return False, 404, "Discount Code not found!"
            else:
                result_flag, result = self.get_specific_discount_code(order_id)
                if result_flag:
                    return result_flag, 200, result
                else:
                    return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific discount code")
            # print("[INFO] Cleaning up variables")
            try:
                del discount_code_exists, result_flag, result
            except:
                pass
