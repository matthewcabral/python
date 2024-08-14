from database.DataController import DataController
from utils.UtilsController import *
from utils.LogsController import *

class DiscountApplicationsController(DataController):
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "DiscountApplicationsController"

    def get_module_name(self):
        return self.module_name

    class DiscountApplication:
        def __init__(self):
            self.order_id = None
            self.code = None
            self.type = None
            self.value = None
            self.value_type = None
            self.target_type = None
            self.target_selection = None
            self.allocation_method = None

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

        def get_value(self):
            return self.value

        def set_value(self, value):
            self.value = value

        def get_value_type(self):
            return self.value_type

        def set_value_type(self, value_type):
            self.value_type = value_type

        def get_target_type(self):
            return self.target_type

        def set_target_type(self, target_type):
            self.target_type = target_type

        def get_target_selection(self):
            return self.target_selection

        def set_target_selection(self, target_selection):
            self.target_selection = target_selection

        def get_allocation_method(self):
            return self.allocation_method

        def set_allocation_method(self, allocation_method):
            self.allocation_method = allocation_method

    def verify_discount_application_exists(self, order_id, code):
        columns = ["COUNT(*) AS TOTAL"]
        condition = f"ORDER_ID = '{order_id}'"
        condition += f"\nAND CODE = '{code}'"
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_DISCOUNT_APPLICATIONS(), columns, condition)

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
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    # Database Functions
    def get_all_discount_applications(self):
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        discount_applications = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_DISCOUNT_APPLICATIONS(), columns, condition)

            if result_flag:
                for row in result_query:
                    discount_application = {
                        "order_id": row.get('ORDER_ID'),
                        "code": row.get('CODE'),
                        "type": row.get('TYPE'),
                        "value": row.get('VALUE'),
                        "value_type": row.get('VALUE_TYPE'),
                        "target_type": row.get('TARGET_TYPE'),
                        "target_selection": row.get('TARGET_SELECTION'),
                        "allocation_method": row.get('ALLOCATION_METHOD')
                    }
                    discount_applications.append(discount_application)
                return True, discount_applications
            else:
                return False, "Error getting discount applications"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            try:
                del columns, condition, result_flag, result_query, discount_applications
            except:
                pass

    def get_specific_discount_application(self, order_id, code):
        columns = ["*"]
        condition = f"ORDER_ID = '{order_id}'"
        condition += f"\nAND CODE = '{code}'"
        result_flag = False
        result_query = None
        discount_application = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_DISCOUNT_APPLICATIONS(), columns, condition)

            if result_flag:
                for row in result_query:
                    discount_application = {
                        "order_id": row.get('ORDER_ID'),
                        "code": row.get('CODE'),
                        "type": row.get('TYPE'),
                        "value": row.get('VALUE'),
                        "value_type": row.get('VALUE_TYPE'),
                        "target_type": row.get('TARGET_TYPE'),
                        "target_selection": row.get('TARGET_SELECTION'),
                        "allocation_method": row.get('ALLOCATION_METHOD')
                    }
                return True, discount_application
            else:
                return False, "Error getting discount application"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            try:
                del columns, condition, result_flag, result_query, discount_application
            except:
                pass

    def upsert_discount_application(self, order_id, discount_application:DiscountApplication):
        result_flag = False
        rowcount = 0
        result_string = "Success"

        try:
            discount_application_exists = self.verify_discount_application_exists(order_id, discount_application.get_code())
            if discount_application_exists:
                result_flag, rowcount, result_string = self.update_discount_application(order_id, discount_application)
            else:
                result_flag, rowcount, result_string = self.insert_discount_application(order_id, discount_application)

            if result_flag:
                return result_flag, rowcount, result_string
            else:
                return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            try:
                del discount_application, result_flag, rowcount, result_string
            except:
                pass

    def insert_discount_application(self, order_id, discount_application:DiscountApplication):
        result_flag = False
        result_string = "Success"
        rowcount = 0
        keep_trying = True
        count_try = 0
        maximum_insert_try = 100
        code = None
        condition = ""

        if order_id is None or order_id == "" or discount_application is None:
            return False, rowcount, "Order ID and Discount Application are required."

        try:
            code = discount_application.get_code()
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("CODE", code)
            self.utils.validate_columns_values("TYPE", discount_application.get_type())
            self.utils.validate_columns_values("VALUE", discount_application.get_value())
            self.utils.validate_columns_values("VALUE_TYPE", discount_application.get_value_type())
            self.utils.validate_columns_values("TARGET_TYPE", discount_application.get_target_type())
            self.utils.validate_columns_values("TARGET_SELECTION", discount_application.get_target_selection())
            self.utils.validate_columns_values("ALLOCATION_METHOD", discount_application.get_allocation_method())

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_DISCOUNT_APPLICATIONS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Discount Application Inserted...\t\tOrder: {order_id}")
            else:
                if "1062" in result_string or "Duplicate entry" in result_string:
                    if "PRIMARY" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            print(f"[INFO] Trying to insert Discount Apps again...\tOrder: {order_id}.Try: {count_try}")
                            super().generate_next_id()
                            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_DISCOUNT_APPLICATIONS(), self.utils.get_columns_array(), self.utils.get_values_array())

                            if result_flag == True:
                                print(f"[INFO] Discount Applications Inserted...\t\tOrder: {order_id}")
                                keep_trying = False
                            else:
                                if "ID" in result_string:
                                    print(f"[INFO] Updating Discount Applications...\t\tOrder: {order_id}")
                                    condition = "1=1"
                                    condition += f"\nAND ORDER_ID = '{order_id}'"
                                    condition += f"\nAND CODE = '{code}'"
                                    result_flag, rowcount, result_string = super().update_record(super().get_tbl_DISCOUNT_APPLICATIONS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

                                    if result_flag == True:
                                        print(f"[INFO] Discount Applications Updated...\t\tOrder: {order_id}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Discount Applications NOT Inserted/Maximum tries...\tOrder: {order_id}.Try: {count_try}")
                                            keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Discount Applications NOT Inserted/Maximum tries...\tOrder: {order_id}.Try: {count_try}")
                                        keep_trying = False
                    else:
                        print(f"[INFO] Updating Discount Applications...\t\tOrder: {order_id}")
                        condition = "1=1"
                        condition += f"\nAND ORDER_ID = '{order_id}'"
                        condition += f"\nAND CODE = '{code}'"
                        result_flag, rowcount, result_string = super().update_record(super().get_tbl_DISCOUNT_APPLICATIONS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)
                else:
                    print(f"[ERROR] Error inserting Discount Application...\tOrder: {order_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            try:
                del result_flag, rowcount, result_string, keep_trying, count_try, maximum_insert_try, code, condition
            except:
                pass

    def update_discount_application(self, order_id, discount_application:DiscountApplication):
        result_flag = False
        result_string = "Success"
        condition = f"ORDER_ID = '{order_id}'"
        condition += f"\nAND CODE = '{discount_application.get_code()}'"
        rowcount = 0

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("CODE", discount_application.get_code())
            self.utils.validate_columns_values("TYPE", discount_application.get_type())
            self.utils.validate_columns_values("VALUE", discount_application.get_value())
            self.utils.validate_columns_values("VALUE_TYPE", discount_application.get_value_type())
            self.utils.validate_columns_values("TARGET_TYPE", discount_application.get_target_type())
            self.utils.validate_columns_values("TARGET_SELECTION", discount_application.get_target_selection())
            self.utils.validate_columns_values("ALLOCATION_METHOD", discount_application.get_allocation_method())

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_DISCOUNT_APPLICATIONS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Discount Application Updated...\t\tOrder: {order_id}")
            else:
                print(f"[ERROR] Error updating Discount Application...\tOrder: {order_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            try:
                del result_flag, rowcount, result_string, condition
            except:
                pass

    def delete_discount_application(self, order_id, code):
        result_flag = False
        result_string = "Success"
        condition = f"ORDER_ID = '{order_id}'"
        condition += f"\nAND CODE = '{code}'"
        rowcount = 0

        try:
            result_flag, rowcount, result_string = super().delete_record(super().get_tbl_DISCOUNT_APPLICATIONS(), condition)

            if result_flag:
                print(f"[INFO] Discount Application Deleted...\t\tOrder: {order_id}")
            else:
                print(f"[ERROR] Error deleting Discount Application...\tOrder: {order_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            try:
                del result_flag, rowcount, result_string, condition
            except:
                pass

    # API Functions
    def upsert_discount_application_api(self, order_id, discount_application_json):
        discount_application = self.DiscountApplication()
        result_flag = False
        result_all_applications_flag = True
        rowcount = 0
        result_string = "Success"
        discount_application_exists = False

        if order_id is None or order_id == "":
            return False, 400, 0, "Order ID is required."

        try:
            for application in discount_application_json:
                code = application.get('code')
                type = application.get('type')
                value = application.get('value')
                value_type = application.get('value_type')
                target_type = application.get('target_type')
                target_selection = application.get('target_selection')
                allocation_method = application.get('allocation_method')

                discount_application.order_id = order_id
                discount_application.code = code
                discount_application.type = type
                discount_application.value = value
                discount_application.value_type = value_type
                discount_application.target_type = target_type
                discount_application.target_selection = target_selection
                discount_application.allocation_method = allocation_method

                result_flag, rowcount, result_string = self.upsert_discount_application(order_id, discount_application)

                if not result_flag:
                    result_all_applications_flag = False

            if result_all_applications_flag:
                return result_all_applications_flag, 200, rowcount, result_string
            else:
                return result_all_applications_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            try:
                del discount_application, result_flag, rowcount, result_string, result_all_applications_flag
            except:
                pass

    def add_discount_application(self, order_id, discount_application_json):
        discount_application = self.DiscountApplication()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_application_exists = False

        code = discount_application_json.get('code')
        type = discount_application_json.get('type')
        value = discount_application_json.get('value')
        value_type = discount_application_json.get('value_type')
        target_type = discount_application_json.get('target_type')
        target_selection = discount_application_json.get('target_selection')
        allocation_method = discount_application_json.get('allocation_method')

        try:
            discount_application_exists = self.verify_discount_application_exists(order_id, code)
            if discount_application_exists:
                return False, 409, 0, "Discount Application already exists"
            else:
                discount_application.order_id = order_id
                discount_application.code = code
                discount_application.type = type
                discount_application.value = value
                discount_application.value_type = value_type
                discount_application.target_type = target_type
                discount_application.target_selection = target_selection
                discount_application.allocation_method = allocation_method

                result_flag, rowcount, result_string = self.insert_discount_application(order_id, discount_application)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            try:
                del discount_application, result_flag, rowcount, result_string
            except:
                pass

    def edit_discount_application(self, order_id, discount_application_json):
        discount_application = self.DiscountApplication()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_application_exists = False

        code = discount_application_json.get('code')
        type = discount_application_json.get('type')
        value = discount_application_json.get('value')
        value_type = discount_application_json.get('value_type')
        target_type = discount_application_json.get('target_type')
        target_selection = discount_application_json.get('target_selection')
        allocation_method = discount_application_json.get('allocation_method')

        if order_id is None or order_id == "":
            return False, 400, 0, "Order ID is required."

        try:
            discount_application_exists = self.verify_discount_application_exists(order_id, code)
            if not discount_application_exists:
                return False, 404, 0, "Discount Application not found"
            else:
                discount_application.order_id = order_id
                discount_application.code = code
                discount_application.type = type
                discount_application.value = value
                discount_application.value_type = value_type
                discount_application.target_type = target_type
                discount_application.target_selection = target_selection
                discount_application.allocation_method = allocation_method

                result_flag, rowcount, result_string = self.update_discount_application(order_id, discount_application)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            try:
                del discount_application, result_flag, rowcount, result_string
            except:
                pass

    def delete_discount_application_api(self, order_id, code):
        result_flag = False
        rowcount = 0
        result_string = "Success"

        if order_id is None or order_id == "":
            return False, 400, 0, "Order ID is required."

        try:
            discount_application_exists = self.verify_discount_application_exists(order_id, code)
            if not discount_application_exists:
                return False, 404, 0, "Discount Application not found"
            else:
                result_flag, rowcount, result_string = self.delete_discount_application(order_id, code)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            try:
                del result_flag, rowcount, result_string
            except:
                pass
