from utils.UtilsController import *
from utils.LogsController import *
from apis.inbound.ShopifyController import *
from database.DataController import *
from system.ReportsController import *

class PhoneCasesController(DataController):

    def __init__(self):
        super().__init__()
        self.utils = UtilsController()
        self.shopify = ShopifyController()
        self.log = LogsController()
        self.reports = ReportsController()

        self.phone_cases_with_error_array = []
        self.phone_case_count = 0
        self.total_phone_case_count = 0
        self.total_phone_case_inserted_count = 0
        self.total_phone_case_updated_count = 0
        self.total_phone_case_deleted_count = 0
        self.total_phone_case_with_errors = 0

        self.batch_mapping = {
            'ROW_ID': 'batch_id',
            'BATCH_NUMBER': 'batch_number',
            'TOTAL_ORDERS': 'total_orders',
            'TOTAL_CASES': 'total_cases',
            'FIRST_ORDER_ID': 'first_order_id',
            'LAST_ORDER_ID': 'last_order_id',
            'COUNTRY_CODE': 'country_code',
            'PDFS_URL': 'pdf_url',
            'SLIPS_PDFS_URL': 'slip_pdf_url'
        }

        self.module_name = "OrderController"

    class PhoneCases():
        def __init__(self):
            self.PHONE_CASE_ID = None
            self.PHONE_CASE_NAME = None
            self.HAS_VARIANT_FLG = None
            self.VARIANT_NAME = None
            self.PHONE_CASE_FILE_PATH = None

        # GETTERS
        def get_PHONE_CASE_ID(self):
            return self.PHONE_CASE_ID

        def get_PHONE_CASE_NAME(self):
            return self.PHONE_CASE_NAME

        def get_HAS_VARIANT_FLG(self):
            return self.HAS_VARIANT_FLG

        def get_VARIANT_NAME(self):
            return self.VARIANT_NAME

        def get_PHONE_CASE_FILE_PATH(self):
            return self.PHONE_CASE_FILE_PATH

        # SETTERS
        def set_PHONE_CASE_ID(self, phone_case_id):
            self.PHONE_CASE_ID = phone_case_id

        def set_PHONE_CASE_NAME(self, phone_case_name):
            self.PHONE_CASE_NAME = phone_case_name

        def set_HAS_VARIANT_FLG(self, has_variant_flg):
            self.HAS_VARIANT_FLG = has_variant_flg

        def set_VARIANT_NAME(self, variant_name):
            self.VARIANT_NAME = variant_name

        def set_PHONE_CASE_FILE_PATH(self, phone_case_file_path):
            self.PHONE_CASE_FILE_PATH = phone_case_file_path

    class PhoneCaseBatch():

        def __init__(self):
            self.BATCH_NUMBER = None
            self.TOTAL_ORDERS = None
            self.TOTAL_CASES = None
            self.FIRST_ORDER_ID = None
            self.LAST_ORDER_ID = None
            self.COUNTRY_CODE = None
            self.PDFS_URL = None
            self.SLIPS_PDFS_URL = None

        def get_BATCH_NUMBER(self):
            return self.BATCH_NUMBER

        def get_TOTAL_ORDERS(self):
            return self.TOTAL_ORDERS

        def get_TOTAL_CASES(self):
            return self.TOTAL_CASES

        def get_FIRST_ORDER_ID(self):
            return self.FIRST_ORDER_ID

        def get_LAST_ORDER_ID(self):
            return self.LAST_ORDER_ID

        def get_COUNTRY_CODE(self):
            return self.COUNTRY_CODE

        def get_PDFS_URL(self):
            return self.PDFS_URL

        def get_SLIPS_PDFS_URL(self):
            return self.SLIPS_PDFS_URL

        def set_BATCH_NUMBER(self, batch_number):
            self.BATCH_NUMBER = batch_number

        def set_TOTAL_ORDERS(self, total_orders):
            self.TOTAL_ORDERS = total_orders

        def set_TOTAL_CASES(self, total_cases):
            self.TOTAL_CASES = total_cases

        def set_FIRST_ORDER_ID(self, first_order_id):
            self.FIRST_ORDER_ID = first_order_id

        def set_LAST_ORDER_ID(self, last_order_id):
            self.LAST_ORDER_ID = last_order_id

        def set_COUNTRY_CODE(self, country_code):
            self.COUNTRY_CODE = country_code

        def set_PDFS_URL(self, pdfs_url):
            self.PDFS_URL = pdfs_url

        def set_SLIPS_PDFS_URL(self, slips_pdfs_url):
            self.SLIPS_PDFS_URL = slips_pdfs_url

    def get_batch_mapping(self):
        return self.batch_mapping

    def get_batch_mapping_key(self, key):
        return self.batch_mapping.get(key)

    def get_batch_mapping_value(self, value):
        return next((k for k, v in self.batch_mapping.items() if v == value), None)

    def get_batch_mapping_keys(self):
        return self.batch_mapping.keys()

    def get_batch_mapping_values(self):
        return self.batch_mapping.values()

    def get_batch_mapping_items(self):
        return self.batch_mapping.items()

    # GETTERS
    def get_phone_case_count(self):
        return self.phone_case_count

    def get_total_phone_case_count(self):
        return self.total_phone_case_count

    def get_total_phone_case_inserted_count(self):
        return self.total_phone_case_inserted_count

    def get_total_phone_case_updated_count(self):
        return self.total_phone_case_updated_count

    def get_total_phone_case_deleted_count(self):
        return self.total_phone_case_deleted_count

    def get_total_phone_case_with_errors(self):
        return self.total_phone_case_with_errors

    def get_phone_cases_with_error_array(self):
        return self.phone_cases_with_error_array

    def get_module_name(self):
        return self.module_name

    # SETTERS
    def set_phone_case_count(self, count):
        self.phone_case_count = count

    def set_total_phone_case_count(self, count):
        self.total_phone_case_count = count

    def set_total_phone_case_inserted_count(self, count):
        self.total_phone_case_inserted_count = count

    def set_total_phone_case_updated_count(self, count):
        self.total_phone_case_updated_count = count

    def set_total_phone_case_deleted_count(self, count):
        self.total_phone_case_deleted_count = count

    def set_total_phone_case_with_errors(self, count):
        self.total_phone_case_with_errors = count

    # APPENDS
    def append_phone_cases_with_error_array(self, phone_case_id):
        self.phone_cases_with_error_array.append(phone_case_id)

    # DATABASE FUNCTIONS
    def count_phone_cases(self, phone_case_id, has_variant, variant_name):
        columns = ["COUNT(*) AS TOTAL"]
        self.utils.clear_condition()
        self.utils.set_condition(f"PHONE_CASE_ID = '{phone_case_id}'")

        if has_variant:
            if variant_name is not None and variant_name != "":
                self.utils.set_condition(f"{self.utils.get_condition()}\nAND VARIANT_NAME = '{variant_name}'")
            else:
                self.utils.set_condition(f"{self.utils.get_condition()}\nAND HAS_VARIANT_FLG = TRUE")

        if has_variant == False:
            self.utils.set_condition(f"{self.utils.get_condition()}\nAND HAS_VARIANT_FLG = FALSE")

        result_flag, result_query = super().query_record(super().get_tbl_PHONE_CASE_IMAGES(), columns, self.utils.get_condition())

        if result_flag:
            for row in result_query:
                return row.get("TOTAL")
        else:
            return 0

    def insert_phone_case_batch(self, phone_case_batch:PhoneCaseBatch):
        print(f"\n[INFO] BEGIN - Inserting Phone Case Batch...")
        print(f"[INFO] Total Orders: {phone_case_batch.get_TOTAL_ORDERS()}")
        print(f"[INFO] Total Cases: {phone_case_batch.get_TOTAL_CASES()}")
        print(f"[INFO] First Order Id: {phone_case_batch.get_FIRST_ORDER_ID()}")
        print(f"[INFO] Last Order Id: {phone_case_batch.get_LAST_ORDER_ID()}")

        # Variables
        batch_inserted_flag = False
        batch_inserted_count = 0
        result_string = ""
        columns = ["TOTAL_ORDERS", "TOTAL_CASES", "FIRST_ORDER_ID", "LAST_ORDER_ID", "COUNTRY_CODE"]
        values = [phone_case_batch.get_TOTAL_ORDERS(), phone_case_batch.get_TOTAL_CASES(), phone_case_batch.get_FIRST_ORDER_ID(), phone_case_batch.get_LAST_ORDER_ID(), phone_case_batch.get_COUNTRY_CODE()]

        try:
            batch_inserted_flag, batch_inserted_count, result_string  = super().insert_record(super().get_tbl_PHONE_CASE_BATCH(), columns, values)

            return batch_inserted_flag, batch_inserted_count, result_string
        except Exception as e:
            return False, 0, f"{str(e)}"
        finally:
            print(f"[INFO] Clearing Variables...")
            try:
                del columns
                del values
                del batch_inserted_flag
                del batch_inserted_count
                del result_string
            except:
                pass
            print(f"[INFO] END - Inserting Phone Case Batch...")

    def update_phone_case_batch(self, phone_case_batch_number, phone_case_batch:PhoneCaseBatch):
        print(f"\n[INFO] BEGIN - Updating Phone Case Batch... Batch Number: {phone_case_batch_number}")
        print(f"[INFO] Total Orders: {phone_case_batch.get_TOTAL_ORDERS()}")
        print(f"[INFO] Total Cases: {phone_case_batch.get_TOTAL_CASES()}")
        print(f"[INFO] First Order Id: {phone_case_batch.get_FIRST_ORDER_ID()}")
        print(f"[INFO] Last Order Id: {phone_case_batch.get_LAST_ORDER_ID()}")
        print(f"[INFO] PDFs URL: {phone_case_batch.get_PDFS_URL()}")
        print(f"[INFO] Slips PDFs URL: {phone_case_batch.get_SLIPS_PDFS_URL()}")

        # Variables
        batch_updated_flag = False
        batch_updated_count = 0
        result_string = ""
        columns = ["PDFS_URL", "SLIPS_PDFS_URL"]
        values = [phone_case_batch.get_PDFS_URL(), phone_case_batch.get_SLIPS_PDFS_URL()]
        condition = "1=1"
        condition += f"\nAND BATCH_NUMBER = {phone_case_batch_number}"

        try:
            batch_updated_flag, batch_updated_count, result_string  = super().update_record(super().get_tbl_PHONE_CASE_BATCH(), columns, values, condition)
            print(f"[INFO] Batch Updated: {batch_updated_flag}")
            return batch_updated_flag, batch_updated_count, result_string
        except Exception as e:
            print(f"[ERROR] {str(e)}")
            return False, 0, f"{str(e)}"
        finally:
            print(f"[INFO] END - Updating Phone Case Batch...")
            print(f"[INFO] Clearing Variables...")
            try:
                del columns, values, batch_updated_flag, batch_updated_count, result_string
            except:
                pass

    def upsert_phone_cases(self, phone_cases:PhoneCases):
        phone_cases_upserted_flag = False # Upsert means: Inserted or Updated
        maximum_insert_try = 100
        keep_trying = True
        count_try = 0
        phone_cases_inserted_count = 0
        phone_cases_updated_count = 0
        columns = None
        rows = None

        self.utils.clear_columns_values_arrays()

        phone_case_id = phone_cases.get_PHONE_CASE_ID()
        phone_case_name = phone_cases.get_PHONE_CASE_NAME()
        has_variant = phone_cases.get_HAS_VARIANT_FLG()
        variant_name = phone_cases.get_VARIANT_NAME()
        phone_case_file_path = phone_cases.get_PHONE_CASE_FILE_PATH()

        self.utils.validate_columns_values("PHONE_CASE_ID", phone_case_id)
        self.utils.validate_columns_values("PHONE_CASE_NAME", phone_case_name)
        self.utils.validate_columns_values("HAS_VARIANT_FLG", has_variant)
        self.utils.validate_columns_values("VARIANT_NAME", variant_name)
        self.utils.validate_columns_values("PHONE_CASE_FILE_PATH", phone_case_file_path)

        self.set_phone_case_count(0)
        self.set_phone_case_count(self.count_phone_cases(phone_case_id=phone_case_id, has_variant=has_variant, variant_name=variant_name))

        if self.get_phone_case_count() <= 0:
            print(f"[INFO] Inserting Phone Case...\t\t\tId: {phone_case_id}")
            phone_cases_upserted_flag, phone_cases_inserted_count, result_string = super().insert_record(super().get_tbl_PHONE_CASE_IMAGES(), self.utils.get_columns_array(), self.utils.get_values_array())

            if phone_cases_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                if "PRIMARY" in result_string:
                    while keep_trying:
                        count_try = count_try + 1
                        print(f"[INFO] Trying to insert Phone Case again...\t\tId: {phone_case_id}.Try: {count_try}")
                        super().generate_next_id()
                        phone_cases_upserted_flag, phone_cases_inserted_count, result_string = super().insert_record(super().get_tbl_PHONE_CASE_IMAGES(), self.utils.get_columns_array(), self.utils.get_values_array())

                        if phone_cases_upserted_flag == True:
                            print(f"[INFO] Phone Case Inserted...\t\t\tId: {phone_case_id}")
                            keep_trying = False
                        else:
                            if "PHONE_CASE_ID" in result_string:
                                print(f"[INFO] Updating Phone Case...\t\t\tId: {phone_case_id}")
                                self.utils.clear_condition()
                                self.utils.set_condition(f"PHONE_CASE_ID = '{phone_case_id}'")
                                if has_variant:
                                    if variant_name is not None or variant_name != "":
                                        self.utils.set_condition(f"{self.utils.get_condition()}\nAND VARIANT_NAME = '{variant_name}'")
                                    else:
                                        self.utils.set_condition(f"{self.utils.get_condition()}\nAND VARIANT_NAME IS NULL")
                                phone_cases_upserted_flag, phone_cases_updated_count, result_string = super().update_record(super().get_tbl_PHONE_CASE_IMAGES(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                                if phone_cases_upserted_flag == True:
                                    print(f"[INFO] Phone Case Updated...\t\t\t\tId: {phone_case_id}")
                                    keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Phone Case NOT Inserted because of Maximum tries...\tId: {phone_case_id}.Try: {count_try}")
                                        keep_trying = False
                            else:
                                if count_try >= maximum_insert_try:
                                    print(f"[INFO] Phone Case NOT Inserted because of Maximum tries...\tId: {phone_case_id}.Try: {count_try}")
                                    keep_trying = False
                else:
                    print(f"[INFO] Updating Phone Case...\t\t\tId: {phone_case_id}")
                    self.utils.clear_condition()
                    self.utils.set_condition(f"PHONE_CASE_ID = '{phone_case_id}'")
                    if has_variant:
                        if variant_name is not None or variant_name != "":
                            self.utils.set_condition(f"{self.utils.get_condition()}\nAND VARIANT_NAME = '{variant_name}'")
                        else:
                            self.utils.set_condition(f"{self.utils.get_condition()}\nAND VARIANT_NAME IS NULL")
                    phone_cases_upserted_flag, phone_cases_updated_count, result_string = super().update_record(super().get_tbl_PHONE_CASE_IMAGES(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())
        else:
            print(f"[INFO] Updating Phone Case...\t\t\tId: {phone_case_id}")
            self.utils.clear_condition()
            self.utils.set_condition(f"PHONE_CASE_ID = '{phone_case_id}'")
            if has_variant:
                if variant_name is not None or variant_name != "":
                    self.utils.set_condition(f"{self.utils.get_condition()}\nAND VARIANT_NAME = '{variant_name}'")
                else:
                    self.utils.set_condition(f"{self.utils.get_condition()}\nAND VARIANT_NAME IS NULL")
            phone_cases_upserted_flag, phone_cases_updated_count, result_string = super().update_record(super().get_tbl_PHONE_CASE_IMAGES(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

            if phone_cases_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                if "PHONE_CASE_ID" in result_string:
                    while keep_trying:
                        count_try = count_try + 1
                        print(f"[INFO] Trying to update Phone Case again...\t\tId: {phone_case_id}.Try: {count_try}")
                        phone_cases_upserted_flag, phone_cases_updated_count, result_string = super().update_record(super().get_tbl_PHONE_CASE_IMAGES(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                        if phone_cases_upserted_flag == True:
                            print(f"[INFO] Phone Case Updated...\t\t\tId: {phone_case_id}")
                            keep_trying = False
                        else:
                            if count_try >= maximum_insert_try:
                                print(f"[INFO] Phone Case NOT Updated because of Maximum tries...\tId: {phone_case_id}.Try: {count_try}")
                                keep_trying = False

        if phone_cases_upserted_flag == False:
            self.append_phone_cases_with_error_array(phone_case_id)
            self.set_total_phone_case_with_errors(self.get_total_phone_case_with_errors() + 1)

        self.set_total_phone_case_count(self.get_total_phone_case_count() + phone_cases_inserted_count + phone_cases_updated_count)
        self.set_total_phone_case_inserted_count(self.get_total_phone_case_inserted_count() + phone_cases_inserted_count)
        self.set_total_phone_case_updated_count(self.get_total_phone_case_updated_count() + phone_cases_updated_count)

        del phone_cases
        del maximum_insert_try
        del keep_trying
        del count_try
        del phone_cases_inserted_count
        del phone_cases_updated_count
        del columns
        del rows

        return phone_cases_upserted_flag, self.get_total_phone_case_count(), result_string

    def update_variant(self, phone_case_id, phone_case_name, current_variant_name, new_variant_name, new_variant_file_path):
        print("entrou no update_variant")
        try:
            self.utils.clear_condition()
            self.utils.set_condition(f"PHONE_CASE_ID = '{phone_case_id}'")
            self.utils.set_condition(f"{self.utils.get_condition()}\nAND VARIANT_NAME = '{current_variant_name}'")

            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("PHONE_CASE_NAME", phone_case_name)
            self.utils.validate_columns_values("VARIANT_NAME", new_variant_name)
            self.utils.validate_columns_values("PHONE_CASE_FILE_PATH", new_variant_file_path)
            print("condition: ", self.utils.get_condition())
            print("columns: ", self.utils.get_columns_array())
            print("values: ", self.utils.get_values_array())

            phone_cases_upserted_flag, phone_cases_updated_count, result_string = super().update_record(super().get_tbl_PHONE_CASE_IMAGES(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

            print("phone_cases_upserted_flag: ", phone_cases_upserted_flag)
            print("phone_cases_updated_count: ", phone_cases_updated_count)
            print("result_string: ", result_string)

            if phone_cases_upserted_flag == False:
                return False, 0, f"{result_string}"
            else:
                return True, phone_cases_updated_count, result_string
        except Exception as e:
            return False, 0, f"{str(e)}"
        finally:
            try:
                del phone_case_id
                del current_variant_name
                del new_variant_name
                del new_variant_file_path
            except:
                pass

    def update_phone_case_name(self, phone_case_id, phone_case_name):
        try:
            self.utils.clear_condition()
            self.utils.set_condition(f"PHONE_CASE_ID = '{phone_case_id}'")

            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("PHONE_CASE_NAME", phone_case_name)

            phone_cases_upserted_flag, phone_cases_updated_count, result_string = super().update_record(super().get_tbl_PHONE_CASE_IMAGES(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

            if phone_cases_upserted_flag == False:
                return False, 0, f"{result_string}"
            else:
                return True, phone_cases_updated_count, result_string
        except Exception as e:
            return False, 0, f"{str(e)}"
        finally:
            try:
                del phone_case_id
                del phone_case_name
            except:
                pass

    def query_phone_cases(self, type, phone_case_id, has_variant, variant_name, columns, condition):
        try:
            columns_array = ["PHONE_CASE_ID", "PHONE_CASE_NAME", "HAS_VARIANT_FLG", "VARIANT_NAME", "PHONE_CASE_FILE_PATH"] if columns is None or columns == "" else columns
            final_condition = "1=1"

            if type == "specific":
                final_condition += f"\nAND PHONE_CASE_ID = '{phone_case_id}'"

                if has_variant is not None:
                    if has_variant:
                        if variant_name is not None and variant_name != "":
                            final_condition += f"\nAND VARIANT_NAME = '{variant_name}'"
                        else:
                            final_condition += f"\nAND HAS_VARIANT_FLG = TRUE"

            final_condition += f"\n{condition}" if condition is not None and condition != "" else ""

            result_flag, result_query = super().query_record(super().get_tbl_PHONE_CASE_IMAGES(), columns_array, final_condition)

            if result_flag:
                return result_query
            else:
                return None
        except Exception as e:
            error_message = f"{str(e)}"
            additional_details = f"Phone Case Id: {phone_case_id}. Has Variant: {has_variant}. Variant Name: {variant_name}."
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="query_phone_cases", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
            return None
        finally:
            print("Clearing Variables...")
            try:
                del columns_array
                del final_condition
                del result_query
                del result_flag
            except:
                pass

    def delete_phone_cases(self, phone_case_id, has_variant, variant_name):
        phone_cases_deleted_flag = False
        phone_cases_deleted_count = 0
        condition = f"PHONE_CASE_ID = '{phone_case_id}'"

        if has_variant and (variant_name is not None and variant_name != ""):
            condition += f"\nAND VARIANT_NAME = '{variant_name}'"

        try:
            phone_cases_deleted_flag, phone_cases_deleted_count, result_string = super().delete_record(super().get_tbl_PHONE_CASE_IMAGES(), condition)

            if phone_cases_deleted_flag == False:
                return False, phone_cases_deleted_count, result_string
            else:
                return phone_cases_deleted_flag, phone_cases_deleted_count, result_string
        except Exception as e:
            return False, 0, f"{str(e)}"
        finally:
            del condition

    def temp_delete_phone_cases(self, phone_case_id):
        phone_cases_deleted_flag = False
        phone_cases_deleted_count = 0
        condition = f"PHONE_CASE_ID = '{phone_case_id}'"
        condition += f"\nAND HAS_VARIANT_FLAG = FALSE"

        try:
            phone_cases_deleted_flag, phone_cases_deleted_count, result_string = super().delete_record(super().get_tbl_PHONE_CASE_IMAGES(), condition)

            if phone_cases_deleted_flag == False:
                return False, phone_cases_deleted_count, result_string
            else:
                return phone_cases_deleted_flag, phone_cases_deleted_count, result_string
        except Exception as e:
            return False, 0, f"{str(e)}"
        finally:
            del condition

    def get_last_phone_case_batch_order_id(self, country_code):
        last_order_id = None
        columns = ["MAX(LAST_ORDER_ID) AS LAST_ORDER_ID"]
        condition = "1=1"
        if country_code is not None and country_code != "":
            if country_code == 'US':
                condition += f"\nAND COUNTRY_CODE = '{country_code}'" if country_code is not None and country_code != "" else ""
        result_flag, result_query = super().query_record(super().get_tbl_PHONE_CASE_BATCH(), columns, condition)

        if result_flag:
            for row in result_query:
                last_order_id = row.get("LAST_ORDER_ID")

            return str(last_order_id) if last_order_id is not None else "0"
        else:
            return "0"

    def get_last_phone_case_batch_number(self, country_code):
        last_batch_number = None
        row_id = None
        columns = ["BATCH_NUMBER", "ROW_ID"]
        condition = "1=1"
        condition += f"\nAND BATCH_NUMBER = (SELECT MAX(BATCH_NUMBER) FROM COMPANY_NAME.PHONE_CASE_BATCH)"
        if country_code is not None and country_code != "":
            if country_code == 'US':
                condition += f"\nAND COUNTRY_CODE = '{country_code}'" if country_code is not None and country_code != "" else ""
        result_query = None
        result_flag = False
        try:
            result_flag, result_query = super().query_record(super().get_tbl_PHONE_CASE_BATCH(), columns, condition)

            if result_flag:
                for row in result_query:
                    last_batch_number = row.get("BATCH_NUMBER")
                    row_id = row.get("ROW_ID")

                if last_batch_number is not None:
                    return last_batch_number, row_id
                else:
                    return "0", None
            else:
                return "0", None
        except Exception as e:
            return 0
        finally:
            try:
                del last_batch_number, row_id, columns, condition, result_query, result_flag
            except:
                pass

    def get_last_phone_case_batch_info(self, country_code):
        last_batch_number = None
        row_id = None
        columns = ["*"]
        condition = "1=1"
        condition += f"\nAND BATCH_NUMBER = (SELECT MAX(BATCH_NUMBER) FROM COMPANY_NAME.PHONE_CASE_BATCH)"

        if country_code is not None and country_code != "":
            if country_code == 'US':
                condition += f"\nAND COUNTRY_CODE = '{country_code}'" if country_code is not None and country_code != "" else ""

        result_query = None
        result_flag = False
        batch_info = []
        batch = {}
        len_batch_info = 0

        try:
            # print(f"condition: {condition}")
            result_flag, result_query = super().query_record(super().get_tbl_PHONE_CASE_BATCH(), columns, condition)

            if result_flag:
                len_batch_info = len(result_query)
                print(f"[INFO] Got {len_batch_info} batch items.")
                if len_batch_info == 0:
                    return False, 404, batch_info
                else:
                    for row in result_query:
                        batch = {}
                        if columns == ['*']:
                            for mapping_row in self.get_batch_mapping():
                                key = self.get_batch_mapping_key(mapping_row)
                                if key is not None:
                                    value = row.get(mapping_row)
                                    try:
                                        value = value.strftime('%Y-%m-%d') if isinstance(value, datetime.datetime) else value
                                    except:
                                        pass
                                    if "FLG" in mapping_row.upper():
                                        value = True if value == 1 else False
                                    if "TIME" in mapping_row.upper():
                                        if "UNIT" in mapping_row.upper():
                                            value = value if value is not None else "days"
                                        else:
                                            value = value if value is not None else 0
                                    if "JSON" in key.upper():
                                        value = self.utils.convert_json_to_object(value) if value is not None else None
                                    batch[key] = value
                        else:
                            for column in columns:
                                if column in self.get_batch_mapping():
                                    key = self.get_batch_mapping_key(column)
                                    if key is not None:
                                        value = row.get(column)
                                        try:
                                            value = value.strftime('%Y-%m-%d') if isinstance(value, datetime.datetime) else value
                                        except:
                                            pass
                                        if "FLG" in column.upper():
                                            value = True if value == 1 else False
                                        if "TIME" in column.upper():
                                            if "UNIT" in column.upper():
                                                value = value if value is not None else "days"
                                            else:
                                                value = value if value is not None else 0
                                        if "JSON" in key.upper():
                                            value = self.utils.convert_json_to_object(value) if value is not None else None
                                        batch[key] = value
                        batch_info.append(batch)

                    return True, 200, batch_info
            else:
                return False, 500, batch_info
        except Exception as e:
            print(f"[ERROR] {str(e)}")
            return False, 500, batch_info
        finally:
            try:
                del last_batch_number, row_id, columns, condition, result_query, result_flag
            except:
                pass

    def get_total_available_to_print(self):
        result_query = None
        result_flag = False
        return_code = 200
        total = {
            "total": 0
        }

        try:
            result_flag, return_code, result_query = self.reports.extract_report_data(report_type="PHONE_CASES_REPORTS", report_date=self.utils.get_current_date())

            if result_flag:
                total["total"] = int(result_query.get("total_orders_available_to_print"))
                return True, 200, total
            else:
                return False, return_code, result_query
        except Exception as e:
            print(f"[ERROR] {str(e)}")
            return False, 500, total
        finally:
            try:
                del last_batch_number, row_id, columns, condition, result_query, result_flag
            except:
                pass

    def get_all_phone_cases_ids(self):
        print(f"\n[INFO] BEGIN - Getting Phone Case Ids...")
        phone_case_ids = []

        columns = ["DISTINCT(PHONE_CASE_ID)"]
        result_flag, result_query = super().query_record(super().get_tbl_PHONE_CASE_IMAGES(), columns, None)

        if result_flag:
            for row in result_query:
                phone_case_ids.append(row.get("PHONE_CASE_ID"))

        print(f"[INFO] END - Got a Total of {len(phone_case_ids)} phone case ids...")

        return phone_case_ids

    # OTHER FUNCTIONS
    def process_result_query(self, result_query):
        phone_cases_array = []
        variants_array = {}
        has_variant_flag = False
        count_variants = 0
        found_phone_case = False

        try:
            for row in result_query:
                count_variants = 0
                found_phone_case = False
                has_variant_flag = True if row["HAS_VARIANT_FLG"] == 1 else False

                if has_variant_flag:
                    for phone_case in phone_cases_array:
                        if phone_case["phone_case_id"] == row["PHONE_CASE_ID"]:
                            found_phone_case = True
                            break
                        count_variants += 1

                    variants_array = {
                        "variant_name": row["VARIANT_NAME"],
                        "variant_file_path": row["PHONE_CASE_FILE_PATH"]
                    }

                    if found_phone_case:
                        if "variants" not in phone_cases_array[count_variants]:
                            phone_cases_array[count_variants]["variants"] = []

                        phone_cases_array[count_variants]["variants"].append(variants_array)
                    else:
                        phone_cases_array.append({
                            "phone_case_id": row["PHONE_CASE_ID"],
                            "phone_case_name": row["PHONE_CASE_NAME"],
                            "has_variant_flag": has_variant_flag,
                            "variants": [
                                variants_array
                            ],
                            "phone_case_file_path": None
                        })
                else:
                    phone_cases_array.append({
                        "phone_case_id": row["PHONE_CASE_ID"],
                        "phone_case_name": row["PHONE_CASE_NAME"],
                        "has_variant_flag": has_variant_flag,
                        "variants": [],
                        "phone_case_file_path": row["PHONE_CASE_FILE_PATH"]
                    })
            return phone_cases_array
        except Exception as e:
            print(f"[ERROR] {str(e)}")
            return phone_cases_array
        finally:
            del phone_cases_array
            del variants_array
            del has_variant_flag
            del count_variants
            del found_phone_case

    # WEBHOOK FUNCTIONS
    def api_save_phone_case(self, phone_case_json, function):
        # Initiating Variables (Do not remove because the variables are being removed from memory in the finally block)
        response_description = "Success"
        status_code = 200
        phone_case_id = None
        phone_case_name = None
        action_phone_case = None
        action_variant = None
        phone_case_file_path = None
        result_query = None
        result_string = None
        variants_array = []
        count_phone_cases = 0
        count_without_variants = 0
        count_with_variants = 0
        total_length = 0
        variants_removed_count = 0
        phone_case_upserted_count = 0
        phone_cases_deleted_count = 0
        has_variant_flag = False
        can_update_phone_case_flag = False
        phone_cases_upserted_flag = False
        phone_cases_deleted_flag = False
        variant_name = None
        current_variant_name = None

        try:
            if len(phone_case_json) == 0:
                return False, f"Empty JSON.", 400
            else:
                phone_case = phone_case_json

                phone_case_id = phone_case.get("phone_case_id")
                phone_case_name = self.utils.replace_special_chars(str(phone_case.get("phone_case_name")).replace("null", "None"))
                has_variant_flag = phone_case.get("has_variant_flag")
                phone_case_file_path = phone_case.get("phone_case_file_path")
                action_phone_case = phone_case.get("action")
                variants_array = phone_case.get("variants", [])

                count_phone_cases = self.count_phone_cases(phone_case_id=phone_case_id, has_variant=None, variant_name=None)
                count_without_variants = self.count_phone_cases(phone_case_id=phone_case_id, has_variant=False, variant_name=None)

                if function == "insert":
                    if count_phone_cases > 0 or count_without_variants > 0:
                        return False, f"Phone Case already exists with Id: {phone_case_id}", 409
                    if has_variant_flag and len(variants_array) <= 0:
                        return False, f"Variants not found for Phone Case Id: {phone_case_id}", 400
                elif function == "update":
                    if count_phone_cases <= 0:
                        return False, f"Phone Case Not Found with Id: {phone_case_id}", 404
                    if has_variant_flag and len(variants_array) <= 0:
                        return self.update_phone_case_name(phone_case_id=phone_case_id, phone_case_name=phone_case_name)
                else:
                    return False, f"Invalid Function: {function}", 400

                if has_variant_flag:
                    if function == "insert":
                        for variant in variants_array:
                            phone_case_obj = self.PhoneCases()
                            phone_case_obj.set_PHONE_CASE_ID(phone_case_id)
                            phone_case_obj.set_PHONE_CASE_NAME(phone_case_name)
                            phone_case_obj.set_HAS_VARIANT_FLG(has_variant_flag)
                            phone_case_obj.set_VARIANT_NAME(variant.get("variant_name"))
                            phone_case_obj.set_PHONE_CASE_FILE_PATH(variant.get("variant_file_path"))
                            phone_cases_upserted_flag, phone_case_upserted_count, result_string = self.upsert_phone_cases(phone_cases=phone_case_obj)
                            if phone_cases_upserted_flag == False:
                                return False, response_description, 400
                    else:
                        if count_without_variants > 0:
                            phone_cases_upserted_flag, phone_cases_deleted_count, response_description = self.delete_phone_cases(phone_case_id=phone_case_id, has_variant=False, variant_name=None)
                            if phone_cases_upserted_flag == False:
                                return False, response_description, 400

                        for variant in variants_array:
                            current_variant_name = variant.get("current_variant_name", None)
                            variant_name = variant.get("variant_name") if current_variant_name is None else current_variant_name
                            count_with_variants = self.count_phone_cases(phone_case_id=phone_case_id, has_variant=True, variant_name=variant_name)

                            if count_with_variants > 0:
                                phone_cases_upserted_flag, phone_case_upserted_count, result_string = self.update_variant(
                                    phone_case_id=phone_case_id,
                                    phone_case_name=phone_case_name,
                                    current_variant_name=variant_name,
                                    new_variant_name=variant.get("variant_name"),
                                    new_variant_file_path=variant.get("variant_file_path")
                                )
                            else:
                                if current_variant_name is not None:
                                    return False, f"Phone Case not found with Variant: {current_variant_name}", 404
                                else:
                                    phone_case_obj = self.PhoneCases()
                                    phone_case_obj.set_PHONE_CASE_ID(phone_case_id)
                                    phone_case_obj.set_PHONE_CASE_NAME(phone_case_name)
                                    phone_case_obj.set_HAS_VARIANT_FLG(has_variant_flag)
                                    phone_case_obj.set_VARIANT_NAME(variant.get("variant_name"))
                                    phone_case_obj.set_PHONE_CASE_FILE_PATH(variant.get("variant_file_path"))
                                    phone_cases_upserted_flag, phone_case_upserted_count, result_string = self.upsert_phone_cases(phone_cases=phone_case_obj)

                        if count_phone_cases > 0:
                            phone_cases_upserted_flag, phone_case_upserted_count, result_string = self.update_phone_case_name(phone_case_id=phone_case_id, phone_case_name=phone_case_name)
                else:
                    count_with_variants = self.count_phone_cases(phone_case_id=phone_case_id, has_variant=True, variant_name=None)

                    if count_with_variants > 0:
                        phone_cases_upserted_flag, phone_cases_deleted_count, response_description = self.delete_phone_cases(phone_case_id=phone_case_id, has_variant=True, variant_name=None)
                        if phone_cases_upserted_flag == False:
                            return False, response_description, 400

                    phone_case_obj = self.PhoneCases()
                    phone_case_obj.set_PHONE_CASE_ID(phone_case_id)
                    phone_case_obj.set_PHONE_CASE_NAME(phone_case_name)
                    phone_case_obj.set_HAS_VARIANT_FLG(has_variant_flag)
                    phone_case_obj.set_VARIANT_NAME(None)
                    phone_case_obj.set_PHONE_CASE_FILE_PATH(phone_case_file_path)
                    phone_cases_upserted_flag, phone_case_upserted_count, result_string = self.upsert_phone_cases(phone_cases=phone_case_obj)

                if phone_cases_upserted_flag == False:
                    status_code = 409 if "already exists" in result_string else 400
                    return False, result_string, status_code

                return phone_cases_upserted_flag, response_description, status_code
        except Exception as e:
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="webhook_save_phone_case", error_code=None, error_message=f"{str(e)}", additional_details=phone_case_json, error_severity=self.utils.get_error_severity(6))
            return False, f"{str(e)}", 400
        finally:
            try:
                del phone_case_id
                del phone_case_name
                del has_variant_flag
                del action_phone_case
                del action_variant
                del phone_case_file_path
                del variants_array
                del count_phone_cases
                del count_without_variants
                del count_with_variants
                del result_query
                del variants_removed_count
                del total_length
                del can_update_phone_case_flag
                del response_description
                del status_code
                del phone_case_obj
                del phone_cases_upserted_flag
                del phone_case_upserted_count
                del result_string
                del phone_cases_deleted_flag
                del phone_cases_deleted_count
                del variant_name
                del current_variant_name
            except:
                pass

    def api_get_all_phone_cases(self):
        response_description = "Success"
        status_code = 200
        response_json = {}

        try:
            result_query = self.query_phone_cases(type="*", phone_case_id=None, has_variant=None, variant_name=None, columns=None, condition=None)

            if result_query:
                response_json["phone_cases"] = self.process_result_query(result_query)
                response_json = json.dumps(response_json)

                return True, response_json, response_description, status_code
            else:
                return False, response_json, self.utils.get_error_message(500), 500
        except Exception as e:
            return False, response_json, f"{str(e)}", 400
        finally:
            try:
                del response_json
                del columns
                del result_query
                del response_description
                del status_code
            except:
                pass

    def api_get_specific_phone_case(self, phone_case_id):
        response_description = "Success"
        status_code = 200
        response_json = {}
        return_flag = False
        columns = ["PHONE_CASE_ID", "PHONE_CASE_NAME", "HAS_VARIANT_FLG", "VARIANT_NAME", "PHONE_CASE_FILE_PATH"]
        condition = f"PHONE_CASE_ID = '{phone_case_id}'"

        try:
            return_flag, result_query = super().query_record(super().get_tbl_PHONE_CASE_IMAGES(), columns, condition)

            if return_flag:
                response_json["phone_cases"] = self.process_result_query(result_query)
                response_json = json.dumps(response_json)

                return True, response_json, response_description, status_code
            else:
                return False, response_json, self.utils.get_error_message(500), 500
        except Exception as e:
            return False, response_json, f"{str(e)}", 400
        finally:
            del response_json
            del columns
            del condition
            del result_query
            del return_flag
            del response_description
            del status_code

    def api_delete_phone_case(self, phone_case_id, phone_case_variant_name=None):
        response_description = "Success"
        status_code = 200
        condition = f"PHONE_CASE_ID = '{phone_case_id}'"
        condition += f"\nAND VARIANT_NAME = '{phone_case_variant_name}'" if phone_case_variant_name is not None else ""

        try:
            phone_case_deleted_flag, phone_case_deleted_count, result_string = super().delete_record(super().get_tbl_PHONE_CASE_IMAGES(), condition)

            if phone_case_deleted_flag == False:
                response_description = f"{result_string}"
                status_code = 400
                return False, phone_case_deleted_count, response_description, status_code

            return phone_case_deleted_flag, phone_case_deleted_count, response_description, status_code
        except Exception as e:
            return False, 0, f"{str(e)}", 400
        finally:
            del condition
            del response_description
            del status_code

    def count_available_to_print(self):
        print(f"\n[INFO] BEGIN - Counting available to print")
        last_batch_order_id = self.get_last_phone_case_batch_order_id(country_code=None)
        return_flag = False
        total_orders = 0
        columns = ["COUNT(ID) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND ID > '{last_batch_order_id}'"
        condition += f"\nAND CLOSED_AT IS NULL"
        condition += f"\nAND CANCELLED_AT IS NULL"
        condition += f"\nAND PRINTED_DATE IS NULL"
        condition += f"\nAND (FULFILLMENT_STATUS IS NULL OR FULFILLMENT_STATUS = 'partial')"
        condition += f"\nAND FULFILLED_DATE IS NULL"
        condition += f"\nAND (TAGS LIKE '%has_phone_case%' AND (TAGS NOT LIKE '%DOUBLE_RESHIPPED%' AND TAGS NOT LIKE '%double_reshipped%' AND TAGS NOT LIKE '%DELIVERED%' AND TAGS NOT LIKE '%printed%' AND TAGS NOT LIKE '%is_colab_or_pr%' AND TAGS NOT LIKE '%custom%' AND TAGS NOT LIKE '%CUSTOM%'))"
        condition += f"\nAND CREATED <= DATE_SUB(NOW(), INTERVAL 120 MINUTE)"

        try:
            return_flag, result_query = super().query_record(super().get_tbl_ORDER(), columns, condition)

            if return_flag:
                for row in result_query:
                    total_orders = row.get("TOTAL")

                return True, total_orders, "Success"
            else:
                return False, total_orders, "Error counting orders."
        except Exception as e:
            return False, total_orders, f"{str(e)}"
        finally:
            try:
                del last_batch_order_id, return_flag, total_orders, columns, condition
            except:
                pass

    # Routine every minute
    def update_phone_case_reports(self):
        print(f"\n[INFO] BEGIN - Updating customs reports")
        today = self.utils.get_current_date_time()
        today_date = today.split(' ')[0]
        print(f"[INFO] Today's date time: {today}")
        print(f"[INFO] Today's date: {today_date}")
        reports_management_exists = False
        reports_data_exists = False
        report_id = None
        reports_data_id = None
        reports_data_id_array = []
        return_message = "Success"
        reports_management = None
        reports_data = None
        result_flag = False
        reports = {
            "total_orders_available_to_print": 0,
        }

        try:
            print(f"[INFO] Getting customs reports info...")
            result_flag, count_total_orders, return_message = self.count_available_to_print()

            reports["total_orders_available_to_print"] = count_total_orders

            reports_management_exists, report_id, return_message = self.reports.verify_reports_exists("PHONE_CASES_REPORTS", today_date)

            if not reports_management_exists:
                reports_management = self.reports.ReportsManagement()
                reports_management.set_report_type("PHONE_CASES_REPORTS")
                reports_management.set_report_name("PHONE_CASES_DASHBOARD_REPORTS")
                reports_management.set_report_description(f"Phone Cases reports for the dashboard up to {today_date}")

                reports_management_exists, report_id, return_message = self.reports.insert_reports(ReportsManagement=reports_management, date=today)

            if reports_management_exists:
                for column, value in reports.items():
                    reports_data = self.reports.ReportsData()
                    reports_data.set_report_id(report_id)
                    reports_data.set_report_type(column.upper())
                    reports_data.set_report_value(str(value))

                    reports_data_exists, reports_data_id, return_message = self.reports.upsert_reports_data(ReportsData=reports_data)

                    if reports_data_exists:
                        reports_data_id_array.append(reports_data_id)
                    else:
                        print(f"[ERROR] {return_message}")
                return True
            else:
                print(f"[ERROR] {return_message}")
                return False
        except Exception as e:
            print(f"[ERROR] {e}")
            return False
        finally:
            print(f"[INFO] END - Updating customs reports")
            # print(f"[INFO] Cleaning up variables")
            try:
                del today, reports_management_exists, reports_data_exists, report_id, reports_data_id, reports_data_id_array, return_message, reports_management, reports_data, result_flag, processing_analytics, reports
            except:
                pass
