from utils.UtilsController import *
from database.DataController import *
from utils.LogsController import *

class PrintingController(DataController):
    def __init__(self):
        super().__init__()
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "PrintingController"

        self.printing_history_mapping = {
            'ROW_ID': 'row_id',
            'BATCH_ID': 'batch_id',
            'BATCH_NUMBER': 'batch_number',
            'PDF_NAME': 'pdf_name',
            'PDF_TYPE': 'pdf_type',
            'PDF_URL': 'pdf_url',
            'SLIP_PDF_URL': 'slip_pdf_url',
            'PRINTED_DATE': 'printed_date',
            # 'ORDER_IDS': 'order_ids_json',
            'TOTAL_ORDERS': 'total_orders'
        }

    class printingHistory:
        def __init__(self):
            self.batch_id = None
            self.batch_number = None
            self.pdf_name = None
            self.pdf_type = None
            self.pdf_url = None
            self.slip_pdf_url = None
            self.printed_date = None
            self.order_ids = None
            self.total_orders = None

        def set_batch_id(self, batch_id):
            self.batch_id = batch_id

        def get_batch_id(self):
            return self.batch_id

        def set_batch_number(self, batch_number):
            self.batch_number = batch_number

        def get_batch_number(self):
            return self.batch_number

        def set_pdf_name(self, pdf_name):
            self.pdf_name = pdf_name

        def get_pdf_name(self):
            return self.pdf_name

        def set_pdf_type(self, pdf_type):
            self.pdf_type = pdf_type

        def get_pdf_type(self):
            return self.pdf_type

        def set_pdf_url(self, pdf_url):
            self.pdf_url = pdf_url

        def get_pdf_url(self):
            return self.pdf_url

        def set_slip_pdf_url(self, slip_pdf_url):
            self.slip_pdf_url = slip_pdf_url

        def get_slip_pdf_url(self):
            return self.slip_pdf_url

        def set_printed_date(self, printed_date):
            self.printed_date = printed_date

        def get_printed_date(self):
            return self.printed_date

        def set_order_ids(self, order_ids):
            self.order_ids = order_ids

        def get_order_ids(self):
            return self.order_ids

        def set_total_orders(self, total_orders):
            self.total_orders = total_orders

        def get_total_orders(self):
            return self.total_orders

    def get_module_name(self):
        return self.module_name

    def get_printing_history_mapping(self):
        return self.printing_history_mapping

    def get_printing_history_mapping_key(self, key):
        return self.printing_history_mapping.get(key)

    def get_printing_history_mapping_value(self, value):
        return next((k for k, v in self.printing_history_mapping.items() if v == value), None)

    def get_printing_history_mapping_keys(self):
        return self.printing_history_mapping.keys()

    def get_printing_history_mapping_values(self):
        return self.printing_history_mapping.values()

    def get_printing_history_mapping_items(self):
        return self.printing_history_mapping.items()

    def insert_printing_history(self, printing_history: printingHistory):
        inserted_flag = False # Upsert means: Inserted or Updated
        inserted_count = 0
        result_string = None
        error_message = None
        additional_details = None

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("BATCH_ID", printing_history.get_batch_id())
            self.utils.validate_columns_values("BATCH_NUMBER", printing_history.get_batch_number())
            self.utils.validate_columns_values("PDF_NAME", printing_history.get_pdf_name())
            self.utils.validate_columns_values("PDF_TYPE", printing_history.get_pdf_type())
            self.utils.validate_columns_values("PDF_URL", printing_history.get_pdf_url())
            self.utils.validate_columns_values("SLIP_PDF_URL", printing_history.get_slip_pdf_url())
            self.utils.validate_columns_values("PRINTED_DATE", printing_history.get_printed_date())
            self.utils.validate_columns_values("ORDER_IDS", printing_history.get_order_ids())
            self.utils.validate_columns_values("TOTAL_ORDERS", printing_history.get_total_orders())

            inserted_flag, inserted_count, result_string = super().insert_record(super().get_tbl_PRINTING_HISTORY(), self.utils.get_columns_array(), self.utils.get_values_array())
            return inserted_flag, inserted_count, result_string
        except Exception as e:
            error_message = f"[ERROR] Error while upserting printing history - Error: {str(e)}."
            additional_details = result_string
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="insert_printing_history", error_code=500, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
            return False, 0, str(e)
        finally:
            try:
                del inserted_flag, inserted_count, result_string, error_message, additional_details
            except Exception as e:
                pass

    def get_all_printing_history(self, limit, since_id):
        print(f"[INFO] BEGIN - Getting all printing history{" since_id: " + str(since_id) if since_id is not None else ""}{" limit: " + str(limit) if limit is not None else ""}")
        columns = ["*"]
        condition = "1=1"
        condition += f"\nAND ROW_ID < '{since_id}'" if since_id is not None else ""
        condition += f"\nORDER BY CREATED DESC"
        condition += f"\nLIMIT {limit}" if limit is not None else "\nLIMIT 50"
        result_flag = False
        result_query = None
        printing_list = []
        printing = {}
        len_printing_list = 0

        try:
            # print(f"condition: {condition}")
            result_flag, result_query = super().query_record(super().get_tbl_PRINTING_HISTORY(), columns, condition)

            if result_flag:
                len_printing_list = len(result_query)
                print(f"[INFO] Got {len_printing_list} Printing History items.")
                if len_printing_list == 0:
                    return False, 404, printing_list
                else:
                    for row in result_query:
                        printing = {}
                        if columns == ['*']:
                            for mapping_row in self.get_printing_history_mapping():
                                key = self.get_printing_history_mapping_key(mapping_row)
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
                                    printing[key] = value
                        else:
                            for column in columns:
                                if column in self.get_printing_history_mapping():
                                    key = self.get_printing_history_mapping_key(column)
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
                                        printing[key] = value
                        printing_list.append(printing)

                    return True, 200, printing_list
            else:
                print(f"[ERROR] Error getting Printing History. Error: {result_query}")
                return False, 404, result_query
        except Exception as e:
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_all_printing_history", error_code=500, error_message=str(e), additional_details=None, error_severity=self.utils.get_error_severity(3))
            print(f"[ERROR] - {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Getting all printing history")
            try:
                del columns, condition, result_flag, result_query, printing_list, printing
            except:
                pass