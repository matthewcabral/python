from utils.UtilsController import *
from database.DataController import *

class LogsController(DataController):
    def __init__(self):
        super().__init__()
        self.utils = UtilsController()

    class log:
        def __init__(self):
            self.repository_name = None
            self.module_name = None
            self.function_name = None
            self.error_code = None
            self.error_message = None
            self.additional_details = None
            self.error_severity = None

        def set_repository_name(self, repository_name):
            self.repository_name = repository_name

        def get_repository_name(self):
            return self.repository_name

        def set_module_name(self, module_name):
            self.module_name = module_name

        def get_module_name(self):
            return self.module_name

        def set_function_name(self, function_name):
            self.function_name = function_name

        def get_function_name(self):
            return self.function_name

        def set_error_code(self, error_code):
            self.error_code = error_code

        def get_error_code(self):
            return self.error_code

        def set_error_message(self, error_message):
            self.error_message = error_message

        def get_error_message(self):
            return self.error_message

        def set_additional_details(self, additional_details):
            self.additional_details = additional_details

        def get_additional_details(self):
            return self.additional_details

        def set_error_severity(self, error_severity):
            self.error_severity = error_severity

        def get_error_severity(self):
            return self.error_severity

    def upsert_log_error(self, log:log):
        log_inserted_flag = False # Upsert means: Inserted or Updated
        maximum_insert_try = 100

        keep_trying = True
        count_try = 0
        
        self.utils.clear_columns_values_arrays()
        self.utils.validate_columns_values("REPOSITORY_NAME", self.utils.replace_special_chars(log.get_repository_name()))
        self.utils.validate_columns_values("MODULE_NAME", self.utils.replace_special_chars(log.get_module_name()))
        self.utils.validate_columns_values("FUNCTION_NAME", self.utils.replace_special_chars(log.get_function_name()))
        self.utils.validate_columns_values("ERROR_CODE", self.utils.replace_special_chars(log.get_error_code()))
        self.utils.validate_columns_values("ERROR_MESSAGE", self.utils.replace_special_chars(log.get_error_message()))
        self.utils.validate_columns_values("ADDITIONAL_DETAILS", self.utils.replace_special_chars(log.get_additional_details()))
        self.utils.validate_columns_values("ERROR_SEVERITY", self.utils.replace_special_chars(log.get_error_severity()))

        log_inserted_flag, log_inserted_count, result_string = super().insert_record(super().get_tbl_LOGS(), self.utils.get_columns_array(), self.utils.get_values_array())

        if log_inserted_flag == False and (("1062" in result_string or "Duplicate entry" in result_string) and "PRIMARY" in result_string):
            while keep_trying:
                count_try += 1
                print(f"[INFO] Trying to insert Log again....Try: {count_try}")
                super().generate_next_id()
                log_inserted_flag, log_inserted_count, result_string = super().insert_record(super().get_tbl_LOGS(), self.utils.get_columns_array(), self.utils.get_values_array())
                
                if log_inserted_flag == True:
                    print(f"[INFO] Log Inserted...\t")
                    keep_trying = False
                else:
                    if count_try >= maximum_insert_try:
                        print(f"[INFO] Log NOT Inserted because of Maximum tries...Try: {count_try}")
                        keep_trying = False

        return log_inserted_flag, log_inserted_count, result_string

    def log_error(self, repository_name, module_name, function_name, error_code, error_message, additional_details, error_severity):
        try:
            log = self.log()
            log.set_repository_name(repository_name)
            log.set_module_name(module_name)
            log.set_function_name(function_name)
            log.set_error_code(error_code)
            log.set_error_message(error_message)
            log.set_additional_details(additional_details)
            log.set_error_severity(error_severity)

            log_inserted_flag, log_inserted_count, result_string = self.upsert_log_error(log)

            del log

            return log_inserted_flag, log_inserted_count, result_string
        except Exception as e:
            return False, 0, f"Error: {str(e)}"

    def insert_webhook_log(self, webhook_name):
        row_Inserted_Flag = False
        sql_header = ""
        sql_column = ""
        sql_value = ""
        sql_final_cmd = ""

        return_string = "Success"
        sql_header += f"""INSERT INTO {super().get_DB_OWNER()}.{super().get_tbl_WEBHOOK_LOGS()}"""
        sql_column += f""" (WEBHOOK_NAME)"""
        sql_value += f" VALUES ('{webhook_name}')"

        sql_final_cmd += sql_header + sql_column + sql_value
        row_Inserted_Flag, rowcount, return_string = super().exec_db_cmd(sql_final_cmd)

        return row_Inserted_Flag, rowcount, return_string
