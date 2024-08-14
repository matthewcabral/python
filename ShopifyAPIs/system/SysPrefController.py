from database.DataController import DataController
from utils.UtilsController import *
from utils.LogsController import *

class SysPrefController(DataController):

    def __init__(self):
        super().__init__()
        self.utils = UtilsController()
        self.log = LogsController()

        # sys_pref Variables
        self.sys_pref_count = 0
        self.total_sys_pref_count = 0
        self.total_sys_pref_inserted_count = 0
        self.total_sys_pref_updated_count = 0
        self.total_sys_pref_deleted_count = 0
        self.total_sys_pref_with_errors = 0
        self.sys_pref_with_error_array = []

        self.module_name = "SysPrefController"

    class SysPref():
        def __init__(self):
            self.pref_name = None
            self.pref_val = None
            self.comments = None

        def get_pref_name(self):
            return self.pref_name

        def set_pref_name(self, pref_name):
            self.pref_name = pref_name

        def get_pref_val(self):
            return self.pref_val

        def set_pref_val(self, pref_val):
            self.pref_val = pref_val

        def get_comments(self):
            return self.comments

        def set_comments(self, comments):
            self.comments = comments

    # GETTERS
    def get_sys_pref_count(self):
        return self.sys_pref_count

    def get_total_sys_pref_count(self):
        return self.total_sys_pref_count

    def get_total_sys_pref_inserted_count(self):
        return self.total_sys_pref_inserted_count

    def get_total_sys_pref_updated_count(self):
        return self.total_sys_pref_updated_count

    def get_total_sys_pref_deleted_count(self):
        return self.total_sys_pref_deleted_count

    def get_total_sys_pref_with_errors(self):
        return self.total_sys_pref_with_errors

    def get_sys_pref_with_error_array(self):
        return self.sys_pref_with_error_array

    def get_module_name(self):
        return self.module_name

    # SETTERS
    def set_sys_pref_count(self, value):
        self.sys_pref_count = value

    def set_total_sys_pref_count(self, value):
        self.total_sys_pref_count = value

    def set_total_sys_pref_inserted_count(self, value):
        self.total_sys_pref_inserted_count = value

    def set_total_sys_pref_updated_count(self, value):
        self.total_sys_pref_updated_count = value

    def set_total_sys_pref_deleted_count(self, value):
        self.total_sys_pref_deleted_count = value

    def set_total_sys_pref_with_errors(self, value):
        self.total_sys_pref_with_errors = value

    # UTILS
    def append_sys_pref_with_error_array(self, sys_pref_name):
        self.sys_pref_with_error_array.append(sys_pref_name)

    def clear_sys_pref_with_error_array(self):
        self.sys_pref_with_error_array = []

    def clear_counters(self):
        self.set_total_sys_pref_count(0)
        self.set_total_sys_pref_inserted_count(0)
        self.set_total_sys_pref_updated_count(0)
        self.set_total_sys_pref_deleted_count(0)
        self.set_total_sys_pref_with_errors(0)

    def print_log(self, log_level, function, sys_pref_name, try_count, message):
        final_message = f"[INFO] " if log_level == "info" else f"[ERROR] " if log_level == "error" else f"[WARNING] " if log_level == "warning" else f"[DEBUG] " if log_level == "debug" else f"[CRITICAL] "
        if try_count is not None and try_count != "":
            final_message += f"Trying to "
            final_message += f"insert " if function == "insert" else f"update " if function == "update" else f"delete " if function == "delete" else f"get " if function == "get" else f"process " if function == "process" else f"save " if function == "save" else f"verify " if function == "verify" else f"execute "
            final_message += f"Sys Pref "
            final_message += f"again...\t\tSys Pref: {sys_pref_name}"
            final_message += f".Try: {try_count}"
        else:
            final_message += f"Sys Pref "
            final_message += f"Inserted..." if function == "insert" else f"Updated... " if function == "update" else f"Deleted... "
            final_message += f"\t\t\t"
            final_message += f"Sys Pref: {sys_pref_name}"
        final_message += f"{message}" if message is not None and message != "" else ""

        print(final_message)

    # DATABASE FUNCTIONS
    def verify_sys_pref_exits(self, sysPrefName):
        count = 0
        columns = ["COUNT(*) AS TOTAL"]
        condition = (f"PREF_NAME = '{sysPrefName}'")

        result_query = super().query_record(super().get_tbl_SYS_PREF() , columns, condition)

        if result_query:
            columns, rows = result_query
            if columns and rows:
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    count = row_dict["TOTAL"]
            else:
                count = 0
        else:
            count = 0

        try:
            del result_query
            del columns
            del rows
            del condition
        except:
            pass

        return False if count <= 0 else True

    def get_sys_pref(self, sysPrefName):
        columns_array = ["*"]
        condition = (f"PREF_NAME = '{sysPrefName}'")
        value = None
        return_flag = False
        result_query = None

        try:
            return_flag, result_query = super().query_record(super().get_tbl_SYS_PREF(), columns_array, condition)

            if return_flag:
                for row in result_query:
                    value = row["PREF_VAL"]

                return True, value, 200
            else:
                return False, f"[INFO] Sys Pref not found. Sys Pref: {sysPrefName}", 404
        except Exception as e:
            return False, str(e), 500
        finally:
            try:
                del columns_array, return_flag, result_query, columns, row, condition, value
            except:
                pass

    def get_all_sys_pref(self):
        columns = ["*"]
        sys_pref_array = []

        try:
            return_flag, result_query = super().query_record(super().get_tbl_SYS_PREF(), columns, None)

            if return_flag:
                for row in result_query:
                    sys_pref_array.append(row)

                return True, sys_pref_array, 200
            else:
                sys_pref_array = []
                return False, f"[INFO] Sys Pref not found.", 404
        except Exception as e:
            return False, str(e), 500
        finally:
            try:
                del result_query
                del sys_pref_array
                del columns
                del row
            except:
                pass

    def upsert_sys_pref(self, sysPref: SysPref):
        sys_pref_upserted_flag = False # Upsert means: Inserted or Updated
        maximum_insert_try = 100
        keep_trying = True
        count_try = 0
        sys_pref_inserted_count = 0
        sys_pref_updated_count = 0
        sys_pref_name = None
        columns = None
        rows = None
        result_query = None
        result_string = None
        error_message = None
        additional_details = None

        try:
            sys_pref_name = sysPref.get_pref_name()
            self.utils.clear_columns_values_arrays()

            self.utils.validate_columns_values("PREF_NAME", sys_pref_name)
            self.utils.validate_columns_values("PREF_VAL", sysPref.get_pref_val())
            self.utils.validate_columns_values("COMMENTS", sysPref.get_comments())

            if self.verify_sys_pref_exits(sys_pref_name) == False:
                self.print_log(log_level="info", function="insert", sys_pref_name=sys_pref_name, try_count=None, message=None)
                sys_pref_upserted_flag, sys_pref_inserted_count, result_string = super().insert_record(super().get_tbl_SYS_PREF(), self.utils.get_columns_array(), self.utils.get_values_array())

                if sys_pref_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                    if "PRIMARY" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            # print(f"[INFO] Trying to insert Sys Pref again...\t\tSys Pref: {sys_pref_name}.Try: {count_try}")
                            self.print_log(log_level="info", function="insert", sys_pref_name=sys_pref_name, try_count=count_try, message=None)
                            super().generate_next_id()
                            sys_pref_upserted_flag, sys_pref_inserted_count, result_string = super().insert_record(super().get_tbl_SYS_PREF(), self.utils.get_columns_array(), self.utils.get_values_array())

                            if sys_pref_upserted_flag == True:
                                self.print_log(log_level="info", function="insert", sys_pref_name=sys_pref_name, try_count=None, message=None)
                                # print(f"[INFO] Sys Pref Inserted...\t\t\tSys Pref: {sys_pref_name}")
                                keep_trying = False
                            else:
                                if "ID" in result_string:
                                    print(f"[INFO] Updating Sys Pref...\t\t\tSys Pref: {sys_pref_name}")
                                    self.utils.clear_condition()
                                    self.utils.set_condition(f"PREF_NAME = '{sys_pref_name}'")
                                    sys_pref_upserted_flag, sys_pref_updated_count, result_string = super().update_record(super().get_tbl_SYS_PREF(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                                    if sys_pref_upserted_flag == True:
                                        self.print_log(log_level="info", function="update", sys_pref_name=sys_pref_name, try_count=None, message=None)
                                        # print(f"[INFO] Sys Pref Updated...\t\t\t\tSys Pref: {sys_pref_name}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Sys Pref NOT Inserted/Maximum tries...\tSys Pref: {sys_pref_name}.Try: {count_try}")
                                            keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Sys Pref NOT Inserted/Maximum tries...\tSys Pref: {sys_pref_name}.Try: {count_try}")
                                        keep_trying = False
                    else:
                        print(f"[INFO] Updating Sys Pref...\t\t\tSys Pref: {sys_pref_name}")
                        self.utils.clear_condition()
                        self.utils.set_condition(f"PREF_NAME = '{sys_pref_name}'")
                        sys_pref_upserted_flag, sys_pref_updated_count, result_string = super().update_record(super().get_tbl_SYS_PREF(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())
            else:
                self.utils.clear_condition()
                self.utils.set_condition(f"PREF_NAME = '{sys_pref_name}'")
                self.print_log(log_level="info", function="update", sys_pref_name=sys_pref_name, try_count=None, message=None)
                sys_pref_upserted_flag, sys_pref_updated_count, result_string = super().update_record(super().get_tbl_SYS_PREF(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                if sys_pref_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                    if "ID" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            self.print_log(log_level="info", function="update", sys_pref_name=sys_pref_name, try_count=count_try, message=None)
                            # print(f"[INFO] Trying to update Sys Pref again...\t\tSys Pref: {sys_pref_name}.Try: {count_try}")
                            sys_pref_upserted_flag, sys_pref_updated_count, result_string = super().update_record(super().get_tbl_SYS_PREF(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                            if sys_pref_upserted_flag == True:
                                # print(f"[INFO] Sys Pref Updated...\t\t\t\tSys Pref: {sys_pref_name}")
                                self.print_log(log_level="info", function="update", sys_pref_name=sys_pref_name, try_count=None, message=None)
                                keep_trying = False
                            else:
                                if count_try >= maximum_insert_try:
                                    print(f"[INFO] Sys Pref NOT Updated/Maximum tries...\tSys Pref: {sys_pref_name}.Try: {count_try}")
                                    keep_trying = False

            if sys_pref_upserted_flag == False:
                self.append_sys_pref_with_error_array(sys_pref_name)
                self.set_total_sys_pref_with_errors(self.get_total_sys_pref_with_errors() + 1)

            self.set_total_sys_pref_count(self.get_total_sys_pref_count() + sys_pref_inserted_count + sys_pref_updated_count)
            self.set_total_sys_pref_inserted_count(self.get_total_sys_pref_inserted_count() + sys_pref_inserted_count)
            self.set_total_sys_pref_updated_count(self.get_total_sys_pref_updated_count() + sys_pref_updated_count)
        except Exception as e:
            error_message = f"[ERROR] Error while upserting Sys Pref. Sys Pref id: {sys_pref_name} - Error: {str(e)}."
            additional_details = result_string
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="upsert_sys_pref", error_code=500, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
        finally:
            try:
                del maximum_insert_try
                del keep_trying
                del count_try
                del sys_pref_name
                del columns
                del rows
                del result_query
                del error_message
                del additional_details
            except Exception as e:
                print(f"[INFO] Variable not found: {e}")
                pass

            return sys_pref_upserted_flag, self.get_total_sys_pref_count(), sys_pref_inserted_count, sys_pref_updated_count, result_string

    def webhook_save_sys_pref(self, sys_pref_json):
        #Variables
        sys_pref = None
        upserted_flag = None
        total_sys_pref_count = None
        sys_pref_updated_count = None
        result_string = None

        try:
            if len(sys_pref_json) == 0:
                return False, f"Empty JSON.", 400
            else:
                sys_pref = sys_pref_json.get("system_preferences")
                print(f"[INFO] Sys Pref: {sys_pref}")

                for pref in sys_pref:
                    print(f"[INFO] Pref: {pref}")
                    print(f"[INFO] Pref Name: {pref.get('name')}")
                    sys_pref_obj = self.SysPref()
                    sys_pref_obj.set_pref_name(pref.get("name"))
                    sys_pref_obj.set_pref_val(pref.get("value"))
                    sys_pref_obj.set_comments(pref.get("comments"))

                    upserted_flag, total_sys_pref_count, sys_pref_inserted_count, sys_pref_updated_count, result_string = self.upsert_sys_pref(sys_pref_obj)

                if upserted_flag == False:
                    return False, f"Error while saving Sys Pref. Error: {result_string}.", 500
                else:
                    return True, f"Sys Pref saved successfully.", 200

        except Exception as e:
            error_message = f"[ERROR] Error while saving Sys Pref. Error: {str(e)}."
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="webhook_save_sys_pref", error_code=500, error_message=error_message, additional_details=None, error_severity=self.utils.get_error_severity(2))
            return False, f"Error while saving Sys Pref. Error: {str(e)}.", 500
        finally:
            try:
                del sys_pref
                del pref
                del sys_pref_obj
                del sys_pref_json
                del upserted_flag
                del total_sys_pref_count
                del sys_pref_updated_count
                del result_string
            except Exception as e:
                print(f"[INFO] Variable not found: {e}")
                pass
