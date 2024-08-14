from database.DataController import *
from utils.UtilsController import *
from utils.LogsController import *

class ReportsController(DataController):
    def __init__(self):
        super().__init__()
        self.utils = UtilsController()
        self.logs = LogsController()

    class ReportsManagement():
        def __init__(self):
            self.report_type = None
            self.report_name = None
            self.report_description = None

        # Getters
        def get_report_type(self):
            return self.report_type

        def get_report_name(self):
            return self.report_name

        def get_report_description(self):
            return self.report_description

        # Setters
        def set_report_type(self, report_type):
            self.report_type = report_type

        def set_report_name(self, report_name):
            self.report_name = report_name

        def set_report_description(self, report_description):
            self.report_description = report_description

    class ReportsData():
        def __init__(self):
            self.report_id = None
            self.report_type = None
            self.report_value = None
            self.report_value_json = None
            self.report_description = None

        # Getters
        def get_report_id(self):
            return self.report_id

        def get_report_type(self):
            return self.report_type

        def get_report_value(self):
            return self.report_value

        def get_report_value_json(self):
            return self.report_value_json

        def get_report_description(self):
            return self.report_description

        # Setters
        def set_report_id(self, report_id):
            self.report_id = report_id

        def set_report_type(self, report_type):
            self.report_type = report_type

        def set_report_value(self, report_value):
            self.report_value = report_value

        def set_report_value_json(self, report_value_json):
            self.report_value_json = report_value_json

        def set_report_description(self, report_description):
            self.report_description = report_description

    def verify_reports_exists(self, report_type, date):
        today = date if date is not None else self.utils.get_current_date()
        print(f"\n[INFO] BEGIN - Verifying if report type {report_type} exists a report for the date {today}...")
        count = 0
        report_id = None
        result_flag = False
        columns = ["COUNT(*) AS TOTAL", "MAX(ROW_ID) AS ROW_ID"]
        condition = "1=1"
        condition += f"\nAND REPORT_TYPE = '{report_type}'"
        condition += f"\nAND DATE(REPORT_DATE) = '{today}'"
        condition += f"\nGROUP BY ROW_ID"

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REPORTS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    count = row.get("TOTAL")
                    report_id = row.get("ROW_ID")
            else:
                count = 0
                report_id = None

            if count > 0:
                print(f"[INFO] Report exists with ID: {report_id}")
            else:
                print(f"[INFO] Report does not exist")

            return True if count > 0 else False, report_id, "Success"
        except Exception as e:
            print(f"\n[ERROR] {str(e)}")
            return False, 0, None, str(e)
        finally:
            print(f"[INFO] END - Verifying if report exists...")
            try:
                del result_query, result_flag, columns, condition, today, count, report_id
            except:
                pass

    def verify_reports_data_exists(self, report_id, report_type):
        print(f"[INFO] BEGIN - Verifying if exists a report data with the report ID {report_id}...")
        today = self.utils.get_current_date()
        count = 0
        report_data_id = None
        columns = ["COUNT(*) AS TOTAL", "ROW_ID"]
        condition = "1=1"
        condition += f"\nAND REPORT_ID = '{report_id}'"
        condition += f"\nAND REPORT_TYPE = '{str(report_type).upper()}'"
        condition += f"\nGROUP BY ROW_ID"

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REPORTS_DATA(), columns, condition)

            if result_flag:
                for row in result_query:
                    count = row.get("TOTAL")
                    report_data_id = row.get("ROW_ID")
                print(f"[INFO] Report data exists with ID: {report_data_id}")
            else:
                count = 0
                report_data_id = None

            return True if count > 0 else False, report_data_id, "Success"
        except Exception as e:
            return False, 0, None, str(e)
        finally:
            print(f"[INFO] END - Verifying if report data exists...")
            try:
                del result_query, result_flag, columns, condition, today, count, report_data_id
            except:
                pass

    def insert_reports(self, ReportsManagement:ReportsManagement, date):
        print(f"\n[INFO] BEGIN - Inserting report...")
        today = date if date is not None else self.utils.get_current_date_time()
        today_date = today.split(' ')[0]
        report_id = None
        report_type = ReportsManagement.get_report_type()
        report_name = ReportsManagement.get_report_name()
        report_description = ReportsManagement.get_report_description()
        result_flag = False
        message = None
        rowcount = 0

        try:
            # Verify if report exists
            print(f"[INFO] Verifying if report exists...")
            result_flag, report_id, message = self.verify_reports_exists(report_type, today_date)

            if result_flag:
                print(f"[INFO] Report already exists with ID: {report_id}")
                return True, report_id, "Report already exists"
            else:
                print(f"[INFO] Report does not exist")
                self.utils.clear_columns_values_arrays()
                self.utils.validate_columns_values("REPORT_DATE", f"DATE_FORMAT('{today}', '%Y-%m-%d %H:%i:%s')")
                self.utils.validate_columns_values("REPORT_TYPE", report_type)
                self.utils.validate_columns_values("REPORT_NAME", report_name)
                self.utils.validate_columns_values("REPORT_DESCRIPTION", report_description)

                print(f"[INFO] Inserting report...")
                result_flag, rowcount, result_query = super().insert_record(super().get_tbl_REPORTS_MANAGEMENT(), self.utils.get_columns_array(), self.utils.get_values_array())

                if result_flag:
                    print(f"[INFO] Verifying if report was inserted...")
                    result_flag, report_id, message = self.verify_reports_exists(report_type, today_date)
                    if result_flag:
                        print(f"[INFO] Report inserted successfully with ID: {report_id}")
                        return True, report_id, "Report inserted successfully"
                    else:
                        print(f"[ERROR] {message}")
                        return False, 0, message
                else:
                    print(f"[ERROR] {str(result_query)}")
                    return False, None, str(result_query)
        except Exception as e:
            print(f"[ERROR] {str(e)}")
            return False, None, str(e)
        finally:
            print(f"[INFO] END - Inserting report...")
            try:
                del today, condition, report_id, report_type, report_name, report_description, result_flag, message
            except:
                pass

    def upsert_reports_data(self, ReportsData:ReportsData):
        report_data_id = None
        report_id = ReportsData.get_report_id()
        report_type = ReportsData.get_report_type()
        report_value = ReportsData.get_report_value()
        report_value_json = ReportsData.get_report_value_json()
        report_description = ReportsData.get_report_description()
        result_flag = False
        message = None
        condition = "1=1"
        rowcount = 0
        print(f"\n[INFO] BEGIN - Upserting report data with TYPE [{report_type}] and VALUE [{report_value}] and VALUE JSON [{report_value_json}]...")

        try:
            # print(f"[INFO] Verifying if report data exists for the report id {report_id} and report type {report_type}...")
            result_flag, report_data_id, message = self.verify_reports_data_exists(report_id, report_type)

            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("REPORT_VALUE", report_value)
            self.utils.validate_columns_values("REPORT_VALUE_JSON", report_value_json)
            self.utils.validate_columns_values("REPORT_DESCRIPTION", report_description)

            if result_flag:
                # print(f"[INFO] Report data exists with ID: {report_data_id}")
                print(f"[INFO] Updating report data...")
                condition += f"\nAND ROW_ID = '{report_data_id}'"
                result_flag, rowcount, result_query = super().update_record(super().get_tbl_REPORTS_DATA(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

                if result_flag:
                    print(f"[INFO] Report data updated successfully")
                    return True, report_data_id, "Report data updated successfully"
                else:
                    print(f"[ERROR] {str(result_query)}")
                    return False, None, str(result_query)
            else:
                print(f"[INFO] Report data does not exist")
                self.utils.validate_columns_values("REPORT_ID", report_id)
                self.utils.validate_columns_values("REPORT_TYPE", report_type)

                print(f"[INFO] Inserting report data...")
                result_flag, rowcount, result_query = super().insert_record(super().get_tbl_REPORTS_DATA(), self.utils.get_columns_array(), self.utils.get_values_array())

                if result_flag:
                    print(f"[INFO] Verifying if report data was inserted...")
                    result_flag, report_data_id, message = self.verify_reports_data_exists(report_id, report_type)

                    if result_flag:
                        print(f"[INFO] Report data inserted successfully with ID: {report_data_id}")
                        return True, report_data_id, "Report data inserted successfully"
                    else:
                        print(f"[ERROR] {message}")
                        return False, report_data_id, message
                else:
                    print(f"[ERROR] {str(result_query)}")
                    return False, None, str(result_query)
        except Exception as e:
            print(f"[ERROR] {str(e)}")
            return False, None, str(e)
        finally:
            print(f"[INFO] END - Upserting report data...")
            try:
                del report_id, report_type, report_value, report_description, result_flag, message, condition
            except:
                pass

    def get_reports_data(self, report_id, columns):
        print(f"\n[INFO] BEGIN - Getting report data...")
        columns = ["*"] if columns is None else columns
        condition = "1=1"
        condition += f"\nAND REPORT_ID = '{report_id}'"

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REPORTS_DATA(), columns, condition)

            if result_flag:
                print(f"[INFO] Report data retrieved successfully")
                return True, result_query, "Report data retrieved successfully"
            else:
                print(f"[ERROR] {str(result_query)}")
                return False, None, str(result_query)
        except Exception as e:
            print(f"[ERROR] {str(e)}")
            return False, None, str(e)
        finally:
            print(f"[INFO] END - Getting report data...")
            try:
                del columns, condition
            except:
                pass

    def get_reports(self, report_type, report_date):
        print(f"\n[INFO] BEGIN - Getting reports...")
        columns = ["*"]
        condition = "1=1"
        condition += f"\nAND REPORT_TYPE = '{report_type}'"
        condition += f"\nAND DATE(REPORT_DATE) = '{report_date}'" if report_date is not None else ""

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REPORTS_MANAGEMENT(), columns, condition)

            if result_flag:
                print(f"[INFO] Reports retrieved successfully")
                return True if len(result_query) > 0 else False, result_query, "Reports retrieved successfully"
            else:
                print(f"[ERROR] {str(result_query)}")
                return False, None, str(result_query)
        except Exception as e:
            print(f"[ERROR] {str(e)}")
            return False, None, str(e)
        finally:
            print(f"[INFO] END - Getting reports...")
            try:
                del columns, condition
            except:
                pass

    def extract_report_data(self, report_type, report_date):
        print(f"\n[INFO] BEGIN - Extracting customs report data for {report_type} on {report_date}")
        try:
            result_flag_reports, result_query_reports, return_description = self.get_reports(report_type=report_type, report_date=report_date)

            if not result_flag_reports:
                print(f"[ERROR] Error getting customs reports")
                return False, 500, return_description

            report_id = [report.get('ROW_ID') for report in result_query_reports]

            if not report_id:
                print(f"[ERROR] Error getting customs reports")
                return False, 500, return_description

            report_id = report_id[0]
            print(f"[INFO] Got Report ID: {report_id}")
            print(f"[INFO] Getting reports data")
            result_flag_data, result_query_data, return_description = self.get_reports_data(report_id=report_id, columns=["REPORT_TYPE", "REPORT_VALUE", "REPORT_VALUE_JSON"])

            if not result_flag_data:
                print(f"[ERROR] Error getting customs reports data")
                return False, 500, return_description

            data = {str(row.get("REPORT_TYPE")).lower(): row.get("REPORT_VALUE_JSON") if row.get("REPORT_VALUE_JSON") is not None else row.get("REPORT_VALUE") for row in result_query_data}
            return True, 200, data
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished extracting customs report data for {report_type} on {report_date}")
            try:
                del result_flag_reports, result_query_reports, result_flag_data, result_query_data, report_id, report_type, report_value, columns_counter
            except:
                pass
