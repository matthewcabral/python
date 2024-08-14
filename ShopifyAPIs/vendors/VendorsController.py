from database.DataController import *
from utils.UtilsController import *
from utils.LogsController import *

class VendorsController(DataController):
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.module_name = "VendorsController"

    def get_module_name(self):
        return self.module_name

    class Vendor:
        def __init__(self):
            self.row_id = None
            self.vendor_name = None
            self.status_cd = None
            self.default_vendor = None

        def get_id(self):
            return self.row_id

        def get_vendor_name(self):
            return self.vendor_name

        def set_vendor_name(self, vendor_name):
            self.vendor_name = vendor_name

        def get_status_cd(self):
            return self.status_cd

        def set_status_cd(self, status_cd):
            self.status_cd = status_cd

        def get_default_vendor(self):
            return self.default_vendor

        def set_default_vendor(self, default_vendor):
            self.default_vendor = default_vendor

    # Database Functions
    def get_all_vendors(self):
        print(f"\n[INFO] BEGIN - Getting all vendors")
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        vendors = []
        vendor = {
            "id": None,
            "name": None,
            "status": None,
            "default_vendor": None
        }
        try:
            result_flag, result_query = super().query_record(super().get_tbl_VENDOR(), columns, condition)

            if result_flag:
                for row in result_query:
                    vendor = {
                        "id": row.get('ROW_ID'),
                        "name": row.get('VENDOR_NAME'),
                        "status": row.get('STATUS_CD'),
                        "default_vendor": True if row.get('DEFAULT_VENDOR') else False
                    }
                    vendors.append(vendor)
                return True, vendors
            else:
                return False, "Error getting vendors"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all vendors")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, vendors, vendor
            except:
                pass

    def get_specific_vendor(self, vendor_name):
        print(f"\n[INFO] BEGIN - Getting specific vendor")
        columns = ["*"]
        condition = "1=1"
        condition += f"\nAND VENDOR_NAME = '{vendor_name}'"
        result_flag = False
        result_query = None
        vendor = {
            "id": None,
            "name": None,
            "status": None,
            "default_vendor": None
        }

        try:
            result_flag, result_query = super().query_record(super().get_tbl_VENDOR(), columns, condition)

            if result_flag:
                for row in result_query:
                    vendor = {
                        "id": row.get('ROW_ID'),
                        "name": row.get('VENDOR_NAME'),
                        "status": row.get('STATUS_CD'),
                        "default_vendor": True if row.get('DEFAULT_VENDOR') else False
                    }
                return True, vendor
            else:
                return False, "Error getting vendor"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting specific vendor")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, vendor, vendor_exists
            except:
                pass

    def insert_vendor(self, vendor:Vendor):
        print(f"\n[INFO] BEGIN - Inserting Custom Product Font")
        result_flag = False
        result_string = "Success"
        rowcount = 0

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("VENDOR_NAME", self.utils.replace_special_chars(vendor.get_vendor_name()))
            self.utils.validate_columns_values("STATUS_CD", vendor.get_status_cd())
            self.utils.validate_columns_values("DEFAULT_VENDOR", vendor.get_default_vendor())

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_VENDOR(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Vendor {vendor.get_vendor_name()} inserted successfully")
            else:
                print(f"[ERROR] Error inserting Vendor {vendor.get_vendor_name()}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            print(f"[INFO] END - Finished inserting Vendor")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount
            except:
                pass

    def update_vendor(self, row_id, vendor:Vendor):
        print(f"\n[INFO] BEGIN - Updating Vendor")
        result_flag = False
        result_string = "Success"
        condition = "1=1"
        condition += f"\nAND ROW_ID = '{row_id}'"
        rowcount = 0

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("VENDOR_NAME", self.utils.replace_special_chars(vendor.get_vendor_name()))
            self.utils.validate_columns_values("STATUS_CD", vendor.get_status_cd())
            self.utils.validate_columns_values("DEFAULT_VENDOR", vendor.get_default_vendor())

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_VENDOR(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Vendor {vendor.get_vendor_name()} updated successfully")
            else:
                print(f"[ERROR] Error updating Vendor {vendor.get_vendor_name()}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            print(f"[INFO] END - Finished updating Vendor")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, condition
            except:
                pass

    def delete_vendor(self, row_id):
        print(f"\n[INFO] BEGIN - Deleting Custom Product Font")
        result_flag = False
        result_flag_variant = False
        result_string = "Success"
        condition = "1=1"
        condition += f"\nAND ROW_ID = '{row_id}'"
        rowcount = 0

        try:
            result_flag, rowcount, result_string = super().delete_record(super().get_tbl_VENDOR(), condition)
            if result_flag:
                print(f"[INFO] Vendor {row_id} deleted successfully")
            else:
                print(f"[ERROR] Error deleting Vendor {row_id}")

            if result_flag:
                return result_flag, 200, rowcount, result_string
            else:
                return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, rowcount, str(e)
        finally:
            print(f"[INFO] END - Finished deleting Custom Product Font")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_flag_variant, result_string, condition, rowcount
            except:
                pass

    def verify_vendor_exists(self, vendor_id, vendor_name):
        print(f"\n[INFO] BEGIN - Verifying Vendor exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        if vendor_id is not None and vendor_id != "":
            condition += f"\nAND ROW_ID = '{vendor_id}'"
        else:
            condition += f"\nAND VENDOR_NAME = '{vendor_name}'"
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_VENDOR(), columns, condition)

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
            print(f"[INFO] END - Finished verifying Vendor exists")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    def get_all_vendors_locations(self):
        print(f"\n[INFO] BEGIN - Getting all vendors")
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        vendors = {}
        vendor = {
            "vendor_id": None,
            "vendor_name": None,
            "vendor_status": None,
            "default_vendor": None,
            "loc_id": None,
            "loc_name": None,
            "loc_country": None,
            "loc_country_code": None
        }
        try:
            result_flag, result_query = super().query_record(super().get_view_VENDOR_LOCATION_VIEW(), columns, condition)

            if result_flag:
                for row in result_query:
                    if not vendors.get(row.get('VENDOR_ID')):
                        vendors[row.get('VENDOR_ID')] = []
                    vendor = {
                        "vendor_name": row.get('VENDOR_NAME'),
                        "vendor_status": row.get('VENDOR_STATUS'),
                        "default_vendor": True if row.get('DEFAULT_VENDOR') else False,
                        "loc_id": row.get('LOC_ID'),
                        "loc_name": row.get('LOC_NAME'),
                        "loc_country": row.get('LOC_COUNTRY'),
                        "loc_country_code": row.get('LOC_COUNTRY_CODE')
                    }
                    vendors[row.get('VENDOR_ID')].append(vendor)
                return True, vendors
            else:
                return False, "Error getting vendors"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all vendors")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, vendors, vendor
            except:
                pass

    # API Functions
    def add_vendor(self, vendor_json):
        print(f"\n[INFO] BEGIN - Adding Vendor")
        vendor = self.Vendor()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        vendor_exists = False
        vendor_name = vendor_json.get('vendor_name')
        status_cd = vendor_json.get('status')
        default_vendor = vendor_json.get('default_vendor')

        if vendor_name is None or vendor_name == "":
            return False, 400, 0, "Vendor name is required."

        if status_cd is None or status_cd == "":
            status_cd = "active"

        try:
            vendor_exists = self.verify_vendor_exists(vendor_id=None, vendor_name=vendor_name)
            if vendor_exists:
                return False, 409, 0, "Vendor already exists"
            else:
                vendor.set_vendor_name(vendor_name)
                vendor.set_status_cd(status_cd)
                vendor.set_default_vendor(default_vendor)

                result_flag, rowcount, result_string = self.insert_vendor(vendor)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            print(f"[INFO] END - Finished adding Vendor")
            print("[INFO] Cleaning up variables")
            try:
                del vendor, vendor_exists, result_flag, rowcount, result_string, vendor_name, status_cd
            except:
                pass

    def edit_vendor(self, row_id, vendor_json):
        print(f"\n[INFO] BEGIN - Editing Vendor")
        vendor = self.Vendor()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        vendor_exists = False
        vendor_name = vendor_json.get('vendor_name')
        status_cd = vendor_json.get('status')
        default_vendor = vendor_json.get('default_vendor')

        if row_id is None or row_id == "":
            return False, 400, 0, "Row ID is required."
        if vendor_name is None or vendor_name == "":
            return False, 400, 0, "Vendor name is required."
        if status_cd is None or status_cd == "":
            status_cd = "active"

        try:
            vendor_exists = self.verify_vendor_exists(vendor_id=row_id, vendor_name=None)
            if not vendor_exists:
                return False, 404, 0, "Vendor not found!"
            else:
                vendor.set_vendor_name(vendor_name)
                vendor.set_status_cd(status_cd)
                vendor.set_default_vendor(default_vendor)

                result_flag, rowcount, result_string = self.update_vendor(row_id, vendor)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            print(f"[INFO] END - Finished editing Vendor")
            print("[INFO] Cleaning up variables")
            try:
                del vendor, vendor_exists, result_flag, rowcount, result_string, vendor_name, status_cd
            except:
                pass

    def remove_vendor(self, row_id):
        print(f"\n[INFO] BEGIN - Removing Vendor")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        vendor_exists = False

        if row_id is None or row_id == "":
            return False, 400, 0, "Row ID is required."

        try:
            vendor_exists = self.verify_vendor_exists(vendor_id=row_id, vendor_name=None)
            if not vendor_exists:
                return False, 404, 0, "Vendor not found!"
            else:
                result_flag, rowcount, result_string = self.delete_vendor(row_id)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            print(f"[INFO] END - Finished removing Vendor")
            print("[INFO] Cleaning up variables")
            try:
                del vendor_exists, result_flag, rowcount, result_string
            except:
                pass

    def get_all_vendors_api(self):
        print(f"\n[INFO] BEGIN - Getting all vendors")
        result_flag = False
        result = None

        try:
            result_flag, result = self.get_all_vendors()
            if result_flag:
                return result_flag, 200, result
            else:
                return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished getting all vendors")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass

    def get_specific_vendor_api(self, vendor_name):
        print(f"\n[INFO] BEGIN - Getting specific vendor")
        result_flag = False
        result = None
        vendor_exists = False

        try:
            vendor_exists = self.verify_vendor_exists(vendor_id=None, vendor_name=vendor_name)
            if not vendor_exists:
                return False, 404, "Vendor not found!"
            else:
                result_flag, result = self.get_specific_vendor(vendor_name)
                if result_flag:
                    return result_flag, 200, result
                else:
                    return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished getting specific vendor")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass
