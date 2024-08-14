from database.DataController import DataController
from utils.UtilsController import *
from utils.LogsController import *

class LocationsController(DataController):
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "LocationsController"

    def get_module_name(self):
        return self.module_name

    class Location:
        def __init__(self):
            self.code = None
            self.name = None
            self.active_flg = None
            self.phone_number = None
            self.address_line1 = None
            self.address_line2 = None
            self.city = None
            self.state_province = None
            self.state_province_code = None
            self.country = None
            self.country_code = None
            self.zip_code = None

        def get_code(self):
            return self.code

        def get_name(self):
            return self.name

        def set_name(self, name):
            self.name = name

        def get_active_flg(self):
            return self.active_flg

        def set_active_flg(self, active_flg):
            self.active_flg = active_flg

        def get_phone_number(self):
            return self.phone_number

        def set_phone_number(self, phone_number):
            self.phone_number = phone_number

        def get_address_line1(self):
            return self.address_line1

        def set_address_line1(self, address_line1):
            self.address_line1 = address_line1

        def get_address_line2(self):
            return self.address_line2

        def set_address_line2(self, address_line2):
            self.address_line2 = address_line2

        def get_city(self):
            return self.city

        def set_city(self, city):
            self.city = city

        def get_state_province(self):
            return self.state_province

        def set_state_province(self, state_province):
            self.state_province = state_province

        def get_state_province_code(self):
            return self.state_province_code

        def set_state_province_code(self, state_province_code):
            self.state_province_code = state_province_code

        def get_country(self):
            return self.country

        def set_country(self, country):
            self.country = country

        def get_country_code(self):
            return self.country_code

        def set_country_code(self, country_code):
            self.country_code = country_code

        def get_zip_code(self):
            return self.zip_code

        def set_zip_code(self, zip_code):
            self.zip_code = zip_code

    # Database Functions
    def get_all_locations(self):
        print(f"\n[INFO] BEGIN - Getting all locations")
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        locations = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_LOCATIONS(), columns, condition)

            if result_flag:
                for row in result_query:
                    location = {
                        "id": row.get('ROW_ID'),
                        "code": row.get('CODE'),
                        "name": row.get('NAME'),
                        "active_flg": bool(row.get('ACTIVE_FLG')),
                        "phone_number": row.get('PHONE_NUMBER'),
                        "address_line1": row.get('ADDRESS_LINE1'),
                        "address_line2": row.get('ADDRESS_LINE2'),
                        "city": row.get('CITY'),
                        "state_province": row.get('STATE_PROVINCE'),
                        "state_province_code": row.get('STATE_PROVINCE_CODE'),
                        "country": row.get('COUNTRY'),
                        "country_code": row.get('COUNTRY_CODE'),
                        "zip_code": row.get('ZIP_CODE')
                    }
                    locations.append(location)
                return True, locations
            else:
                return False, "Error getting locations"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all locations")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, locations
            except:
                pass

    def get_all_active_locations(self):
        print(f"\n[INFO] BEGIN - Getting all locations")
        columns = ["*"]
        condition = "1=1"
        condition += "\nAND ACTIVE_FLG = TRUE"
        result_flag = False
        result_query = None
        locations = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_LOCATIONS(), columns, condition)

            if result_flag:
                for row in result_query:
                    location = {
                        "id": row.get('ROW_ID'),
                        "code": row.get('CODE'),
                        "name": row.get('NAME'),
                        "active_flg": bool(row.get('ACTIVE_FLG')),
                        "phone_number": row.get('PHONE_NUMBER'),
                        "address_line1": row.get('ADDRESS_LINE1'),
                        "address_line2": row.get('ADDRESS_LINE2'),
                        "city": row.get('CITY'),
                        "state_province": row.get('STATE_PROVINCE'),
                        "state_province_code": row.get('STATE_PROVINCE_CODE'),
                        "country": row.get('COUNTRY'),
                        "country_code": row.get('COUNTRY_CODE'),
                        "zip_code": row.get('ZIP_CODE')
                    }
                    locations.append(location)
                return True, locations
            else:
                return False, "Error getting locations"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all locations")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, locations
            except:
                pass

    def get_specific_location(self, code):
        print(f"\n[INFO] BEGIN - Getting specific location")
        columns = ["*"]
        condition = f"CODE = '{code}'"
        result_flag = False
        result_query = None
        location = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_LOCATIONS(), columns, condition)

            if result_flag:
                for row in result_query:
                    location = {
                        "id": row.get('ROW_ID'),
                        "code": row.get('CODE'),
                        "name": row.get('NAME'),
                        "active_flg": bool(row.get('ACTIVE_FLG')),
                        "phone_number": row.get('PHONE_NUMBER'),
                        "address_line1": row.get('ADDRESS_LINE1'),
                        "address_line2": row.get('ADDRESS_LINE2'),
                        "city": row.get('CITY'),
                        "state_province": row.get('STATE_PROVINCE'),
                        "state_province_code": row.get('STATE_PROVINCE_CODE'),
                        "country": row.get('COUNTRY'),
                        "country_code": row.get('COUNTRY_CODE'),
                        "zip_code": row.get('ZIP_CODE')
                    }
                return True, location
            else:
                return False, "Error getting location"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting specific location")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, location
            except:
                pass

    def insert_location(self, location: Location):
        print(f"\n[INFO] BEGIN - Inserting Location")
        result_flag = False
        result_string = "Success"
        rowcount = 0

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("CODE", location.get_code())
            self.utils.validate_columns_values("NAME", self.utils.replace_special_chars(location.get_name()))
            self.utils.validate_columns_values("ACTIVE_FLG", location.get_active_flg())
            self.utils.validate_columns_values("PHONE_NUMBER", self.utils.replace_special_chars(location.get_phone_number()))
            self.utils.validate_columns_values("ADDRESS_LINE1", self.utils.replace_special_chars(location.get_address_line1()))
            self.utils.validate_columns_values("ADDRESS_LINE2", self.utils.replace_special_chars(location.get_address_line2()))
            self.utils.validate_columns_values("CITY", self.utils.replace_special_chars(location.get_city()))
            self.utils.validate_columns_values("STATE_PROVINCE", self.utils.replace_special_chars(location.get_state_province()))
            self.utils.validate_columns_values("STATE_PROVINCE_CODE", self.utils.replace_special_chars(location.get_state_province_code()))
            self.utils.validate_columns_values("COUNTRY", self.utils.replace_special_chars(location.get_country()))
            self.utils.validate_columns_values("COUNTRY_CODE", self.utils.replace_special_chars(location.get_country_code()))
            self.utils.validate_columns_values("ZIP_CODE", self.utils.replace_special_chars(location.get_zip_code()))

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_LOCATIONS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Location {location.get_code()} inserted successfully")
            else:
                print(f"[ERROR] Error inserting Location {location.get_code()}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            print(f"[INFO] END - Finished inserting Location")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount
            except:
                pass

    def update_location(self, id, location: Location):
        print(f"\n[INFO] BEGIN - Updating Location")
        result_flag = False
        result_string = "Success"
        condition = f"ROW_ID = '{id}'"
        rowcount = 0
        code = location.get_code()

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("CODE", code)
            self.utils.validate_columns_values("NAME", self.utils.replace_special_chars(location.get_name()))
            self.utils.validate_columns_values("ACTIVE_FLG", location.get_active_flg())
            self.utils.validate_columns_values("PHONE_NUMBER", self.utils.replace_special_chars(location.get_phone_number()))
            self.utils.validate_columns_values("ADDRESS_LINE1", self.utils.replace_special_chars(location.get_address_line1()))
            self.utils.validate_columns_values("ADDRESS_LINE2", self.utils.replace_special_chars(location.get_address_line2()))
            self.utils.validate_columns_values("CITY", self.utils.replace_special_chars(location.get_city()))
            self.utils.validate_columns_values("STATE_PROVINCE", self.utils.replace_special_chars(location.get_state_province()))
            self.utils.validate_columns_values("STATE_PROVINCE_CODE", self.utils.replace_special_chars(location.get_state_province_code()))
            self.utils.validate_columns_values("COUNTRY", self.utils.replace_special_chars(location.get_country()))
            self.utils.validate_columns_values("COUNTRY_CODE", self.utils.replace_special_chars(location.get_country_code()))
            self.utils.validate_columns_values("ZIP_CODE", self.utils.replace_special_chars(location.get_zip_code()))

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_LOCATIONS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Location {code} updated successfully")
            else:
                print(f"[ERROR] Error updating Location {code}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            print(f"[INFO] END - Finished updating Location")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, condition
            except:
                pass

    def delete_location(self, id):
        print(f"\n[INFO] BEGIN - Deleting Location")
        result_flag = False
        result_string = "Success"
        condition = f"ROW_ID = '{id}'"
        rowcount = 0

        try:
            result_flag, rowcount, result_string = super().delete_record(super().get_tbl_LOCATIONS(), condition)
            if result_flag:
                print(f"[INFO] Location {id} deleted successfully")
            else:
                print(f"[ERROR] Error deleting Location {id}")

            if result_flag:
                return result_flag, 200, rowcount, result_string
            else:
                return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, rowcount, str(e)
        finally:
            print(f"[INFO] END - Finished deleting Location")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, condition, rowcount
            except:
                pass

    def verify_location_exists(self, id, code, name):
        print(f"\n[INFO] BEGIN - Verifying Location exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        if id is not None and id != "":
            condition += f"\nAND ROW_ID = '{id}'"
        else:
            condition += f"\nAND CODE = '{code}'" if code is not None and code != "" else ""
            condition += f"\nAND NAME = '{code}'" if name is not None and name != "" else ""
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_LOCATIONS(), columns, condition)

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
            print(f"[INFO] END - Finished verifying Location exists")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    # API Functions
    def add_location(self, location_json):
        print(f"\n[INFO] BEGIN - Adding Location")
        location = self.Location()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        location_exists = False

        code = location_json.get('code')
        name = location_json.get('name')
        active_flg = location_json.get('active_flg')
        phone_number = location_json.get('phone_number')
        address_line1 = location_json.get('address_line1')
        address_line2 = location_json.get('address_line2')
        city = location_json.get('city')
        state_province = location_json.get('state_province')
        state_province_code = location_json.get('state_province_code')
        country = location_json.get('country')
        country_code = location_json.get('country_code')
        zip_code = location_json.get('zip_code')

        if code is None or code == "":
            return False, 400, 0, "Location code is required."
        if name is None or name == "":
            return False, 400, 0, "Location name is required."
        if active_flg is None:
            active_flg = True

        try:
            location_exists = self.verify_location_exists(None, code, name)
            if location_exists:
                return False, 409, 0, "Location already exists"
            else:
                location.code = code
                location.name = name
                location.active_flg = active_flg
                location.phone_number = phone_number
                location.address_line1 = address_line1
                location.address_line2 = address_line2
                location.city = city
                location.state_province = state_province
                location.state_province_code = state_province_code
                location.country = country
                location.country_code = country_code
                location.zip_code = zip_code

                result_flag, rowcount, result_string = self.insert_location(location)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            print(f"[INFO] END - Finished adding Location")
            print("[INFO] Cleaning up variables")
            try:
                del location, location_exists, result_flag, rowcount, result_string, code, name, active_flg, phone_number, address_line1, address_line2, city, state_province, state_province_code, country, country_code, zip_code
            except:
                pass

    def edit_location(self, id, location_json):
        print(f"\n[INFO] BEGIN - Editing Location")
        location = self.Location()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        location_exists = False

        code = location_json.get('code')
        name = location_json.get('name')
        active_flg = location_json.get('active_flg')
        phone_number = location_json.get('phone_number')
        address_line1 = location_json.get('address_line1')
        address_line2 = location_json.get('address_line2')
        city = location_json.get('city')
        state_province = location_json.get('state_province')
        state_province_code = location_json.get('state_province_code')
        country = location_json.get('country')
        country_code = location_json.get('country_code')
        zip_code = location_json.get('zip_code')

        if code is None or code == "":
            return False, 400, 0, "Location code is required."
        if name is None or name == "":
            return False, 400, 0, "Location name is required."

        try:
            location_exists = self.verify_location_exists(id, code, name)
            if not location_exists:
                return False, 404, 0, "Location not found!"
            else:
                location.name = name
                location.code = code
                location.active_flg = active_flg
                location.phone_number = phone_number
                location.address_line1 = address_line1
                location.address_line2 = address_line2
                location.city = city
                location.state_province = state_province
                location.state_province_code = state_province_code
                location.country = country
                location.country_code = country_code
                location.zip_code = zip_code

                result_flag, rowcount, result_string = self.update_location(id, location)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            print(f"[INFO] END - Finished editing Location")
            print("[INFO] Cleaning up variables")
            try:
                del location, location_exists, result_flag, rowcount, result_string, name, active_flg, phone_number, address_line1, address_line2, city, state_province, state_province_code, country, country_code, zip_code
            except:
                pass

    def remove_location(self, id):
        print(f"\n[INFO] BEGIN - Removing Location")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        location_exists = False

        if id is None or id == "":
            return False, 400, 0, "Location ID is required."

        try:
            location_exists = self.verify_location_exists(id, None, None)
            if not location_exists:
                return False, 404, 0, "Location not found!"
            else:
                result_flag, rowcount, result_string = self.delete_location(id)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            print(f"[INFO] END - Finished removing Location")
            print("[INFO] Cleaning up variables")
            try:
                del location_exists, result_flag, rowcount, result_string
            except:
                pass

    def get_all_locations_api(self):
        print(f"\n[INFO] BEGIN - Getting all locations")
        result_flag = False
        result = None

        try:
            result_flag, result = self.get_all_locations()
            if result_flag:
                return result_flag, 200, result
            else:
                return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished getting all locations")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass

    def get_all_active_locations_api(self):
        print(f"\n[INFO] BEGIN - Getting all active locations")
        result_flag = False
        result = None

        try:
            result_flag, result = self.get_all_active_locations()
            if result_flag:
                return result_flag, 200, result
            else:
                return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished getting all active locations")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass

    def get_specific_location_api(self, code):
        print(f"\n[INFO] BEGIN - Getting specific location")
        result_flag = False
        result = None
        location_exists = False

        try:
            location_exists = self.verify_location_exists(None, code, None)
            if not location_exists:
                return False, 404, "Location not found!"
            else:
                result_flag, result = self.get_specific_location(code)
                if result_flag:
                    return result_flag, 200, result
                else:
                    return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished getting specific location")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass
