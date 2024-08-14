from database.DataController import DataController
from utils.UtilsController import *
from utils.LogsController import *

class ShippingAddressController(DataController):
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "OrderShippingAddressController"

    def get_module_name(self):
        return self.module_name

    class ShippingAddress:
        def __init__(self):
            self.order_id = None
            self.first_name = None
            self.last_name = None
            self.name = None
            self.company = None
            self.address1 = None
            self.address2 = None
            self.city = None
            self.zip = None
            self.province = None
            self.province_code = None
            self.country = None
            self.country_code = None
            self.phone = None
            self.latitude = None
            self.longitude = None

        def get_order_id(self):
            return self.order_id

        def get_first_name(self):
            return self.first_name

        def set_first_name(self, first_name):
            self.first_name = first_name

        def get_last_name(self):
            return self.last_name

        def set_last_name(self, last_name):
            self.last_name = last_name

        def get_name(self):
            return self.name

        def set_name(self, name):
            self.name = name

        def get_company(self):
            return self.company

        def set_company(self, company):
            self.company = company

        def get_address1(self):
            return self.address1

        def set_address1(self, address1):
            self.address1 = address1

        def get_address2(self):
            return self.address2

        def set_address2(self, address2):
            self.address2 = address2

        def get_city(self):
            return self.city

        def set_city(self, city):
            self.city = city

        def get_zip(self):
            return self.zip

        def set_zip(self, zip):
            self.zip = zip

        def get_province(self):
            return self.province

        def set_province(self, province):
            self.province = province

        def get_province_code(self):
            return self.province_code

        def set_province_code(self, province_code):
            self.province_code = province_code

        def get_country(self):
            return self.country

        def set_country(self, country):
            self.country = country

        def get_country_code(self):
            return self.country_code

        def set_country_code(self, country_code):
            self.country_code = country_code

        def get_phone(self):
            return self.phone

        def set_phone(self, phone):
            self.phone = phone

        def get_latitude(self):
            return self.latitude

        def set_latitude(self, latitude):
            self.latitude = latitude

        def get_longitude(self):
            return self.longitude

        def set_longitude(self, longitude):
            self.longitude = longitude

    # Database Functions
    def get_all_shipping_addresses(self):
        print(f"\n[INFO] BEGIN - Getting all billing addresses")
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        addresses = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_SHIPPING_ADDRESS(), columns, condition)

            if result_flag:
                for row in result_query:
                    address = {
                        "order_id": row.get('ORDER_ID'),
                        "first_name": row.get('FIRST_NAME'),
                        "last_name": row.get('LAST_NAME'),
                        "address1": row.get('ADDRESS1'),
                        "address2": row.get('ADDRESS2'),
                        "city": row.get('CITY'),
                        "zip": row.get('ZIP'),
                        "province": row.get('PROVINCE'),
                        "country": row.get('COUNTRY'),
                        "phone": row.get('PHONE'),
                        "email": row.get('EMAIL'),
                        "created_at": row.get('CREATED_AT'),
                        "updated_at": row.get('UPDATED_AT')
                    }
                    addresses.append(address)
                return True, addresses
            else:
                return False, "Error getting billing addresses"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all billing addresses")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, addresses
            except:
                pass

    def get_specific_shipping_address(self, order_id):
        # print(f"\n[INFO] BEGIN - Getting specific billing address")
        columns = ["*"]
        condition = f"ORDER_ID = '{order_id}'"
        result_flag = False
        result_query = None
        address = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_SHIPPING_ADDRESS(), columns, condition)

            if result_flag:
                for row in result_query:
                    address = {
                        "order_id": row.get('ORDER_ID'),
                        "first_name": row.get('FIRST_NAME'),
                        "last_name": row.get('LAST_NAME'),
                        "address1": row.get('ADDRESS1'),
                        "address2": row.get('ADDRESS2'),
                        "city": row.get('CITY'),
                        "zip": row.get('ZIP'),
                        "province": row.get('PROVINCE'),
                        "country": row.get('COUNTRY'),
                        "phone": row.get('PHONE'),
                        "email": row.get('EMAIL'),
                        "created_at": row.get('CREATED_AT'),
                        "updated_at": row.get('UPDATED_AT')
                    }
                return True, address
            else:
                return False, "Error getting billing address"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific billing address")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, address
            except:
                pass

    def insert_shipping_address(self, order_id, address: ShippingAddress):
        # print(f"\n[INFO] BEGIN - Inserting Shipping Address")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        keep_trying = True
        count_try = 0
        maximum_insert_try = 100

        if order_id is None or order_id == "":
            return False, rowcount, "Order ID is required."

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("FIRST_NAME", self.utils.replace_special_chars(address.get_first_name()))
            self.utils.validate_columns_values("LAST_NAME", self.utils.replace_special_chars(address.get_last_name()))
            self.utils.validate_columns_values("FULL_NAME", self.utils.replace_special_chars(address.get_name()))
            self.utils.validate_columns_values("COMPANY", self.utils.replace_special_chars(address.get_company()))
            self.utils.validate_columns_values("PHONE", self.utils.replace_special_chars(address.get_phone()))
            self.utils.validate_columns_values("ADDRESS_LINE_1", self.utils.replace_special_chars(address.get_address1()))
            self.utils.validate_columns_values("ADDRESS_LINE_2", self.utils.replace_special_chars(address.get_address2()))
            self.utils.validate_columns_values("CITY", self.utils.replace_special_chars(address.get_city()))
            self.utils.validate_columns_values("STATE_PROVINCE", self.utils.replace_special_chars(address.get_province()))
            self.utils.validate_columns_values("STATE_PROVINCE_CODE", self.utils.replace_special_chars(address.get_province_code()))
            self.utils.validate_columns_values("COUNTRY", self.utils.replace_special_chars(address.get_country()))
            self.utils.validate_columns_values("COUNTRY_CODE", self.utils.replace_special_chars(address.get_country_code()))
            self.utils.validate_columns_values("ZIP_CODE", self.utils.replace_special_chars(address.get_zip()))
            self.utils.validate_columns_values("LATITUDE", self.utils.replace_special_chars(address.get_latitude()))
            self.utils.validate_columns_values("LONGITUDE", self.utils.replace_special_chars(address.get_longitude()))

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_SHIPPING_ADDRESS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Shipping Address Inserted...\t\tOrder: {order_id}")
            else:
                if "1062" in result_string or "Duplicate entry" in result_string:
                    if "PRIMARY" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            print(f"[INFO] Trying to insert Shipping Address again...\tOrder: {order_id}.Try: {count_try}")
                            super().generate_next_id()
                            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_SHIPPING_ADDRESS(), self.utils.get_columns_array(), self.utils.get_values_array())

                            if result_flag == True:
                                print(f"[INFO] Shipping Address Inserted...\t\tOrder: {order_id}")
                                keep_trying = False
                            else:
                                if "ID" in result_string:
                                    print(f"[INFO] Updating Shipping Address...\t\tOrder: {order_id}")
                                    self.utils.clear_condition()
                                    self.utils.set_condition(f"ORDER_ID = '{order_id}'")
                                    result_flag, rowcount, result_string = super().update_record(super().get_tbl_SHIPPING_ADDRESS(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                                    if result_flag == True:
                                        print(f"[INFO] Shipping Address Updated...\t\tOrder: {order_id}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Shipping Address NOT Inserted/Maximum tries...\tOrder: {order_id}.Try: {count_try}")
                                            keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Shipping Address NOT Inserted/Maximum tries...\tOrder: {order_id}.Try: {count_try}")
                                        keep_trying = False
                    else:
                        print(f"[INFO] Updating Shipping Address...\t\tOrder: {order_id}")
                        self.utils.clear_condition()
                        self.utils.set_condition(f"ORDER_ID = '{order_id}'")
                        result_flag, rowcount, result_string = super().update_record(super().get_tbl_SHIPPING_ADDRESS(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())
                else:
                    print(f"[ERROR] Error inserting Shipping Address...\tOrder: {order_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            # print(f"[INFO] END - Finished inserting Shipping Address")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount, keep_trying, count_try, maximum_insert_try
            except:
                pass

    def update_shipping_address(self, order_id, address: ShippingAddress):
        # print(f"\n[INFO] BEGIN - Updating Shipping Address")
        result_flag = False
        result_string = "Success"
        condition = f"ORDER_ID = '{order_id}'"
        rowcount = 0

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ORDER_ID", order_id)
            self.utils.validate_columns_values("FIRST_NAME", self.utils.replace_special_chars(address.get_first_name()))
            self.utils.validate_columns_values("LAST_NAME", self.utils.replace_special_chars(address.get_last_name()))
            self.utils.validate_columns_values("FULL_NAME", self.utils.replace_special_chars(address.get_name()))
            self.utils.validate_columns_values("COMPANY", self.utils.replace_special_chars(address.get_company()))
            self.utils.validate_columns_values("PHONE", self.utils.replace_special_chars(address.get_phone()))
            self.utils.validate_columns_values("ADDRESS_LINE_1", self.utils.replace_special_chars(address.get_address1()))
            self.utils.validate_columns_values("ADDRESS_LINE_2", self.utils.replace_special_chars(address.get_address2()))
            self.utils.validate_columns_values("CITY", self.utils.replace_special_chars(address.get_city()))
            self.utils.validate_columns_values("STATE_PROVINCE", self.utils.replace_special_chars(address.get_province()))
            self.utils.validate_columns_values("STATE_PROVINCE_CODE", self.utils.replace_special_chars(address.get_province_code()))
            self.utils.validate_columns_values("COUNTRY", self.utils.replace_special_chars(address.get_country()))
            self.utils.validate_columns_values("COUNTRY_CODE", self.utils.replace_special_chars(address.get_country_code()))
            self.utils.validate_columns_values("ZIP_CODE", self.utils.replace_special_chars(address.get_zip()))
            self.utils.validate_columns_values("LATITUDE", self.utils.replace_special_chars(address.get_latitude()))
            self.utils.validate_columns_values("LONGITUDE", self.utils.replace_special_chars(address.get_longitude()))

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_SHIPPING_ADDRESS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Shipping Address Updated...\t\tOrder: {order_id}")
            else:
                print(f"[ERROR] Error updating Shipping Address...\tOrder: {order_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            # print(f"[INFO] END - Finished updating Shipping Address")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, condition
            except:
                pass

    def delete_shipping_address(self, order_id):
        # print(f"\n[INFO] BEGIN - Deleting Shipping Address")
        result_flag = False
        result_string = "Success"
        condition = f"ORDER_ID = '{order_id}'"
        rowcount = 0

        try:
            result_flag, rowcount, result_string = super().delete_record(super().get_tbl_SHIPPING_ADDRESS(), condition)
            if result_flag:
                print(f"[INFO] Shipping Address Deleted...\t\tOrder: {order_id}")
            else:
                print(f"[ERROR] Error deleting Shipping Address...\tOrder: {order_id}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            # print(f"[INFO] END - Finished deleting Shipping Address")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, condition, rowcount
            except:
                pass

    def verify_shipping_address_exists(self, order_id):
        # print(f"\n[INFO] BEGIN - Verifying Shipping Address exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = f"ORDER_ID = '{order_id}'"
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_SHIPPING_ADDRESS(), columns, condition)

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
            # print(f"[INFO] END - Finished verifying Shipping Address exists")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    def upsert_shipping_address(self, order_id, address: ShippingAddress):
        # print(f"\n[INFO] BEGIN - Upserting Shipping Address")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        address_exists = False

        try:
            address_exists = self.verify_shipping_address_exists(order_id)
            if address_exists:
                result_flag, rowcount, result_string = self.update_shipping_address(order_id, address)
            else:
                result_flag, rowcount, result_string = self.insert_shipping_address(order_id, address)

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Shipping Address")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, address_exists
            except:
                pass

    # API Functions
    def upsert_shipping_address_api(self, order_id, address_json):
        # print(f"\n[INFO] BEGIN - Upserting Shipping Address")
        address = self.ShippingAddress()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        address_exists = False

        if order_id is None or order_id == "":
            return False, 400, 0, "Order ID is required."

        first_name = address_json.get('first_name')
        last_name = address_json.get('last_name')
        name = address_json.get('name')
        company = address_json.get('company')
        address1 = address_json.get('address1')
        address2 = address_json.get('address2')
        city = address_json.get('city')
        zip = address_json.get('zip')
        province = address_json.get('province')
        province_code = address_json.get('province_code')
        country = address_json.get('country')
        country_code = address_json.get('country_code')
        phone = address_json.get('phone')
        latitude = address_json.get('latitude')
        longitude = address_json.get('longitude')

        try:
            address.order_id = order_id
            address.first_name = first_name
            address.last_name = last_name
            address.name = name
            address.company = company
            address.address1 = address1
            address.address2 = address2
            address.city = city
            address.zip = zip
            address.province = province
            address.province_code = province_code
            address.country = country
            address.country_code = country_code
            address.phone = phone
            address.latitude = latitude
            address.longitude = longitude

            result_flag, rowcount, result_string = self.upsert_shipping_address(order_id, address)
            if result_flag:
                return result_flag, 200, rowcount, result_string
            else:
                return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Shipping Address")
            # print("[INFO] Cleaning up variables")
            try:
                del address, address_exists, result_flag, rowcount, result_string, order_id, first_name, last_name, name, company, address1, address2, city, zip, province, province_code, country, country_code, phone, latitude, longitude
            except:
                pass

    def add_shipping_address(self, order_id, address_json):
        # print(f"\n[INFO] BEGIN - Adding Shipping Address")
        address = self.ShippingAddress()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        address_exists = False

        if order_id is None or order_id == "":
            return False, 400, 0, "Order ID is required."

        first_name = address_json.get('first_name')
        last_name = address_json.get('last_name')
        name = address_json.get('name')
        company = address_json.get('company')
        address1 = address_json.get('address1')
        address2 = address_json.get('address2')
        city = address_json.get('city')
        zip = address_json.get('zip')
        province = address_json.get('province')
        province_code = address_json.get('province_code')
        country = address_json.get('country')
        country_code = address_json.get('country_code')
        phone = address_json.get('phone')
        latitude = address_json.get('latitude')
        longitude = address_json.get('longitude')

        try:
            address_exists = self.verify_shipping_address_exists(order_id)
            if address_exists:
                return False, 409, 0, "Shipping Address already exists"
            else:
                # address.order_id = order_id
                address.first_name = first_name
                address.last_name = last_name
                address.name = name
                address.company = company
                address.address1 = address1
                address.address2 = address2
                address.city = city
                address.zip = zip
                address.province = province
                address.province_code = province_code
                address.country = country
                address.country_code = country_code
                address.phone = phone
                address.latitude = latitude
                address.longitude = longitude

                result_flag, rowcount, result_string = self.insert_shipping_address(order_id, address)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished adding Shipping Address")
            # print("[INFO] Cleaning up variables")
            try:
                del address, address_exists, result_flag, rowcount, result_string, order_id, first_name, last_name, name, company, address1, address2, city, zip, province, province_code, country, country_code, phone, latitude, longitude
            except:
                pass

    def edit_shipping_address(self, order_id, address_json):
        # print(f"\n[INFO] BEGIN - Editing Shipping Address")
        address = self.ShippingAddress()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        address_exists = False

        first_name = address_json.get('first_name')
        last_name = address_json.get('last_name')
        name = address_json.get('name')
        company = address_json.get('company')
        address1 = address_json.get('address1')
        address2 = address_json.get('address2')
        city = address_json.get('city')
        zip = address_json.get('zip')
        province = address_json.get('province')
        province_code = address_json.get('province_code')
        country = address_json.get('country')
        country_code = address_json.get('country_code')
        phone = address_json.get('phone')
        latitude = address_json.get('latitude')
        longitude = address_json.get('longitude')

        if order_id is None or order_id == "":
            return False, 400, 0, "Order ID is required."

        try:
            address_exists = self.verify_shipping_address_exists(order_id)
            if not address_exists:
                return False, 404, 0, "Shipping Address not found!"
            else:
                # address.order_id = order_id
                address.first_name = first_name
                address.last_name = last_name
                address.name = name
                address.company = company
                address.address1 = address1
                address.address2 = address2
                address.city = city
                address.zip = zip
                address.province = province
                address.province_code = province_code
                address.country = country
                address.country_code = country_code
                address.phone = phone
                address.latitude = latitude
                address.longitude = longitude

                result_flag, rowcount, result_string = self.update_shipping_address(order_id, address)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished editing Shipping Address")
            # print("[INFO] Cleaning up variables")
            try:
                del address, address_exists, result_flag, rowcount, result_string, first_name, last_name, name, company, address1, address2, city, zip, province, province_code, country, country_code, phone, latitude, longitude
            except:
                pass

    def remove_shipping_address(self, order_id):
        # print(f"\n[INFO] BEGIN - Removing Shipping Address")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        address_exists = False

        if order_id is None or order_id == "":
            return False, 400, 0, "Order ID is required."

        try:
            address_exists = self.verify_shipping_address_exists(order_id)
            if not address_exists:
                return False, 404, 0, "Shipping Address not found!"
            else:
                result_flag, rowcount, result_string = self.delete_shipping_address(order_id)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished removing Shipping Address")
            # print("[INFO] Cleaning up variables")
            try:
                del address_exists, result_flag, rowcount, result_string
            except:
                pass

    def get_all_shipping_addresses_api(self):
        # print(f"\n[INFO] BEGIN - Getting all billing addresses")
        result_flag = False
        result = None

        try:
            result_flag, result = self.get_all_shipping_addresses()
            if result_flag:
                return result_flag, 200, result
            else:
                return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all billing addresses")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass

    def get_specific_shipping_address_api(self, order_id):
        # print(f"\n[INFO] BEGIN - Getting specific billing address")
        result_flag = False
        result = None
        address_exists = False

        try:
            address_exists = self.verify_shipping_address_exists(order_id)
            if not address_exists:
                return False, 404, "Shipping Address not found!"
            else:
                result_flag, result = self.get_specific_shipping_address(order_id)
                if result_flag:
                    return result_flag, 200, result
                else:
                    return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific billing address")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass
