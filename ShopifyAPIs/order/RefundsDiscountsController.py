from database.DataController import DataController
from utils.UtilsController import *
from utils.LogsController import *

class RefundsDiscountsController(DataController):
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "RefundsDiscountsController"

    def get_module_name(self):
        return self.module_name

    class RefundsDiscounts:
        def __init__(self):
            self.discount_code = None
            self.discount_title = None
            self.discount_type = None
            self.divide_by = None
            self.shopify_discount_url = None

        def get_discount_code(self):
            return self.discount_code

        def set_discount_code(self, discount_code):
            self.discount_code = discount_code

        def get_discount_title(self):
            return self.discount_title

        def set_discount_title(self, discount_title):
            self.discount_title = discount_title

        def get_discount_type(self):
            return self.discount_type

        def set_discount_type(self, discount_type):
            self.discount_type = discount_type

        def get_divide_by(self):
            return self.divide_by

        def set_divide_by(self, divide_by):
            self.divide_by = divide_by

        def get_shopify_discount_url(self):
            return self.shopify_discount_url

        def set_shopify_discount_url(self, shopify_discount_url):
            self.shopify_discount_url = shopify_discount_url

    # Database Functions
    def get_all_refunds_discounts(self):
        # print(f"\n[INFO] BEGIN - Getting all refunds discounts")
        columns = ["*"]
        condition = "1=1"
        result_flag = False
        result_query = None
        refunds_discounts = []

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REFUNDS_DISCOUNTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    refunds_discount = {
                        "id": row.get('ID'),
                        "discount_code": row.get('DISCOUNT_CODE'),
                        "discount_title": row.get('DISCOUNT_TITLE'),
                        "discount_type": row.get('DISCOUNT_TYPE'),
                        "divide_by": row.get('DIVIDE_BY'),
                        "shopify_discount_url": row.get('SHOPIFY_DISCOUNT_URL')
                    }
                    refunds_discounts.append(refunds_discount)
                return True, refunds_discounts
            else:
                return False, "Error getting refunds discounts"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all refunds discounts")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, refunds_discounts
            except:
                pass

    def get_specific_refunds_discount(self, discount_code=None, discount_title=None, discount_type=None, divide_by=None, shopify_discount_url=None):
        print(f"\n[INFO] BEGIN - Getting specific refunds discount")
        columns = ["*"]
        condition = "1=1"
        condition += f"\nAND DISCOUNT_CODE = '{discount_code}'" if discount_code is not None and discount_code != "" else ""
        condition += f"\nAND DISCOUNT_TITLE = '{discount_title}'" if discount_title is not None and discount_title != "" else ""
        condition += f"\nAND DISCOUNT_TYPE = '{discount_type}'" if discount_type is not None and discount_type != "" else ""
        condition += f"\nAND DIVIDE_BY = {divide_by}" if divide_by is not None and divide_by != "" else ""
        condition += f"\nAND SHOPIFY_DISCOUNT_URL = '{shopify_discount_url}'" if shopify_discount_url is not None and shopify_discount_url != "" else ""
        result_flag = False
        result_query = None
        refunds_discount = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REFUNDS_DISCOUNTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    refunds_discount = {
                        "id": row.get('ID'),
                        "discount_code": row.get('DISCOUNT_CODE'),
                        "discount_title": row.get('DISCOUNT_TITLE'),
                        "discount_type": row.get('DISCOUNT_TYPE'),
                        "divide_by": row.get('DIVIDE_BY'),
                        "shopify_discount_url": row.get('SHOPIFY_DISCOUNT_URL')
                    }
                return True, refunds_discount
            else:
                return False, "Error getting refunds discount"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting specific refunds discount")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, refunds_discount
            except:
                pass

    def insert_refunds_discount(self, refunds_discount: RefundsDiscounts):
        # print(f"\n[INFO] BEGIN - Inserting Refunds Discount")
        result_flag = False
        result_string = "Success"
        rowcount = 0
        keep_trying = True
        count_try = 0
        maximum_insert_try = 100

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("DISCOUNT_CODE", refunds_discount.get_discount_code())
            self.utils.validate_columns_values("DISCOUNT_TITLE", refunds_discount.get_discount_title())
            self.utils.validate_columns_values("DISCOUNT_TYPE", refunds_discount.get_discount_type())
            self.utils.validate_columns_values("DIVIDE_BY", refunds_discount.get_divide_by())
            self.utils.validate_columns_values("SHOPIFY_DISCOUNT_URL", refunds_discount.get_shopify_discount_url())

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_REFUNDS_DISCOUNTS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Refunds Discount Inserted...\t\tDiscount Code: {refunds_discount.get_discount_code()}")
            else:
                if "1062" in result_string or "Duplicate entry" in result_string:
                    if "PRIMARY" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            print(f"[INFO] Trying to insert Refunds Discount again...\tDiscount Code: {refunds_discount.get_discount_code()}.Try: {count_try}")
                            super().generate_next_id()
                            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_REFUNDS_DISCOUNTS(), self.utils.get_columns_array(), self.utils.get_values_array())

                            if result_flag == True:
                                print(f"[INFO] Refunds Discount Inserted...\t\tDiscount Code: {refunds_discount.get_discount_code()}")
                                keep_trying = False
                            else:
                                if "ID" in result_string:
                                    print(f"[INFO] Updating Refunds Discount...\t\tDiscount Code: {refunds_discount.get_discount_code()}")
                                    self.utils.clear_condition()
                                    self.utils.set_condition(f"DISCOUNT_CODE = '{refunds_discount.get_discount_code()}'")
                                    result_flag, rowcount, result_string = super().update_record(super().get_tbl_REFUNDS_DISCOUNTS(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                                    if result_flag == True:
                                        print(f"[INFO] Refunds Discount Updated...\t\tDiscount Code: {refunds_discount.get_discount_code()}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Refunds Discount NOT Inserted/Maximum tries...\tDiscount Code: {refunds_discount.get_discount_code()}.Try: {count_try}")
                                            keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Refunds Discount NOT Inserted/Maximum tries...\tDiscount Code: {refunds_discount.get_discount_code()}.Try: {count_try}")
                                        keep_trying = False
                    else:
                        print(f"[INFO] Updating Refunds Discount...\t\tDiscount Code: {refunds_discount.get_discount_code()}")
                        self.utils.clear_condition()
                        self.utils.set_condition(f"DISCOUNT_CODE = '{refunds_discount.get_discount_code()}'")
                        result_flag, rowcount, result_string = super().update_record(super().get_tbl_REFUNDS_DISCOUNTS(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())
                else:
                    print(f"[ERROR] Error inserting Refunds Discount...\tDiscount Code: {refunds_discount.get_discount_code()}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            # print(f"[INFO] END - Finished inserting Refunds Discount")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount, keep_trying, count_try, maximum_insert_try
            except:
                pass

    def update_refunds_discount(self, row_id, discount_code, refunds_discount: RefundsDiscounts):
        # print(f"\n[INFO] BEGIN - Updating Refunds Discount")
        result_flag = False
        result_string = "Success"
        condition = "1=1"
        condition += f"AND ROW_ID = '{row_id}'" if row_id is not None and row_id != "" else ""
        condition += f"DISCOUNT_CODE = '{discount_code}'" if discount_code is not None and discount_code != "" else ""
        rowcount = 0

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("DISCOUNT_CODE", refunds_discount.get_discount_code())
            self.utils.validate_columns_values("DISCOUNT_TITLE", refunds_discount.get_discount_title())
            self.utils.validate_columns_values("DISCOUNT_TYPE", refunds_discount.get_discount_type())
            self.utils.validate_columns_values("DIVIDE_BY", refunds_discount.get_divide_by())
            self.utils.validate_columns_values("SHOPIFY_DISCOUNT_URL", refunds_discount.get_shopify_discount_url())

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_REFUNDS_DISCOUNTS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Refunds Discount Updated...\t\tDiscount Code: {refunds_discount.get_discount_code()}")
            else:
                print(f"[ERROR] Error updating Refunds Discount...\tDiscount Code: {refunds_discount.get_discount_code()}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            # print(f"[INFO] END - Finished updating Refunds Discount")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, condition
            except:
                pass

    def delete_refunds_discount(self, discount_code):
        # print(f"\n[INFO] BEGIN - Deleting Refunds Discount")
        result_flag = False
        result_string = "Success"
        condition = f"DISCOUNT_CODE = '{discount_code}'"
        rowcount = 0

        try:
            result_flag, rowcount, result_string = super().delete_record(super().get_tbl_REFUNDS_DISCOUNTS(), condition)
            if result_flag:
                print(f"[INFO] Refunds Discount Deleted...\t\tDiscount Code: {discount_code}")
            else:
                print(f"[ERROR] Error deleting Refunds Discount...\tDiscount Code: {discount_code}")

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, rowcount, str(e)
        finally:
            # print(f"[INFO] END - Finished deleting Refunds Discount")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, condition, rowcount
            except:
                pass

    def verify_refunds_discount_exists(self, row_id, discount_code):
        # print(f"\n[INFO] BEGIN - Verifying Refunds Discount exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = '1=1'
        condition += f"DISCOUNT_CODE = '{discount_code}'" if discount_code is not None and discount_code != "" else ""
        condition += f"AND ROW_ID = '{row_id}'" if row_id is not None and row_id != "" else ""
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_REFUNDS_DISCOUNTS(), columns, condition)

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
            # print(f"[INFO] END - Finished verifying Refunds Discount exists")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    def upsert_refunds_discount(self, discount_code, discount: RefundsDiscounts):
        # print(f"\n[INFO] BEGIN - Upserting Refunds Discount")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_exists = False

        try:
            discount_exists = self.verify_refunds_discount_exists(None, discount_code)
            if discount_exists:
                result_flag, rowcount, result_string = self.update_refunds_discount(None, discount_code, discount)
            else:
                result_flag, rowcount, result_string = self.insert_refunds_discount(discount_code, discount)

            return result_flag, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Refunds Discount")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, rowcount, result_string, discount_exists
            except:
                pass

    # APIs Functions
    def upsert_refunds_discount_api(self, discount_code, discount_json):
        # print(f"\n[INFO] BEGIN - Upserting Refunds Discount")
        discount = self.RefundsDiscount()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_exists = False

        if discount_code is None or discount_code == "":
            return False, 400, 0, "Discount code is required."

        discount_type = discount_json.get('discount_type')
        discount_amount = discount_json.get('discount_amount')
        discount_percentage = discount_json.get('discount_percentage')
        expiration_date = discount_json.get('expiration_date')
        max_redemptions = discount_json.get('max_redemptions')
        times_redeemed = discount_json.get('times_redeemed')

        try:
            discount.discount_code = discount_code
            discount.discount_type = discount_type
            discount.discount_amount = discount_amount
            discount.discount_percentage = discount_percentage
            discount.expiration_date = expiration_date
            discount.max_redemptions = max_redemptions
            discount.times_redeemed = times_redeemed

            result_flag, rowcount, result_string = self.upsert_refunds_discount(discount_code, discount)
            if result_flag:
                return result_flag, 200, rowcount, result_string
            else:
                return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished upserting Refunds Discount")
            # print("[INFO] Cleaning up variables")
            try:
                del discount, discount_exists, result_flag, rowcount, result_string, discount_code, discount_type, discount_amount, discount_percentage, expiration_date, max_redemptions, times_redeemed
            except:
                pass

    def add_refunds_discount(self, discount_code, discount_json):
        # print(f"\n[INFO] BEGIN - Adding Refunds Discount")
        discount = self.RefundsDiscount()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_exists = False

        if discount_code is None or discount_code == "":
            return False, 400, 0, "Discount code is required."

        discount_type = discount_json.get('discount_type')
        discount_amount = discount_json.get('discount_amount')
        discount_percentage = discount_json.get('discount_percentage')
        expiration_date = discount_json.get('expiration_date')
        max_redemptions = discount_json.get('max_redemptions')
        times_redeemed = discount_json.get('times_redeemed')

        try:
            discount_exists = self.verify_refunds_discount_exists(None, discount_code)
            if discount_exists:
                return False, 409, 0, "Refunds Discount already exists"
            else:
                discount.discount_code = discount_code
                discount.discount_type = discount_type
                discount.discount_amount = discount_amount
                discount.discount_percentage = discount_percentage
                discount.expiration_date = expiration_date
                discount.max_redemptions = max_redemptions
                discount.times_redeemed = times_redeemed

                result_flag, rowcount, result_string = self.insert_refunds_discount(discount_code, discount)
                if result_flag:
                    return result_flag, 201, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished adding Refunds Discount")
            # print("[INFO] Cleaning up variables")
            try:
                del discount, discount_exists, result_flag, rowcount, result_string, discount_code, discount_type, discount_amount, discount_percentage, expiration_date, max_redemptions, times_redeemed
            except:
                pass

    def edit_refunds_discount(self, row_id, discount_code, discount_json):
        # print(f"\n[INFO] BEGIN - Editing Refunds Discount")
        discount = self.RefundsDiscount()
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_exists = False

        discount_code_new = discount_json.get('discount_code')
        discount_type = discount_json.get('discount_type')
        discount_amount = discount_json.get('discount_amount')
        discount_percentage = discount_json.get('discount_percentage')
        expiration_date = discount_json.get('expiration_date')
        max_redemptions = discount_json.get('max_redemptions')
        times_redeemed = discount_json.get('times_redeemed')

        if discount_code is None or discount_code == "":
            return False, 400, 0, "Discount code is required."

        try:
            discount_exists = self.verify_refunds_discount_exists(row_id, discount_code)
            if not discount_exists:
                return False, 404, 0, "Refunds Discount not found!"
            else:
                discount.discount_code = discount_code_new
                discount.discount_type = discount_type
                discount.discount_amount = discount_amount
                discount.discount_percentage = discount_percentage
                discount.expiration_date = expiration_date
                discount.max_redemptions = max_redemptions
                discount.times_redeemed = times_redeemed

                result_flag, rowcount, result_string = self.update_refunds_discount(row_id, discount_code, discount)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished editing Refunds Discount")
            # print("[INFO] Cleaning up variables")
            try:
                del discount, discount_exists, result_flag, rowcount, result_string, discount_code, discount_type, discount_amount, discount_percentage, expiration_date, max_redemptions, times_redeemed
            except:
                pass

    def remove_refunds_discount(self, row_id, discount_code):
        # print(f"\n[INFO] BEGIN - Removing Refunds Discount")
        result_flag = False
        rowcount = 0
        result_string = "Success"
        discount_exists = False

        if discount_code is None or discount_code == "":
            return False, 400, 0, "Discount code is required."

        try:
            discount_exists = self.verify_refunds_discount_exists(row_id, discount_code)
            if not discount_exists:
                return False, 404, 0, "Refunds Discount not found!"
            else:
                result_flag, rowcount, result_string = self.delete_refunds_discount(discount_code)
                if result_flag:
                    return result_flag, 200, rowcount, result_string
                else:
                    return result_flag, 500, rowcount, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            # print(f"[INFO] END - Finished removing Refunds Discount")
            # print("[INFO] Cleaning up variables")
            try:
                del discount_exists, result_flag, rowcount, result_string
            except:
                pass

    def get_all_refunds_discounts_api(self):
        # print(f"\n[INFO] BEGIN - Getting all refunds discounts")
        result_flag = False
        result = None

        try:
            result_flag, result = self.get_all_refunds_discounts()
            if result_flag:
                return result_flag, 200, result
            else:
                return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting all refunds discounts")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass

    def get_specific_refunds_discount_api(self, row_id, discount_code, discount_title=None, discount_type=None, divide_by=None, shopify_discount_url=None):
        # print(f"\n[INFO] BEGIN - Getting specific refunds discount")
        result_flag = False
        result = None
        discount_exists = False

        try:
            discount_exists = self.verify_refunds_discount_exists(row_id, discount_code)
            if not discount_exists:
                return False, 404, "Refunds Discount not found!"
            else:
                result_flag, result = self.get_specific_refunds_discount(discount_code, discount_title, discount_type, divide_by, shopify_discount_url)
                if result_flag:
                    return result_flag, 200, result
                else:
                    return result_flag, 500, result
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            # print(f"[INFO] END - Finished getting specific refunds discount")
            # print("[INFO] Cleaning up variables")
            try:
                del result_flag, result
            except:
                pass


