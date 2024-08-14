from database.DataController import *
from order.OrderController import OrderController
from system.SysPrefController import *
from system.FontsController import *
# from product.ProductController import *
from utils.UtilsController import *
from apis.inbound.ShopifyController import *
from system.ReportsController import *
from utils.LogsController import *
from vendors.VendorsController import *
from locations.LocationsController import *

class CustomsProductController(DataController):
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.order = OrderController()
        self.utils = UtilsController()
        self.shopify = ShopifyController()
        self.log = LogsController()
        self.syspref = SysPrefController()
        self.reports = ReportsController()
        self.vendors = VendorsController()
        self.locations = LocationsController()
        self.fonts_ctrl = FontsController()
        self.module_name = "CustomsProductController"
        self.start_time = None
        self.end_time = None
        self.custom_order_mapping = {
            'ID': 'order_id',
            'NAME': 'order_number',
            'CREATED_AT': 'created_at',
            'PROD_NAME': 'product_name',
            'FONT': 'font',
            'CUSTOM_TEXT': 'custom_text',
            'PROD_SKU': 'sku',
            'QUANTITY': 'quantity',
            'VENDOR_NAME': 'vendor_name',
            'SORORITY_FLG': 'sorority_flag',
            'RUSH_FLG': 'rush_flag',
            'SENT_TO_VENDOR_DATE': 'sent_to_vendor_date',
            'SENT_BY_VENDOR_FLG': 'sent_back_from_vendor_flag',
            'COUNTRY': 'country',
            'COUNTRY_CODE': 'country_code',
            'LOCATION_NAME': 'location_name',
            'SHIPPED_DATE': 'sent_back_from_vendor_date',
            'TRACKING_NUMBER': 'tracking_number',
            'TRACKING_URL': 'tracking_url',
            'RECEIVED_FLG': 'received_flag',
            'RECEIVED_DATE': 'received_date',
            'RECEIVED_BY': 'received_by',
            'RECEIVED_NOTES': 'received_notes',
            'FULFILLED_FLG': 'is_fulfilled_flag',
            'FULFILLMENT_STATUS': 'fulfillment_status',
            'FULFILLED_DATE': 'fulfilled_date',
            'CANCELLED_FLG': 'is_cancelled_flag',
            'VENDOR_PROCESSING_TIME': 'vendor_processing_time',
            'VENDOR_PROCESSING_TIME_UNIT': 'vendor_processing_time_unit',
            'TOTAL_SHIPPING_TIME': 'total_shipping_time',
            'TOTAL_SHIPPING_TIME_UNIT': 'total_shipping_time_unit',
            'TOTAL_PROCESSING_TIME': 'total_processing_time',
            'TOTAL_PROCESSING_TIME_UNIT': 'total_processing_time_unit'
        }

    class CustomProductFont():
        def __init__(self):
            self.id = None
            self.title = None
            self.has_variant_flg = None
            self.font_name = None
            self.vendor_id = None
            self.vendor_name = None

        # GETTERS
        def get_id(self):
            return self.id

        def get_title(self):
            return self.title

        def get_has_variant_flg(self):
            return self.has_variant_flg

        def get_font_name(self):
            return self.font_name

        def get_vendor_id(self):
            return self.vendor_id

        def get_vendor_name(self):
            return self.vendor_name

        # SETTERS
        def set_id(self, value):
            self.id = value

        def set_title(self, value):
            self.title = value

        def set_has_variant_flg(self, value):
            self.has_variant_flg = value

        def set_font_name(self, value):
            self.font_name = value

        def set_vendor_id(self, value):
            self.vendor_id = value

        def set_vendor_name(self, value):
            self.vendor_name = value

    class CustomProductVariantFont():
        def __init__(self):
            self.product_id = None
            self.name = None
            self.font_name = None

        # GETTERS
        def get_product_id(self):
            return self.product_id

        def get_name(self):
            return self.name

        def get_font_name(self):
            return self.font_name

        # SETTERS
        def set_product_id(self, value):
            self.product_id = value

        def set_name(self, value):
            self.name = value

        def set_font_name(self, value):
            self.font_name = value

    # Getters
    def get_module_name(self):
        return self.module_name

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    def get_custom_order_mapping(self):
        return self.custom_order_mapping

    def get_custom_order_mapping_key(self, key):
        return self.custom_order_mapping.get(key)

    def get_custom_order_mapping_value(self, value):
        return next((k for k, v in self.custom_order_mapping.items() if v == value), None)

    def get_custom_order_mapping_keys(self):
        return self.custom_order_mapping.keys()

    def get_custom_order_mapping_values(self):
        return self.custom_order_mapping.values()

    def get_custom_order_mapping_items(self):
        return self.custom_order_mapping.items()

    # Setters
    def set_start_time(self, value):
        self.start_time = value

    def set_end_time(self, value):
        self.end_time = value

    # Custom processing functions
    def get_product_font_name(self, product_id, variant_name):
        custom_prod_columns = ["ID", "TITLE", "HAS_VARIANT_FLG", "FONT_NAME"]
        custom_prod_var_columns = ["PROD_ID", "TITLE", "FONT_NAME"]
        custom_prod_condition = "1=1"
        custom_prod_var_condition = "1=1"
        custom_prod_result_flag = False
        custom_prod_var_result_flag = False
        custom_prod_result_query = None
        custom_prod_var_result_query = None
        prod_has_variant_flg = False
        font_name = None

        try:
            custom_prod_condition += f"\nAND ID = {product_id}"
            custom_prod_result_flag, custom_prod_result_query = super().query_record(super().get_tbl_CUSTOM_PROD_FONTS(), custom_prod_columns, custom_prod_condition)
            
            if custom_prod_result_flag:
                for prod_row in custom_prod_result_query:
                    prod_has_variant_flg = prod_row.get("HAS_VARIANT_FLG")

                    if prod_has_variant_flg:
                        custom_prod_var_condition += f"\nAND PROD_ID = {product_id}"
                        custom_prod_var_condition += f"\nAND TITLE = '{variant_name}'"
                        custom_prod_var_result_flag, custom_prod_var_result_query = super().query_record(super().get_tbl_CUSTOM_PROD_VARIANT_FONTS(), custom_prod_var_columns, custom_prod_var_condition)

                        if custom_prod_var_result_flag:
                            for var_row in custom_prod_var_result_query:
                                font_name = var_row.get("FONT_NAME")
                    else:
                        font_name = prod_row.get("FONT_NAME")
            return font_name
        except Exception as e:
            print(f"[ERROR] Error while querying custom product fonts. Error: {str(e)}")
            return False
        finally:
            # print(f"[INFO] Cleaning up variables...")
            try:
                del custom_prod_columns
                del custom_prod_var_columns
                del custom_prod_condition
                del custom_prod_var_condition
                del custom_prod_result_flag
                del custom_prod_var_result_flag
                del custom_prod_result_query
                del custom_prod_var_result_query
                del prod_has_variant_flg
                del font_name
            except:
                pass

    def get_product_and_variants_fonts(self, vendor_id):
        print(f"\n[INFO] BEGIN - Getting custom product fonts...")
        custom_prod_columns = ["ID", "TITLE", "HAS_VARIANT_FLG", "FONT_TITLE", "FONT_NAME", "VENDOR_ID", "VENDOR_NAME"]
        custom_prod_var_columns = ["PROD_ID", "TITLE", "FONT_NAME"]
        custom_prod_condition = "1=1"
        custom_prod_condition += f"\nAND STATUS_CD = 'active'"
        custom_prod_condition += f"\nAND VENDOR_ID = '{vendor_id}'"
        custom_prod_var_condition = "1=1"
        custom_prod_var_condition += f"\nAND STATUS_CD = 'active'"
        custom_prod_result_flag = False
        custom_prod_var_result_flag = False
        custom_prod_result_query = None
        custom_prod_var_result_query = None
        prod_has_variant_flg = False
        font_name = None
        products_fonts = []
        variants_fonts = []
        product_info = {}
        variant_info = {}

        try:
            print(f"[INFO] Getting custom product fonts...")
            custom_prod_result_flag, custom_prod_result_query = super().query_record(super().get_tbl_CUSTOM_PROD_FONTS(), custom_prod_columns, custom_prod_condition)

            if custom_prod_result_flag:
                print(f"[INFO] Got {len(custom_prod_result_query)} custom product fonts.")
                for prod_row in custom_prod_result_query:
                    product_info = {
                        "product_id": prod_row.get("ID"),
                        "title": prod_row.get("TITLE"),
                        "has_variant": prod_row.get("HAS_VARIANT_FLG"),
                        "font_title": prod_row.get("FONT_TITLE"),
                        "font": prod_row.get("FONT_NAME"),
                        "vendor_id": prod_row.get("VENDOR_ID"),
                        "vendor_name": prod_row.get("VENDOR_NAME")
                    }
                    products_fonts.append(product_info)

            print(f"[INFO] Getting custom product variant fonts...")
            custom_prod_var_result_flag, custom_prod_var_result_query = super().query_record(super().get_tbl_CUSTOM_PROD_VARIANT_FONTS(), custom_prod_var_columns, custom_prod_var_condition)

            if custom_prod_var_result_flag:
                print(f"[INFO] Got {len(custom_prod_var_result_query)} custom product variant fonts.")
                for var_row in custom_prod_var_result_query:
                    variant_info = {
                        "product_id": var_row.get("PROD_ID"),
                        "title": var_row.get("TITLE"),
                        "font": var_row.get("FONT_NAME")
                    }
                    variants_fonts.append(variant_info)
            return products_fonts, variants_fonts
        except Exception as e:
            print(f"[ERROR] Error while querying custom product fonts. Error: {str(e)}")
            return False
        finally:
            print(f"[INFO] END - Getting custom product fonts...")
            print(f"[INFO] Cleaning up variables...")
            try:
                del custom_prod_columns
                del custom_prod_var_columns
                del custom_prod_condition
                del custom_prod_var_condition
                del custom_prod_result_flag
                del custom_prod_var_result_flag
                del custom_prod_result_query
                del custom_prod_var_result_query
                del prod_has_variant_flg
                del font_name
            except:
                pass

    def replace_vendors_if_multiple(self, order, default_vendor):
        line_items = order['line_items']
        # Get the verndors list
        vendors = {item['vendor'] for item in line_items}
        # If there is more than one vendor, replace all vendors with default_vendor
        if len(vendors) > 1:
            for item in line_items:
                print(f"[INFO] Replacing vendor {item['vendor']} with {default_vendor} for order {order['order_number']} item {item['title']}")
                item['vendor'] = default_vendor

        return order

    def get_delivery_location(self, vendors_data, vendor_id, location_id):
        """
        Determine if a vendor can deliver to a specific location. If the location is available,
        return its location_id. If not, return the vendor's default location_id (the first one
        in the vendor's locations list).
        Parameters:
        vendors_data (list): A list of dictionaries containing vendor data, where each vendor
                            has an 'id' and a list of 'locations'.
        vendor_id (str): The ID of the vendor to check.
        location_id (str): The ID of the location to check.

        Returns:
        str: The location_id where the vendor can deliver. If the requested location_id is not available,
        returns the vendor's default location_id.
        """
        # Iterate through the vendors list to find the specified vendor_id
        for vendor in vendors_data:
            if vendor['id'] == vendor_id:
                # Check if the requested location_id is in the vendor's locations list
                for location in vendor['locations']:
                    if location['id'] == location_id:
                        return location['id'], location['name'], location['country'], location['country_code']
                # If the requested location is not found, return the default location (first in the list)
                return vendor['locations'][0]['id'], vendor['locations'][0]['name'], vendor['locations'][0]['country'], vendor['locations'][0]['country_code']
        # If the vendor_id is not found, return None or handle the case as needed
        return None, None, None, None

    def create_vendor_summary(self, order):
        summary = {}

        for item in order['line_items']:
            vendor = item['vendor']
            if vendor not in summary:
                summary[vendor] = []
            summary[vendor].append(item)

        return summary

    # Function to process custom orders and send to vendor - Starting Function
    def process_custom_orders(self):
        print(f"\n[INFO] BEGIN - Processing custom orders")
        # Variables
        self.set_start_time(self.utils.get_current_date_time())
        default_location_name = None
        try:
            return_flag, rush_file_path, return_code = self.syspref.get_sys_pref("CUSTOMS_RUSH_FILES_PATH")
            return_flag, vendor_rush_sheet_file_name, return_code = self.syspref.get_sys_pref("CUSTOM_RUSH_SHEET_FILE_NAME")
            return_flag, alice_rush_sheet_file_name, return_code = self.syspref.get_sys_pref("CUSTOM_RUSH_SHEET_FILE_NAME_ALICE")
            return_flag, rush_sheet_file_title, return_code = self.syspref.get_sys_pref("CUSTOM_RUSH_SHEET_FILE_TITLE")
            return_flag, normal_file_path, return_code = self.syspref.get_sys_pref("CUSTOMS_FILES_PATH")
            return_flag, vendor_normal_sheet_file_name, return_code = self.syspref.get_sys_pref("CUSTOM_SHEET_FILE_NAME")
            return_flag, alice_normal_sheet_file_name, return_code = self.syspref.get_sys_pref("CUSTOM_SHEET_FILE_NAME_ALICE")
            return_flag, normal_sheet_file_title, return_code = self.syspref.get_sys_pref("CUSTOM_SHEET_FILE_TITLE")
            return_flag, default_location_name, return_code = self.syspref.get_sys_pref("CUSTOM_DEFAULT_LOCATION_NAME")
        except Exception as e:
            print(f"[ERROR] {e}")
            return False

        locations = None
        location_id = None
        location_name = None
        location_country = None
        location_country_code = None
        rush_file_path = self.utils.get_base_directory() + rush_file_path
        normal_file_path = self.utils.get_base_directory() + normal_file_path
        two_hours_ago = self.utils.get_x_hours_ago(2)
        today = self.utils.get_current_date()
        pdfmetrics = self.utils.register_custom_fonts()
        last_processed_order_id = self.order.get_last_processed_order_id(process_type="custom")
        # last_processed_order_id = '4948019052621'
        return_flag, return_code, fonts = self.fonts_ctrl.get_all_fonts()
        result_flag_order = False
        result_flag_item = False
        is_sorority = False
        is_rush = False
        result_flag_variant = False
        result_query_order = None
        rush_files_generated = False
        normal_files_generated = False
        order_processing_flag = False
        customs_orders_emails_sended = False
        customs_orders_emails_sended_rush = False
        order_inserted_flag = False
        result_query_item = None
        order_id = None
        order_name = None
        order_created_at = None
        order_country_code = None
        order_tags = None
        item_id = None
        item_name = None
        item_title = None
        item_variant_id = None
        item_variant_title = None
        item_variants = None
        matching_variant = None
        font_size = None
        item_properties = None
        item_quantity = None
        item_product_id = None
        item_sku = None
        font_title = None
        font_name = None
        custom_text = None
        first_order_id = None
        last_order_id = None
        count_orders = 0
        order_inserted_count = 0
        total_orders = 0
        response_description = None # Temp
        sorority_variant = None
        vendor_items_for_sheet = []
        vendor_items_for_rush_sheet = []
        vendor_items_array = {}
        products_fonts = None
        variants_fonts = None
        vendor_pdfs_file_names_rush = None
        vendor_pdfs_file_names = None
        orders_numbers_without_custom_text = None
        order_processing_id = None
        order_processing_id_inserted = False
        result_string = None
        vendor_id = None
        vendor_name = None
        vendor_status = None
        result_flag_vendor = None
        vendors_data = None
        return_status = True
        rowcount = 0
        result_flag_locations = False
        is_us_order = False
        # Deletar depois
        count_items = 0
        processed_orders = []
        default_vendor = None
        default_vendor_name = None
        total_items = 0
        vendor = None
        vendors_orders_dict = {}
        rush = []
        normal = []
        space = '\t'
        result_flag_vendor_location = None
        vendors_location_data = None
        loc_id = None
        loc_name = None
        loc_country = None
        loc_country_code = None

        try:
            if last_processed_order_id is not None:
                result_flag_vendor, vendors_data = self.vendors.get_all_vendors()
                result_flag_vendor_location, vendors_location_data = self.vendors.get_all_vendors_locations()
                result_flag_locations, locations = self.locations.get_all_active_locations()
                result_flag_order, result_query_order = self.order.get_all_custom_orders(since_id=last_processed_order_id, created_at=two_hours_ago)

                if result_flag_order:
                    total_orders = len(result_query_order)
                    print(f"[INFO] Total CUSTOM orders: {total_orders}")
                    print(f"\n[INFO] Getting CUSTOM items...")
                    # print(f"[INFO] Order Type\t\t\tNumber\t\tSorority  Variant\tFont\tCustom Text\tItem Title")
                    if result_flag_vendor:
                        for ven in vendors_data:
                            vendor_id = ven.get('id')
                            vendor_name = ven.get('name')
                            vendor_status = ven.get('status')
                            vendors_orders_dict[vendor_name] = {
                                "rush": [],
                                "normal": [],
                            }

                            if vendor_status == 'active':
                                ven["locations"] = []
                                for id, ven_locs in vendors_location_data.items():
                                    if id == vendor_id:
                                        for vloc in ven_locs:
                                            location = {
                                                "id": vloc.get('loc_id'),
                                                "name": vloc.get('loc_name'),
                                                "country": vloc.get('loc_country'),
                                                "country_code": vloc.get('loc_country_code')
                                            }
                                            ven["locations"].append(location)

                                ven["products_fonts"], ven["variants_fonts"] = self.get_product_and_variants_fonts(vendor_id=vendor_id)

                                if default_vendor_name is None and ven.get('default_vendor'):
                                    default_vendor_name = vendor_name
                                print(f"[INFO] Vendor: {vendor_name} - Status: {vendor_status} - Default Vendor: {True if vendor_name == default_vendor_name else False}")

                    for order in result_query_order:
                        try:
                            is_us_order = False
                            count_orders += 1
                            order_id = order.get('ID')
                            order_name = order.get('NAME')
                            order_tags = order.get('TAGS')
                            order_created_at = str(order.get('CREATED_AT')).split(' ')[0]
                            order_country_code = order.get('COUNTRY_CODE')
                            location_name = default_location_name
                            location_id = None
                            location_country = None
                            location_country_code = None

                            is_us_order = True if order_country_code == 'US' or order_country_code == 'MX' else False

                            for loc in locations:
                                location_country = loc.get('country')
                                location_country_code = loc.get('country_code')
                                location_name = loc.get('name')
                                location_id = loc.get('id')

                                # Temp - To be removed
                                if location_country_code == 'CA':
                                    break

                                # Temp - To be uncommented
                                # if (is_us_order and location_country_code == 'US') or (not is_us_order and location_country_code != 'US'):
                                #     break

                            is_rush = True if 'rush' in order_tags.lower() else False
                            first_order_id = order_id if count_orders == 1 else first_order_id
                            last_order_id = order_id

                            if count_orders == 1:
                                if self.utils.get_DEBUG_MODE() == "FALSE":
                                    if not order_processing_id_inserted:
                                        order_processing_flag, rowcount, return_string, order_processing_id = self.order.insert_processed_order_id(process_type="custom", first_id=first_order_id, last_id=first_order_id, total_orders=total_orders, status="Created")
                                        if not order_processing_flag:
                                            self.utils.send_exception_email(module=self.get_module_name(), function="process_custom_orders", error=" Error inserting last processed order id", additional_info=return_string, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                                            print(f"[ERROR] Error inserting last processed order id. Error: {return_string}")
                                            return False
                                        else:
                                            order_processing_id_inserted = True
                                            print(f"\n[INFO] Order Type\tOrder Number\tSorority\tVariant\t\tCustom Text\tItem Title")
                                else:
                                    print(f"\n[INFO] Order Type\tOrder Number\tSorority\tVariant\t\tCustom Text\tItem Title")
                                    order_processing_flag = True
                                    order_processing_id = None

                            vendor_items_array[order_id] = {
                                "order_number": order_name,
                                "created_at": order_created_at,
                                "country": location_country,
                                "country_code": location_country_code,
                                "location_id": location_id,
                                "location_name": location_name,
                                "line_items": []
                            }

                            result_flag_item, result_query_item = self.order.get_all_custom_order_line_items(order_id=order_id)

                            if result_flag_item:
                                count_items = len(result_query_item)
                                for item in result_query_item:
                                    item_id = str(item.get('ID'))
                                    item_name = item.get('NAME')
                                    item_title = item.get('TITLE')
                                    item_variant_id = item.get('VARIANT_ID')
                                    item_properties = self.utils.convert_json_to_object(item.get('PROPERTIES'))
                                    item_variant_title = item.get('VARIANT_TITLE')
                                    # Split the item_variant_title by " / "
                                    item_variants = item_variant_title.split(" / ")
                                    # Find the matching variant and get its font size
                                    matching_variant = next((variant for variant in item_variants if any(font['title'] == variant for font in fonts)), None)
                                    if matching_variant is None:
                                        matching_variant = next((font['title'] for font in fonts if font['title'] in item_title), None)
                                    font_size = next((font['font_size'] for font in fonts if font['title'] == matching_variant), 12)
                                    # Print the result and font size
                                    font_title = matching_variant if matching_variant else None
                                    item_quantity = item.get('QUANTITY')
                                    item_product_id = str(item.get('PRODUCT_ID'))
                                    item_sku = item.get('SKU')

                                    is_sorority = True if 'Sorority' in item_name else False

                                    if item_properties is not None and item_properties != [] and item_properties != {} and len(item_properties) > 0:
                                        for prop in item_properties:
                                            if prop.get('name') == 'Customize Text':
                                                custom_text = prop.get('value')
                                            else:
                                                custom_text = None
                                    else:
                                        custom_text = None

                                    if is_sorority:
                                        result_flag_variant, variant = self.temp_get_product_variant(variant_id=item_variant_id)
                                        sorority_variant = variant.get('title') if result_flag_variant else None
                                    else:
                                        sorority_variant = None

                                    if item_product_id is not None:
                                        vendor_items_array[order_id]["line_items"].append(
                                            {
                                                "title": item_title,
                                                "font": None,
                                                "font_size": font_size,
                                                "custom_text": custom_text,
                                                "name" : item_name,
                                                "sku": item_sku,
                                                "quantity": item_quantity,
                                                "is_sorority": is_sorority,
                                                "sorority_variant": sorority_variant,
                                                "item_variant_title": item_variant_title,
                                                "vendor_id": None,
                                                "vendor": None,
                                                "is_rush": is_rush,
                                                "item_product_id": item_product_id,
                                                "font_title": font_title
                                            }
                                        )
                                        print(f"[INFO] {"Rush" if is_rush else "Normal"}\t\t{order_name}\t{is_sorority}\t\t{item_variant_title[:13] + ' ' * (13 - len(item_variant_title))}\t{(custom_text[:16] + ' ' * (16 - len(custom_text)) if custom_text is not None else space + space)}{item_title}")

                        except Exception as e:
                            print(f"[ERROR] \t\t\t{order_name}\t\t\t\t\t\t\t{item_title} - Description: {e}")
                            pass

                    print(f"\n[INFO] Order Type\t\tOrder Number\tSorority  Variant\tFont\tCustom Text\tItem Title")
                    for order_id, order in vendor_items_array.items():
                        for line_item in order.get('line_items'):
                            item_product_id = line_item.get('item_product_id')
                            font_title = line_item.get('font_title')

                            for ven in vendors_data:
                                vendor_id = ven.get('id')
                                vendor_name = ven.get('name')
                                vendor_status = ven.get('status')

                                if vendor_status == 'active':
                                    font_name = next((prod_row['font'] for prod_row in ven["products_fonts"] if prod_row.get('product_id') == item_product_id and not prod_row.get('has_variant') and prod_row.get('font_title') == font_title), None)

                                    if font_name is None:
                                        font_name = next((variant_row['font'] for prod_row in ven["products_fonts"] if prod_row.get('product_id') == item_product_id for variant_row in ven["variants_fonts"] if prod_row.get('has_variant') and variant_row.get('product_id') == item_product_id and variant_row.get('title') == font_title), None)

                                    for prod_row in ven["products_fonts"]:
                                        found_item_for_vendor = True if prod_row.get('product_id') == item_product_id else False
                                        if found_item_for_vendor:
                                            line_item["vendor_id"] = vendor_id
                                            line_item["vendor"] = vendor_name
                                            break

                                    line_item["font"] = font_name
                                    if found_item_for_vendor:
                                        break

                        total_items = len(order.get('line_items'))
                        if total_items > 1:
                            order = self.replace_vendors_if_multiple(order, default_vendor_name)

                        for line_item in order.get('line_items'):
                            vendor_id = line_item.get("vendor_id")
                            vendor = line_item.get("vendor")
                            loc_id, loc_name, loc_country, loc_country_code = self.get_delivery_location(vendors_data=vendors_data, vendor_id=vendor_id, location_id=order.get('location_id'))

                            item_data = {
                                "order_id": order_id,
                                "order_number": order.get('order_number'),
                                "created_at": order.get('created_at'),
                                "country": loc_country,
                                "country_code": loc_country_code,
                                "location_id": loc_id,
                                "location_name": loc_name,
                                "title": line_item.get('title'),
                                "name" : line_item.get('name'),
                                "sku": line_item.get('sku'),
                                "custom_text": line_item.get('custom_text'),
                                "quantity": line_item.get('quantity'),
                                "is_sorority": line_item.get('is_sorority'),
                                "sorority_variant": line_item.get('sorority_variant'),
                                "item_variant_title": line_item.get('item_variant_title'),
                                "is_rush": line_item.get('is_rush'),
                                "item_product_id": line_item.get('item_product_id'),
                                "font_title": line_item.get('font_title'),
                                "font": line_item.get('font'),
                                "font_size": line_item.get('font_size'),
                                "vendor": vendor
                            }
                            vendors_orders_dict[vendor]["rush" if line_item.get('is_rush') else "normal"].append(
                                item_data
                            )
                            print(
                                f"[INFO] {vendor} {"Rush" if line_item.get('is_rush') else "Normal"} Order" +
                                f"{space if vendor is not None and len(vendor) < 5 else ''}\t" +
                                f"{order.get('order_number')}\t"+
                                f"{line_item.get('is_sorority')}\t" +
                                f"{line_item.get('item_variant_title')[:13] + ' ' * (13 - len(line_item.get('item_variant_title')))}\t" +
                                f"{space if line_item.get('font') is None else line_item.get('font')[0:7] + ' ' * (7 - len(line_item.get('font'))) + ' '}" +
                                f"{(line_item.get('custom_text')[:16] + ' ' * (16 - len(line_item.get('custom_text'))) if line_item.get('custom_text') is not None else space + space)}" +
                                f"{line_item.get('title')}"
                            )

                    for vendor, orders_data in vendors_orders_dict.items():
                        vendor_items_for_sheet = []
                        vendor_items_for_rush_sheet = []
                        customs_orders_emails_sended_rush = False
                        rush_files_generated = False
                        customs_orders_emails_sended = False
                        normal_files_generated = False
                        rush = orders_data["rush"]
                        normal = orders_data["normal"]
                        vendor_id = next((ven.get('id') for ven in vendors_data if ven.get('name') == vendor), None)

                        if len(rush) > 0:
                            vendor_items_for_rush_sheet.extend(rush)
                        if len(normal) > 0:
                            vendor_items_for_sheet.extend(normal)

                        print(f"[INFO] Total Items for Normal sheet {vendor}: {len(vendor_items_for_sheet)}")
                        print(f"[INFO] Total Items for Rush sheet {vendor}: {len(vendor_items_for_rush_sheet)}")

                        if len(vendor_items_for_rush_sheet) > 0:
                            vendor_items_for_rush_sheet = sorted(vendor_items_for_rush_sheet, key=lambda x: x['title'])
                            vendor_items_for_rush_sheet = [list(group) for key, group in groupby(vendor_items_for_rush_sheet, lambda x: x['title'])]
                            rush_files_generated, vendor_pdfs_file_names_rush, orders_numbers_without_custom_text = self.generate_custom_files(items_sheet_array=vendor_items_for_rush_sheet, sheet_file_path=rush_file_path, pdf_file_path=rush_file_path, file_name=vendor_rush_sheet_file_name, file_title=rush_sheet_file_title, type="rush")

                            if rush_files_generated:
                                if self.utils.get_DEBUG_MODE() == "FALSE":
                                    print(f"[INFO] Inserting RUSH Custom PO orders in the Custom Management Table")
                                    order_inserted_flag, order_inserted_count, result_string = self.insert_custom_orders_management(order_processing_id=order_processing_id, vendor_id=vendor_id, vendor_name=vendor, items_sheet_array=vendor_items_for_rush_sheet, type="rush")
                                else:
                                    order_inserted_flag = True

                                print(f"[INFO] Sending RUSH Custom PO Sheet to {vendor}")
                                customs_orders_emails_sended_rush = self.send_processed_customs_orders_emails(vendor_id=vendor_id, vendor_name=vendor, sheet_file_path=rush_file_path, pdf_file_path=rush_file_path, sheet_file_names=[vendor_rush_sheet_file_name], pdf_file_names=vendor_pdfs_file_names_rush, orders_numbers_without_custom_text=orders_numbers_without_custom_text, type="rush")

                        if len(vendor_items_for_sheet) > 0:
                            vendor_items_for_sheet = sorted(vendor_items_for_sheet, key=lambda x: x['title'])
                            vendor_items_for_sheet = [list(group) for key, group in groupby(vendor_items_for_sheet, lambda x: x['title'])]
                            normal_files_generated, vendor_pdfs_file_names, orders_numbers_without_custom_text = self.generate_custom_files(items_sheet_array=vendor_items_for_sheet, sheet_file_path=normal_file_path, pdf_file_path=normal_file_path, file_name=vendor_normal_sheet_file_name, file_title=normal_sheet_file_title, type="normal")

                            if normal_files_generated:
                                if self.utils.get_DEBUG_MODE() == "FALSE":
                                    print(f"[INFO] Inserting Custom PO orders in the Custom Management Table")
                                    order_inserted_flag, order_inserted_count, result_string = self.insert_custom_orders_management(order_processing_id=order_processing_id, vendor_id=vendor_id, vendor_name=vendor, items_sheet_array=vendor_items_for_sheet, type="normal")
                                else:
                                    order_inserted_flag = True

                                print(f"[INFO] Sending Custom PO Sheet to Vendor")
                                customs_orders_emails_sended = self.send_processed_customs_orders_emails(vendor_id=vendor_id, vendor_name=vendor, sheet_file_path=normal_file_path, pdf_file_path=normal_file_path, sheet_file_names=[vendor_normal_sheet_file_name], pdf_file_names=vendor_pdfs_file_names, orders_numbers_without_custom_text=orders_numbers_without_custom_text, type="normal")

                        if order_processing_id is not None:
                            if self.utils.get_DEBUG_MODE() == "FALSE":
                                order_processing_flag, rowcount, return_string = self.order.update_processed_order_id(row_id=order_processing_id, process_type="custom", first_id=first_order_id, last_id=last_order_id, total_orders=total_orders, status="Processing Done")
                            else:
                                order_processing_flag = True

                        if order_processing_flag:
                            if customs_orders_emails_sended_rush and rush_files_generated:
                                self.utils.delete_files(sheet_file_path=rush_file_path, pdf_file_path=rush_file_path, sheet_file_names=[vendor_rush_sheet_file_name], pdf_file_names=vendor_pdfs_file_names_rush)

                            if customs_orders_emails_sended and normal_files_generated:
                                self.utils.delete_files(sheet_file_path=normal_file_path, pdf_file_path=normal_file_path, sheet_file_names=[vendor_normal_sheet_file_name], pdf_file_names=vendor_pdfs_file_names)

                            return_status = True
                        else:
                            self.utils.send_exception_email(module=self.get_module_name(), function="process_custom_orders", error=return_string, additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                            return_status = False
                else:
                    self.utils.send_exception_email(module=self.get_module_name(), function="process_custom_orders", error="Error getting custom orders", additional_info=response_description, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                    print(f"[ERROR] Error getting custom orders")
                    return_status = False
                return return_status
            else:
                self.utils.send_exception_email(module=self.get_module_name(), function="process_custom_orders", error="Error getting last processed order id", additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                print(f"[ERROR] Error getting last processed order id")
                return False
        except Exception as e:
            if order_name is not None:
                print(f"[ERROR] - Order with error: {order_name} - Error: {e}")
            elif last_order_id is not None:
                print(f"[ERROR] - Last Order with error: {last_order_id} - Error: {e}")
            else:
                print(f"[ERROR] - Error: {e}")
            self.utils.send_exception_email(module=self.get_module_name(), function="process_custom_orders", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
            return False
        finally:
            print("\n[INFO] END - Finished processing custom orders")
            print("[INFO] Cleaning up variables")
            try:
                del last_processed_order_id, rush_file_path, vendor_rush_sheet_file_name, rush_sheet_file_title, normal_file_path, vendor_normal_sheet_file_name, normal_sheet_file_title, two_hours_ago, today, pdfmetrics, return_flag, fonts, result_flag_order, result_flag_item, is_sorority, is_rush, result_flag_variant, result_query_order, rush_files_generated, normal_files_generated, order_processing_flag, customs_orders_emails_sended, customs_orders_emails_sended_rush, order_inserted_flag, result_query_item, order_id, order_name, order_created_at, order_tags, item_id, item_name, item_title, item_variant_id, item_variant_title, item_variants, matching_variant, font_size, item_properties, item_quantity, item_product_id, item_sku, font_title, font_name, custom_text, first_order_id, last_order_id, count_orders, order_inserted_count, total_orders, response_description, sorority_variant, vendor_items_for_sheet, vendor_items_for_rush_sheet, vendor_items_array, products_fonts, variants_fonts, vendor_pdfs_file_names_rush, vendor_pdfs_file_names, orders_numbers_without_custom_text, order_processing_id, result_string, vendor_id, vendor_name, vendor_status, result_flag_vendor, vendors_data, return_status, default_location_name, locations, location_id, location_name, rowcount, result_flag_locations, order_country_code, order_processing_id_inserted, count_items, processed_orders, default_vendor, default_vendor_name, total_items, vendor, vendors_orders_dict, rush, normal, space, result_flag_vendor_location, vendors_location_data, loc_id, loc_name, loc_country, loc_country_code
            except Exception as e:
                print(f"[ERROR] - {e}")
                pass

    # Function to generate an Excel sheet and save it to an output file
    def generate_custom_report_sheet(self, items_sheet_array, sheet_file_path, file_name, file_title):
        print(f"\n[INFO] BEGIN - Generating sheet. Sheet Title: {file_title}")
        wb = False
        array_of_items = []
        items = []
        order_number = None
        font = None
        custom_text = None
        name = None
        sku = None
        quantity = None
        created_at = None
        is_sorority = None
        is_sorority_text = None
        sent_to_vendor_date = None
        vendor_name = None
        country = None
        country_code = None
        location = None
        sent_back_from_vendor_flag = None
        sent_back_from_vendor_date = None
        tracking_number = None
        tracking_url = None
        received_date = None
        received_by = None
        received_notes = None
        is_fulfilled_flag = None
        fulfillment_status = None
        fulfilled_date = None
        is_cancelled_flag = None
        vendor_processing_time = None
        vendor_processing_time_unit = None
        total_shipping_time = None
        total_shipping_time_unit = None
        total_processing_time = None
        total_processing_time_unit = None
        wb = False
        item_variant_title = None
        font_size = None

        try:
            wb = self.utils.create_excel_sheet_if_not_exists(sheet_file_path, file_name, file_title)
            if not wb:
                print(f"[ERROR] Error creating sheet {file_title}")
                return False
            else:
                ws = wb.active
                ws.append(["Order Number", "Created At", "Product Name", "Sorority", "Font", "Custom Text", "SKU", "Quantity", "Sent to Vendor Date", "Vendor Name", "Ship To", "Location", "Sent Back from Vendor", "Sent Back from Vendor Date", "Tracking Number", "Tracking URL", "Received Date", "Received By", "Received Notes", "Fulfilled", "Fulfillment Status", "Fulfilled Date", "Cancelled", "Vendor Processing Time", "Total Shipping Time", "Total Processing Time"])
                print(f"[INFO] Order Number\tSorority Flag\tFont\tCustom Text\t\tItem Title")
                for item in items_sheet_array:
                    order_number = str(item.get('order_number')) if item.get('order_number') is not None else ""
                    font = str(item.get('font')) if item.get('font') is not None else ""
                    name = str(item.get('product_name')) if item.get('product_name') is not None else ""
                    sku = str(item.get('sku')) if item.get('sku') is not None else ""
                    quantity = item.get('quantity') if item.get('quantity') is not None else "1"
                    created_at = str(item.get('created_at')) if item.get('created_at') is not None else ""
                    is_sorority_text = "Yes" if item.get('sorority_flag') is not None and item.get('sorority_flag') == True else "No"
                    sent_to_vendor_date = str(item.get('sent_to_vendor_date')) if item.get('sent_to_vendor_date') is not None else ""
                    vendor_name = str(item.get('vendor_name')) if item.get('vendor_name') is not None else ""
                    country = str(item.get('country')) if item.get('country') is not None else ""
                    country_code = str(item.get('country_code')) if item.get('country_code') is not None else ""
                    location = str(item.get('location_name')) if item.get('location_name') is not None else ""
                    sent_back_from_vendor_flag = "Yes" if item.get('sent_back_from_vendor_flag') is not None and item.get('sent_back_from_vendor_flag') == True else "No"
                    sent_back_from_vendor_date = str(item.get('sent_back_from_vendor_date')) if item.get('sent_back_from_vendor_date') is not None else ""
                    tracking_number = str(item.get('tracking_number')) if item.get('tracking_number') is not None else ""
                    tracking_url = str(item.get('tracking_url')) if item.get('tracking_url') is not None else ""
                    received_date = str(item.get('received_date')) if item.get('received_date') is not None else ""
                    received_by = str(item.get('received_by')) if item.get('received_by') is not None else ""
                    received_notes = str(item.get('received_notes')) if item.get('received_notes') is not None else ""
                    is_fulfilled_flag = "Yes" if item.get('is_fulfilled_flag') is not None and item.get('is_fulfilled_flag') == True else "No"
                    fulfillment_status = 'unfulfilled' if item.get('fulfillment_status') is None else item.get('fulfillment_status')
                    fulfilled_date = str(item.get('fulfilled_date')) if item.get('fulfilled_date') is not None else ""
                    is_cancelled_flag = 'Yes' if item.get('is_cancelled_flag') is not None else "No"
                    vendor_processing_time = item.get('vendor_processing_time') if item.get('vendor_processing_time') is not None else 0
                    vendor_processing_time_unit = str(item.get('vendor_processing_time_unit')) if item.get('vendor_processing_time_unit') is not None else "days"
                    total_shipping_time = item.get('total_shipping_time') if item.get('total_shipping_time') is not None else 0
                    total_shipping_time_unit = str(item.get('total_shipping_time_unit')) if item.get('total_shipping_time_unit') is not None else "days"
                    total_processing_time = item.get('total_processing_time') if item.get('total_processing_time') is not None else 0
                    total_processing_time_unit = str(item.get('total_processing_time_unit')) if item.get('total_processing_time_unit') is not None else "days"
                    custom_text = str(item.get('custom_text')).replace("\\x00", "/") if item.get('custom_text') is not None else ""

                    print(f"[INFO] {order_number}\t{is_sorority_text}\t\t{f'\t' if font is None else ((font[0:7] + (' ' * (7 - len(font)))) + ' ')}{custom_text[:16] + (' ' * (16 - len(custom_text)))}\t{name}")

                    for i in range(quantity):
                        items = [order_number, created_at, name, is_sorority_text, font, custom_text, sku, "1", sent_to_vendor_date, vendor_name, f"{country}-{country_code}", location, sent_back_from_vendor_flag, sent_back_from_vendor_date, tracking_number, tracking_url, received_date, received_by, received_notes, is_fulfilled_flag, fulfillment_status, fulfilled_date, is_cancelled_flag, f"{vendor_processing_time} {vendor_processing_time_unit}", f"{total_shipping_time} {total_shipping_time_unit}", f"{total_processing_time} {total_processing_time_unit}"]
                        try:
                            ws.append(items)
                        except Exception as e:
                            print(f"[ERROR] Error appending items to the sheet. Error: {e}")

                print(f"[INFO] Saving sheet {file_title}...")
                wb.save(f'{sheet_file_path}{file_name}')
                print(f"[INFO] Sheet {file_title} saved successfully")
                return True, sheet_file_path + file_name
        except Exception as e:
            print(f"[ERROR] {e}")
            self.utils.send_exception_email(module=self.get_module_name(), function="generate_custom_files", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished generating sheet. Sheet Title: {file_title}")
            print("[INFO] Cleaning up variables")
            try:
                print(set(locals().keys()))
                del wb, array_of_items, items, order_number, font, custom_text, name, sku, quantity, created_at, is_sorority, is_sorority_text, vendor_name, sent_to_vendor_date, sent_back_from_vendor_flag, sent_back_from_vendor_date, tracking_number, tracking_url, received_date, received_by, received_notes, is_fulfilled_flag, fulfillment_status, fulfilled_date, is_cancelled_flag, vendor_processing_time, vendor_processing_time_unit, total_shipping_time, total_shipping_time_unit, total_processing_time, total_processing_time_unit, item_variant_title, font_size
            except:
                pass

    def generate_custom_files(self, items_sheet_array, sheet_file_path, pdf_file_path, file_name, file_title, type="normal"):
        print(f"\n[INFO] BEGIN - Generating {type} custom sheet for Vendor. Sheet Title: {file_title}")

        orders_numbers_without_custom_text = []
        array_of_items = []
        items = []
        vendor_pdfs_file_names = []
        order_number = None
        font = None
        custom_text = None
        name = None
        sku = None
        quantity = None
        created_at = None
        is_sorority = None
        is_sorority_text = None
        sorority_variant = None
        wb = False
        has_at_least_one_font = False
        inches = 28.3465
        font_size = 12
        height = 800
        counter = 0
        pdf_title = None
        pdf_title_slug = None
        item_variant_title = None
        font_size = None
        item_country_code = None

        try:
            wb = self.utils.create_excel_sheet_if_not_exists(sheet_file_path, f"{file_name}", file_title)
            if not wb:
                print(f"[ERROR] Error creating sheet {file_title}")
                return False
            else:
                ws = wb.active
                ws.append(["Order Number", "Font", "Custom Text", "Name", "SKU", "Quantity", "Created At", "Is Sorority", "Ship To"])
                for array_of_items in items_sheet_array:
                    print(f"\n[INFO] Checking if there is at least one font for the Item: {array_of_items[0].get('title')}")
                    for item in array_of_items:
                        font = item.get('font')
                        has_at_least_one_font = True if font is not None and font is not False and font != "" else False
                        if has_at_least_one_font:
                            print(f"[INFO] Found at least one font in the items array")
                            break

                    if has_at_least_one_font:
                        pdf_title = array_of_items[0]["title"]
                        print(f'[INFO] Creating sheet for {pdf_title}')
                        pdf_title_slug = f'{"RUSH_" if type == "rush" else ""}{pdf_title.lower().replace("/", " ").replace(" ", "_")}_PO.pdf'
                        print(f"[INFO] Creating PDF file {pdf_title_slug}")
                        vendor_pdfs_file_names.append(f'{pdf_title_slug}')

                        pdf = canvas.Canvas(f'{pdf_file_path}{pdf_title_slug}')
                        pdf.setPageSize((21*inches, 29.7*inches))
                        height = 800
                        counter = 0
                        print(f"\n[INFO] Processing items for sheet [{file_title}] and pdf [{pdf_title_slug}]")
                    else:
                        print(f"[INFO] No font found in the items array. Skipping PDF creation for this item.")
                        print(f"\n[INFO] Processing items for sheet [{file_title}].")

                    # print(f"[INFO] Processing items for sheet {file_title}")
                    print(f"[INFO] Order Number\tSorority  Variant\tFont\tCustom Text\tFont Size\tItem Title")
                    for item in array_of_items:
                        order_number = item.get('order_number')
                        font = item.get('font')
                        font_size = item.get('font_size')
                        name = item.get('name')
                        sku = item.get('sku')
                        quantity = item.get('quantity')
                        created_at = item.get('created_at')
                        is_sorority = item.get('is_sorority')
                        item_variant_title = item.get('item_variant_title')
                        item_country_code = item.get('country_code')
                        is_sorority_text = 'TRUE' if is_sorority else 'FALSE'

                        if is_sorority:
                            try:
                                sorority_variant = item.get('sorority_variant').split('/')[0].strip()
                            except:
                                sorority_variant = item.get('sorority_variant')

                        custom_text = item.get('custom_text') if not is_sorority else sorority_variant

                        print(f"[INFO] {order_number}\t{is_sorority}\t  {item_variant_title[:13] + ' ' * (13 - len(item_variant_title))}\t{f'\t' if font is None else ((font[0:7] + (' ' * (7 - len(font)))) + ' ')}{(custom_text[:16] + (' ' * (16 - len(custom_text)))) if custom_text is not None else "\t\t"}{font_size}\t\t{name}")

                        for i in range(quantity):
                            if is_sorority or (custom_text != '' and custom_text is not None):
                                items = [order_number, font, custom_text, name, sku, "1", created_at, is_sorority_text, item_country_code]
                                ws.append(items)

                        if has_at_least_one_font:
                            # font_size = 16 if font == "exmouth" else 14 if font == "old_london" else 12
                            if is_sorority or (custom_text != '' and custom_text is not None):
                                for i in range(quantity):
                                    pdf.setFont('roboto', 10)
                                    pdf.drawString(20, height, f"{order_number} - {name.split('-')[0].strip() if is_sorority else name}{f" - {sorority_variant}" if is_sorority else ""} - 1")
                                    try:
                                        pdf.setFont(font, font_size)
                                    except:
                                        if not is_sorority:
                                            print(f"[ERROR] Font [{font}] not found. Using default font [roboto].")
                                            pdf.setFont('roboto', font_size)
                                    pdf.drawString(20, height-20, sorority_variant.split('/')[0].strip() if is_sorority else custom_text)
                                    height -= 50
                                    counter += 1

                                    if counter == 16:
                                        pdf.showPage()
                                        counter = 0
                                        height = 800
                            else:
                                print(f'[INFO] Order {order_number} does not have custom text.')
                                orders_numbers_without_custom_text.append(order_number)

                    if has_at_least_one_font:
                        pdf.save()
                        print(f"[INFO] PDF {pdf_title_slug} saved successfully")
                wb.save(f'{sheet_file_path}{file_name}')
                print(f"[INFO] Sheet {file_title} saved successfully")
                return True, vendor_pdfs_file_names, orders_numbers_without_custom_text
        except Exception as e:
            self.utils.send_exception_email(module=self.get_module_name(), function="generate_custom_files", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
            print(f"[ERROR] - {e}")
            return False, None, None
        finally:
            print(f"[INFO] Orders without custom text: {orders_numbers_without_custom_text}")
            print(f"[INFO] END - Finished generating {type} custom sheet for Vendor. Sheet Title: {file_title}")
            print("[INFO] Cleaning up variables")
            try:
                del orders_numbers_without_custom_text, array_of_items, items, order_number, font, custom_text, name, sku, quantity, created_at, is_sorority, is_sorority_text, sorority_variant, file_path, wb, ws, item_country_code
            except:
                pass

    def insert_custom_orders_management(self, order_processing_id, vendor_id, vendor_name, items_sheet_array, type="normal"):
        print(f"\n[INFO] BEGIN - Inserting {type.upper()} custom orders on Management Table.")

        array_of_items = []
        order_id = None
        order_number = None
        font = None
        custom_text = None
        name = None
        sku = None
        quantity = None
        created_at = None
        is_sorority = None
        is_sorority_text = None
        sorority_variant = None
        custom_order = None
        order_upserted_flag = False
        order_inserted_count = 0
        total_orders_inserted_count = 0
        result_string = "Success"
        item_variant_title = None
        item_country_code = None
        item_country = None
        item = None
        item_location_id = None
        item_location_name = None

        try:
            for array_of_items in items_sheet_array:
                for item in array_of_items:
                    order_id = item.get('order_id')
                    order_number = item.get('order_number')
                    font = item.get('font')
                    name = item.get('name')
                    sku = item.get('sku')
                    quantity = item.get('quantity')
                    created_at = item.get('created_at')
                    is_sorority = item.get('is_sorority')
                    item_variant_title = item.get('item_variant_title')
                    item_country_code = item.get('country_code')
                    item_country = item.get('country')
                    item_location_id = item.get('location_id')
                    item_location_name = item.get('location_name')
                    is_sorority_text = 'TRUE' if is_sorority else 'FALSE'

                    if is_sorority:
                        try:
                            sorority_variant = item.get('sorority_variant').split('/')[0].strip()
                        except:
                            sorority_variant = item.get('sorority_variant')

                    custom_text = item.get('custom_text') if not is_sorority else sorority_variant

                    custom_order = self.order.CustomOrdersManagement()
                    custom_order.set_order_processing_id(order_processing_id)
                    custom_order.set_id(order_id)
                    custom_order.set_name(order_number)
                    custom_order.set_country(item_country)
                    custom_order.set_country_code(item_country_code)
                    custom_order.set_location_id(item_location_id)
                    custom_order.set_location_name(item_location_name)
                    custom_order.set_font(font)
                    custom_order.set_custom_text(custom_text)
                    custom_order.set_prod_name(name)
                    custom_order.set_prod_sku(sku)
                    custom_order.set_quantity(quantity)
                    custom_order.set_vendor_id(vendor_id)
                    custom_order.set_vendor_name(vendor_name)
                    custom_order.set_created_at(created_at)
                    custom_order.set_sent_to_vendor_date("CURRENT_TIMESTAMP(6)")
                    custom_order.set_sorority_flg(is_sorority)

                    order_upserted_flag, order_inserted_count, result_string = self.order.insert_custom_orders_managemet(order=custom_order)

                    if not order_upserted_flag or order_inserted_count == 0:
                        print(f"[ERROR] - {result_string}")
                        additional_details = f"Order ID: {order_id}\nOrder Number: {order_number}\nFont: {font}\nCustom Text: {custom_text}\nName: {name}\nSKU: {sku}\nQuantity: {quantity}\nCreated At: {created_at}\nIs Sorority: {is_sorority_text}\nSorority Variant: {sorority_variant}"
                        self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="insert_custom_orders_management", error_code=500, error_message=str(result_string), additional_details=additional_details, error_severity=self.utils.get_error_severity(3))
                    else:
                        total_orders_inserted_count += 1
                        print(f"[INFO] Order {order_number} inserted successfully.")

            print(f"[INFO] Total Orders Items inserted: {total_orders_inserted_count}")
            return True, total_orders_inserted_count, result_string
        except Exception as e:
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="insert_custom_orders_management", error_code=500, error_message=str(e), additional_details=None, error_severity=self.utils.get_error_severity(3))
            print(f"[ERROR] - {e}")
            return False, total_orders_inserted_count, None
        finally:
            print("[INFO] Cleaning up variables")
            try:
                del array_of_items, order_id, order_number, font, custom_text, name, sku, quantity, created_at, is_sorority, is_sorority_text, sorority_variant, custom_order, order_upserted_flag, order_inserted_count, orders_count, total_orders_inserted_count, result_string, item_variant_title, item_country_code, item_country, item_location_id, item_location_name
            except:
                pass

    def temp_get_all_orders(self, since_id):
        print(f"\n[INFO] BEGIN - Getting all orders since id: {since_id}")
        two_hours_ago = self.utils.get_x_hours_ago(2)
        response_description = "Success"
        response_code = 200
        continue_process = True
        result_flag_order = False
        orders = None
        custom_orders_array = []
        last_order_id = since_id
        tags = None
        created_at = None

        try:
            while continue_process:
                result_flag_order, orders, response_description, response_code = self.shopify.get_shopify_list_of_orders(
                    fields="id,name,created_at,line_items,tags",
                    limit=250,
                    status=None,
                    created_at_min=None,
                    created_at_max=two_hours_ago.replace(' ', 'T'),
                    processed_at_min=None,
                    processed_at_max=None,
                    since_id=last_order_id,
                    # fulfillment_status='any',
                    fulfillment_status='unfulfilled',
                    api_version=self.utils.get_SHOPIFY_API_VERSION()
                )
                if len(orders) == 0:
                    print('[DONE] Finished getting orders because it found 0.')
                    continue_process = False
                    return True, custom_orders_array, response_description
                else:
                    for order in orders:
                        last_order_id = order.get('id', '')
                        tags = order.get('tags', '')
                        created_at = order.get('created_at', '')
                        if 'custom' in tags.lower():
                            custom_orders_array.append(order)
            return True, custom_orders_array, response_description
        except Exception as e:
            self.utils.send_exception_email(module=self.get_module_name(), function="temp_get_all_orders", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
            print(f'[ERROR] - {e}')
            continue_process = False
            return False, custom_orders_array, str(e)
        finally:
            print(f"[INFO] END - Finished getting all orders. Got a total of {len(custom_orders_array)} orders")
            print("[INFO] Cleaning up variables")
            try:
                del two_hours_ago, response_description, response_code, continue_process, result_flag_order, orders, custom_orders_array, last_order_id, tags, created_at
            except Exception as e:
                print(f'[ERROR] - {e}')
                pass

    def temp_get_product_variant(self, variant_id):
        # print(f"\n[INFO] BEGIN - Getting product variant with id: {variant_id}")
        response_description = "Success"
        response_code = 200
        variant = None
        result_flag = False

        try:
            result_flag, variant, response_description, response_code = self.shopify.get_shopify_single_product_variant(
                variant_id=variant_id,
                fields="id,title,product_id,sku",
                api_version=self.utils.get_SHOPIFY_API_VERSION()
            )
            return result_flag, variant
        except Exception as e:
            self.utils.send_exception_email(module=self.get_module_name(), function="temp_get_product_variant", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
            print(f'[ERROR] - {e}')
            return result_flag, variant
        finally:
            # print(f"[INFO] END - Finished getting product variant with id: {variant_id}")
            # print("[INFO] Cleaning up variables\n")
            try:
                del response_description, response_code, variant, result_flag
            except:
                pass

    def send_processed_customs_orders_emails(self, vendor_id, vendor_name, sheet_file_path, pdf_file_path, sheet_file_names, pdf_file_names, orders_numbers_without_custom_text, type="normal", vendor="Lee"):
        today = self.utils.get_current_date()
        # @TODO QUERY VENDOR EMAILS IN THE CONTACTS TABLE
        if self.utils.get_DEBUG_MODE() == "FALSE":
            if vendor_name.lower() == "lee":
                email_to = ['xxxxxxxxxx@COMPANY_NAME.com', 'xxxxxxxxxx@COMPANY_NAME.com', 'xxxxxxxxxx@xxxxxxxxxx.com', 'xxxxxxxxxx@xxxxxxxxxx.com', 'xxxxxxxxxx@xxxxxxxxxx.com', 'xxxxxxxxxx@xxxxxxxxxx.com.cn']
            else:
                email_to = ['xxxxxxxxxx@COMPANY_NAME.com', 'xxxxxxxxxx@COMPANY_NAME.com', 'xxxxxxxxxx@xxxxxxxxxx.com', 'xxxxxxxxxx@xxxxxxxxxx.com']
        else:
            email_to = ['xxxxxxxxxx@COMPANY_NAME.com', 'xxxxxxxxxx@COMPANY_NAME.com']
        # email_body = None
        file_names = []
        file_names.extend(sheet_file_names)
        file_names.extend(pdf_file_names)

        try:
            return_flag, email_from, return_code = self.syspref.get_sys_pref("CUSTOMS_EMAIL_FROM")
            return_flag, email_subject, return_code = self.syspref.get_sys_pref("PROCESSED_CUSTOMS_ORDERS_EMAIL_SUBJECT")
            return_flag, email_body, return_code = self.syspref.get_sys_pref("PROCESSED_CUSTOMS_ORDERS_EMAIL_BODY")
            return_flag, email_subject_without_text, return_code = self.syspref.get_sys_pref("CUSTOMS_ORDERS_WITHOUT_CUSTOM_TEXT_SUBJECT")
            return_flag, email_body_without_text, return_code = self.syspref.get_sys_pref("CUSTOMS_ORDERS_WITHOUT_CUSTOM_TEXT_BODY")
        except Exception as e:
            print(f"[ERROR] {e}")
            return False

        try:
            if type == "rush":
                print(f"\n[INFO] Sending RUSH Custom Orders FILES to {vendor}")
                email_subject = email_subject.format("RUSH ", "FILES", "Rush ", today)
                email_body = email_body.format('RUSH', today)
                try:
                    self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=sheet_file_path, file_names=file_names)
                except Exception as e:
                    self.utils.send_exception_email(module=self.get_module_name(), function="send_processed_customs_orders_emails", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                    print(f"[ERROR] - {e}")
                    return False

            else:
                print(f"\n[INFO] Sending NORMAL Custom Orders PDFS to {vendor}")
                email_subject = email_subject.format("", "FILES", "", today)
                email_body = email_body.format('', today)
                try:
                    self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=sheet_file_path, file_names=file_names)
                except Exception as e:
                    self.utils.send_exception_email(module=self.get_module_name(), function="send_processed_customs_orders_emails", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                    print(f"[ERROR] - {e}")
                    return False

            if len(orders_numbers_without_custom_text) > 0:
                email_subject = email_subject_without_text.format(today)
                email_body = email_body_without_text
                email_body += f"\n{orders_numbers_without_custom_text}"
                email_to = ['xxxxxxxxxx@COMPANY_NAME.com', 'xxxxxxxxxx@COMPANY_NAME.com'] if self.utils.get_DEBUG_MODE() == "FALSE" else ['xxxxxxxxxx@COMPANY_NAME.com']
                try:
                    self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=None, file_names=None)
                except Exception as e:
                    self.utils.send_exception_email(module=self.get_module_name(), function="send_processed_customs_orders_emails", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                    print(f"[ERROR] - {e}")
                    return False
            return True
        except Exception as e:
            self.utils.send_exception_email(module=self.get_module_name(), function="send_processed_customs_orders_emails", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
            print(f"[ERROR] - {e}")
            return False
        finally:
            try:
                del today, email_from, email_subject, email_body, email_subject_without_text, email_body_without_text, email_to, file_names
            except:
                pass

    # Counters
    def verify_customs_orders_with_tracking_number_exists(self, tracking_number):
        print(f"\n[INFO] BEGIN - Counting customs orders with tracking number: {tracking_number}")

        response_description = "Success"
        response_code = 200
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND TRACKING_NUMBER = '{tracking_number if 'SF' in str(tracking_number).upper() else 'SF' + tracking_number}'"
        count = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    count = row.get("TOTAL")

            return False if count <= 0 else True
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False
        finally:
            print(f"[INFO] END - Finished counting customs orders with tracking number: {tracking_number}")
            print("[INFO] Cleaning up variables")
            try:
                del response_description, response_code, columns, condition, count, result_flag, result_query
            except:
                pass

    def verify_customs_orders_with_tracking_number_and_location_exists(self, tracking_number, location_id):
        print(f"\n[INFO] BEGIN - Counting customs orders with tracking number: {tracking_number} and location id: {location_id}")

        response_description = "Success"
        response_code = 200
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND TRACKING_NUMBER = '{tracking_number if 'SF' in str(tracking_number).upper() else 'SF' + tracking_number}'"
        condition += f"\nAND LOCATION_ID = '{location_id}'"
        count = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    count = row.get("TOTAL")

            return False if count <= 0 else True
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False
        finally:
            print(f"[INFO] END - Finished counting customs orders with tracking number: {tracking_number}")
            print("[INFO] Cleaning up variables")
            try:
                del response_description, response_code, columns, condition, count, result_flag, result_query
            except:
                pass

    def count_customs_received_not_fullfilled_orders(self, since_date):
        # print(f"\n[INFO] BEGIN - Counting customs orders received and not fulfilled")

        response_description = "Success"
        response_code = 200
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND RECEIVED_FLG = TRUE"
        condition += f"\nAND FULFILLED_FLG = FALSE"
        condition += f"\nAND RECEIVED_DATE <= DATE_FORMAT('{since_date}', '%Y-%m-%d')"
        count = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    count = row.get("TOTAL")

            return count
        except Exception as e:
            print(f"[ERROR] - {e}")
            return count
        finally:
            # print(f"[INFO] END - Finished counting customs orders received and not fulfilled")
            # print("[INFO] Cleaning up variables")
            try:
                del response_description, response_code, columns, condition, count, result_flag, result_query
            except:
                pass

    def count_customs_fulfilled_not_received_orders(self):
        # print(f"\n[INFO] BEGIN - Counting customs orders received and not fulfilled")

        response_description = "Success"
        response_code = 200
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND RECEIVED_FLG = FALSE"
        condition += f"\nAND FULFILLED_FLG = TRUE"
        count = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    count = row.get("TOTAL")

            return count
        except Exception as e:
            print(f"[ERROR] - {e}")
            return count
        finally:
            # print(f"[INFO] END - Finished counting customs orders received and not fulfilled")
            # print("[INFO] Cleaning up variables")
            try:
                del response_description, response_code, columns, condition, count, result_flag, result_query
            except:
                pass

    # API Functions
    def mark_customs_as_received(self, json_data):
        print(f"\n[INFO] BEGIN - Marking customs as received")

        tracking_number = None
        received_by = None
        received_notes = None
        received_date = self.utils.get_current_date_time()
        location_id = None
        condition = "1=1"
        order_exists = 0

        try:
            tracking_number = 'SF' + str(json_data.get('tracking_number')) if 'SF' not in str(json_data.get('tracking_number')).upper() else str(json_data.get('tracking_number'))
            received_by = json_data.get('received_by')
            received_notes = json_data.get('received_notes')
            location_id = json_data.get('location_id')

            if tracking_number is None or tracking_number == "":
                print(f"[ERROR] Error getting tracking number")
                return 400, False, 0, f"'Tracking Number' is a Mandatory field"

            if location_id is None or location_id == "":
                print(f"[ERROR] Error getting location id")
                return 400, False, 0, f"'Location ID' is a Mandatory field"

            if received_by is None or received_by == "":
                print(f"[ERROR] Received by is a Mandatory field")
                return 400, False, 0, f"'Received by' is a Mandatory field"

            order_exists = self.verify_customs_orders_with_tracking_number_exists(tracking_number=tracking_number)

            if not order_exists:
                print(f"[ERROR] No customs orders found with tracking number {tracking_number}")
                return 404, False, 0, f"No customs orders found with tracking number {tracking_number}"

            order_exists = self.verify_customs_orders_with_tracking_number_and_location_exists(tracking_number=tracking_number, location_id=location_id)

            if not order_exists:
                print(f"[ERROR] No customs orders found with tracking number {tracking_number} and location id {location_id}")
                return 404, False, 0, f"No customs orders found with tracking number {tracking_number} for this location"

            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("RECEIVED_FLG", "TRUE")
            self.utils.validate_columns_values("RECEIVED_DATE", f"DATE_FORMAT('{received_date}', '%Y-%m-%d %H:%i:%s')")
            self.utils.validate_columns_values("RECEIVED_BY", self.utils.replace_special_chars(received_by))
            self.utils.validate_columns_values("RECEIVED_NOTES", self.utils.replace_special_chars(received_notes))
            self.utils.validate_columns_values("TOTAL_SHIPPING_TIME", "DATEDIFF(RECEIVED_DATE, SHIPPED_DATE)")

            condition += f"\nAND TRACKING_NUMBER = '{tracking_number}'"
            condition += f"\nAND LOCATION_ID = '{location_id}'"

            print(f"[INFO] Updating customs with the following tracking number {tracking_number} as received.")
            orders_updated_flag, rowcount, result_string = super().update_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if orders_updated_flag:
                print(f"[INFO] Orders updated successfully.")
                return 200, True, rowcount, result_string
            else:
                print(f"[ERROR] {result_string}")
                return 500, False, 0, result_string

        except Exception as e:
            print(f"[ERROR] - {e}")
            return 500, False, 0, str(e)
        finally:
            print(f"[INFO] END - Finished marking customs as received")
            print("[INFO] Cleaning up variables")
            try:
                del tracking_number, received_by, received_notes, received_date, condition, data
            except:
                pass

    def process_file_from_vendor(self, file_path, file_name):
        self.set_start_time(self.utils.get_current_date_time())
        print(f"\n[INFO] BEGIN - Processing file from Vendor")

        sheet_readed_flag = False
        sheet_data = []
        response_description = None
        orders_updated_flag = False
        rowcount = 0
        result_string = "Success"
        file_deleted_flag = False

        try:
            time.sleep(1) # Do not remove. If removed, it will cause the program to crash because the file will not be fully created/saved in the path yet.
            sheet_readed_flag, sheet_data, response_description = self.utils.read_excel_file(file_path=file_path, file_name=file_name, sheet_name=None)
            if sheet_readed_flag:
                orders_updated_flag, rowcount, result_string = self.update_custom_orders_management_from_sheet(sheet_data=sheet_data)

                if orders_updated_flag:
                    print(f"[INFO] Deleting file {file_name}...")
                    file_deleted_flag = self.utils.delete_files(sheet_file_path=file_path, pdf_file_path=None, sheet_file_names=[file_name], pdf_file_names=None)

                    if file_deleted_flag:
                        print(f"[INFO] File {file_name} deleted successfully.")
                        return True
                    else:
                        print(f"[ERROR] Error deleting file {file_name}")
                        return False
                else:
                    print(f"[ERROR] {result_string}")
                    self.utils.send_exception_email(module=self.get_module_name(), function="process_file_from_vendor", error=result_string, additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                    return False
            else:
                print(f"[ERROR] Error reading file {file_name}")
                self.utils.send_exception_email(module=self.get_module_name(), function="process_file_from_vendor", error=response_description, additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                return False
        except Exception as e:
            self.utils.send_exception_email(module=self.get_module_name(), function="process_file_from_vendor", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
            print(f"[ERROR] {e}")
            return False
        finally:
            print(f"[INFO] END - Finished processing file from Vendor")
            print("[INFO] Cleaning up variables")
            try:
                del sheet_readed_flag, sheet_data, response_description, orders_updated_flag, rowcount, result_string, file_deleted_flag
            except:
                pass

    def process_font_file(self, file_path, file_name):
        self.set_start_time(self.utils.get_current_date_time())
        print(f"\n[INFO] BEGIN - Processing file from Vendor")
        rowcount = 0
        result_string = "Success"
        file_deleted_flag = False
        results_file = None
        result_flag = False
        result_url = None
        try:
            time.sleep(1) # Do not remove. If removed, it will cause the program to crash because the file will not be fully created/saved in the path yet.
            results_file = self.utils.upload_files_to_s3(file_directory=file_path, file_names=[file_name], bucket_name="COMPANY_NAME-customs-files")

            for result in results_file:
                result_flag = result.get('success')
                result_string = result.get('error')
                result_url = result.get('url')

            if result_flag:

                print(f"[INFO] Deleting file {file_name}...")
                file_deleted_flag = self.utils.delete_files(sheet_file_path=file_path, pdf_file_path=None, sheet_file_names=[file_name], pdf_file_names=None)

                if file_deleted_flag:
                    print(f"[INFO] File {file_name} deleted successfully.")
                    return True, 200, "Success"
                else:
                    print(f"[ERROR] Error deleting file {file_name}")
                    return False, 500, "Error deleting file"
            else:
                return False, 500, "Error uploading file to S3"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished processing file from Vendor")
            print("[INFO] Cleaning up variables")
            try:
                del sheet_readed_flag, sheet_data, response_description, orders_updated_flag, rowcount, result_string, file_deleted_flag
            except:
                pass

    def get_list_of_custom_orders(self, since_id, order_id, order_number, fields, product_name, product_variant, is_sorority_flag, is_rush_flag, created_at_min, created_at_max, font, prod_sku, vendor_name, sent_to_vendor_flag, sent_to_vendor_date_min, sent_to_vendor_date_max, country, country_code, location_name, sent_back_from_vendor_flag, sent_back_from_vendor_date_min, sent_back_from_vendor_date_max, received_flag, received_back_date_min, received_back_date_max, tracking_number, is_fulfilled_flag, fulfilled_date_min, fulfilled_date_max, is_cancelled_flag, fulfillment_status, limit="50", order_by="asc"):
        print(f"\n[INFO] BEGIN - Getting list of custom orders from Management Table.")
        orders_list = []
        order = {}
        columns = []
        tracking_number = 'SF' + tracking_number if tracking_number is not None and 'SF' not in str(tracking_number).upper() else tracking_number

        if order_number is not None and order_number != "":
            order_number = f"#{str(order_number).upper()}" if "#" not in order_number else str(order_number).upper()
        if is_cancelled_flag is not None:
            is_cancelled_flag = 'TRUE' if str(is_cancelled_flag).lower() == 'true' else 'FALSE'
        if is_sorority_flag is not None:
            is_sorority_flag = 'TRUE' if str(is_sorority_flag).lower() == 'true' else 'FALSE'
        if sent_back_from_vendor_flag is not None:
            sent_back_from_vendor_flag = 'TRUE' if str(sent_back_from_vendor_flag).lower() == 'true' else 'FALSE'
        if is_fulfilled_flag is not None:
            is_fulfilled_flag = 'TRUE' if str(is_fulfilled_flag).lower() == 'true' else 'FALSE'
        if received_flag is not None:
            received_flag = 'TRUE' if str(received_flag).lower() == 'true' else 'FALSE'
        if fulfillment_status is not None:
            fulfillment_status = str(fulfillment_status).lower() if str(fulfillment_status).lower() != "unfulfilled" else "IS NULL"

        if fields is not None:
            fields = fields.replace(" ", "")
            fields.upper()
            if ',' in fields:
                for field in fields.split(','):
                    for key, value in self.get_custom_order_mapping_items():
                        if value == field.lower():
                            columns.append(key.upper())
                            break
            else:
                for key, value in self.get_custom_order_mapping_items():
                    if value == fields.lower():
                        columns.append(key.upper())
                        break
        else:
            columns = ['*']
        columns = ['*'] if len(columns) == 0 else columns

        condition = "1=1"
        condition += (f"\nAND ID >= '{since_id}'" if order_by.lower() == "asc" else f"\nAND ID <= '{since_id}'") if since_id is not None else ""
        condition += f"\nAND ID LIKE '%{order_id}%'" if order_id is not None else ""
        condition += f"\nAND NAME LIKE '%{order_number}%'" if order_number is not None else ""
        condition += f"\nAND PROD_NAME LIKE '%{product_name}%'" if product_name is not None else ""
        condition += f"\nAND PROD_NAME LIKE '%{product_variant}%'" if product_variant is not None else ""
        condition += f"\nAND SORORITY_FLG = {is_sorority_flag}" if is_sorority_flag is not None else ""
        condition += f"\nAND CREATED_AT >= '{created_at_min}'" if created_at_min is not None else ""
        condition += f"\nAND CREATED_AT <= '{created_at_max}'" if created_at_max is not None else ""
        condition += f"\nAND FONT LIKE '%{font}%'" if font is not None else ""
        condition += f"\nAND PROD_SKU LIKE '%{prod_sku}%'" if prod_sku is not None else ""
        condition += f"\nAND VENDOR_NAME LIKE '%{vendor_name}%'" if vendor_name is not None else ""
        if sent_to_vendor_flag is not None:
            condition += f"\nAND SENT_TO_VENDOR_DATE IS NOT NULL" if str(sent_to_vendor_flag).lower() == 'true' else f"\nAND SENT_TO_VENDOR_DATE IS NULL"
        condition += f"\nAND SENT_TO_VENDOR_DATE >= '{sent_to_vendor_date_min}'" if sent_to_vendor_date_min is not None else ""
        condition += f"\nAND SENT_TO_VENDOR_DATE <= '{sent_to_vendor_date_max}'" if sent_to_vendor_date_max is not None else ""
        condition += f"\nAND COUNTRY LIKE '%{country}%'" if country is not None else ""
        condition += f"\nAND COUNTRY_CODE LIKE '%{country_code}%'" if country_code is not None else ""
        condition += f"\nAND LOCATION_NAME LIKE '%{location_name}%'" if location_name is not None else ""
        condition += f"\nAND SENT_BY_VENDOR_FLG = {sent_back_from_vendor_flag}" if sent_back_from_vendor_flag is not None else ""
        condition += f"\nAND SHIPPED_DATE >= '{sent_back_from_vendor_date_min}'" if sent_back_from_vendor_date_min is not None else ""
        condition += f"\nAND SHIPPED_DATE <= '{sent_back_from_vendor_date_max}'" if sent_back_from_vendor_date_max is not None else ""
        condition += f"\nAND TRACKING_NUMBER = '{tracking_number}'" if tracking_number is not None else ""
        condition += f"\nAND FULFILLED_FLG = {is_fulfilled_flag}" if is_fulfilled_flag is not None else ""
        condition += f"\nAND RECEIVED_FLG = {received_flag}" if received_flag is not None else ""
        condition += f"\nAND RECEIVED_DATE >= '{received_back_date_min}'" if received_back_date_min is not None else ""
        condition += f"\nAND RECEIVED_DATE <= '{received_back_date_max}'" if received_back_date_max is not None else ""
        condition += f"\nAND FULFILLED_DATE >= '{fulfilled_date_min}'" if fulfilled_date_min is not None else ""
        condition += f"\nAND FULFILLED_DATE <= '{fulfilled_date_max}'" if fulfilled_date_max is not None else ""
        if fulfillment_status is not None:
            condition += f"\nAND FULFILLMENT_STATUS = '{fulfillment_status}'" if fulfillment_status is not None and fulfillment_status != "IS NULL" else f"\nAND FULFILLMENT_STATUS {fulfillment_status}"
        if is_cancelled_flag is not None:
            condition += f"\nAND CANCELLED_FLG = {is_cancelled_flag}" if is_cancelled_flag is not None else ""
        if is_rush_flag is not None:
            condition += f"\nAND RUSH_FLG = TRUE" if str(is_rush_flag).lower() == 'true' else f"\nAND RUSH_FLG = FALSE"
        condition += f"\nORDER BY ID ASC" if order_by.lower() == "asc" else f"\nORDER BY ID DESC"
        if limit is None or str(limit) == "0" or limit == "":
            condition += f"\nLIMIT 50"
        else:
            if str(limit).lower() != "all":
                condition += f"\nLIMIT {str(limit)}"

        result_flag = False
        result_query = None
        row = None
        value = None

        try:
            print(f"[INFO] Condition: {condition}")
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)
            if result_flag:
                print(f"[INFO] Got {len(result_query)} custom orders from Management Table.")
                for row in result_query:
                    order = {}
                    if columns == ['*']:
                        for mapping_row in self.get_custom_order_mapping():
                            key = self.get_custom_order_mapping_key(mapping_row)
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
                                order[key] = value
                    else:
                        for column in columns:
                            if column in self.get_custom_order_mapping():
                                key = self.get_custom_order_mapping_key(column)
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
                                    order[key] = value
                    orders_list.append(order)

                return True, orders_list, 200
            else:
                print(f"[ERROR] Error getting custom orders from Management Table. Error: {result_query}")
                return False, result_query, 404
        except Exception as e:
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_list_of_custom_orders_management", error_code=500, error_message=str(e), additional_details=None, error_severity=self.utils.get_error_severity(3))
            print(f"[ERROR] - {e}")
            return False, str(e), 500
        finally:
            print("[INFO] Cleaning up variables")
            try:
                del orders_list, order, columns, condition, result_flag, result_query, row
            except:
                pass

    def export_list_of_custom_orders(self, email_to, since_id, order_id, order_number, fields, product_name, product_variant, is_sorority_flag, is_rush_flag, created_at_min, created_at_max, font, prod_sku, vendor_name, sent_to_vendor_flag, sent_to_vendor_date_min, sent_to_vendor_date_max, country, country_code, location_name, sent_back_from_vendor_flag, sent_back_from_vendor_date_min, sent_back_from_vendor_date_max, received_flag, received_back_date_min, received_back_date_max, tracking_number, is_fulfilled_flag, fulfilled_date_min, fulfilled_date_max, is_cancelled_flag, fulfillment_status, limit="all", order_by="asc"):
        print(f"\n[INFO] BEGIN - Exporting list of custom orders from Management Table.")
        orders_list = {}
        result_flag = False
        return_code = 200
        sheet_file_path = None
        file_name = None
        file_title = None
        result_file = None
        result_string = None
        email_list = []
        email_from = "COMPANY_NAMEheroku@gmail.com"
        email_subject = f"Automatic Custom Orders Export - {self.utils.get_current_date_time()}"
        email_body = None
        today = self.utils.get_current_date()
        base_dir = self.utils.get_base_directory()

        if email_to is None or email_to == "":
            print(f"[ERROR] Email To is required.")
            return False, "Email To is required", 400

        try:
            result_flag, sheet_file_path, return_code = self.syspref.get_sys_pref("CUSTOMS_EXPORT_FILE_PATH")
            result_flag, file_name, return_code = self.syspref.get_sys_pref("CUSTOM_EXPORT_FILE_NAME")
            result_flag, file_title, return_code = self.syspref.get_sys_pref("CUSTOM_EXPORT_FILE_TITLE")
        except Exception as e:
            print(f"[ERROR] {e}")
            return False

        file_name = file_name.format(today)

        if not sheet_file_path or sheet_file_path == "":
            print(f"[ERROR] Sheet File Path is not defined.")
            return False
        if not file_name or file_name == "":
            print(f"[ERROR] File Name is not defined.")
            return False
        if not file_title or file_title == "":
            print(f"[ERROR] File Title is not defined.")
            return False

        try:
            result_flag, orders_list, return_code = self.get_list_of_custom_orders(since_id=since_id, order_id=order_id, order_number=order_number, fields=fields, product_name=product_name, product_variant=product_variant, is_sorority_flag=is_sorority_flag, is_rush_flag=is_rush_flag, created_at_min=created_at_min, created_at_max=created_at_max, font=font, prod_sku=prod_sku, vendor_name=vendor_name, sent_to_vendor_flag=sent_to_vendor_flag, sent_to_vendor_date_min=sent_to_vendor_date_min, sent_to_vendor_date_max=sent_to_vendor_date_max, country=country, country_code=country_code, location_name=location_name, sent_back_from_vendor_flag=sent_back_from_vendor_flag, sent_back_from_vendor_date_min=sent_back_from_vendor_date_min, sent_back_from_vendor_date_max=sent_back_from_vendor_date_max, received_flag=received_flag, received_back_date_min=received_back_date_min, received_back_date_max=received_back_date_max, tracking_number=tracking_number, is_fulfilled_flag=is_fulfilled_flag, fulfilled_date_min=fulfilled_date_min, fulfilled_date_max=fulfilled_date_max, is_cancelled_flag=is_cancelled_flag, fulfillment_status=fulfillment_status, limit=limit, order_by=order_by)

            if result_flag:
                result_flag, result_file = self.generate_custom_report_sheet(items_sheet_array=orders_list, sheet_file_path=f"{base_dir}{sheet_file_path}", file_name=file_name, file_title=file_title)

                if result_flag:
                    print(f"[INFO] Sending email to {email_to}...")
                    email_body = f"Hello,\n\nThis is an automated email regarding the Customs export process."
                    email_body += f"\n\nAttached is the result of the custom orders export generated on {self.utils.get_current_date_time()}."
                    email_body += f"\n\nA Total of {len(orders_list)} orders was/were exported."

                    for email in str(email_to.replace(' ', '')).split(','):
                        email_list.append(email)

                    result_flag, result_string = self.utils.send_email(email_from="COMPANY_NAMEheroku@gmail.com", email_to=email_list, email_subject=email_subject, email_body=email_body, file_path=f"{base_dir}{sheet_file_path}", file_names=[file_name])

                    result_flag = self.utils.delete_files(sheet_file_path=f"{base_dir}{sheet_file_path}", pdf_file_path=None, sheet_file_names=[file_name], pdf_file_names=None)

                    return result_flag, result_string, 200
                else:
                    print(f"[ERROR] Error generating custom report sheet. Error: {result_file}")
                    return False, result_file, 500
            else:
                print(f"[ERROR] Error getting list of custom orders. Error: {orders_list}")
                return False, orders_list, 404
        except Exception as e:
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), 
                               module_name=self.get_module_name(), 
                               function_name="export_list_of_custom_orders", 
                               error_code=500, error_message=str(e), 
                               additional_details=None, 
                               error_severity=self.utils.get_error_severity(3))
            print(f"[ERROR] - {e}")
            return False, str(e), 500
        finally:
            print(f"[INFO] END - Exporting list of custom orders from Management Table.")
            print("[INFO] Cleaning up variables")
            try:
                del orders_list, result_flag, return_code, sheet_file_path, file_name, file_title, result_file, result_string, email_list, email_from, email_subject, email_body, today, base_dir
            except:
                pass

    def get_specific_custom_order(self, order_id, fields):
        print(f"\n[INFO] BEGIN - Getting the custom order with id {order_id} from Management Table.")
        order = {}
        columns = []

        if order_id is None:
            return False, "Order ID is required", 400

        if fields is not None:
            fields = fields.replace(" ", "")
            fields.upper()
            if ',' in fields:
                for field in fields.split(','):
                    for key, value in self.get_custom_order_mapping_items():
                        if value == field.lower():
                            columns.append(key.upper())
                            break
            else:
                for key, value in self.get_custom_order_mapping_items():
                    if value == fields.lower():
                        columns.append(key.upper())
                        break
        else:
            columns = ['*']
        columns = ['*'] if len(columns) == 0 else columns

        condition = "1=1"
        condition += f"\nAND ID = '{order_id}'"
        result_flag = False
        result_query = None
        row = None
        value = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)
            if result_flag:
                print(f"[INFO] Got {len(result_query)} custom orders from Management Table.")
                for row in result_query:
                    order = {}
                    if columns == ['*']:
                        for mapping_row in self.get_custom_order_mapping():
                            key = self.get_custom_order_mapping_key(mapping_row)
                            if key is not None:
                                value = row.get(mapping_row)
                                try:
                                    value = value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime.datetime) else value
                                except:
                                    pass

                                order[key] = value
                    else:
                        for column in columns:
                            if column in self.get_custom_order_mapping():
                                key = self.get_custom_order_mapping_key(column)
                                if key is not None:
                                    value = row.get(column)
                                    try:
                                        value = value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime.datetime) else value
                                    except:
                                        pass
                                    order[key] = value

                return True, order, 200
            else:
                print(f"[ERROR] Error getting custom orders from Management Table. Error: {result_query}")
                return False, result_query, 404
        except Exception as e:
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_list_of_custom_orders_management", error_code=500, error_message=str(e), additional_details=None, error_severity=self.utils.get_error_severity(3))
            print(f"[ERROR] - {e}")
            return False, str(e), 500
        finally:
            print("[INFO] Cleaning up variables")
            try:
                del order, columns, condition, result_flag, result_query, row
            except:
                pass

    def get_customs_analytics(self):
        print(f"\n[INFO] BEGIN - Getting customs analytics")
        result_flag_reports = False
        result_flag_reports_one_day_ago = False
        current_date_time = datetime.datetime.strptime(self.utils.get_current_date_time(), "%Y-%m-%d %H:%M:%S")
        one_day_ago = self.utils.get_x_days_ago(2 if current_date_time.time() < datetime.time(19, 0, 0) else 1).split(' ')[0]
        today = self.utils.get_current_date()
        print(f"[INFO] Today: {today} - One Day Ago: {one_day_ago}")
        analytics_return = {
            "processing_times": {},
            "totals": {},
            "details": {},
            "fulfillments": {},
            "performance_history": {}
        }
        processing_analytics = {
            "min_vendor_process_time": 0,
            "avg_vendor_process_time": 0,
            "max_vendor_process_time": 0,
            "avg_ship_time": 0,
            "max_ship_time": 0,
            "avg_total_process_time": 0,
            "max_total_process_time": 0
        }
        result_query_reports = None
        result_query_reports_one_day_ago = None
        dynamic_vars = {}
        dynamic_vars_one_day_ago = {}
        report_type = "CUSTOMS_REPORTS"

        try:
            result_flag_reports, result_query_reports, dynamic_vars = self.reports.extract_report_data(report_type=report_type, report_date=today)
            result_flag_reports_one_day_ago, result_query_reports_one_day_ago, dynamic_vars_one_day_ago = self.reports.extract_report_data(report_type=report_type, report_date=one_day_ago)

            print(f"\n[INFO] Getting processing analytics")
            processing_analytics = {
                "min_vendor_process_time": self.utils.return_int_or_float(dynamic_vars['min_vendor_process_time']),
                "avg_vendor_process_time": self.utils.return_int_or_float(dynamic_vars['avg_vendor_process_time']),
                "max_vendor_process_time": self.utils.return_int_or_float(dynamic_vars['max_vendor_process_time']),
                "avg_ship_time": self.utils.return_int_or_float(dynamic_vars['avg_ship_time']),
                "max_ship_time": self.utils.return_int_or_float(dynamic_vars['max_ship_time']),
                "avg_total_process_time": self.utils.return_int_or_float(dynamic_vars['avg_total_process_time']),
                "max_total_process_time": self.utils.return_int_or_float(dynamic_vars['max_total_process_time'])
            }

            # Total Orders and Items
            print(f"[INFO] Getting total orders")
            count_total_orders = self.utils.return_int_or_float(dynamic_vars['count_total_orders'])
            count_total_orders_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_total_orders']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_sorority_orders = self.utils.return_int_or_float(dynamic_vars['count_sorority_orders'])
            count_sorority_orders_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_sorority_orders']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_non_sorority_orders = self.utils.return_int_or_float(dynamic_vars['count_non_sorority_orders'])
            count_non_sorority_orders_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_non_sorority_orders']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0

            total_orders = {
                "total": count_total_orders,
                "total_yesterday": count_total_orders_one_day_ago,
                "total_increase_percentage": self.utils.calculate_percentage(count_total_orders_one_day_ago, count_total_orders),
                "sorority": {
                    "total": count_sorority_orders,
                    "total_yesterday": count_sorority_orders_one_day_ago,
                    "increase_percentage": self.utils.calculate_percentage(count_sorority_orders_one_day_ago, count_sorority_orders)
                },
                "non_sorority": {
                    "total": count_non_sorority_orders,
                    "total_yesterday": count_non_sorority_orders_one_day_ago,
                    "increase_percentage": self.utils.calculate_percentage(count_non_sorority_orders_one_day_ago, count_non_sorority_orders)
                }
            }

            print(f"[INFO] Getting total items")
            count_total_items = self.utils.return_int_or_float(dynamic_vars['count_total_items'])
            count_total_items_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_total_items']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_sorority_items = self.utils.return_int_or_float(dynamic_vars['count_sorority_items'])
            count_sorority_items_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_sorority_items']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_non_sorority_items = self.utils.return_int_or_float(dynamic_vars['count_non_sorority_items'])
            count_non_sorority_items_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_non_sorority_items']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            total_items = {
                "total": count_total_items,
                "total_yesterday": count_total_items_one_day_ago,
                "total_increase_percentage": self.utils.calculate_percentage(count_total_items_one_day_ago, count_total_items),
                "sorority": {
                    "total": self.utils.return_int_or_float(dynamic_vars['count_sorority_items']),
                    "total_yesterday": count_sorority_items_one_day_ago,
                    "increase_percentage": self.utils.calculate_percentage(count_sorority_items_one_day_ago, count_sorority_items)
                },
                "non_sorority": {
                    "total": self.utils.return_int_or_float(dynamic_vars['count_non_sorority_items']),
                    "total_yesterday": count_non_sorority_items_one_day_ago,
                    "increase_percentage": self.utils.calculate_percentage(count_non_sorority_items_one_day_ago, count_non_sorority_items)
                }
            }
            totals = {
                "orders": total_orders,
                "items": total_items
            }

            print(f"[INFO] Getting orders and items details")
            count_orders_sent_to_vendor_not_sent_back = self.utils.return_int_or_float(dynamic_vars['count_orders_sent_to_vendor_not_sent_back'])
            count_orders_sent_to_vendor_not_sent_back_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_orders_sent_to_vendor_not_sent_back']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_items_sent_to_vendor_not_sent_back = self.utils.return_int_or_float(dynamic_vars['count_items_sent_to_vendor_not_sent_back'])
            count_items_sent_to_vendor_not_sent_back_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_items_sent_to_vendor_not_sent_back']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_orders_in_transit = self.utils.return_int_or_float(dynamic_vars['count_orders_in_transit'])
            count_orders_in_transit_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_orders_in_transit']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_items_in_transit = self.utils.return_int_or_float(dynamic_vars['count_items_in_transit'])
            count_items_in_transit_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_items_in_transit']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_orders_received_back = self.utils.return_int_or_float(dynamic_vars['count_orders_received_back'])
            count_orders_received_back_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_orders_received_back']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_items_received_back = self.utils.return_int_or_float(dynamic_vars['count_items_received_back'])
            count_items_received_back_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_items_received_back']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0

            # Sent to vendor Orders and Items
            details = {
                "orders": {
                    "sent_to_vendor": {
                        "total": count_orders_sent_to_vendor_not_sent_back,
                        "total_yesterday": count_orders_sent_to_vendor_not_sent_back_one_day_ago,
                        "total_increase_percentage": self.utils.calculate_percentage(count_orders_sent_to_vendor_not_sent_back_one_day_ago, count_orders_sent_to_vendor_not_sent_back)
                    },
                    "in_transit_back": {
                        "total": self.utils.return_int_or_float(dynamic_vars['count_orders_in_transit']),
                        "total_yesterday": count_orders_in_transit_one_day_ago,
                        "total_increase_percentage": self.utils.calculate_percentage(count_orders_in_transit_one_day_ago, count_orders_in_transit)
                    },
                    "received_back": {
                        "total": count_orders_received_back,
                        "total_yesterday": count_orders_received_back_one_day_ago,
                        "total_increase_percentage": self.utils.calculate_percentage(count_orders_received_back_one_day_ago, count_orders_received_back)
                    }
                },
                "items": {
                    "sent_to_vendor": {
                        "total": count_items_sent_to_vendor_not_sent_back,
                        "total_yesterday": count_items_sent_to_vendor_not_sent_back_one_day_ago,
                        "total_increase_percentage": self.utils.calculate_percentage(count_items_sent_to_vendor_not_sent_back_one_day_ago, count_items_sent_to_vendor_not_sent_back)
                    },
                    "in_transit_back": {
                        "total": count_items_in_transit,
                        "total_yesterday": count_items_in_transit_one_day_ago,
                        "total_increase_percentage": self.utils.calculate_percentage(count_items_in_transit_one_day_ago, count_items_in_transit)
                    },
                    "received_back": {
                        "total": count_items_received_back,
                        "total_yesterday": count_items_received_back_one_day_ago,
                        "total_increase_percentage": self.utils.calculate_percentage(count_items_received_back_one_day_ago, count_items_received_back)
                    }
                }
            }

            print(f"[INFO] Getting fulfillments")
            count_fulfilled = self.utils.return_int_or_float(dynamic_vars['count_fulfilled'])
            count_unfulfilled = self.utils.return_int_or_float(dynamic_vars['count_unfulfilled'])
            count_fulfilled_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_fulfilled']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_unfulfilled_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_unfulfilled']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_fulfilled_items = self.utils.return_int_or_float(dynamic_vars['count_fulfilled_items'])
            count_unfulfilled_items = self.utils.return_int_or_float(dynamic_vars['count_unfulfilled_items'])
            count_fulfilled_items_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_fulfilled_items']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0
            count_unfulfilled_items_one_day_ago = self.utils.return_int_or_float(dynamic_vars_one_day_ago['count_unfulfilled_items']) if dynamic_vars_one_day_ago != {} and dynamic_vars_one_day_ago is not None else 0

            fulfillments = {
                "orders": {
                    "total_fulfilled": count_fulfilled,
                    "total_unfulfilled": count_unfulfilled,
                    "total_fulfilled_yesterday": count_fulfilled_one_day_ago,
                    "total_unfulfilled_yesterday": count_unfulfilled_one_day_ago,
                    "total_fulfilled_increase": self.utils.calculate_percentage(count_fulfilled_one_day_ago, count_fulfilled),
                    "total_unfulfilled_increase": self.utils.calculate_percentage(count_unfulfilled_one_day_ago, count_unfulfilled)
                },
                "items": {
                    "total_fulfilled": count_fulfilled_items,
                    "total_unfulfilled": count_unfulfilled_items,
                    "total_fulfilled_yesterday": count_fulfilled_items_one_day_ago,
                    "total_unfulfilled_yesterday": count_unfulfilled_items_one_day_ago,
                    "total_fulfilled_increase": self.utils.calculate_percentage(count_fulfilled_items_one_day_ago, count_fulfilled_items),
                    "total_unfulfilled_increase": self.utils.calculate_percentage(count_unfulfilled_items_one_day_ago, count_unfulfilled_items)
                }
            }

            print(f"[INFO] Attaching results to analytics return")
            analytics_return["processing_times"] = processing_analytics
            analytics_return["totals"] = totals
            analytics_return["details"] = details
            analytics_return["fulfillments"] = fulfillments
            analytics_return["performance_history"] = self.utils.convert_json_to_object(dynamic_vars['performance_history_json'])

            return True, 200, analytics_return
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, analytics_return
        finally:
            print(f"[INFO] END - Finished getting customs analytics")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag_reports, result_flag_reports_one_day_ago, current_date_time, one_day_ago, today, analytics_return, processing_analytics, result_query_reports, result_query_reports_one_day_ago, dynamic_vars, dynamic_vars_one_day_ago, report_type
            except:
                pass

    def get_customs_filters(self):
        print(f"\n[INFO] BEGIN - Getting customs filters")
        return_code = 200
        response_description = ""
        has_error = False

        customs_filters = {
            "products": [],
            "variants": [],
            "fonts": [],
            "tracking_numbers": [],
            "fulfillment_status": [],
            "skus": [],
            "locations": []
        }
        products = []
        variants = []
        fonts = []
        tracking_numbers = []
        fulfillment_status = []
        skus = []
        locations = []
        locations_result = []

        try:
            print(f"[INFO] Getting Filters Data")
            result_flag, products = self.get_all_custom_products_names()
            if not result_flag:
                print(f"[ERROR] Error getting products. Error: {products}")
                return_code = 500
                response_description += products
                has_error = True

            result_flag, variants = self.get_all_custom_product_variants()
            if not result_flag:
                print(f"[ERROR] Error getting products. Error: {products}")
                return_code = 500
                response_description = variants if not has_error else response_description + ", " + variants
                has_error = True

            result_flag, fonts = self.get_all_fonts()
            if not result_flag:
                print(f"[ERROR] Error getting fonts. Error: {fonts}")
                return_code = 500
                response_description = fonts if not has_error else response_description + ", " + fonts
                has_error = True

            result_flag, tracking_numbers = self.get_all_tracking_numbers()
            if not result_flag:
                print(f"[ERROR] Error getting tracking numbers. Error: {tracking_numbers}")
                return_code = 500
                response_description = tracking_numbers if not has_error else response_description + ", " + tracking_numbers
                has_error = True

            result_flag, fulfillment_status = self.get_all_fulfillment_statuses()
            if not result_flag:
                print(f"[ERROR] Error getting fulfillment statuses. Error: {fulfillment_status}")
                return_code = 500
                response_description = fulfillment_status if not has_error else response_description + ", " + fulfillment_status
                has_error = True

            result_flag, skus = self.get_all_skus()
            if not result_flag:
                print(f"[ERROR] Error getting skus. Error: {skus}")
                return_code = 500
                response_description = skus if not has_error else response_description + ", " + skus
                has_error = True

            result_flag, return_code, locations_result = self.locations.get_all_locations_api()
            if not result_flag:
                print(f"[ERROR] Error getting locations. Error: {locations_result}")
                return_code = 500
                response_description = locations_result if not has_error else response_description + ", " + locations_result
                has_error = True
            else:
                for loc in locations_result:
                    locations.append({
                        "id": loc.get('id'),
                        "name": loc.get('name'),
                        "country": loc.get('country'),
                        "country_code": loc.get('country_code')
                    })

            if not has_error:
                response_description = "Success"
                return_code = 200
                result_flag = True

            print(f"[INFO] Got Reports Data")
            customs_filters["products"] = products
            customs_filters["variants"] = variants
            customs_filters["fonts"] = fonts
            customs_filters["tracking_numbers"] = tracking_numbers
            customs_filters["fulfillment_status"] = fulfillment_status
            customs_filters["skus"] = skus
            customs_filters["locations"] = locations

            return result_flag, return_code, customs_filters
        except Exception as e:
            print(f"[ERROR] {e}")
            return result_flag, return_code, customs_filters
        finally:
            print(f"[INFO] END - Finished getting customs filters")
            print("[INFO] Cleaning up variables")
            try:
                del products, fonts, tracking_numbers, fulfillment_status, skus, result_flag, return_code
            except:
                pass

    def add_custom_font(self, custom_font_json):
        print(f"\n[INFO] BEGIN - Adding product font...")
        result_flag = False
        result_flag_font = False
        result_flag_font_variant = False
        result_string = None
        product_id = None
        product_title = None
        has_variant = None
        font_name = None
        variants = []
        variant_font_title = None
        variant_font_name = None
        len_variants = 0
        product_font = self.CustomProductFont()
        product_font_variant = self.CustomProductVariantFont()
        font_exists = False
        font_variant_exists = False

        try:
            product_id = custom_font_json.get('product_id', None)
            product_title = custom_font_json.get('product_title', None)
            has_variant = custom_font_json.get('has_variant', False)
            font_name = custom_font_json.get('font_name', None)
            variants = custom_font_json.get('variants', [])
            len_variants = len(variants)

            font_exists = self.verify_product_font_extists(product_id)

            if font_exists:
                return False, 400, "Product Font already exists"
            if product_id is None:
                return False, 400, "Product ID is required"
            if product_title is None:
                return False, 400, "Product Title is required"

            product_font.set_id(product_id)
            product_font.set_title(product_title)
            product_font.set_has_variant_flg(has_variant)
            product_font.set_font_name(font_name)
            product_font.set_vendor_id(custom_font_json.get('vendor_id', None))
            product_font.set_vendor_name(custom_font_json.get('vendor_name', None))

            if has_variant:
                if len_variants < 1:
                    return False, 400, "Variants cannot be empty"
                if len_variants < 2:
                    return False, 400, "Variants should have at least 2 items"

                result_flag_font, result_string = self.insert_custom_font(product_font)

                if result_flag_font:
                    for variant in variants:
                        variant_font_title = variant.get('font_title')
                        variant_font_name = variant.get('font_name')

                        font_variant_exists = self.verify_product_font_variant_extists(product_id, variant_font_title)

                        if font_variant_exists:
                            return False, 400, f"Product Font Variant {variant_font_title} already exists"
                        if variant_font_title is None:
                            return False, 400, "Variant Font Title is required"
                        if variant_font_name is None:
                            return False, 400, "Variant Font Name is required"

                        product_font_variant.set_product_id(product_id)
                        product_font_variant.set_name(variant_font_title)
                        product_font_variant.set_font_name(variant_font_name)
                        result_flag_font_variant, result_string = self.insert_custom_variant_font(product_font_variant)

                        if not result_flag_font_variant:
                            return False, 400, result_string
                else:
                    return False, 400, result_string
            else:
                if len_variants > 0:
                    return False, 400, "Variants should be empty"
                # if font_name is None:
                #     return False, 400, "Font Name is required"

                result_flag_font, result_string = self.insert_custom_font(product_font)

                if not result_flag_font:
                    return False, 400, result_string

            return True, 200, "Product Font added successfully"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished adding product font")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_flag_font, result_flag_font_variant, product_id, product_title, has_variant, font_name, variants, variant_font_title, variant_font_name, len_variants, product_font, product_font_variant
            except:
                pass

    def upd_custom_font(self, product_id, custom_font_json):
        print(f"\n[INFO] BEGIN - Updating product font...")
        result_flag = False
        result_flag_font = False
        result_flag_font_variant = False
        result_string = None
        product_title = None
        has_variant = None
        font_name = None
        variants = []
        variant_font_title = None
        variant_font_name = None
        len_variants = 0
        product_font = self.CustomProductFont()
        product_font_variant = self.CustomProductVariantFont()
        font_exists = False
        font_variant_exists = False

        try:
            product_title = custom_font_json.get('product_title', None)
            has_variant = custom_font_json.get('has_variant', False)
            font_name = custom_font_json.get('font_name', None)
            variants = custom_font_json.get('variants', [])
            len_variants = len(variants)

            font_exists = self.verify_product_font_extists(product_id)

            if not font_exists:
                return False, 400, "Product Font does not exists"
            if product_id is None:
                return False, 400, "Product ID is required"
            if product_title is None:
                return False, 400, "Product Title is required"

            product_font.set_title(product_title)
            product_font.set_has_variant_flg(has_variant)
            product_font.set_font_name(font_name)
            product_font.set_vendor_id(custom_font_json.get('vendor_id', None))
            product_font.set_vendor_name(custom_font_json.get('vendor_name', None))

            if has_variant:
                if len_variants < 1:
                    return False, 400, "Variants cannot be empty"
                if len_variants < 2:
                    return False, 400, "Variants should have at least 2 items"

                result_flag_font, result_string = self.update_custom_font(product_font)

                if result_flag_font:
                    for variant in variants:
                        variant_font_title = variant.get('font_title')
                        variant_font_name = variant.get('font_name')

                        font_variant_exists = self.verify_product_font_variant_extists(product_id, variant_font_title)

                        if variant_font_title is None:
                            return False, 400, "Variant Font Title is required"
                        if variant_font_name is None:
                            return False, 400, "Variant Font Name is required"

                        product_font_variant.set_product_id(product_id)
                        product_font_variant.set_name(variant_font_title)
                        product_font_variant.set_font_name(variant_font_name)

                        if not font_variant_exists:
                            result_flag_font_variant, result_string = self.insert_custom_variant_font(product_font_variant)
                        else:
                            result_flag_font_variant, result_string = self.update_custom_variant_font(product_font_variant)

                        if not result_flag_font_variant:
                            return False, 400, result_string
                else:
                    return False, 400, result_string
            else:
                if len_variants > 0:
                    return False, 400, "Variants should be empty"
                # if font_name is None:
                #     return False, 400, "Font Name is required"

                result_flag_font, result_string = self.update_custom_font(product_font)

                if not result_flag_font:
                    return False, 400, result_string
                else:
                    result_flag, font_variant_exists = self.verify_product_font_has_variant(product_id)

                    if font_variant_exists:
                        result_flag, result_string = self.delete_all_custom_variant_fonts(product_id)

                        if not result_flag:
                            return False, 400, result_string

            return True, 200, "Product Font updated successfully"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished updating product font")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_flag_font, result_flag_font_variant, product_id, product_title, has_variant, font_name, variants, variant_font_title, variant_font_name, len_variants, product_font, product_font_variant
            except:
                pass

    # Other Functions
    def update_custom_orders_management_from_sheet(self, sheet_data):
        print(f"\n[INFO] BEGIN - Updating Custom orders on Management Table.")

        orders_updated_flag = False
        rowcount = 0
        result_string = "Success"
        tracking_number = None
        tracking_url = None
        shipped_date = None
        count = 0
        order_names = ""
        order = None
        condition = "1=1"
        # condition += f"\nAND NAME IN ("
        item_name = None
        total_items = 0
        custom_text = None

        try:
            total_items = len(sheet_data)
            print(f"[INFO] Total ITEMS to update: {total_items}")
            print(f"[INFO] Updating Custom orders on Management Table.")
            print(f"[INFO] PROGRESS - ORDER NUMBER - ITEM NAME - TRACKING NUMBER - SHIPPED DATE")
            for items in sheet_data:
                condition = "1=1"
                count += 1

                order = str(items.get('ORDER'))
                item_name = str(items.get('ITEM NAME'))
                custom_text = str(items.get('TEXT'))

                if order is not None:
                    order = order if '#' in order else '#' + order
                    item_name = self.utils.replace_special_chars(item_name.replace("null", "None"))
                    custom_text = self.utils.replace_special_chars(custom_text.replace("null", "None"))

                    condition += f"\nAND NAME = '{order}'"
                    condition += f"\nAND PROD_NAME = '{item_name}'"
                    # condition += f"\nAND CUSTOM_TEXT = '{custom_text}'" if "Sorority" not in item_name else ""

                    tracking_number = str(items.get('TRACKING'))
                    tracking_number = tracking_number if 'SF' in tracking_number.upper() else f"SF{tracking_number}"
                    tracking_url = "https://www.sf-international.com/us/en/dynamic_function/waybill/#search/bill-number/" + tracking_number
                    shipped_date = str(items.get('SHIP DATE'))

                    self.utils.clear_columns_values_arrays()
                    self.utils.validate_columns_values("SENT_BY_VENDOR_FLG", "TRUE")
                    self.utils.validate_columns_values("SHIPPED_DATE", f"DATE_FORMAT('{shipped_date}', '%Y-%m-%d %H:%i:%s')")
                    self.utils.validate_columns_values("TRACKING_NUMBER", tracking_number if 'SF' in tracking_number.upper() else f"SF{tracking_number}")
                    self.utils.validate_columns_values("TRACKING_URL", tracking_url)
                    self.utils.validate_columns_values("VENDOR_PROCESSING_TIME", "DATEDIFF(SHIPPED_DATE, CREATED_AT)")

                    print(f"[INFO] [{count} - {total_items}] - {order} - {item_name} - {tracking_number} - {shipped_date}")
                    orders_updated_flag, rowcount, result_string = super().update_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

                    if not orders_updated_flag:
                        print(f"[ERROR] [{count} - {total_items}] - {result_string}")

                    if rowcount == 0:
                        print(f"[ERROR] [{count} - {total_items}] - Order {order} not found on Management Table.")

            return orders_updated_flag, count, result_string
        except Exception as e:
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="insert_custom_orders_management", error_code=500, error_message=str(e), additional_details=None, error_severity=self.utils.get_error_severity(3))
            print(f"[ERROR] - {e}")
            return False, rowcount, None
        finally:
            print("[INFO] END - Finished updating Custom orders on Management Table.")
            print("[INFO] Cleaning up variables")
            try:
                del array_of_items, order_id, order_number, font, custom_text, name, sku, quantity, created_at, is_sorority, is_sorority_text, sorority_variant, custom_order, order_updated_flag, order_inserted_count, orders_count, total_orders_inserted_count, result_string
            except:
                pass

    def get_all_orders_sent_to_vendor_and_are_not_processed_yet(self):
        print(f"\n[INFO] BEGIN - Getting all orders sent to Vendor and are not processed yet")

        order_id = None
        order_name = None
        orders_array = []
        count_orders = 0
        result_flag = False
        result_query = None
        return_code = 200
        total_days_to_check = 21

        try:
            print(f"[INFO] Getting total days to check orders not sent back from Vendor")
            result_flag, total_days_to_check, return_code = self.syspref.get_sys_pref(sysPrefName="DAYS_TO_CHECK_ORDERS_NOT_SENT_BACK_FROM_VENDOR")
            print(f"[INFO] Total days to check orders not sent back from Vendor: {total_days_to_check} day(s) ago")
        except:
            pass

        since_date = self.utils.get_x_days_ago(int(total_days_to_check))
        columns = ['ID', 'NAME', 'CREATED_AT', 'FONT', 'CUSTOM_TEXT', 'PROD_NAME', 'PROD_SKU', 'QUANTITY', 'SENT_TO_VENDOR_DATE', 'SORORITY_FLG']
        count_columns = ['COUNT(*) AS TOTAL']
        condition = "1=1"
        condition += f"\nAND SENT_TO_VENDOR_DATE <= DATE_FORMAT('{since_date}', '%Y-%m-%d %H:%i:%s')"
        condition += f"\nAND SENT_BY_VENDOR_FLG = FALSE"

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), count_columns, condition)

            if result_flag:
                for row in result_query:
                    count_orders = row.get("TOTAL")

            if count_orders > 0:
                print(f"[INFO] Found {count_orders} orders...")
                print(f"[INFO] Getting orders information...")
                result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

                if result_flag:
                    for row in result_query:
                        order_id = row.get('ID')
                        order_name = row.get('NAME')

                        order = {
                            "order_id": row.get('ID'),
                            "order_name": row.get('NAME'),
                            "order_created_at": row.get('CREATED_AT'),
                            "order_font": row.get('FONT'),
                            "order_custom_text": row.get('CUSTOM_TEXT'),
                            "order_prod_name": row.get('PROD_NAME'),
                            "order_prod_sku": row.get('PROD_SKU'),
                            "order_quantity": row.get('QUANTITY'),
                            "order_sent_to_vendor_date": row.get('SENT_TO_VENDOR_DATE'),
                            "order_sorority_flg": row.get('SORORITY_FLG')
                        }
                        orders_array.append(order)

                    return True, count_orders, orders_array
                else:
                    print(f"[ERROR] Error getting orders")
                    return False, count_orders, "Error getting orders"
            else:
                print(f"[INFO] No orders found.")
                return True, count_orders, "No orders found."
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, count_orders, str(e)
        finally:
            print(f"[INFO] END - Finished getting all orders sent to Vendor more than {total_days_to_check} day(s) ago and are not processed yet")
            print("[INFO] Cleaning up variables")
            try:
                del total_days_to_check, since_date, columns, count_columns, condition, order_id, order_name, order_created_at, order_font, order_custom_text, order_prod_name, order_prod_sku, order_quantity, order_sent_to_vendor_date, order_sorority_flg
            except:
                pass

    def check_if_item_is_fulfilled(self, order_id, item_prod_name):
        # print(f"\n[INFO] BEGIN - Checking Custom Order Item Fulfillment")
        columns = ["ID", "NAME", "FULFILLMENT_STATUS", "FULFILLMENTS"]
        condition = "1=1"
        condition += f"\nAND ID = '{order_id}'"
        order_fulfillment_status = "unfulfilled"
        item_fulfillment_status = "unfulfilled"
        fulfillment_created_at = None
        fulfillments = []
        line_items = []
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_ORDER(), columns, condition)

            if result_flag:
                for order in result_query:
                    order_id = order.get('ID')
                    order_name = order.get('NAME')
                    order_fulfillment_status = order.get('FULFILLMENT_STATUS')
                    fulfillments = self.utils.convert_json_to_object(order.get('FULFILLMENTS'))

                    if order_fulfillment_status == "fulfilled" or order_fulfillment_status == "partial":
                        for fulfill in fulfillments:
                            fulfillment_created_at = fulfill.get('created_at').split('T')[0] + ' ' + fulfill.get('created_at').split('T')[1].split('-')[0]
                            line_items = fulfill.get('line_items')

                            for item in line_items:
                                if item.get('name') == item_prod_name:
                                    item_fulfillment_status = item.get('fulfillment_status')

                                    return True if item_fulfillment_status == "fulfilled" else False, order_fulfillment_status, item_fulfillment_status, fulfillment_created_at

                        return False, order_fulfillment_status, item_fulfillment_status, fulfillment_created_at
                    else:
                        return False, order_fulfillment_status, "unfulfilled", fulfillment_created_at
            else:
                return False, order_fulfillment_status, item_fulfillment_status
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, order_fulfillment_status, item_fulfillment_status, fulfillment_created_at
        finally:
            # print(f"[INFO] END - Finished checking Custom Order Item Fulfillment")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, order_fulfillment_status, item_fulfillment_status, fulfillment_created_at, fulfillments, line_items, result_flag, result_query
            except:
                pass

    def generate_received_not_fullfilled_orders_sheet(self, sheet_file_path, file_name):
        print(f"\n[INFO] BEGIN - Getting all received not fullfilled orders")
        self.utils.set_start_time(self.utils.get_current_date_time())
        return_flag, file_title, return_code  = self.syspref.get_sys_pref("RECEIVED_NOT_FULFILLED_SHEET_FILE_TITLE")
        try:
            return_flag, total_days, return_code = self.syspref.get_sys_pref("DAYS_TO_CHECK_ORDERS_RECEIVED_NOT_FULFILLED")
        except:
            total_days = 3

        total_days = int(total_days)
        x_days_ago = self.utils.get_x_days_ago(total_days).split(' ')[0]
        columns = ["ID", "NAME", "RECEIVED_FLG", "RECEIVED_DATE", "FULFILLED_FLG", "FULFILLED_DATE", "PROD_NAME"]
        condition = "1=1"
        condition += f"\nAND RECEIVED_FLG = TRUE"
        condition += f"\nAND FULFILLED_FLG = FALSE"
        condition += f"\nAND RECEIVED_DATE <= DATE_FORMAT('{x_days_ago}', '%Y-%m-%d')"
        order_id = None
        order_name = None
        prod_name = None
        received_date = None
        order_fulfilled_flag = False
        item_fulfillment_status = "unfulfilled"
        order_fulfillment_status = "unfulfilled"
        order_count = 0

        try:
            order_count = self.count_customs_received_not_fullfilled_orders(since_date=x_days_ago)

            if order_count > 0:
                result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

                if result_flag:
                    try:
                        wb = self.utils.create_excel_sheet_if_not_exists(sheet_file_path, file_name, file_title)

                        if not wb:
                            print(f"[ERROR] Error creating sheet {file_title}")
                            return False
                        else:
                            ws = wb.active
                            ws.append(["ORDER_NUMBER", "PROD_NAME", "RECEIVED_DATE"])

                            for order in result_query:
                                order_id = order.get('ID')
                                order_name = order.get('NAME')
                                prod_name = order.get('PROD_NAME')
                                received_date = order.get('RECEIVED_DATE')

                                if received_date is not None:
                                    received_date = str(received_date).split(' ')[0]

                                order_fulfilled_flag, order_fulfillment_status, item_fulfillment_status, fulfillment_created_at = self.check_if_item_is_fulfilled(order_id=order_id, item_prod_name=prod_name)

                                if order_fulfilled_flag:
                                    print(f"[INFO] Order {order_name} is fulfilled since {fulfillment_created_at}. Updating order as fulfilled...")

                                    self.utils.clear_columns_values_arrays()
                                    self.utils.validate_columns_values("RECEIVED_FLG", "TRUE")
                                    self.utils.validate_columns_values("RECEIVED_DATE", f"DATE_FORMAT('{fulfillment_created_at}', '%Y-%m-%d %H:%i:%s')")
                                    self.utils.validate_columns_values("FULFILLED_FLG", "TRUE")
                                    self.utils.validate_columns_values("FULFILLED_DATE", f"DATE_FORMAT('{fulfillment_created_at}', '%Y-%m-%d %H:%i:%s')")
                                    self.utils.validate_columns_values("TOTAL_PROCESSING_TIME", "DATEDIFF(FULFILLED_DATE, CREATED_AT)")
                                    self.utils.validate_columns_values("TOTAL_PROCESSING_TIME_UNIT", "days")

                                    condition = f"ID = {order_id}"
                                    result_flag, rowcount, result_string = super().update_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

                                    if not result_flag:
                                        print(f"[ERROR] {result_string}")

                                else:
                                    print(f"[INFO] Order {order_name} - Product {prod_name} is not fulfilled yet.")
                                    items = [order_name, prod_name, received_date]
                                    ws.append(items)

                            try:
                                wb.save(f'{sheet_file_path}{file_name}')
                                print(f"[INFO] Sheet {file_title} saved successfully")
                            except Exception as e:
                                print(f"[ERROR] {e}")
                                return False

                            return True
                    except Exception as e:
                        self.utils.send_exception_email(module=self.get_module_name(), function="generate_custom_files", error=str(e), additional_info=None, start_time=self.utils.get_start_time(), end_time=self.utils.get_current_date_time())
                        print(f"[ERROR] {e}")
                        return False
                else:
                    print(f"[INFO] No orders found")
                    return False
            else:
                print(f"[INFO] No orders found")
                return False
        except Exception as e:
            print(f"[ERROR] {e}")
            return False
        finally:
            print(f"[INFO] END - Finished getting all received not fullfilled orders")
            print("[INFO] Cleaning up variables")
            try:
                del three_days_ago, order_id, order_name, columns, condition, result_flag, result_query
            except:
                pass

    def generate_fullfilled_not_received_orders_sheet(self, sheet_file_path, file_name):
        print(f"\n[INFO] BEGIN - Getting all fulfilled not received orders")
        return_flag, file_title, return_code  = self.syspref.get_sys_pref("FULFILLED_NOT_RECEIVED_SHEET_FILE_TITLE")
        columns = ["NAME", "FULFILLED_DATE", "PROD_NAME"]
        condition = "1=1"
        condition += f"\nAND RECEIVED_FLG = FALSE"
        condition += f"\nAND FULFILLED_FLG = TRUE"
        order_name = None
        prod_name = None
        fulfilled_date = None
        order_count = 0

        try:
            order_count = self.count_customs_fulfilled_not_received_orders()

            if order_count > 0:
                print(f"[INFO] Found {order_count} orders fulfilled not received.")
                result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

                if result_flag:
                    try:
                        wb = self.utils.create_excel_sheet_if_not_exists(sheet_file_path, file_name, file_title)

                        if not wb:
                            print(f"[ERROR] Error creating sheet {file_title}")
                            return False
                        else:
                            ws = wb.active
                            ws.append(["ORDER_NUMBER", "PROD_NAME", "FULFILLED_DATE"])
                            for order in result_query:
                                order_name = order.get('NAME')
                                prod_name = order.get('PROD_NAME')
                                fulfilled_date = order.get('FULFILLED_DATE')

                                print(f"[INFO] Order {order_name} - Product {prod_name} is fulfilled but is not marked as received.")
                                items = [order_name, prod_name, fulfilled_date]
                                ws.append(items)

                            try:
                                wb.save(f'{sheet_file_path}{file_name}')
                                print(f"[INFO] Sheet {file_title} saved successfully")
                            except Exception as e:
                                print(f"[ERROR] {e}")
                                return False

                            return True
                    except Exception as e:
                        self.utils.send_exception_email(module=self.get_module_name(), function="generate_custom_files", error=str(e), additional_info=None, start_time=self.utils.get_start_time(), end_time=self.utils.get_current_date_time())
                        print(f"[ERROR] {e}")
                        return False
                else:
                    print(f"[INFO] No orders found")
                    return False
            else:
                print(f"[INFO] No orders found")
                return False
        except Exception as e:
            print(f"[ERROR] {e}")
            return False
        finally:
            print(f"[INFO] END - Finished getting all fulfilled not received orders")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, order_name, prod_name, fulfilled_date, order_count, return_flag, file_title, return_code
            except:
                pass

    def daily_customs_reports(self):
        print(f"\n[INFO] BEGIN - Sending daily customs reports")
        self.utils.set_start_time(self.utils.get_current_date_time())
        try:
            print(f"[INFO] Getting system preferences...")
            return_flag, sheet_file_path, return_code = self.syspref.get_sys_pref("FULFILLED_NOT_RECEIVED_SHEET_FILE_PATH")
            print(f"[INFO] Got System Preference - sheet_file_path: {sheet_file_path}")
            return_flag, received_unfulfilled_file_name, return_code = self.syspref.get_sys_pref("RECEIVED_NOT_FULFILLED_SHEET_FILE_NAME")
            print(f"[INFO] Got System Preference - received_NOT_FULFILLED_file_name: {received_unfulfilled_file_name}")
            return_flag, fulfilled_not_received_file_name, return_code = self.syspref.get_sys_pref("FULFILLED_NOT_RECEIVED_SHEET_FILE_NAME")
            print(f"[INFO] Got System Preference - fulfilled_not_received_file_name: {fulfilled_not_received_file_name}")
            return_flag, email_from, return_code = self.syspref.get_sys_pref("CUSTOMS_EMAIL_FROM")
            print(f"[INFO] Got System Preference - email_from: {email_from}")
            return_flag, email_subject, return_code = self.syspref.get_sys_pref("CUSTOMS_DAILY_REPORTS_EMAIL_SUBJECT")
            print(f"[INFO] Got System Preference - email_subject: {email_subject}")
            return_flag, email_body, return_code = self.syspref.get_sys_pref("CUSTOMS_DAILY_REPORTS_EMAIL_BODY")
            print(f"[INFO] Got System Preference - email_body: {email_body}")
        except Exception as e:
            print(f"[ERROR] {e}")
            return False

        sheet_file_path = self.utils.get_base_directory() + sheet_file_path
        email_subject = email_subject.format(self.utils.get_current_date())
        email_body = email_body.format(self.utils.get_current_date())
        email_to = ['xxxxxxxxxx@COMPANY_NAME.com', 'xxxxxxxxxx@COMPANY_NAME.com'] if self.utils.get_DEBUG_MODE() == "FALSE" else ['xxxxxxxxxx@COMPANY_NAME.com']
        file_names = []

        received_not_fullfilled_orders_file_generated = self.generate_received_not_fullfilled_orders_sheet(sheet_file_path=sheet_file_path, file_name=received_unfulfilled_file_name)
        if received_not_fullfilled_orders_file_generated:
            file_names.append(received_unfulfilled_file_name)

        fullfilled_not_received_orders_file_generated = self.generate_fullfilled_not_received_orders_sheet(sheet_file_path=sheet_file_path, file_name=fulfilled_not_received_file_name)
        if fullfilled_not_received_orders_file_generated:
            file_names.append(fulfilled_not_received_file_name)

        try:
            print(f"[INFO] Sending email to {email_to}...")
            self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=sheet_file_path, file_names=file_names)
            self.utils.delete_files(sheet_file_path=sheet_file_path, pdf_file_path=None, sheet_file_names=file_names, pdf_file_names=None)
            return True
        except Exception as e:
            self.utils.send_exception_email(module=self.get_module_name(), function="daily_customs_reports", error=str(e), additional_info=None, start_time=self.utils.get_start_time(), end_time=self.utils.get_current_date_time())
            print(f"[ERROR] {e}")
            return False
        finally:
            print(f"[INFO] END - Finished sending daily customs reports")
            print("[INFO] Cleaning up variables")
            try:
                del sheet_file_path, email_from, email_subject, email_body, email_to, file_names, received_not_fullfilled_orders_file_generated, fullfilled_not_received_orders_file_generated
            except:
                pass

    def get_all_fonts(self):
        print(f"\n[INFO] BEGIN - Getting all fonts")
        columns = ["DISTINCT(FONT_NAME)"]
        condition = "1=1"
        condition += f"\nAND FONT_NAME IS NOT NULL"
        fonts_array = []
        result_flag = False
        result_query = None
        font = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_PROD_FONTS(), columns, condition)

            if result_flag:
                condition += f"\nAND FONT_NAME NOT IN ("
                for row in result_query:
                    condition += f"\n\t'{row.get('FONT_NAME')}'" if result_query.index(row) == 0 else f",\n\t'{row.get('FONT_NAME')}'"
                    font = {
                        "name": row.get('FONT_NAME')
                    }
                    fonts_array.append(font)
                condition += f"\n)"
            else:
                return False, "Error getting fonts"

            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_PROD_VARIANT_FONTS(), columns, condition)
            if result_flag:
                for row in result_query:
                    if any(font.get('font_name') == row.get('FONT_NAME') for font in fonts_array):
                        continue
                    else:
                        font = {
                            "name": row.get('FONT_NAME')
                        }
                        fonts_array.append(font)

                return True, fonts_array
            else:
                return False, "Error getting fonts"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all fonts")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, font
            except:
                pass

    def get_all_custom_products_names(self):
        print(f"\n[INFO] BEGIN - Getting all custom products names")
        columns = ["DISTINCT(PROD_NAME)"]
        condition = "1=1"
        condition += f"\nAND PROD_NAME IS NOT NULL"
        condition += f"\nAND PROD_CATEGORY = 'Customs'"
        condition += f"\nORDER BY PROD_NAME ASC"
        products_array = []
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_PROD_INV(), columns, condition)

            if result_flag:
                for row in result_query:
                    product = {
                        "name": row.get('PROD_NAME')
                    }
                    products_array.append(product)
            else:
                return False, "Error getting products"

            return True, products_array
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all custom products names")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    def get_all_custom_product_variants(self):
        print(f"\n[INFO] BEGIN - Getting all custom products names")
        columns = ["DISTINCT TRIM(BOTH ' /' FROM CASE WHEN SUBSTRING_INDEX(PROD_NAME, ' - ', -1) LIKE '%/%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(PROD_NAME, ' / ', 2), ' / ', -1) ELSE SUBSTRING_INDEX(SUBSTRING_INDEX(PROD_NAME, ' - ', -1), ' ', 1) END) AS VARIANTS"]
        condition = "1=1"
        condition += f"\nAND PROD_NAME IS NOT NULL"
        condition += f"\nORDER BY VARIANTS ASC"
        variants_array = []
        product = {}
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    product = {
                        "name": row.get('VARIANTS')
                    }
                    variants_array.append(product)
            else:
                return False, "Error getting products"

            return True, variants_array
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all custom products names")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, product, variants_array
            except:
                pass

    def get_all_tracking_numbers(self):
        print(f"\n[INFO] BEGIN - Getting all fonts")
        columns = ["DISTINCT(TRACKING_NUMBER)"]
        condition = "1=1"
        condition += f"\nAND TRACKING_NUMBER IS NOT NULL"
        condition += f"\nORDER BY TRACKING_NUMBER ASC"
        tracking_numbers_array = []
        result_flag = False
        result_query = None
        tracking_number = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    tracking_number = {
                        "number": row.get('TRACKING_NUMBER')
                    }
                    tracking_numbers_array.append(tracking_number)
                return True, tracking_numbers_array
            else:
                return False, "Error getting fonts"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all fonts")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, tracking_number
            except:
                pass

    def get_all_fulfillment_statuses(self):
        print(f"\n[INFO] BEGIN - Getting all fulfillment statuses")
        columns = ["DISTINCT(FULFILLMENT_STATUS)"]
        condition = "1=1"
        condition += f"\nAND FULFILLMENT_STATUS IS NOT NULL"
        condition += f"\nORDER BY FULFILLMENT_STATUS ASC"
        fulfillment_statuses_array = []
        result_flag = False
        result_query = None
        fulfillment_status = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                fulfillment_status = {
                    "status": "unfulfilled"
                }
                fulfillment_statuses_array.append(fulfillment_status)
                for row in result_query:
                    fulfillment_status = {
                        "status": row.get('FULFILLMENT_STATUS')
                    }
                    fulfillment_statuses_array.append(fulfillment_status)

                # Order by status asc
                fulfillment_statuses_array = sorted(fulfillment_statuses_array, key=lambda k: k['status'])

                return True, fulfillment_statuses_array
            else:
                return False, "Error getting fonts"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all fulfillment statuses")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, fulfillment_status
            except:
                pass

    def get_all_skus(self):
        print(f"\n[INFO] BEGIN - Getting all SKUs")
        columns = ["DISTINCT(PROD_SKU)"]
        condition = "1=1"
        condition += f"\nAND PROD_SKU IS NOT NULL"
        condition += f"\nORDER BY PROD_SKU ASC"
        skus_array = []
        result_flag = False
        result_query = None
        sku = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    sku = {
                        "sku": row.get('PROD_SKU')
                    }
                    skus_array.append(sku)
                return True, skus_array
            else:
                return False, "Error getting fonts"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished getting all SKUs")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, sku
            except:
                pass

    def insert_custom_font(self, font:CustomProductFont):
        print(f"\n[INFO] BEGIN - Inserting Custom Product Font")
        result_flag = False
        result_string = "Success"
        rowcount = 0

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ID", font.get_id())
            self.utils.validate_columns_values("TITLE", self.utils.replace_special_chars(str(font.get_title()).replace("null", "None")))
            self.utils.validate_columns_values("HAS_VARIANT_FLG", "TRUE" if font.get_has_variant_flg() else "FALSE")
            self.utils.validate_columns_values("FONT_NAME", self.utils.replace_special_chars(font.get_font_name()) if font.get_font_name() is not None else "NULL")
            self.utils.validate_columns_values("VENDOR_ID", font.get_vendor_id())
            self.utils.validate_columns_values("VENDOR_NAME", font.get_vendor_name())

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_CUSTOM_PROD_FONTS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Custom Product Font {font.get_font_name()} inserted successfully")
                return True, result_string
            else:
                print(f"[ERROR] Error inserting Custom Product Font {font.get_font_name()}")
                return False, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished inserting Custom Product Font")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount
            except:
                pass

    def insert_custom_variant_font(self, font:CustomProductVariantFont):
        print(f"\n[INFO] BEGIN - Inserting Custom Product Variant Font")
        result_flag = False
        result_string = "Success"
        rowcount = 0

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("PROD_ID", font.get_product_id())
            self.utils.validate_columns_values("TITLE", font.get_name())
            self.utils.validate_columns_values("FONT_NAME", font.get_font_name())

            result_flag, rowcount, result_string = super().insert_record(super().get_tbl_CUSTOM_PROD_VARIANT_FONTS(), self.utils.get_columns_array(), self.utils.get_values_array())

            if result_flag:
                print(f"[INFO] Custom Product Variant Font {font.get_font_name()} inserted successfully")
                return True, result_string
            else:
                print(f"[ERROR] Error inserting Custom Product Variant Font {font.get_font_name()}")
                return False, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, result_string
        finally:
            print(f"[INFO] END - Finished inserting Custom Product Variant Font")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, rowcount
            except:
                pass

    def update_custom_font(self, font:CustomProductFont):
        print(f"\n[INFO] BEGIN - Updating Custom Product Font")
        result_flag = False
        result_string = "Success"
        condition = "1=1"
        condition += f"\nAND ID = '{font.get_id()}'"

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("TITLE", self.utils.replace_special_chars(str(font.get_title()).replace("null", "None")))
            self.utils.validate_columns_values("HAS_VARIANT_FLG", "TRUE" if font.get_has_variant_flg() else "FALSE")
            self.utils.validate_columns_values("FONT_NAME", self.utils.replace_special_chars(font.get_font_name()) if font.get_font_name() is not None else "NULL")
            self.utils.validate_columns_values("VENDOR_ID", font.get_vendor_id())
            self.utils.validate_columns_values("VENDOR_NAME", font.get_vendor_name())

            result_flag, result_string = super().update_record(super().get_tbl_CUSTOM_PROD_FONTS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Custom Product Font {font.get_font_name()} updated successfully")
                return True
            else:
                print(f"[ERROR] Error updating Custom Product Font {font.get_font_name()}")
                return False
        except Exception as e:
            print(f"[ERROR] {e}")
            return False
        finally:
            print(f"[INFO] END - Finished updating Custom Product Font")
            print("[INFO] Cleaning up variables")
            try:
                del columns, values, result_flag, result_string, condition
            except:
                pass

    def update_custom_variant_font(self, font:CustomProductVariantFont):
        print(f"\n[INFO] BEGIN - Updating Custom Product Variant Font")
        result_flag = False
        result_string = "Success"
        condition = "1=1"
        condition += f"\nAND PROD_ID = '{font.get_product_id()}'"
        condition += f"\nAND TITLE = '{font.get_name()}'"

        try:
            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("TITLE", font.get_name())
            self.utils.validate_columns_values("FONT_NAME", font.get_font_name())

            result_flag, result_string = super().update_record(super().get_tbl_CUSTOM_PROD_VARIANT_FONTS(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Custom Product Variant Font {font.get_font_name()} updated successfully")
                return True
            else:
                print(f"[ERROR] Error updating Custom Product Variant Font {font.get_font_name()}")
                return False
        except Exception as e:
            print(f"[ERROR] {e}")
            return False
        finally:
            print(f"[INFO] END - Finished updating Custom Product Variant Font")
            print("[INFO] Cleaning up variables")
            try:
                del columns, values, result_flag, result_string, condition
            except:
                pass

    def delete_custom_font(self, product_id):
        print(f"\n[INFO] BEGIN - Deleting Custom Product Font")
        result_flag = False
        result_flag_variant = False
        result_string = "Success"
        condition = "1=1"
        condition += f"\nAND ID = '{product_id}'"
        condition_variant = "1=1"
        condition_variant += f"\nAND PROD_ID = '{product_id}'"
        has_variant_flg = False
        rowcount = 0
        rowcountvariant = 0

        try:
            result_flag, has_variant_flg = self.verify_product_font_has_variant(product_id)
            if has_variant_flg:
                result_flag_variant, rowcountvariant, result_string = super().delete_record(super().get_tbl_CUSTOM_PROD_VARIANT_FONTS(), condition_variant)
                if result_flag_variant:
                    print(f"[INFO] Custom Product Variant Font {product_id} deleted successfully")
                else:
                    print(f"[ERROR] Error deleting Custom Product Variant Font {product_id}")
                    return False, 500, 0, 0, result_string

            result_flag, rowcount, result_string = super().delete_record(super().get_tbl_CUSTOM_PROD_FONTS(), condition)
            if result_flag:
                print(f"[INFO] Custom Product Font {product_id} deleted successfully")
                return True, 200, rowcount, rowcountvariant, result_string
            else:
                print(f"[ERROR] Error deleting Custom Product Font {product_id}")
                return False, 500, 0, 0, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, 0, str(e)
        finally:
            print(f"[INFO] END - Finished deleting Custom Product Font")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_flag_variant, result_string, condition, condition_variant, has_variant_flg, rowcount, rowcountvariant
            except:
                pass

    def delete_custom_variant_font(self, product_id, title):
        print(f"\n[INFO] BEGIN - Deleting Custom Product Variant Font")
        result_flag = False
        result_string = "Success"
        condition = "1=1"
        condition += f"\nAND PROD_ID = '{product_id}'"
        condition += f"\nAND TITLE = '{title}'"
        rowcount = 0
        prod_variant_exists = False

        try:
            prod_variant_exists = self.verify_product_font_variant_extists(product_id=product_id, title=title)
            if not prod_variant_exists:
                print(f"[INFO] Custom Product Variant Font {title} does not exist")
                return False, 404, 0, result_string
            else:
                result_flag, rowcount, result_string = super().delete_record(super().get_tbl_CUSTOM_PROD_VARIANT_FONTS(), condition)
                if result_flag:
                    print(f"[INFO] Custom Product Variant Font {title} deleted successfully")
                    return True, 200, rowcount, result_string
                else:
                    print(f"[ERROR] Error deleting Custom Product Variant Font {title}")
                    return False, 500, 0, result_string
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, 0, str(e)
        finally:
            print(f"[INFO] END - Finished deleting Custom Product Variant Font")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, condition
            except:
                pass

    def delete_all_custom_variant_fonts(self, product_id):
        print(f"\n[INFO] BEGIN - Deleting all Custom Product Variant Fonts")
        result_flag = False
        result_string = "Success"
        condition = "1=1"
        condition += f"\nAND PROD_ID = '{product_id}'"

        try:
            result_flag, result_string = super().delete_record(super().get_tbl_CUSTOM_PROD_VARIANT_FONTS(), condition)
            if result_flag:
                print(f"[INFO] All Custom Product Variant Fonts for product {product_id} deleted successfully")
                return True
            else:
                print(f"[ERROR] Error deleting all Custom Product Variant Fonts for product {product_id}")
                return False
        except Exception as e:
            print(f"[ERROR] {e}")
            return False
        finally:
            print(f"[INFO] END - Finished deleting all Custom Product Variant Fonts")
            print("[INFO] Cleaning up variables")
            try:
                del result_flag, result_string, condition
            except:
                pass

    def verify_product_font_has_variant(self, product_id):
        print(f"\n[INFO] BEGIN - Verifying if Custom Product Font has variant")
        columns = ["HAS_VARIANT_FLG"]
        condition = "1=1"
        condition += f"\nAND PROD_ID = '{product_id}'"
        has_variant_flg = False
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_PROD_FONTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    has_variant_flg = row.get('HAS_VARIANT_FLG')

                return True, has_variant_flg
            else:
                return False, "Error getting fonts"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Finished verifying if Custom Product Font has variant")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query, has_variant_flg
            except:
                pass

    def verify_product_font_extists(self, product_id):
        print(f"\n[INFO] BEGIN - Verifying if Custom Product Font exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND ID = '{product_id}'"
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_PROD_FONTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    if row.get('TOTAL') > 0:
                        return True
                    else:
                        return False
            else:
                return False
        except Exception as e:
            print(f"[ERROR] {e}")
            return False
        finally:
            print(f"[INFO] END - Finished verifying if Custom Product Font exists")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    def verify_product_font_variant_extists(self, product_id, title):
        print(f"\n[INFO] BEGIN - Verifying if Custom Product Font Variant exists")
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND PROD_ID = '{product_id}'"
        condition += f"\nAND TITLE = '{title}'"
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_PROD_VARIANT_FONTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    if row.get('TOTAL') > 0:
                        return True
                    else:
                        return False
            else:
                return False
        except Exception as e:
            print(f"[ERROR] {e}")
            return False
        finally:
            print(f"[INFO] END - Finished verifying if Custom Product Font Variant exists")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, result_flag, result_query
            except:
                pass

    def get_all_custom_prod_and_var(self):
        print(f"\n[INFO] BEGIN - Getting all custom fonts...")
        fonts = []
        font = {}
        columns = ["ROW_ID", "ID", "TITLE", "HAS_VARIANT_FLG", "FONT_TITLE", "FONT_NAME", "VENDOR_ID", "VENDOR_NAME", "STATUS_CD"]
        condition = "1=1"
        return_code = 200
        has_variant_flag = False

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_PROD_FONTS(), columns, condition)

            if result_flag:
                for row in result_query:
                    has_variant_flag = True if row.get('HAS_VARIANT_FLG') else False
                    font = {
                        "id": row.get('ROW_ID'),
                        "product_id": row.get('ID'),
                        "title": row.get('TITLE'),
                        "has_variant_flg": has_variant_flag,
                        "font_title": row.get('FONT_TITLE'),
                        "font_name": row.get('FONT_NAME'),
                        "vendor_id": row.get('VENDOR_ID'),
                        "vendor_name": row.get('VENDOR_NAME'),
                        "status_cd": row.get('STATUS_CD'),
                        "variants": []
                    }

                    if has_variant_flag:
                        columns = ["TITLE", "FONT_NAME"]
                        condition = "1=1"
                        condition += f"\nAND PROD_ID = '{row.get('ID')}'"
                        result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_PROD_VARIANT_FONTS(), columns, condition)

                        if result_flag:
                            for row in result_query:
                                font.get('variants').append({
                                    "title": row.get('TITLE'),
                                    "font_name": row.get('FONT_NAME')
                                })

                    fonts.append(font)
                return True, return_code, fonts
            else:
                return False, 500, result_query
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished getting all custom fonts")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, fonts, font, has_variant_flag
            except:
                pass

    # Analytics functions
    def get_customs_processing_time(self):
        print(f"\n[INFO] BEGIN - Getting customs processing time")
        response_description = "Success"
        columns = [
            "MIN(VENDOR_PROCESSING_TIME) AS MIN_VENDOR_PROCESS_TIME",
            "AVG(VENDOR_PROCESSING_TIME) AS AVG_VENDOR_PROCESS_TIME",
            "MAX(VENDOR_PROCESSING_TIME) AS MAX_VENDOR_PROCESS_TIME",
            "AVG(TOTAL_SHIPPING_TIME) AS AVG_SHIPPING_TIME",
            "MAX(TOTAL_SHIPPING_TIME) AS MAX_SHIPPING_TIME",
            "AVG(TOTAL_PROCESSING_TIME) AS AVG_TOTAL_PROCESSING_TIME",
            "MAX(TOTAL_PROCESSING_TIME) AS MAX_TOTAL_PROCESSING_TIME"
        ]
        condition = "1=1"
        min_vendor_processing_time = 0
        max_vendor_processing_time = 0
        avg_vendor_processing_time = 0
        avg_shipping_time = 0
        max_shipping_time = 0
        avg_total_processing_time = 0
        max_total_processing_time = 0
        result_flag = False
        result_query = None
        processing_analytics = {}

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    min_vendor_processing_time = row.get("MIN_VENDOR_PROCESS_TIME")
                    avg_vendor_processing_time = row.get("AVG_VENDOR_PROCESS_TIME")
                    max_vendor_processing_time = row.get("MAX_VENDOR_PROCESS_TIME")
                    avg_shipping_time = row.get("AVG_SHIPPING_TIME")
                    max_shipping_time = row.get("MAX_SHIPPING_TIME")
                    avg_total_processing_time = row.get("AVG_TOTAL_PROCESSING_TIME")
                    max_total_processing_time = row.get("MAX_TOTAL_PROCESSING_TIME")

                min_vendor_processing_time = round(min_vendor_processing_time, 2) if isinstance(min_vendor_processing_time, Decimal) else min_vendor_processing_time
                avg_vendor_processing_time = round(avg_vendor_processing_time, 2) if isinstance(avg_vendor_processing_time, Decimal) else avg_vendor_processing_time
                max_vendor_processing_time = round(max_vendor_processing_time, 2) if isinstance(max_vendor_processing_time, Decimal) else max_vendor_processing_time
                avg_shipping_time = round(avg_shipping_time, 2) if isinstance(avg_shipping_time, Decimal) else avg_shipping_time
                max_shipping_time = round(max_shipping_time, 2) if isinstance(max_shipping_time, Decimal) else max_shipping_time
                avg_total_processing_time = round(avg_total_processing_time, 2) if isinstance(avg_total_processing_time, Decimal) else avg_total_processing_time
                max_total_processing_time = round(max_total_processing_time, 2) if isinstance(max_total_processing_time, Decimal) else max_total_processing_time

                processing_analytics = {
                    "min_vendor_process_time": min_vendor_processing_time,
                    "avg_vendor_process_time": avg_vendor_processing_time,
                    "max_vendor_process_time": max_vendor_processing_time,
                    "avg_ship_time": avg_shipping_time,
                    "max_ship_time": max_shipping_time,
                    "avg_total_process_time": avg_total_processing_time,
                    "max_total_process_time": max_total_processing_time
                }

                return result_flag, processing_analytics
            else:
                print(f"[ERROR] Error getting customs processing time")
                return False, None
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, None
        finally:
            print(f"[INFO] END - Finished getting customs processing time")
            print("[INFO] Cleaning up variables")
            try:
                del response_description, columns, condition, min_vendor_processing_time, max_vendor_processing_time, avg_vendor_processing_time, avg_total_processing_time, max_total_processing_time, result_flag, result_query, processing_analytics
            except:
                pass

    def count_orders(self, max_date=None):
        print(f"\n[INFO] BEGIN - Counting customs orders fulfilled and not fulfilled")
        response_description = "Success"
        columns = ["COUNT(DISTINCT ID) AS TOTAL_ORDERS", "COUNT(*) AS TOTAL_ITEMS"]
        condition = "1=1"
        condition += f"\nAND CUS.CREATED_AT <= DATE_FORMAT('{max_date}', '%Y-%m-%d')" if max_date is not None else ""
        condition += f"\nAND CUS.CANCELLED_FLG = FALSE"
        # condition += f"\nAND CUS.ID = ("
        # condition += f"\n\tSELECT"
        # condition += f"\n\t\tID"
        # condition += f"\n\tFROM COMPANY_NAME.ORDER ODR"
        # condition += f"\n\tWHERE ODR.ID = CUS.ID"
        # condition += f"\n\tAND ODR.CANCELLED_AT IS NULL"
        # condition += f"\n)"
        count_orders = 0
        count_items = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT() + " CUS", columns, condition)

            if result_flag:
                for row in result_query:
                    count_orders = row.get("TOTAL_ORDERS")
                    count_items = row.get("TOTAL_ITEMS")

            return result_flag, count_orders, count_items
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, count_orders, count_items
        finally:
            print(f"[INFO] END - Finished counting customs orders fulfilled and not fulfilled")
            # print("[INFO] Cleaning up variables")
            try:
                del response_description, columns, condition, count_orders, count_items, result_flag, result_query
            except:
                pass

    def count_fulfilled_and_unfullfilled_orders(self, max_date=None):
        print(f"\n[INFO] BEGIN - Counting customs orders fulfilled and not fulfilled")
        response_description = "Success"
        columns = ["COUNT(DISTINCT ID) AS TOTAL_ORDERS", "COUNT(*) AS TOTAL_ITEMS"]
        condition = "1=1"
        condition += f"\nAND CUS.FULFILLED_DATE <= DATE_FORMAT('{max_date}', '%Y-%m-%d')" if max_date is not None else ""
        condition += f"\nAND CUS.FULFILLED_FLG = TRUE"
        condition += f"\nAND ("
        condition += f"\n\tCUS.FULFILLMENT_STATUS = 'fulfilled'"
        condition += f"\n\tAND CUS.FULFILLMENT_STATUS IS NOT NULL"
        condition += f"\n)"
        condition += f"\nAND CUS.CANCELLED_FLG = FALSE"
        # condition += f"\nAND CUS.ID = ("
        # condition += f"\n\tSELECT"
        # condition += f"\n\t\tID"
        # condition += f"\n\tFROM COMPANY_NAME.ORDER ODR"
        # condition += f"\n\tWHERE ODR.ID = CUS.ID"
        # condition += f"\n\tAND ODR.FULFILLMENT_STATUS = 'fulfilled'"
        # condition += f"\n\tAND ODR.CANCELLED_AT IS NULL"
        # condition += f"\n)"
        count_fulfilled_orders = 0
        count_unfulfilled_orders = 0
        count_fulfilled_items = 0
        count_unfulfilled_items = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT() + " CUS", columns, condition)

            if result_flag:
                for row in result_query:
                    count_fulfilled_orders = row.get("TOTAL_ORDERS")
                    count_fulfilled_items = row.get("TOTAL_ITEMS")

            condition = "1=1"
            condition += f"\nAND CUS.CREATED_AT <= DATE_FORMAT('{max_date}', '%Y-%m-%d')" if max_date is not None else ""
            condition += f"\nAND CUS.FULFILLED_FLG = FALSE"
            condition += f"\nAND ("
            condition += f"\n\tCUS.FULFILLMENT_STATUS <> 'fulfilled'"
            condition += f"\n\tOR CUS.FULFILLMENT_STATUS IS NULL"
            condition += f"\n)"
            condition += f"\nAND CUS.CANCELLED_FLG = FALSE"
            # condition += f"\nAND CUS.ID = ("
            # condition += f"\n\tSELECT"
            # condition += f"\n\t\tID"
            # condition += f"\n\tFROM COMPANY_NAME.ORDER ODR"
            # condition += f"\n\tWHERE ODR.ID = CUS.ID"
            # condition += f"\n\tAND (ODR.FULFILLMENT_STATUS <> 'fulfilled' AND ODR.FULFILLMENT_STATUS IS NOT NULL)"
            # condition += f"\n\tAND ODR.CANCELLED_AT IS NULL"
            # condition += f"\n)"

            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT() + " CUS", columns, condition)

            if result_flag:
                for row in result_query:
                    count_unfulfilled_orders = row.get("TOTAL_ORDERS")
                    count_unfulfilled_items = row.get("TOTAL_ITEMS")

            return result_flag, count_fulfilled_orders, count_unfulfilled_orders, count_fulfilled_items, count_unfulfilled_items
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, count_fulfilled_orders, count_unfulfilled_orders, count_fulfilled_items, count_unfulfilled_items
        finally:
            print(f"[INFO] END - Finished counting customs orders fulfilled and not fulfilled")
            # print("[INFO] Cleaning up variables")
            try:
                del response_description, columns, condition, count_fulfilled_orders, count_unfulfilled_orders, count_fulfilled_items, count_unfulfilled_items, result_flag, result_query
            except:
                pass

    def count_customs_shipped_to_vendor_not_sent_back(self, max_date=None):
        print(f"\n[INFO] BEGIN - Counting customs orders shipped to Vendor and not sent back yet")
        columns = ["COUNT(*) AS TOTAL_ITEMS", "COUNT(DISTINCT ID) AS TOTAL_ORDERS"]
        condition = "1=1"
        condition += f"\nAND CUS.SENT_TO_VENDOR_DATE IS NOT NULL"
        condition += f"\nAND CUS.SENT_TO_VENDOR_DATE <= DATE_FORMAT('{max_date}', '%Y-%m-%d')" if max_date is not None else ""
        condition += f"\nAND CUS.SENT_BY_VENDOR_FLG = FALSE"
        condition += f"\nAND CUS.RECEIVED_FLG = FALSE"
        condition += f"\nAND CUS.FULFILLED_FLG = FALSE"
        condition += f"\nAND CUS.FULFILLMENT_STATUS IS NULL"
        condition += f"\nAND CUS.CANCELLED_FLG = FALSE"
        # condition += f"\nAND CUS.ID IN ("
        # condition += f"\n\tSELECT"
        # condition += f"\n\t\tID"
        # condition += f"\n\tFROM COMPANY_NAME.ORDER ODR"
        # condition += f"\n\tWHERE ODR.ID = CUS.ID"
        # condition += f"\n\tAND ODR.CANCELLED_AT IS NULL"
        # condition += f"\n\tAND ODR.CLOSED_AT IS NULL"
        # condition += f"\n)"
        count_orders = 0
        count_items = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT() + " CUS", columns, condition)

            if result_flag:
                for row in result_query:
                    count_orders = row.get("TOTAL_ORDERS")
                    count_items = row.get("TOTAL_ITEMS")

            return result_flag, count_orders, count_items
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, count_orders, count_items
        finally:
            print(f"[INFO] END - Finished counting customs orders shipped to Vendor and not sent back yet")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, count_orders, count_items, result_flag, result_query
            except:
                pass

    def count_customs_in_transit(self, max_date=None):
        print(f"\n[INFO] BEGIN - Counting customs orders in transit")
        columns = ["COUNT(*) AS TOTAL_ITEMS", "COUNT(DISTINCT ID) AS TOTAL_ORDERS"]
        condition = "1=1"
        condition += f"\nAND SENT_TO_VENDOR_DATE IS NOT NULL"
        condition += f"\nAND SENT_BY_VENDOR_FLG = TRUE"
        condition += f"\nAND SHIPPED_DATE IS NOT NULL"
        condition += f"\nAND SHIPPED_DATE <= DATE_FORMAT('{max_date}', '%Y-%m-%d')" if max_date is not None else ""
        condition += f"\nAND RECEIVED_FLG = FALSE"
        condition += f"\nAND FULFILLED_FLG = FALSE"
        condition += f"\nAND FULFILLMENT_STATUS IS NULL"
        condition += f"\nAND CANCELLED_FLG = FALSE"
        count_orders = 0
        count_items = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    count_orders = row.get("TOTAL_ORDERS")
                    count_items = row.get("TOTAL_ITEMS")

            return result_flag, count_orders, count_items
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, count_orders, count_items
        finally:
            print(f"[INFO] END - Finished counting customs orders shipped to Vendor and not sent back yet")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, count_orders, count_items, result_flag, result_query
            except:
                pass

    def count_customs_received_back(self, max_date=None):
        print(f"\n[INFO] BEGIN - Counting customs orders received back")
        columns = ["COUNT(*) AS TOTAL_ITEMS", "COUNT(DISTINCT ID) AS TOTAL_ORDERS"]
        condition = "1=1"
        condition += f"\nAND CUS.SENT_TO_VENDOR_DATE IS NOT NULL"
        condition += f"\nAND CUS.SENT_BY_VENDOR_FLG = TRUE"
        condition += f"\nAND CUS.SHIPPED_DATE IS NOT NULL"
        condition += f"\nAND CUS.RECEIVED_FLG = TRUE"
        condition += f"\nAND CUS.RECEIVED_DATE <= DATE_FORMAT('{max_date}', '%Y-%m-%d')" if max_date is not None else ""
        condition += f"\nAND CUS.FULFILLED_FLG = FALSE"
        condition += f"\nAND CUS.FULFILLMENT_STATUS IS NULL"
        condition += f"\nAND CUS.CANCELLED_FLG = FALSE"
        # condition += f"\nAND CUS.ID IN ("
        # condition += f"\n\tSELECT"
        # condition += f"\n\t\tID"
        # condition += f"\n\tFROM COMPANY_NAME.ORDER ODR"
        # condition += f"\n\tWHERE ODR.ID = CUS.ID"
        # condition += f"\n\tAND ODR.CANCELLED_AT IS NULL"
        # condition += f"\n\tAND ODR.CLOSED_AT IS NULL"
        # condition += f"\n)"
        count_orders = 0
        count_items = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT() + " CUS", columns, condition)

            if result_flag:
                for row in result_query:
                    count_orders = row.get("TOTAL_ORDERS")
                    count_items = row.get("TOTAL_ITEMS")

            return result_flag, count_orders, count_items
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, count_orders, count_items
        finally:
            print(f"[INFO] END - Finished counting customs orders shipped to Vendor and not sent back yet")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, count_orders, count_items, result_flag, result_query
            except:
                pass

    def count_customs_sorority_orders(self, max_date=None):
        print(f"\n[INFO] BEGIN - Counting customs sorority and non-sorority orders")
        columns = ["COUNT(*) AS TOTAL_ITEMS", "COUNT(DISTINCT ID) AS TOTAL_ORDERS"]
        condition = "1=1"
        condition += f"\nAND SORORITY_FLG = TRUE"
        condition += f"\nAND CREATED_AT <= DATE_FORMAT('{max_date}', '%Y-%m-%d')" if max_date is not None else ""
        condition += f"\nAND CANCELLED_FLG = FALSE"
        count_sorority_items = 0
        count_sorority_orders = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    count_sorority_items = row.get("TOTAL_ITEMS")
                    count_sorority_orders = row.get("TOTAL_ORDERS")

            return result_flag, count_sorority_orders, count_sorority_items,
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, count_sorority_orders, count_sorority_items
        finally:
            print(f"[INFO] END - Finished counting customs sorority and non-sorority orders")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, count_sorority_items, count_sorority_orders, result_flag, result_query
            except:
                pass

    def count_customs_normal_orders(self, max_date=None):
        print(f"\n[INFO] BEGIN - Counting customs sorority and non-sorority orders")
        columns = ["COUNT(*) AS TOTAL_ITEMS", "COUNT(DISTINCT ID) AS TOTAL_ORDERS"]
        condition = "1=1"
        condition += f"\nAND SORORITY_FLG = FALSE"
        condition += f"\nAND CREATED_AT <= DATE_FORMAT('{max_date}', '%Y-%m-%d')" if max_date is not None else ""
        condition += f"\nAND CANCELLED_FLG = FALSE"
        count_non_sorority_orders = 0
        count_non_sorority_items = 0
        result_flag = False
        result_query = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    count_non_sorority_orders = row.get("TOTAL_ORDERS")
                    count_non_sorority_items = row.get("TOTAL_ITEMS")

            return result_flag, count_non_sorority_orders, count_non_sorority_items
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, count_non_sorority_orders, count_non_sorority_items
        finally:
            print(f"[INFO] END - Finished counting customs sorority and non-sorority orders")
            # print("[INFO] Cleaning up variables")
            try:
                del columns, condition, count_non_sorority_orders, count_non_sorority_items, result_flag, result_query
            except:
                pass

    def count_orders_per_month(self):
        print(f"\n[INFO] BEGIN - Counting customs orders per month")
        response_description = "Success"
        columns = ["DATE_FORMAT(CREATED_AT, '%Y-%m') AS YEAR_MONTH_DATE", "COUNT(DISTINCT ID) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND CANCELLED_FLG = FALSE"
        condition += f"\nGROUP BY YEAR_MONTH_DATE"
        condition += f"\nORDER BY YEAR_MONTH_DATE ASC"
        condition += f"\nLIMIT 12"
        result_flag = False
        result_query = None
        orders_per_month = {}
        year_month = None
        total = 0

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    year_month = row.get("YEAR_MONTH_DATE")
                    total = row.get("TOTAL")
                    orders_per_month[year_month] = total
                    print(f"[INFO] {year_month} - {total} orders")

            return result_flag, orders_per_month
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, orders_per_month
        finally:
            print(f"[INFO] END - Finished counting customs orders per month")
            try:
                del response_description, columns, condition, result_flag, result_query, orders_per_month, year_month, total
            except:
                pass

    def count_orders_items_per_day(self):
        print(f"\n[INFO] BEGIN - Counting customs orders per month")
        response_description = "Success"
        columns = ["DATE_FORMAT(CREATED_AT, '%Y-%m-%d') AS YEAR_MONTH_DAY_DATE", "COUNT(DISTINCT ID) AS TOTAL_ORDERS", "COUNT(*) AS TOTAL_ITEMS"]
        condition = "1=1"
        condition += f"\nAND CANCELLED_FLG = FALSE"
        condition += f"\nAND DATE(CREATED_AT) >= CURRENT_DATE - INTERVAL 30 DAY"
        condition += f"\nGROUP BY YEAR_MONTH_DAY_DATE"
        condition += f"\nORDER BY YEAR_MONTH_DAY_DATE ASC"
        result_flag = False
        result_query = None
        orders_items_per_day = {}
        year_month = None
        total_orders = 0
        total_items = 0

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    year_month = row.get("YEAR_MONTH_DAY_DATE")
                    total_orders = row.get("TOTAL_ORDERS")
                    total_items = row.get("TOTAL_ITEMS")
                    orders_items_per_day[year_month] = {
                        "orders": total_orders,
                        "items": total_items
                    }

            return result_flag, orders_items_per_day
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, orders_items_per_day
        finally:
            print(f"[INFO] END - Finished counting customs orders per month")
            try:
                del response_description, columns, condition, result_flag, result_query, orders_items_per_day, year_month, total_orders
            except:
                pass

    def count_fulfilled_orders_per_month(self):
        print(f"\n[INFO] BEGIN - Counting customs orders per month")
        response_description = "Success"
        columns = ["DATE_FORMAT(CREATED_AT, '%Y-%m') AS YEAR_MONTH_DATE", "COUNT(DISTINCT ID) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND FULFILLED_FLG = TRUE"
        condition += f"\nAND FULFILLED_DATE IS NOT NULL"
        condition += f"\nAND FULFILLMENT_STATUS = 'fulfilled'"
        condition += f"\nAND CANCELLED_FLG = FALSE"
        condition += f"\nGROUP BY YEAR_MONTH_DATE"
        condition += f"\nORDER BY YEAR_MONTH_DATE ASC"
        condition += f"\nLIMIT 12"
        result_flag = False
        result_query = None
        fulfilled_orders_per_month = {}
        year_month = None
        total = 0

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    year_month = row.get("YEAR_MONTH_DATE")
                    total = row.get("TOTAL")
                    fulfilled_orders_per_month[year_month] = total
                    print(f"[INFO] {year_month} - {total} orders")

            return result_flag, fulfilled_orders_per_month
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, fulfilled_orders_per_month
        finally:
            print(f"[INFO] END - Finished counting customs orders per month")
            try:
                del response_description, columns, condition, result_flag, result_query, fulfilled_orders_per_month, year_month, total
            except:
                pass

    def count_fulfilled_orders_per_day(self):
        print(f"\n[INFO] BEGIN - Counting customs orders per month")
        response_description = "Success"
        columns = ["DATE_FORMAT(FULFILLED_DATE, '%Y-%m-%d') AS YEAR_MONTH_DAY_DATE", "COUNT(DISTINCT ID) AS TOTAL_ORDERS_FULFILLED"]
        condition = "1=1"
        condition += f"\nAND FULFILLED_FLG = TRUE"
        condition += f"\nAND FULFILLED_DATE IS NOT NULL"
        condition += f"\nAND DATE(FULFILLED_DATE) >= CURRENT_DATE - INTERVAL 30 DAY"
        condition += f"\nAND FULFILLMENT_STATUS = 'fulfilled'"
        condition += f"\nAND CANCELLED_FLG = FALSE"
        condition += f"\nGROUP BY YEAR_MONTH_DAY_DATE"
        condition += f"\nORDER BY YEAR_MONTH_DAY_DATE ASC"
        result_flag = False
        result_query = None
        fulfilled_orders_per_day = {}
        year_month = None
        total = 0

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    year_month = row.get("YEAR_MONTH_DAY_DATE")
                    total = row.get("TOTAL_ORDERS_FULFILLED")
                    fulfilled_orders_per_day[year_month] = total
                    print(f"[INFO] {year_month} - {total} orders")

            return result_flag, fulfilled_orders_per_day
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, fulfilled_orders_per_day
        finally:
            print(f"[INFO] END - Finished counting customs orders per month")
            try:
                del response_description, columns, condition, result_flag, result_query, fulfilled_orders_per_day, year_month, total
            except:
                pass

    def count_unfulfilled_orders_per_month(self):
        print(f"\n[INFO] BEGIN - Counting unfulfilled customs orders per month")
        response_description = "Success"
        columns = ["DATE_FORMAT(CREATED_AT, '%Y-%m') AS YEAR_MONTH_DATE", "COUNT(DISTINCT ID) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND FULFILLED_FLG = FALSE"
        condition += f"\nAND FULFILLED_DATE IS NULL"
        condition += f"\nAND FULFILLMENT_STATUS IS NULL"
        condition += f"\nAND CANCELLED_FLG = FALSE"
        condition += f"\nGROUP BY YEAR_MONTH_DATE"
        condition += f"\nORDER BY YEAR_MONTH_DATE ASC"
        condition += f"\nLIMIT 12"
        result_flag = False
        result_query = None
        fulfilled_orders_per_month = {}
        year_month = None
        total = 0

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                for row in result_query:
                    year_month = row.get("YEAR_MONTH_DATE")
                    total = row.get("TOTAL")
                    fulfilled_orders_per_month[year_month] = total
                    print(f"[INFO] {year_month} - {total} orders")

            return result_flag, fulfilled_orders_per_month
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, fulfilled_orders_per_month
        finally:
            print(f"[INFO] END - Finished counting unfulfilled customs orders per month")
            try:
                del response_description, columns, condition, result_flag, result_query, fulfilled_orders_per_month, year_month, total
            except:
                pass

    def extract_report_data(self, report_type, report_date):
        print(f"\n[INFO] BEGIN - Extracting customs report data for {report_type} on {report_date}")
        try:
            result_flag_reports, result_query_reports, return_description = self.reports.get_reports(report_type=report_type, report_date=report_date)

            if not result_flag_reports:
                print(f"[ERROR] Error getting customs reports")
                return False, 500, return_description, None

            report_id = [report.get('ROW_ID') for report in result_query_reports]

            if not report_id:
                print(f"[ERROR] Error getting customs reports")
                return False, 500, return_description, None

            report_id = report_id[0]
            print(f"[INFO] Got Report ID: {report_id}")
            print(f"[INFO] Getting reports data")
            result_flag_data, result_query_data, return_description = self.reports.get_reports_data(report_id=report_id, columns=["REPORT_TYPE", "REPORT_VALUE"])

            if not result_flag_data:
                print(f"[ERROR] Error getting customs reports data")
                return False, 500, return_description, None

            data = {str(row.get("REPORT_TYPE")).lower(): row.get("REPORT_VALUE") for row in result_query_data}
            # print(f"[INFO] Got Reports Data")
            # print(f"[INFO] Data: {data}")
            return True, 200, None, data
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, return_description
        finally:
            print(f"[INFO] END - Finished extracting customs report data for {report_type} on {report_date}")
            try:
                del result_flag_reports, result_query_reports, result_flag_data, result_query_data, report_id, report_type, report_value, columns_counter
            except:
                pass

    # Routine every minute
    def update_customs_reports(self):
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
        processing_analytics = {}
        orders_per_month = {}
        fulfilled_orders_per_month = {}
        unfulfilled_orders_per_month = {}
        performance_per_month = []
        orders_items_per_day = {}
        fulfilled_orders_per_day = {}
        performance_per_day = []
        total_orders_items_set = None
        performance_history = {
            "per_month": [],
            "per_day": []
        }
        reports = {
            "count_total_orders": 0,
            "count_total_items": 0,
            "count_orders_sent_to_vendor_not_sent_back": 0,
            "count_items_sent_to_vendor_not_sent_back": 0,
            "count_orders_in_transit": 0,
            "count_items_in_transit": 0,
            "count_orders_received_back": 0,
            "count_items_received_back": 0,
            "count_sorority_orders": 0,
            "count_sorority_items": 0,
            "count_non_sorority_orders": 0,
            "count_non_sorority_items": 0,
            "count_fulfilled": 0,
            "count_unfulfilled": 0,
            "count_fulfilled_items": 0,
            "count_unfulfilled_items": 0,
            "performance_history_json": {}
        }

        try:
            print(f"[INFO] Getting customs reports info...")
            result_flag, processing_analytics = self.get_customs_processing_time()
            result_flag, count_total_orders, count_total_items = self.count_orders()
            result_flag, count_fulfilled, count_unfulfilled, count_fulfilled_items, count_unfulfilled_items = self.count_fulfilled_and_unfullfilled_orders()
            result_flag, count_orders_sent_to_vendor_not_sent_back, count_items_sent_to_vendor_not_sent_back = self.count_customs_shipped_to_vendor_not_sent_back()
            result_flag, count_orders_in_transit, count_items_in_transit = self.count_customs_in_transit()
            result_flag, count_orders_received_back, count_items_received_back = self.count_customs_received_back()
            result_flag, count_sorority_orders, count_sorority_items = self.count_customs_sorority_orders()
            result_flag, count_non_sorority_orders, count_non_sorority_items = self.count_customs_normal_orders()
            result_flag, orders_per_month = self.count_orders_per_month()
            result_flag, fulfilled_orders_per_month = self.count_fulfilled_orders_per_month()
            result_flag, unfulfilled_orders_per_month = self.count_unfulfilled_orders_per_month()
            result_flag, orders_items_per_day = self.count_orders_items_per_day()
            result_flag, fulfilled_orders_per_day = self.count_fulfilled_orders_per_day()

            print(f"\n[INFO] Sorting and merging days for analytics")
            all_days = sorted(set(orders_items_per_day.keys()).union(fulfilled_orders_per_day.keys()))
            print(f"[INFO] Sorting and merging months for analytics")
            all_months = sorted(set(orders_per_month.keys()).union(fulfilled_orders_per_month.keys()).union(unfulfilled_orders_per_month.keys()))

            print(f"\n[INFO] Getting daily performance history")
            for day in all_days:
                try:
                    total_orders_items_set = orders_items_per_day.get(day, set())
                    total_orders = total_orders_items_set.get("orders", 0)
                    total_items = total_orders_items_set.get("items", 0)
                except:
                    total_orders = 0
                    total_items = 0
                fulfilled_orders = fulfilled_orders_per_day.get(day, 0)

                print(f"[INFO] Day: {day} - Total Orders {total_orders} - Total Items: {total_items} - Total Fulfilled Orders: {fulfilled_orders}")
                performance_per_day.append(
                    {
                        "REPORT_DATE": day,
                        "TOTAL_ORDERS": total_orders,
                        "TOTAL_ITEMS": total_items,
                        "TOTAL_FULFILLED_ORDERS": fulfilled_orders
                    }
                )

            print(f"\n[INFO] Getting monthly performance history")
            for month in all_months:
                total_orders = orders_per_month.get(month, 0)
                fulfilled_orders = fulfilled_orders_per_month.get(month, 0)
                unfulfilled_orders = unfulfilled_orders_per_month.get(month, 0)

                print(f"[INFO] Month: {month} - Total Orders {total_orders} - Total Fulfilled Orders: {fulfilled_orders} - Total Unfulfilled Orders: {unfulfilled_orders}")
                performance_per_month.append(
                    {
                        "REPORT_DATE": month,
                        "TOTAL_ORDERS": total_orders,
                        "TOTAL_FULFILLED_ORDERS": fulfilled_orders,
                        "TOTAL_UNFULFILLED_ORDERS": unfulfilled_orders
                    }
                )

            performance_history["per_month"] = performance_per_month
            performance_history["per_day"] = performance_per_day

            reports["count_total_orders"] = count_total_orders
            reports["count_total_items"] = count_total_items
            reports["count_orders_sent_to_vendor_not_sent_back"] = count_orders_sent_to_vendor_not_sent_back
            reports["count_items_sent_to_vendor_not_sent_back"] = count_items_sent_to_vendor_not_sent_back
            reports["count_orders_in_transit"] = count_orders_in_transit
            reports["count_items_in_transit"] = count_items_in_transit
            reports["count_orders_received_back"] = count_orders_received_back
            reports["count_items_received_back"] = count_items_received_back
            reports["count_sorority_orders"] = count_sorority_orders
            reports["count_sorority_items"] = count_sorority_items
            reports["count_non_sorority_orders"] = count_non_sorority_orders
            reports["count_non_sorority_items"] = count_non_sorority_items
            reports["count_fulfilled"] = count_fulfilled
            reports["count_unfulfilled"] = count_unfulfilled
            reports["count_fulfilled_items"] = count_fulfilled_items
            reports["count_unfulfilled_items"] = count_unfulfilled_items
            reports["performance_history_json"] = performance_history

            reports_management_exists, report_id, return_message = self.reports.verify_reports_exists("CUSTOMS_REPORTS", today_date)

            if not reports_management_exists:
                reports_management = self.reports.ReportsManagement()
                reports_management.set_report_type("CUSTOMS_REPORTS")
                reports_management.set_report_name("CUSTOMS_DASHBOARD_REPORTS")
                reports_management.set_report_description(f"Customs reports for the dashboard up to {today_date}")

                reports_management_exists, report_id, return_message = self.reports.insert_reports(ReportsManagement=reports_management, date=today)

            if reports_management_exists:
                for column, value in processing_analytics.items():
                    reports_data = self.reports.ReportsData()
                    reports_data.set_report_id(report_id)
                    reports_data.set_report_type(column.upper())
                    reports_data.set_report_value(str(value))

                    reports_data_exists, reports_data_id, return_message = self.reports.upsert_reports_data(ReportsData=reports_data)

                    if reports_data_exists:
                        reports_data_id_array.append(reports_data_id)
                    else:
                        print(f"[ERROR] {return_message}")

                for column, value in reports.items():
                    reports_data = self.reports.ReportsData()
                    reports_data.set_report_id(report_id)
                    reports_data.set_report_type(column.upper())
                    if "json" not in column:
                        reports_data.set_report_value(str(value))
                    else:
                        reports_data.set_report_value_json(self.utils.convert_object_to_json(value))

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
            print(f"[INFO] Cleaning up variables")
            try:
                del today, reports_management_exists, reports_data_exists, report_id, reports_data_id, reports_data_id_array, return_message, reports_management, reports_data, result_flag, processing_analytics, reports
            except:
                pass
