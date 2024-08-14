"""
Module: RoutineController

This module provides the `RoutineController` class, which extends the `DataController` class and 
orchestrates various routine tasks essential to the daily operations of an e-commerce platform. It 
integrates with multiple controllers to handle orders, products, customs processes, and interactions
with external APIs like Canada Post and Shopify.

Classes:
    - RoutineController: A controller class responsible for managing and automating daily e-commerce
      routines such as processing orders, updating currency rates, generating reports, and handling 
      custom product orders.

Methods:
    - __init__: Initializes the RoutineController with instances of various controllers for orders, 
      products, customs, phone cases, utilities, logs, and external APIs.
    - Getters and Setters: Provide access to and modification of internal attributes, such as order 
      lists, SKUs, and timestamps.
    - Appends: Methods for adding data to lists storing different categories of orders, processed 
      data, and SKUs.
    - update_currency_rates: Fetches the latest currency rates using the CurrencyFreaks API and 
      stores them in the database.
    - process_canada_post_manifests: Manages the closing and processing of Canada Post manifests, 
      including downloading artifacts and sending email notifications.
    - daily_routine_5am: A scheduled routine that updates currency rates every day at 5 AM.
    - temp_get_all_orders: Retrieves all orders from Shopify since a specified ID, filtering for 
      custom orders.
    - temp_get_product_variant: Fetches details of a specific product variant from Shopify.
    - send_processed_customs_orders_emails: Sends emails containing processed custom orders, 
      including attached PDF and sheet files.
    - generate_customs_report: Prepares and generates a customs report, handling file paths and 
      custom fonts.
    - process_custom_orders: Processes custom orders daily at 7 PM and generates related reports.
    - update_reports: Updates reports related to customs and phone cases on a minute-by-minute 
      basis.

Usage:
    The `RoutineController` is designed to be used in an e-commerce environment where automation of
    daily tasks is necessary. It interacts with various components of the system to ensure smooth 
    operation and timely processing of orders, reports, and external API interactions.
"""

from utils.UtilsController import *
from utils.LogsController import *
from database.DataController import *
from order.OrderController import *
from product.ProductController import *
from product.CustomsProductController import *
from product.PhoneCasesController import *
from apis.inbound.CanadaPostController import *
from apis.inbound.CurrencyFreaksController import *
from apis.inbound.ShopifyController import *

class RoutineController(DataController):
    """
    Class: RoutineController

    Purpose:
        The `RoutineController` class is responsible for automating and managing daily routines 
        within an e-commerce platform. It handles tasks like updating currency rates, processing
        orders, and interacting with external APIs. The class inherits from `DataController`, 
        allowing it to perform database operations related to these routines.

    Attributes:
        - module_name: Name of the module for logging and identification purposes.
        - order: Instance of `OrderController` for managing order-related operations.
        - prod: Instance of `ProductController` for handling product data.
        - customs: Instance of `CustomsProductController` for managing custom orders.
        - phone_cases: Instance of `PhoneCasesController` for phone case-specific operations.
        - utils: Utility functions for various general-purpose tasks.
        - logs: Logging functionality for tracking operations and errors.
        - canada_post: Instance of `CanadaPostController` for interacting with Canada Post API.
        - currency: Instance of `CurrencyFreaksController` for currency rate updates.
        - shopify: Instance of `ShopifyController` for Shopify API interactions.
    """
    def __init__(self):
        self.module_name = "RoutinesController"
        super().__init__()
        self.order = OrderController()
        self.prod = ProductController()
        self.customs = CustomsProductController()
        self.phone_cases = PhoneCasesController()
        self.utils = UtilsController()
        self.logs = LogsController()
        self.canada_post = CanadaPostController()
        self.currency = CurrencyFreaksController()
        self.shopify = ShopifyController()
        self.normal_orders = []
        self.single_orders = []
        self.subscription_boxes_orders = []
        self.priority_orders = []
        self.phone_case_orders = []
        self.processed_chunks = []
        self.missing_items_skus = []
        self.avaiable_skus = []
        self.start_time = None
        self.end_time = None

    # Getters
    def get_module_name(self):
        return self.module_name

    def get_normal_orders(self):
        return self.normal_orders

    def get_single_orders(self):
        return self.single_orders

    def get_subscription_boxes_orders(self):
        return self.subscription_boxes_orders

    def get_priority_orders(self):
        return self.priority_orders

    def get_phone_case_orders(self):
        return self.phone_case_orders

    def get_processed_chunks(self):
        return self.processed_chunks

    def get_missing_items_skus(self):
        return self.missing_items_skus

    def get_avaiable_skus(self):
        return self.avaiable_skus

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    # Setters
    def set_start_time(self, value):
        self.start_time = value

    def set_end_time(self, value):
        self.end_time = value

    # Appends
    def append_normal_orders(self, value):
        self.normal_orders.append(value)

    def append_single_orders(self, value):
        self.single_orders.append(value)

    def append_subscription_boxes_orders(self, value):
        self.subscription_boxes_orders.append(value)

    def append_priority_orders(self, value):
        self.priority_orders.append(value)

    def append_phone_case_orders(self, value):
        self.phone_case_orders.append(value)

    def append_processed_chunks(self, value):
        self.processed_chunks.append(value)

    def append_missing_items_skus(self, value):
        self.missing_items_skus.append(value)

    def append_avaiable_skus(self, value):
        self.avaiable_skus.append(value)

    # functions
    def update_currency_rates(self):
        """
        Method: update_currency_rates

        Purpose:
            Fetches the latest currency exchange rates from the CurrencyFreaks API and inserts them 
            into the database. The method also handles error cases and logs any issues that occur 
            during the process.

        Returns:
            tuple: A tuple containing a success flag (bool), the number of rows inserted (int), a 
            return message (str), a response description (str), an error code (int), and an error 
            message (str).

        Raises:
            Exception: If an error occurs during the API request or database insertion, it is 
            caught, logged, and re-raised as a tuple return.
        """
        # Variables
        return_flag = False
        base_currency = None
        date_rates = None
        rates = None
        response_description = None
        error_code = None
        error_message = None
        columns = ['BASE_CURRENCY', 'DATE_RATE', 'RATES']
        values = []
        rowcount = 0
        return_string = None

        try:
            # Fetch currency rates from the CurrencyFreaks API
            return_flag, base_currency, date_rates, rates, response_description, error_code, error_message = self.currency.get_currencyfreaks_lastest_rates()

            # Check if the API call was successful
            if not return_flag:
                return return_flag, rowcount, return_string, response_description, error_code, error_message
            else:
                # Prepare data for insertion into the database
                values.append(base_currency)
                values.append(date_rates)
                values.append(rates)
                return_flag, rowcount, return_string = super().insert_record(super().get_tbl_DAILY_CURRENCY(), columns, values)

                return return_flag, rowcount, return_string, response_description, error_code, error_message
        except Exception as e:
            response_description = str(e)
            return False, rowcount, return_string, response_description, error_code, error_message
        finally:
            print("[INFO] - Cleaning up variables")
            try:
                del return_flag
                del base_currency
                del date_rates
                del rates
                del response_description
                del error_code
                del error_message
                del columns
                del values
                del rowcount
                del return_string
            except Exception as e:
                print(f'[ERROR] - {e}')
                pass

    def process_canada_post_manifests(self):
        # Variables
        logs = ""
        return_flag = False
        manifest_link = None
        logs_response = None
        response_description = None
        status_code = None
        artifact_link = None
        error_message = None
        response = None
        email_to = ['xxxxxxxx@COMPANY_NAME.com', 'xxxxxxxx@COMPANY_NAME.com', 'xxxxxxxx@COMPANY_NAME.com', 'xxxxxxxx@COMPANY_NAME.com']
        email_subject = f"[MANIFEST] - Canada Post Manifest - {self.utils.get_current_date()}"
        email_body = f"Follow the attached manifest and logs for the Canada Post Manifest of {self.utils.get_current_date()}"
        base_dir = self.utils.get_base_directory()
        # print(f'[INFO] - Base dir: {base_dir}')
        file_path = os.path.join(base_dir, '/files/pdfs/manifests/')
        # print(f'[INFO] - File path: {file_path}')
        file_names = [f'{self.utils.get_current_date()}.pdf', f'{self.utils.get_current_date()}.txt']

        try:
            return_flag, manifest_link, logs_response, response_description, status_code = self.canada_post.post_canadapost_close_manifests()

            if not return_flag or manifest_link is None:
                return return_flag, response_description, status_code

            logs += logs_response
            print(f'[INFO] - Manifest link: {manifest_link}, waiting 40 seconds before getting artifact link')
            time.sleep(40)

            return_flag, artifact_link, response, logs_response, response_description, status_code, error_message = self.canada_post.get_canadapost_artifact_link(manifest_link=manifest_link)
            if not return_flag or artifact_link is None:
                return return_flag, response_description, status_code

            logs += logs_response
            return_flag, response, response_description, error_message, status_code = self.canada_post.get_canadapost_download_manifest_pdf(self, artifact_link=artifact_link)
            if not return_flag:
                return return_flag, response_description, status_code

            self.utils.send_email(email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=file_path, file_names=file_names)
            print(f'[INFO] - Process finished')
            return True, "Process finished", 200
        except Exception as e:
            print(f'[ERROR] - {e}')
            return False, f"{e}", 500
        finally:
            print("[INFO] Cleaning up variables")
            try:
                del logs
                del return_flag
                del manifest_link
                del logs_response
                del response_description
                del status_code
                del artifact_link
                del error_message
                del response
                del email_to
                del email_subject
                del email_body
                del base_dir
                del file_path
                del file_names
            except Exception as e:
                print(f'[ERROR] - {e}')
                pass

    # daily_routine_5am
    def daily_routine_5am(self):
        # Variables
        return_flag = False
        rowcount = 0
        result_query = None
        response_description = None
        error_code = None
        error_message = None
        yesterday = self.utils.get_yesterday_date()

        return_flag, rowcount, result_query, response_description, error_code, error_message = self.update_currency_rates()

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

    def send_processed_customs_orders_emails(self, sheet_file_path, pdf_file_path, sheet_file_names, pdf_file_names, orders_numbers_without_custom_text, type="normal"):
        today = self.utils.get_current_date()
        email_subject = "[{}CUSTOM PO {}] - {}" + f"Custom Orders up to {today}"
        email_from = "COMPANY_NAME1@gmail.com"
        email_to = ['xxxxxxxx@COMPANY_NAME.com'] if self.utils.get_DEBUG_MODE() == "FALSE" else ['xxxxxxxx@COMPANY_NAME.com']
        email_body = None

        try:
            if type == "rush":
                print(f"\n[INFO] Sending RUSH Custom Orders PDFS to Lee")
                email_subject = email_subject.format("RUSH ", "PDFS", "rush ")
                email_body = f"Follow the RUSH custom orders up to {today} in the attached files."
                try:
                    self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=pdf_file_path, file_names=pdf_file_names)
                except Exception as e:
                    self.utils.send_exception_email(module=self.get_module_name(), function="send_processed_customs_orders_emails", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                    print(f"[ERROR] - {e}")
                    pass

                print(f"\n[INFO] Sending RUSH Custom Orders SHEET to Lee")
                email_subject = email_subject.format("RUSH ", "SHEET", "rush ")
                try:
                    self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=sheet_file_path, file_names=sheet_file_names)
                except Exception as e:
                    self.utils.send_exception_email(module=self.get_module_name(), function="send_processed_customs_orders_emails", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                    print(f"[ERROR] - {e}")
                    pass
            else:
                print(f"\n[INFO] Sending NORMAL Custom Orders PDFS to Lee")
                email_subject = email_subject.format("", "PDFS", "")
                email_body = f"Follow the custom orders up to {today} in the attached files."
                self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=pdf_file_path, file_names=pdf_file_names)

                print(f"\n[INFO] Sending NORMAL Custom Orders SHEET to Lee")
                email_subject = email_subject.format("", "SHEET", "")
                try:
                    self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=sheet_file_path, file_names=sheet_file_names)
                except Exception as e:
                    print(f"[ERROR] - {e}")
                    pass

            if len(orders_numbers_without_custom_text) > 0:
                email_subject = f"[ORDERS WITHOUT CUSTOM TEXT] - Custom Orders up to {today}"
                email_body = f"Orders without custom text: {orders_numbers_without_custom_text}"
                email_to = ['matheus@COMPANY_NAME.com', 'customs@COMPANY_NAME.com', 'victor@COMPANY_NAME.com']
                try:
                    self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=None, file_names=None)
                except Exception as e:
                    self.utils.send_exception_email(module=self.get_module_name(), function="send_processed_customs_orders_emails", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                    print(f"[ERROR] - {e}")
                    pass
            return True
        except Exception as e:
            self.utils.send_exception_email(module=self.get_module_name(), function="send_processed_customs_orders_emails", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
            print(f"[ERROR] - {e}")
            return False
        finally:
            try:
                del today, email_subject, email_from, email_to, email_body, sheet_file_path, pdf_file_path, sheet_file_names, pdf_file_names, orders_numbers_without_custom_text
            except:
                pass

    def generate_customs_report(self):
        print(f"\n[INFO] BEGIN - Generating Customs Report")
        self.set_start_time(self.utils.get_current_date_time())
        sheet_file_path = self.utils.get_base_directory() + "/files/customs/sheets/"
        pdf_file_path = self.utils.get_base_directory() + "/files/customs/pdfs/"
        two_hours_ago = self.utils.get_x_hours_ago(2)
        today = self.utils.get_current_date()
        pdfmetrics = self.utils.register_custom_fonts()

    # daily at 7PM
    def process_custom_orders(self):
        self.customs.process_custom_orders()
        self.customs.daily_customs_reports()

    # daily every minute
    def update_reports(self):
        self.customs.update_customs_reports()
        self.phone_cases.update_phone_case_reports()