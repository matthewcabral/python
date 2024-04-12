from database.DataController import *
from utils.UtilsController import *
from apis.inbound.ShopifyController import *
from utils.LogsController import *

class ProductController(DataController):
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.utils = UtilsController()
        self.shopify = ShopifyController()
        self.log = LogsController()
        self._products_array = []
        self._products_with_error_array = []
        self._product_count = 0
        self._total_product_count = 0
        self._total_product_inserted_count = 0
        self._total_product_updated_count = 0
        self._total_product_deleted_count = 0
        self._total_product_with_errors = 0
        self.module_name = "ProductController"

    class Product():

        def __init__(self):
            self.body_html = None
            self.created_at = None
            self.handle = None
            self.id = None
            self.options = None
            self.product_type = None
            self.published_at = None
            self.published_scope = None
            self.status = None
            self.tags = None
            self.template_suffix = None
            self.title = None
            self.updated_at = None
            self.vendor = None
            self.product_images = None
            self.product_image = None
            self.product_variant = None
            self.admin_graphql_api_id = None

        # GETTERS
        def get_body_html(self):
            return self.body_html

        def get_created_at(self):
            return self.created_at

        def get_handle(self):
            return self.handle

        def get_id(self):
            return self.id

        def get_options(self):
            return self.options

        def get_product_type(self):
            return self.product_type

        def get_published_at(self):
            return self.published_at

        def get_published_scope(self):
            return self.published_scope

        def get_status(self):
            return self.status

        def get_tags(self):
            return self.tags

        def get_template_suffix(self):
            return self.template_suffix

        def get_title(self):
            return self.title

        def get_updated_at(self):
            return self.updated_at

        def get_vendor(self):
            return self.vendor

        def get_images(self):
            return self.product_images

        def get_image(self):
            return self.product_image

        def get_variants(self):
            return self.product_variant

        def get_admin_graphql_api_id(self):
            return self.admin_graphql_api_id

        # SETTERS
        def set_body_html(self, value):
            self.body_html = value

        def set_created_at(self, value):
            self.created_at = value

        def set_handle(self, value):
            self.handle = value

        def set_id(self, value):
            self.id = value

        def set_options(self, value):
            self.options = value

        def set_product_type(self, value):
            self.product_type = value

        def set_published_at(self, value):
            self.published_at = value

        def set_published_scope(self, value):
            self.published_scope = value

        def set_status(self, value):
            self.status = value

        def set_tags(self, value):
            self.tags = value

        def set_template_suffix(self, value):
            self.template_suffix = value

        def set_title(self, value):
            self.title = value

        def set_updated_at(self, value):
            self.updated_at = value

        def set_vendor(self, value):
            self.vendor = value

        def set_images(self, value):
            self.product_images = value

        def set_image(self, value):
            self.product_image = value

        def set_variants(self, value):
            self.product_variant = value

        def set_admin_graphql_api_id(self, value):
            self.admin_graphql_api_id = value

    # GETTERS
    def get_products_array(self):
        return self._products_array

    def get_product_count(self):
        return self._product_count

    def get_total_product_count(self):
        return self._total_product_count

    def get_total_product_inserted_count(self):
        return self._total_product_inserted_count

    def get_total_product_updated_count(self):
        return self._total_product_updated_count

    def get_total_product_deleted_count(self):
        return self._total_product_deleted_count

    def get_total_product_with_errors(self):
        return self._total_product_with_errors

    def get_products_with_error_array(self):
        return self._products_with_error_array

    def get_module_name(self):
        return self.module_name

    # SETTERS
    def set_products_array(self, value):
        self._products_array = value

    def set_product_count(self, value):
        self._product_count = value

    def set_total_product_count(self, value):
        self._total_product_count = value

    def set_total_product_inserted_count(self, value):
        self._total_product_inserted_count = value

    def set_total_product_updated_count(self, value):
        self._total_product_updated_count = value

    def set_total_product_deleted_count(self, value):
        self._total_product_deleted_count = value

    def set_total_product_with_errors(self, value):
        self._total_product_with_errors = value

    # APPENDS
    def append_products_array(self, value):
        self._products_array.append(value)

    def append_products_with_error_array(self, value):
        self._products_with_error_array.append(value)

    # CLEARERS
    def clear_products_array(self):
        self._products_array = []

    def clear_columns_values_arrays(self):
        self._columns_array = []
        self._values_array = []

    def clear_condition(self):
        self._condition = ""

    def clear_counters(self):
        self.set_total_product_count(0)
        self.set_total_product_inserted_count(0)
        self.set_total_product_updated_count(0)
        self.set_total_product_deleted_count(0)
        self.set_total_product_with_errors(0)

    def clear_products_with_error_array(self):
        self._products_with_error_array = []

    # DATABASE FUNCTIONS
    def count_products(self, product_id):
        columns = ["COUNT(*) AS TOTAL"]
        self.set_condition(f"ID = '{product_id}'")

        result_flag, result_query = super().query_record(super().get_tbl_PROD(), columns, self.get_condition())

        if result_flag:
            for row in result_query:
                return row.get("TOTAL")
        else:
            return 0

    # def put_shopify_update_product(self, product_id):

    def execution_summary(self, is_webhook=False, send_email=False):
        if not is_webhook:
            print('\n\n[DONE] Finished getting Products because it ran the maximum runs.')
            print('\n===========================================================')
            print('[INFO] Resume of Execution:')
            print('===========================================================')
            print(f'[INFO] Total number of Products:\t\t{self.get_total_product_count()}')
            print(f'[INFO] Total number of Products Inserted:\t{self.get_total_product_inserted_count()}')
            print(f'[INFO] Total number of Products Updated:\t{self.get_total_product_updated_count()}')
            print(f'[INFO] Total number of Products Deleted:\t{self.get_total_product_deleted_count()}')
            print('\n===========================================================')
            print('[INFO] Errors:')
            print('===========================================================')
            print(f'[INFO] Total number of Products with error:\t\t{self.get_total_product_with_errors()}')
            print(f'[INFO] Products with error:\t\t\t\t{self.get_products_with_error_array()}')
            print('===========================================================')
            print('\n[INFO] Execution ended.\n\n')
        else:
            total_errors = self.get_total_product_with_errors() #+ self.get_total_order_line_item_with_errors()

            if total_errors > 0:
                print('===========================================================')
                if self.get_total_product_with_errors():
                    print(f'[ERROR] Products with error:\t\t{self.get_products_with_error_array()}')
                print('===========================================================')

        if send_email:
            print('Sending email notification...')
            email_body = ""
            email_body += '[DONE] Finished getting Products because it ran the maximum runs.'
            email_body += '\n==========================================================='
            email_body += '\n[INFO] Resume of Execution:'
            email_body += '\n==========================================================='
            email_body += f'\n[INFO] Total number of Products:\t\t{self.get_total_product_count()}'
            email_body += f'\n[INFO] Total number of Products Inserted:\t{self.get_total_product_inserted_count()}'
            email_body += f'\n[INFO] Total number of Products Updated:\t{self.get_total_product_updated_count()}'
            email_body += f'\n[INFO] Total number of Products Deleted:\t{self.get_total_product_deleted_count()}'
            email_body += '\n==========================================================='
            email_body += '\n[INFO] Errors:'
            email_body += '\n==========================================================='
            email_body += f'\n[INFO] Total number of Products with error:\t\t{self.get_total_product_with_errors()}'
            email_body += f'\n[INFO] Products with error:\t\t\t\t{self.get_products_with_error_array()}'
            email_body += '\n==========================================================='
            email_body += '\n[INFO] Execution ended.\n\n'

            email_subject = "[Products] Resume of execution"
            email_to = ['xxxx']
            self.utils.send_email(email_to=email_to, email_subject=email_subject, email_body=email_body)

        self.clear_counters()
        self.clear_products_with_error_array()

    def upsert_products(self, product:Product, function):
        product_upserted_flag = False
        maximum_insert_try = 100
        keep_trying = True
        count_try = 0
        product_inserted_count = 0
        product_updated_count = 0
        product_id = product.get_id()
        columns = None
        rows = None

        self.clear_columns_values_arrays()

        self.utils.validate_columns_values("ID", product.get_id())
        self.utils.validate_columns_values("TITLE", self.utils.replace_special_chars(product.get_title()))
        self.utils.validate_columns_values("BODY_HTML", self.utils.replace_special_chars(product.get_body_html()))
        self.utils.validate_columns_values("VENDOR", self.utils.replace_special_chars(product.get_vendor()))
        self.utils.validate_columns_values("PRODUCT_TYPE", self.utils.replace_special_chars(product.get_product_type()))
        self.utils.validate_columns_values("CREATED_AT", product.get_created_at())
        self.utils.validate_columns_values("HANDLE", self.utils.replace_special_chars(product.get_handle()))
        self.utils.validate_columns_values("UPDATED_AT", product.get_updated_at())
        self.utils.validate_columns_values("PUBLISHED_AT", product.get_published_at())
        self.utils.validate_columns_values("TEMPLATE_SUFFIX", self.utils.replace_special_chars(product.get_template_suffix()))
        self.utils.validate_columns_values("PUBLISHED_SCOPE", self.utils.replace_special_chars(product.get_published_scope()))
        self.utils.validate_columns_values("TAGS", self.utils.replace_special_chars(product.get_tags()))
        self.utils.validate_columns_values("STATUS", self.utils.replace_special_chars(product.get_status()))
        self.utils.validate_columns_values("ADMIN_GRAPHQL_API_ID", self.utils.replace_special_chars(product.get_admin_graphql_api_id()))
        self.utils.validate_columns_values("VARIANTS", self.utils.replace_special_chars(json.dumps(product.get_variants())))
        self.utils.validate_columns_values("OPTIONS", self.utils.replace_special_chars(json.dumps(product.get_options())))
        self.utils.validate_columns_values("IMAGES", self.utils.replace_special_chars(json.dumps(product.get_images())))
        self.utils.validate_columns_values("IMAGE", self.utils.replace_special_chars(json.dumps(product.get_image())))

        self.set_product_count(self.count_products(product_id))

        if self.get_product_count() <= 0:
            if function == "update":
                return False, f"Product already exists", 400

            print(f"[INFO] Inserting product...\t\t\tId: {product_id}")
            product_upserted_flag, product_inserted_count, result_string = super().insert_record(super().get_tbl_PROD(), self.get_columns_array(), self.get_values_array())

            if product_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                if "PRIMARY" in result_string:
                    while keep_trying:
                        count_try = count_try + 1
                        print(f"[INFO] Trying to insert product again...\t\tId: {product_id}.Try: {count_try}")
                        super().generate_next_id()
                        product_upserted_flag, product_inserted_count, result_string = super().insert_record(super().get_tbl_PROD(), self.get_columns_array(), self.get_values_array())

                        if product_upserted_flag == True:
                            print(f"[INFO] Product Inserted...\t\t\tId: {product_id}")
                            keep_trying = False
                        else:
                            if "ID" in result_string:
                                print(f"[INFO] Updating product...\t\t\tId: {product_id}")
                                self.clear_condition()
                                self.set_condition(f"ID = '{product_id}'")
                                product_upserted_flag, product_updated_count, result_string = super().update_record(super().get_tbl_PROD(), self.get_columns_array(), self.get_values_array(), self.get_condition())

                                if product_upserted_flag == True:
                                    print(f"[INFO] Product Updated...\t\t\t\tId: {product_id}")
                                    keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Product NOT Inserted because of Maximum tries...\tId: {product_id}.Try: {count_try}")
                                        keep_trying = False
                            else:
                                if count_try >= maximum_insert_try:
                                    print(f"[INFO] Product NOT Inserted because of Maximum tries...\tId: {product_id}.Try: {count_try}")
                                    keep_trying = False
                else:
                    print(f"[INFO] Updating product...\t\t\tId: {product_id}")
                    self.clear_condition()
                    self.set_condition(f"ID = '{product_id}'")
                    product_upserted_flag, product_updated_count, result_string = super().update_record(super().get_tbl_PROD(), self.get_columns_array(), self.get_values_array(), self.get_condition()) 
        else:
            if function == "insert":
                return False, f"Product {self.utils.get_error_message(404)}", 404

            print(f"[INFO] Updating product...\t\t\tId: {product_id}")
            self.clear_condition()
            self.set_condition(f"ID = '{product_id}'")
            product_upserted_flag, product_updated_count, result_string = super().update_record(super().get_tbl_PROD(), self.get_columns_array(), self.get_values_array(), self.get_condition())

            if product_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                if "ID" in result_string:
                    while keep_trying:
                        count_try = count_try + 1
                        print(f"[INFO] Trying to update product again...\t\tId: {product_id}.Try: {count_try}")
                        product_upserted_flag, product_updated_count, result_string = super().update_record(super().get_tbl_PROD(), self.get_columns_array(), self.get_values_array(), self.get_condition())

                        if product_upserted_flag == True:
                            print(f"[INFO] Product Updated...\t\t\tId: {product_id}")
                            keep_trying = False
                        else:
                            if count_try >= maximum_insert_try:
                                print(f"[INFO] Product NOT Updated because of Maximum tries...\tId: {product_id}.Try: {count_try}")
                                keep_trying = False

        if product_upserted_flag == False:
            self.append_products_with_error_array(product_id)
            self.set_total_product_with_errors(self.get_total_product_with_errors() + 1)

        self.set_total_product_count(self.get_total_product_count() + product_inserted_count + product_updated_count)
        self.set_total_product_inserted_count(self.get_total_product_inserted_count() + product_inserted_count)
        self.set_total_product_updated_count(self.get_total_product_updated_count() + product_updated_count)

        del product

        return product_upserted_flag, self.get_total_product_count(), result_string

    def get_all_shopify_products(self, run_counter, max_runs, collection_id, created_at_max, created_at_min, fields, handle, ids, limit, presentment_currencies, product_type, published_at_max, published_at_min, published_status, since_id, status, title, updated_at_max, updated_at_min, vendor, api_version):
        try:
            print(f"[INFO] Getting all products - Run {run_counter} of {max_runs}")

            if run_counter <= 1:
                self.utils.set_start_time(time.time())
            if max_runs == None or max_runs == "" or max_runs == "0":
                print('[DONE] Finished getting products because max_runs is None.')
                return
            if run_counter == None or run_counter == "" or run_counter == "0":
                print('[DONE] Finished getting products because run_counter is None.')
                return
            if limit == None or limit == "" or limit == "0":
                print('[DONE] Finished getting products because limit is None.')
                return

            last_product_id = None

            products = self.shopify.get_shopify_list_of_products(collection_id=collection_id, created_at_max=created_at_max, created_at_min=created_at_min, fields=fields, handle=handle, ids=ids, limit=limit, presentment_currencies=presentment_currencies, product_type=product_type, published_at_max=published_at_max, published_at_min=published_at_min, published_status=published_status, since_id=since_id, status=status, title=title, updated_at_max=updated_at_max, updated_at_min=updated_at_min, vendor=vendor, api_version=api_version)

            if len(products) == 0:
                del products
                print('[DONE] Finished getting products because it found 0.')
                self.execution_summary(is_webhook=False, send_email=True)
            else:
                for product_data in products:
                    product = self.Product()

                    last_product_id = product_data.get('id', "")
                    product.set_id(product_data.get('id', ""))
                    product.set_title(product_data.get('title', ""))
                    product.set_body_html(product_data.get('body_html', ""))
                    product.set_vendor(product_data.get('vendor', ""))
                    product.set_product_type(product_data.get('product_type', ""))
                    product.set_created_at(product_data.get('created_at', ""))
                    product.set_handle(product_data.get('handle', ""))
                    product.set_updated_at(product_data.get('updated_at', ""))
                    product.set_published_at(product_data.get('published_at', ""))
                    product.set_template_suffix(product_data.get('template_suffix', ""))
                    product.set_published_scope(product_data.get('published_scope', ""))
                    product.set_tags(product_data.get('tags', ""))
                    product.set_status(product_data.get('status', ""))
                    product.set_admin_graphql_api_id(product_data.get('admin_graphql_api_id', ""))
                    product.set_variants(product_data.get('variants', ""))
                    product.set_options(product_data.get('options', ""))
                    product.set_images(product_data.get('images', ""))
                    product.set_image(product_data.get('image', ""))

                    self.upsert_products(product=product, Function=None)

                try:
                    del product
                except:
                    pass
                try:
                    del product_data
                except:
                    pass
                try:
                    del products
                except:
                    pass

                if run_counter < max_runs:
                    run_counter += 1
                    print(f'[INFO] Getting more products from Shopify. Run number: {run_counter} of {max_runs}.')
                    self.get_all_shopify_products(run_counter=run_counter, max_runs=max_runs, collection_id=collection_id, created_at_max=created_at_max, created_at_min=created_at_min, fields=fields, handle=handle, ids=ids, limit=limit, presentment_currencies=presentment_currencies, product_type=product_type, published_at_max=published_at_max, published_at_min=published_at_min, published_status=published_status, since_id=last_product_id, status=status, title=title, updated_at_max=updated_at_max, updated_at_min=updated_at_min, vendor=vendor, api_version=api_version)
                    del last_product_id
                else:
                    print('[DONE] Finished getting products because it reached the maximum runs.')
                    self.execution_summary(is_webhook=False, send_email=True)
        except Exception as e:
            self.utils.set_end_time(time.time())
            try:
                del product
            except:
                pass
            try:
                del product_data
            except:
                pass
            try:
                del products
            except:
                pass
            last_order_id = last_order_id if last_order_id is not None else since_id

            additional_info = f'[INFO] runCounter: {run_counter}'
            additional_info += f'\n[INFO] maxRuns: {max_runs}'
            additional_info += f'\n[INFO] collection_id: {collection_id}'
            additional_info += f'\n[INFO] created_at_max: {created_at_max}'
            additional_info += f'\n[INFO] created_at_min: {created_at_min}'
            additional_info += f'\n[INFO] fields: {fields}'
            additional_info += f'\n[INFO] handle: {handle}'
            additional_info += f'\n[INFO] ids: {ids}'
            additional_info += f'\n[INFO] limit: {limit}'
            additional_info += f'\n[INFO] presentment_currencies: {presentment_currencies}'
            additional_info += f'\n[INFO] product_type: {product_type}'
            additional_info += f'\n[INFO] published_at_max: {published_at_max}'
            additional_info += f'\n[INFO] published_at_min: {published_at_min}'
            additional_info += f'\n[INFO] published_status: {published_status}'
            additional_info += f'\n[INFO] since_id: {since_id}'
            additional_info += f'\n[INFO] status: {status}'
            additional_info += f'\n[INFO] title: {title}'
            additional_info += f'\n[INFO] updated_at_max: {updated_at_max}'
            additional_info += f'\n[INFO] updated_at_min: {updated_at_min}'
            additional_info += f'\n[INFO] vendor: {vendor}'
            additional_info += f'\n[INFO] api_version: {api_version}'
            additional_info += f"\n[INFO] last_order_id: {last_order_id}"
            additional_info += f'\n[INFO] The function get_all_shopify_orders() execution kept running on Heroku with since_id = {last_order_id}. Please check the logs for more information.\n\n'

            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_all_shopify_orders", error_code=None, error_message=str(e), additional_details=additional_info)

    def get_specific_product(self, product_id, fields, api_version):
        print(f"[INFO] Getting specific product - Id: {product_id}")

        if product_id == None or product_id == "" or product_id == "0":
            print('[DONE] Finished getting orders because product_id is None.')
            return

        response_description = "Success"
        status_code = 200
        is_upserted_success = False

        return_flag, product_data, response_description, status_code = self.shopify.get_shopify_single_product(product_id=product_id, fields=fields, api_version=api_version)

        if return_flag == True:
            if len(product_data) == 0:
                return False, f'[ERROR] No product found with ID: {product_id}.', status_code
            else:
                product = self.Product()

                product.set_id(product_data.get('id', ""))
                product.set_title(product_data.get('title', ""))
                product.set_body_html(product_data.get('body_html', ""))
                product.set_vendor(product_data.get('vendor', ""))
                product.set_product_type(product_data.get('product_type', ""))
                product.set_created_at(product_data.get('created_at', ""))
                product.set_handle(product_data.get('handle', ""))
                product.set_updated_at(product_data.get('updated_at', ""))
                product.set_published_at(product_data.get('published_at', ""))
                product.set_template_suffix(product_data.get('template_suffix', ""))
                product.set_published_scope(product_data.get('published_scope', ""))
                product.set_tags(product_data.get('tags', ""))
                product.set_status(product_data.get('status', ""))
                product.set_admin_graphql_api_id(product_data.get('admin_graphql_api_id', ""))
                product.set_variants(product_data.get('variants', ""))
                product.set_options(product_data.get('options', ""))
                product.set_images(product_data.get('images', ""))
                product.set_image(product_data.get('image', ""))

                is_upserted_success, product_updated_count, response_description = self.upsert_products(product=product, function=None)

                del product
                del product_data

                if is_upserted_success == False:
                    return False, f"[ERROR] Error while upserting product.\tProduct id: {product_id}", 400
                else:
                    return True, response_description, status_code
        else:
            return False, response_description, status_code

    def webhook_save_product(self, product_json, function):
        response_description = "Success"
        status_code = 200

        if len(product_json) == 0:
            return False, f"[ERROR] Empty JSON.", 400
        else:
            try:
                product = self.Product()

                product.set_id(product_json.get('id', ""))
                product.set_title(product_json.get('title', ""))
                product.set_body_html(product_json.get('body_html', ""))
                product.set_vendor(product_json.get('vendor', ""))
                product.set_product_type(product_json.get('product_type', ""))
                product.set_created_at(product_json.get('created_at', ""))
                product.set_handle(product_json.get('handle', ""))
                product.set_updated_at(product_json.get('updated_at', ""))
                product.set_published_at(product_json.get('published_at', ""))
                product.set_template_suffix(product_json.get('template_suffix', ""))
                product.set_published_scope(product_json.get('published_scope', ""))
                product.set_tags(product_json.get('tags', ""))
                product.set_status(product_json.get('status', ""))
                product.set_admin_graphql_api_id(product_json.get('admin_graphql_api_id', ""))
                product.set_variants(product_json.get('variants', ""))
                product.set_options(product_json.get('options', ""))
                product.set_images(product_json.get('images', ""))
                product.set_image(product_json.get('image', ""))

                is_upserted_success, product_updated_count, response_description = self.upsert_products(product=product, function=function)

                del product

                self.execution_summary(is_webhook=True, send_email=False)

                if is_upserted_success == False:
                    return is_upserted_success, f"[ERROR] Error while upserting product.\tProduct id: {product_json.get("id")}", 400
                else:
                    return is_upserted_success, response_description, status_code
            except Exception as e:
                return False, f"[ERROR] Error while upserting product.\tProduct id: {product_json.get("id")}. {e}", 400
