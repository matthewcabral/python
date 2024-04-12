from utils.UtilsController import *
from utils.LogsController import *

class ShopifyController:

    def __init__(self):
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "ShopifyController"

    def get_module_name(self):
        return self.module_name

    # SHOPIFY ORDER APIs
    def get_shopify_list_of_orders(self, fields, limit, status, fulfillment_status, created_at_min, created_at_max, processed_at_min, processed_at_max, since_id, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        list_orders_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/orders.json?"
        list_orders_endpoint += "limit=250"  if limit is None or limit == "" or limit == "0" else "limit=" + str(limit)
        list_orders_endpoint += "" if fields is None or fields == "" else "&fields=" + str(fields)
        list_orders_endpoint += "" if created_at_min is None or created_at_min == "" else "&created_at_min=" + str(created_at_min)
        list_orders_endpoint += "" if created_at_max is None or created_at_max == "" else "&created_at_max=" + str(created_at_max)
        list_orders_endpoint += "" if processed_at_min is None or processed_at_min == "" else "&processed_at_min=" + str(processed_at_min)
        list_orders_endpoint += "" if processed_at_max is None or processed_at_max == "" else "&processed_at_max=" + str(processed_at_max)
        list_orders_endpoint += "" if since_id is None or since_id == "" else "&since_id=" + str(since_id)
        list_orders_endpoint += "" if status is None or status == "" else "&status=" + str(status)
        list_orders_endpoint += "" if fulfillment_status is None or fulfillment_status == "" else "&fulfillment_status=" + str(fulfillment_status)
        print(f"list_orders_endpoint: {list_orders_endpoint}")
        
        try:
            res = requests.get(list_orders_endpoint)
            orders = res.json().get('orders', [])
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500

            error_message = str(e)
            additional_details = f'[INFO] list_orders_endpoint: {list_orders_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_list_of_orders", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))
            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get order. ({response_description})", code
        finally:
            del list_orders_endpoint

        return True, orders, response_description, res.status_code

    def get_shopify_specific_order(self, orderId, fields, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        if orderId is None or orderId == "":
            return False, self.utils.get_error_message(400), "[ERROR] Order ID is required.", 400

        specific_order_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/orders/{str(orderId)}.json?"
        specific_order_endpoint += "" if fields is None or fields == "" else "fields=" + str(fields)

        try:
            res = requests.get(specific_order_endpoint)
            order = res.json()['order']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] specific_order_endpoint: {specific_order_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_specific_order", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))
            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get order. ({response_description})", code
        finally:
            del specific_order_endpoint

        return True, order, response_description, res.status_code

    def get_shopify_order_count(self, created_at_max, created_at_min, financial_status, fulfillment_status, status, updated_at_max, updated_at_min, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        count_orders_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/orders/count.json?"
        count_orders_endpoint += "" if created_at_min is None or created_at_min == "" else "&created_at_min=" + str(created_at_min)
        count_orders_endpoint += "" if created_at_max is None or created_at_max == "" else "&created_at_max=" + str(created_at_max)
        count_orders_endpoint += "" if financial_status is None or financial_status == "" else "&financial_status=" + str(financial_status)
        count_orders_endpoint += "" if fulfillment_status is None or fulfillment_status == "" else "&fulfillment_status=" + str(fulfillment_status)
        count_orders_endpoint += "" if status is None or status == "" else "&status=" + str(status)
        count_orders_endpoint += "" if updated_at_max is None or updated_at_max == "" else "&updated_at_max=" + str(updated_at_max)
        count_orders_endpoint += "" if updated_at_min is None or updated_at_min == "" else "&updated_at_min=" + str(updated_at_min)

        try:
            # Get the JSON response and assign to orders
            res = requests.get(count_orders_endpoint)
            count = res.json()['count']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] count_orders_endpoint: {count_orders_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_order_count", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get order. ({response_description})", code
        finally:
            del count_orders_endpoint

        return True, count, response_description, res.status_code

    # SHOPIFY PRODUCT APIS
    def get_shopify_list_of_products(self, collection_id, created_at_max, created_at_min, fields, handle, ids, limit, presentment_currencies, product_type, published_at_max, published_at_min, published_status, since_id, status, title, updated_at_max, updated_at_min, vendor, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        list_products_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/products.json?"
        list_products_endpoint += "limit=250"  if limit is None or limit == "" or limit == "0" else "limit=" + str(limit)
        list_products_endpoint += "" if collection_id is None or collection_id == "" else f"&collection_id={str(collection_id)}"
        list_products_endpoint += "" if created_at_min is None or created_at_min == "" else "&created_at_min=" + str(created_at_min)
        list_products_endpoint += "" if created_at_max is None or created_at_max == "" else "&created_at_max=" + str(created_at_max)
        list_products_endpoint += "" if fields is None or fields == "" else "&fields=" + str(fields)
        list_products_endpoint += "" if handle is None or handle == "" else "&handle=" + str(handle)
        list_products_endpoint += "" if ids is None or ids == "" else "&ids=" + str(ids)
        list_products_endpoint += "" if presentment_currencies is None or presentment_currencies == "" else "&presentment_currencies=" + str(presentment_currencies)
        list_products_endpoint += "" if product_type is None or product_type == "" else "&product_type=" + str(product_type)
        list_products_endpoint += "" if published_at_max is None or published_at_max == "" else "&published_at_max=" + str(published_at_max)
        list_products_endpoint += "" if published_at_min is None or published_at_min == "" else "&published_at_min=" + str(published_at_min)
        list_products_endpoint += "" if published_status is None or published_status == "" else "&published_status=" + str(published_status)
        list_products_endpoint += "" if since_id is None or since_id == "" else "&since_id=" + str(since_id)
        list_products_endpoint += "" if status is None or status == "" else "&status=" + str(status)
        list_products_endpoint += "" if title is None or title == "" else "&title=" + str(title)
        list_products_endpoint += "" if updated_at_max is None or updated_at_max == "" else "&updated_at_max=" + str(updated_at_max)
        list_products_endpoint += "" if updated_at_min is None or updated_at_min == "" else "&updated_at_min=" + str(updated_at_min)
        list_products_endpoint += "" if vendor is None or vendor == "" else "&vendor=" + str(vendor)

        print(f"list_products_endpoint: {list_products_endpoint}")

        try:
            res = requests.get(list_products_endpoint)
            products = res.json()['products']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] list_products_endpoint: {list_products_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_list_of_products", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get order. ({response_description})", code
        finally:
            del list_products_endpoint

        return True, products, response_description, res.status_code

    def get_shopify_single_product(self, product_id, fields, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        if product_id is None or product_id == "":
            return False, self.utils.get_error_message(400), "[ERROR] Product ID is required.", 400

        single_product_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/products/{str(product_id)}.json?"
        single_product_endpoint += "" if fields is None or fields == "" else "fields=" + str(fields)

        try:
            res = requests.get(single_product_endpoint)
            product = res.json()['product']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] single_product_endpoint: {single_product_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_single_product", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get Product. ({response_description})", code
        finally:
            del single_product_endpoint

        return True, product, response_description, res.status_code

    def get_shopify_count_products(self, collection_id, created_at_max, created_at_min, product_type, published_at_max, published_at_min, published_status, updated_at_max, updated_at_min, vendor, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        count_products_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/products/count.json?"
        count_products_endpoint += "" if collection_id is None or collection_id == "" else f"&collection_id={str(collection_id)}"
        count_products_endpoint += "" if created_at_max is None or created_at_max == "" else "&created_at_max=" + str(created_at_max)
        count_products_endpoint += "" if created_at_min is None or created_at_min == "" else "&created_at_min=" + str(created_at_min)
        count_products_endpoint += "" if product_type is None or product_type == "" else "&product_type=" + str(product_type)
        count_products_endpoint += "" if published_at_max is None or published_at_max == "" else "&published_at_max=" + str(published_at_max)
        count_products_endpoint += "" if published_at_min is None or published_at_min == "" else "&published_at_min=" + str(published_at_min)
        count_products_endpoint += "" if published_status is None or published_status == "" else "&published_status=" + str(published_status)
        count_products_endpoint += "" if updated_at_max is None or updated_at_max == "" else "&updated_at_max=" + str(updated_at_max)
        count_products_endpoint += "" if updated_at_min is None or updated_at_min == "" else "&updated_at_min=" + str(updated_at_min)
        count_products_endpoint += "" if vendor is None or vendor == "" else "&vendor=" + str(vendor)

        try:
            res = requests.get(count_products_endpoint)
            count = res.json()['count']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] count_products_endpoint: {count_products_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_count_products", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get order. ({response_description})", code
        finally:
            del count_products_endpoint

        return True, count, response_description, res.status_code

    def get_shopify_list_of_all_product_images(self, product_id, fields, since_id, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        if product_id is None or product_id == "":
            return False, self.utils.get_error_message(400), "[ERROR] Product ID is required.", 400

        list_of_all_product_images_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/products/{str(product_id)}/images.json?"
        list_of_all_product_images_endpoint += "" if product_id is None or product_id == "" else f"&product_id={str(product_id)}"
        list_of_all_product_images_endpoint += "" if fields is None or fields == "" else "&fields=" + str(fields)
        list_of_all_product_images_endpoint += "" if since_id is None or since_id == "" else "&since_id=" + str(since_id)

        try:
            res = requests.get(list_of_all_product_images_endpoint)
            images = res.json()['images']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] list_of_all_product_images_endpoint: {list_of_all_product_images_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_list_of_all_product_images", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get order. ({response_description})", code
        finally:
            del list_of_all_product_images_endpoint

        return True, images, response_description, res.status_code

    def get_shopify_single_product_image(self, image_id, product_id, fields, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        if image_id is None or image_id == "":
            return False, self.utils.get_error_message(400), "[ERROR] Image ID is required.", 400

        if product_id is None or product_id == "":
            return False, self.utils.get_error_message(400), "[ERROR] Product ID is required.", 400

        single_product_image_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/products/{str(product_id)}/images/{str(image_id)}.json?"
        single_product_image_endpoint += "" if image_id is None or image_id == "" else f"&image_id={str(image_id)}"
        single_product_image_endpoint += "" if product_id is None or product_id == "" else f"&product_id={str(product_id)}"
        single_product_image_endpoint += "" if fields is None or fields == "" else "&fields=" + str(fields)

        try:
            res = requests.get(single_product_image_endpoint)
            image = res.json()['image']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] single_product_image_endpoint: {single_product_image_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_single_product_image", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get product image. ({response_description})", code
        finally:
            del single_product_image_endpoint

        return True, image, response_description, res.status_code

    def get_shopify_count_of_all_product_images(self, product_id, since_id, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        if product_id is None or product_id == "":
            return False, self.utils.get_error_message(400), "[ERROR] Product ID is required.", 400

        count_of_all_product_images_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/products/{str(product_id)}/images/count.json?"
        count_of_all_product_images_endpoint += "" if product_id is None or product_id == "" else f"&product_id={str(product_id)}"
        count_of_all_product_images_endpoint += "" if since_id is None or since_id == "" else "&since_id=" + str(since_id)

        try:
            res = requests.get(count_of_all_product_images_endpoint)
            count = res.json()['count']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] count_of_all_product_images_endpoint: {count_of_all_product_images_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_count_of_all_product_images", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get order. ({response_description})", code
        finally:
            del count_of_all_product_images_endpoint

        return True, count, response_description, res.status_code

    def get_shopify_list_of_product_variants(self, product_id, fields, limit, presentment_currencies, since_id, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        if product_id is None or product_id == "":
            return False, self.utils.get_error_message(400), "[ERROR] Product ID is required.", 400

        list_of_product_variants_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/products//{str(product_id)}/variants.json?"
        list_of_product_variants_endpoint += "limit=250"  if limit is None or limit == "" or limit == "0" else "limit=" + str(limit)
        list_of_product_variants_endpoint += "" if product_id is None or product_id == "" else f"&product_id={str(product_id)}"
        list_of_product_variants_endpoint += "" if fields is None or fields == "" else "&fields=" + str(fields)
        list_of_product_variants_endpoint += "" if presentment_currencies is None or presentment_currencies == "" else "&presentment_currencies=" + str(presentment_currencies)
        list_of_product_variants_endpoint += "" if since_id is None or since_id == "" else "&since_id=" + str(since_id)

        try:
            res = requests.get(list_of_product_variants_endpoint)
            variants = res.json()['variants']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] list_of_product_variants_endpoint: {list_of_product_variants_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_list_of_product_variants", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get order. ({response_description})", code
        finally:
            del list_of_product_variants_endpoint

        return True, variants, response_description, res.status_code

    def get_shopify_count_of_all_product_variants(self, product_id, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        if product_id is None or product_id == "":
            return False, self.utils.get_error_message(400), "[ERROR] Product ID is required.", 400

        count_of_all_product_variants_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/products/{str(product_id)}/variants/count.json?"
        count_of_all_product_variants_endpoint += "" if product_id is None or product_id == "" else f"&product_id={str(product_id)}"

        try:
            res = requests.get(count_of_all_product_variants_endpoint)
            count = res.json()['count']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] count_of_all_product_variants_endpoint: {count_of_all_product_variants_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_count_of_all_product_variants", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get order. ({response_description})", code
        finally:
            del count_of_all_product_variants_endpoint

        return True, count, response_description, res.status_code

    def get_shopify_single_product_variant(self, variant_id, fields, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        if variant_id is None or variant_id == "":
            return False, self.utils.get_error_message(400), "[ERROR] Variant ID is required.", 400

        single_product_variants_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/variants/{str(variant_id)}.json?"
        single_product_variants_endpoint += "" if variant_id is None or variant_id == "" else f"&variant_id={str(variant_id)}"
        single_product_variants_endpoint += "" if fields is None or fields == "" else "&fields=" + str(fields)

        try:
            res = requests.get(single_product_variants_endpoint)
            variant = res.json()['variant']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] single_product_variants_endpoint: {single_product_variants_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_single_product_variant", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get product variant. ({response_description})", code
        finally:
            del single_product_variants_endpoint

        return True, variant, response_description, res.status_code

    # SHOPIFY FULFILLMENTS APIS
    def get_shopify_fulfillment_orders(self, order_id, include_financial_summaries, include_order_reference_fields, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version

        if order_id is None or order_id == "":
            return False, self.utils.get_error_message(400), "[ERROR] Order ID is required.", 400

        fulfillment_orders_endpoint = f"https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/orders/{order_id}/fulfillment_orders.json?"
        fulfillment_orders_endpoint += "" if include_financial_summaries is None or include_financial_summaries == "" else "&include_financial_summaries=" + include_financial_summaries
        fulfillment_orders_endpoint += "" if include_order_reference_fields is None or include_order_reference_fields == "" else "&include_order_reference_fields=" + include_order_reference_fields
        print(f"fulfillment_orders_endpoint: {fulfillment_orders_endpoint}")

        try:
            res = requests.get(fulfillment_orders_endpoint)
            fulfillment = res.json()['fulfillment_orders']
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] fulfillment_orders_endpoint: {fulfillment_orders_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_shopify_fulfillment_orders", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))
            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get fulfillments. ({response_description})", code
        finally:
            del specific_order_endpoint

        return True, fulfillment, response_description, res.status_code

    def post_shopify_fulfillments(self, fullfilment_id, message, notify_customer, origin_address, tracking_number, tracking_url, carrier_name, api_version):
        response_description = "Success"
        api_version_str = self.utils.get_SHOPIFY_API_VERSION() if api_version is None or api_version == "" else api_version
        
        fulfillments_endpoint = f'https://{self.utils.get_SHOPIFY_PUB_KEY()}:{self.utils.get_SHOPIFY_PRIV_KEY()}@XXXXXX.myshopify.com/admin/api/{api_version_str}/fulfillments.json'
        print(f"fulfillments_endpoint: {fulfillments_endpoint}")

        if fullfilment_id is None or fullfilment_id == "":
            return False, self.utils.get_error_message(400), "[ERROR] fullfilment_id is required.", 400
        if tracking_number is None or tracking_number == "":
            return False, self.utils.get_error_message(400), "[ERROR] tracking_number is required.", 400
        if tracking_url is None or tracking_url == "":
            return False, self.utils.get_error_message(400), "[ERROR] tracking_url is required.", 400
        if carrier_name is None or carrier_name == "":
            return False, self.utils.get_error_message(400), "[ERROR] carrier_name is required.", 400

        message = "The package was shipped." if message is None or message == "" else message
        notify_customer = True if notify_customer is None or notify_customer == "" else notify_customer

        headers = {
            'Content-Type': 'application/json'
        }

        request_body = {
            "fulfillment": {
                "line_items_by_fulfillment_order": {
                    "fulfillment_order_id": fullfilment_id
                },
                "message": message,
                "notify_customer": notify_customer,
                "tracking_info": {
                    "number": tracking_number,
                    "url": tracking_url,
                    "company": carrier_name
                }
            }
        }

        if origin_address is not None and origin_address != "":
            request_body["fulfillment"]["origin_address"] = origin_address

        try:
            res = requests.post(url=fulfillments_endpoint, headers=headers, json=request_body)
            fulfillment = res.json()['fulfillment']
            code = res.status_code
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            error_message = str(e)
            additional_details = f'[INFO] fulfillment_orders_endpoint: {fulfillments_endpoint}'
            additional_details += f'\n[INFO] res: {res}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="post_shopify_fulfillments", error_code=code, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))
            response_description = self.utils.get_error_message(code)

            if response_description == False:
                response_description = str(e)

            return False, "", f"[ERROR] Could not get fulfillments. ({response_description})", code
        finally:
            del specific_order_endpoint

        return True, fulfillment, response_description, code

    # SHOPIFY CUSTOMER APIS
    def get_shopify_list_of_customers(self, created_at_max, created_at_min, fields, ids, limit, since_id, updated_at_max, updated_at_min):
        # Variables
        error_response = "Success"
        list_of_customers_url = None
        res = None
        customers = None

        try:
            # Define the API URL
            list_of_customers_url = "https://" + self.utils.get_SHOPIFY_PUB_KEY() + ":" + self.utils.get_SHOPIFY_PRIV_KEY() + "@XXXXXX.myshopify.com/admin/api/2023-10/customers.json?"
            list_of_customers_url += "limit=250"  if limit is None or limit == "" or limit == "0" else "limit=" + str(limit)
            list_of_customers_url += "" if created_at_min is None or created_at_min == "" else "&created_at_min=" + str(created_at_min)
            list_of_customers_url += "" if created_at_max is None or created_at_max == "" else "&created_at_max=" + str(created_at_max)
            list_of_customers_url += "" if fields is None or fields == "" else "&fields=" + str(fields)
            list_of_customers_url += "" if ids is None or ids == "" else "&ids=" + str(ids)
            list_of_customers_url += "" if since_id is None or since_id == "" else "&since_id=" + str(since_id)
            list_of_customers_url += "" if updated_at_max is None or updated_at_max == "" else "&updated_at_max=" + str(updated_at_max)
            list_of_customers_url += "" if updated_at_min is None or updated_at_min == "" else "&updated_at_min=" + str(updated_at_min)

            # Get the JSON response and assign to products
            res = requests.get(list_of_customers_url)
            customers = res.json()['customers']
            return customers
        except Exception as e:
            error_response = self.utils.get_error_message(res.status_code)

            if error_response == False:
                error_response += str(e)

            return f"[ERROR] Could not get order. ({error_response})"
        finally:
            print("Clearing Variables")
            try:
                del error_response
                del list_of_customers_url
                del res
                del customers
            except Exception as e:
                print(f"Could not clear variables: {str(e)}")
                pass

    def get_shopify_single_customer(self, customer_id, fields):
        # Variables
        error_response = "Success"
        single_customer_url = None
        res = None
        customer = None

        try:
            # Define the API URL
            single_customer_url = "https://" + self.utils.get_SHOPIFY_PUB_KEY() + ":" + self.utils.get_SHOPIFY_PRIV_KEY() + f"@XXXXXX.myshopify.com/admin/api/2023-10/customers/{str(customer_id)}.json?"
            single_customer_url += "" if customer_id is None or customer_id == "" else f"&customer_id={str(customer_id)}"
            single_customer_url += "" if fields is None or fields == "" else "&fields=" + str(fields)

            # Get the JSON response and assign to products
            res = requests.get(single_customer_url)
            customer = res.json()['customer']
            return customer
        except Exception as e:
            error_response = self.utils.get_error_message(res.status_code)

            if error_response == False:
                error_response += str(e)

            return f"[ERROR] Could not get order. ({error_response})"
        finally:
            print("Clearing Variables")
            try:
                del error_response
                del single_customer_url
                del res
                del customer
            except Exception as e:
                print(f"Could not clear variables: {str(e)}")
                pass

    # Retrieves all orders that belong to a customer
    def get_shopify_all_customer_orders(self, customer_id, status):
        # Variables
        error_response = "Success"
        all_customer_orders_url = None
        res = None
        orders = None

        try:
            # Define the API URL
            all_customer_orders_url = "https://" + self.utils.get_SHOPIFY_PUB_KEY() + ":" + self.utils.get_SHOPIFY_PRIV_KEY() + f"@XXXXXX.myshopify.com/admin/api/2023-10/customers/{str(customer_id)}/orders.json?"
            all_customer_orders_url += "" if customer_id is None or customer_id == "" else f"&customer_id={str(customer_id)}"
            all_customer_orders_url += "" if status is None or status == "" else "&status=" + str(status)

            # Get the JSON response and assign to products
            res = requests.get(all_customer_orders_url)
            orders = res.json()['orders']
            return orders
        except Exception as e:
            error_response = self.utils.get_error_message(res.status_code)

            if error_response == False:
                error_response += str(e)

            return f"[ERROR] Could not get order. ({error_response})"
        finally:
            print("Clearing Variables")
            try:
                del all_customer_orders_url
                del res
                del orders
            except Exception as e:
                print(f"Could not clear variables: {str(e)}")
                pass

    def get_shopify_count_of_customers(self, created_at_max, created_at_min, updated_at_max, updated_at_min):
        # Variables
        error_response = "Success"
        count_of_customers_url = None
        res = None
        count = None

        try:
            # Define the API URL
            count_of_customers_url = "https://" + self.utils.get_SHOPIFY_PUB_KEY() + ":" + self.utils.get_SHOPIFY_PRIV_KEY() + "@XXXXXX.myshopify.com/admin/api/2023-10/customers/count.json?"
            count_of_customers_url += "" if created_at_max is None or created_at_max == "" else "&created_at_max=" + str(created_at_max)
            count_of_customers_url += "" if created_at_min is None or created_at_min == "" else "&created_at_min=" + str(created_at_min)
            count_of_customers_url += "" if updated_at_max is None or updated_at_max == "" else "&updated_at_max=" + str(updated_at_max)
            count_of_customers_url += "" if updated_at_min is None or updated_at_min == "" else "&updated_at_min=" + str(updated_at_min)

            # Get the JSON response and assign to products
            res = requests.get(count_of_customers_url)
            count = res.json()['count']
            return count
        except Exception as e:
            error_response = self.utils.get_error_message(res.status_code)

            if error_response == False:
                error_response += str(e)

            return f"[ERROR] Could not get order. ({error_response})"
        finally:
            print("Clearing Variables")
            try:
                del count_of_customers_url
                del res
                del count
            except Exception as e:
                print(f"Could not clear variables: {str(e)}")
                pass

    # Searches for customers that match a supplied query
    def get_shopify_customers_query(self, fields, limit, order, query):
        # Variables
        error_response = "Success"
        customers_query_url = None
        res = None
        customers = None

        try:
            # Define the API URL
            customers_query_url = "https://" + self.utils.get_SHOPIFY_PUB_KEY() + ":" + self.utils.get_SHOPIFY_PRIV_KEY() + "@XXXXXX.myshopify.com/admin/api/2023-10/customers/search.json?"
            customers_query_url += "limit=250"  if limit is None or limit == "" or limit == "0" else "limit=" + str(limit)
            customers_query_url += "" if fields is None or fields == "" else "&fields=" + str(fields)
            customers_query_url += "" if order is None or order == "" else "&order=" + str(order)
            customers_query_url += "" if query is None or query == "" else "&query=" + str(query)

            # Get the JSON response and assign to products
            res = requests.get(customers_query_url)
            customers = res.json()['customers']
            return customers
        except Exception as e:
            error_response = self.utils.get_error_message(res.status_code)

            if error_response == False:
                error_response += str(e)

            return f"[ERROR] Could not get order. ({error_response})"
        finally:
            print("Clearing Variables")
            try:
                del customers_query_url
                del res
                del customers
            except Exception as e:
                print(f"Could not clear variables: {str(e)}")
                pass

