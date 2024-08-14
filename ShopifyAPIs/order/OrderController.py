from database.DataController import DataController
from utils.UtilsController import *
from utils.LogsController import *
from apis.inbound.ShopifyController import *
from product.PhoneCasesController import *
from utils.PrintingController import *
from system.SysPrefController import *
# from product.CustomsProductController import CustomsProductController
from mq.MQController import *
from order.BillingAddressController import *
from order.ShippingAddressController import *
from order.DiscountCodes import *
from order.DiscountApplications import *
from order.Fulfillments import *
from order.Refunds import *
import re
import gc

class OrderController(DataController):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(OrderController, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    # CONSTRUCTOR
    def __init__(self):
        if not hasattr(self, '_initialized'):  # Ensure __init__ is only called once
            sys.setrecursionlimit(10**6)
            super().__init__()  # Call the initializer of DataController
            self._initialized = True 
            self.utils = UtilsController()
            self.shopify = ShopifyController()
            self.log = LogsController()
            self.phone_cases = PhoneCasesController()
            self.printing = PrintingController()
            self.syspref = SysPrefController()
            # self.customs = CustomsProductController()
            self.mq = MQController()
            self.billing_address = BillingAddressController()
            self.shipping_address = ShippingAddressController()
            self.discount_cds = DiscountCodeController()
            self.discount_apps = DiscountApplicationsController()
            self.fulfill = FulfillmentsController()
            self.refunds = RefundsController()

            # Order Variables
            self.order_count = 0
            self.total_order_count = 0
            self.total_order_inserted_count = 0
            self.total_order_updated_count = 0
            self.total_order_deleted_count = 0
            self.total_order_with_errors = 0
            self.order_with_error_array = []

            # Order Line Item Variables
            self.order_line_item_count = 0
            self.total_order_line_item_count = 0
            self.total_order_line_item_inserted_count = 0
            self.total_order_line_item_updated_count = 0
            self.total_order_line_item_deleted_count = 0
            self.total_order_line_item_with_errors = 0
            self.order_line_item_with_error_array = []

            self.module_name = "OrderController"

    # ORDER CLASSES
    class Order():
        def __init__(self):
            self.app_id = None
            self.billing_address = None
            self.browser_ip = None
            self.buyer_accepts_marketing = None
            self.cancel_reason = None
            self.cancelled_at = None
            self.cart_token = None
            self.checkout_token = None
            self.client_details = None
            self.closed_at = None
            self.company = None
            self.confirmation_number = None
            self.country_code = None
            self.created_at = None
            self.currency = None
            self.current_total_additional_fees_set = None
            self.current_total_discounts = None
            self.current_total_discounts_set = None
            self.current_total_duties_set = None
            self.current_total_price = None
            self.current_total_price_set = None
            self.current_subtotal_price = None
            self.current_subtotal_price_set = None
            self.current_total_tax = None
            self.current_total_tax_set = None
            self.customer_id = None
            self.customer = None
            self.customer_locale = None
            self.delivery_status = None
            self.discount_applications = None
            self.discount_codes = None
            self.email = None
            self.estimated_taxes = None
            self.financial_status = None
            self.fulfillments = None
            self.fulfillment_status = None
            self.id = None
            self.landing_site = None
            self.location_id = None
            self.merchant_of_record_app_id = None
            self.name = None
            self.note = None
            self.note_attributes = None
            self.number = None
            self.order_number = None
            self.original_total_additional_fees_set = None
            self.original_total_duties_set = None
            self.payment_terms = None
            self.payment_gateway_names = None
            self.phone = None
            self.po_number = None
            self.presentment_currency = None
            self.processed_at = None
            self.referring_site = None
            self.refunds = None
            self.shipping_address = None
            self.shipping_lines = None
            self.source_name = None
            self.source_identifier = None
            self.source_url = None
            self.subtotal_price = None
            self.subtotal_price_set = None
            self.tags = None
            self.tax_lines = None
            self.taxes_included = None
            self.test = None
            self.total_discounts = None
            self.total_discounts_set = None
            self.total_line_items_price = None
            self.total_line_items_price_set = None
            self.total_outstanding = None
            self.total_price = None
            self.total_price_set = None
            self.total_shipping_price_set = None
            self.total_tax = None
            self.total_tax_set = None
            self.total_tip_received = None
            self.total_weight = None
            self.updated_at = None
            self.user_id = None
            self.order_status_url = None
            self.token = None
            self.line_items = None
            self.admin_graphql_api_id = None
            self.checkout_id = None
            self.confirmed = None
            self.contact_email = None
            self.device_id = None
            self.landing_site_ref = None
            self.reference = None
            self.tax_exempt = None
            self.processed_date = None
            self.printed_date = None
            self.fulfilled_date = None
            self.shipping_label_url = None
            self.tracking_info = None
            self.error_description = None

        # Getter methods for each attribute
        def get_app_id(self):
            return self.app_id

        def get_billing_address(self):
            return self.billing_address

        def get_browser_ip(self):
            return self.browser_ip

        def get_buyer_accepts_marketing(self):
            return self.buyer_accepts_marketing

        def get_cancel_reason(self):
            return self.cancel_reason

        def get_cancelled_at(self):
            return self.cancelled_at

        def get_cart_token(self):
            return self.cart_token

        def get_checkout_token(self):
            return self.checkout_token

        def get_client_details(self):
            return self.client_details

        def get_closed_at(self):
            return self.closed_at

        def get_company(self):
            return self.company

        def get_confirmation_number(self):
            return self.confirmation_number

        def get_country_code(self):
            return self.country_code

        def get_created_at(self):
            return self.created_at

        def get_currency(self):
            return self.currency

        def get_current_total_additional_fees_set(self):
            return self.current_total_additional_fees_set

        def get_current_total_discounts(self):
            return self.current_total_discounts

        def get_current_total_discounts_set(self):
            return self.current_total_discounts_set

        def get_current_total_duties_set(self):
            return self.current_total_duties_set

        def get_current_total_price(self):
            return self.current_total_price

        def get_current_total_price_set(self):
            return self.current_total_price_set

        def get_current_subtotal_price(self):
            return self.current_subtotal_price

        def get_current_subtotal_price_set(self):
            return self.current_subtotal_price_set

        def get_current_total_tax(self):
            return self.current_total_tax

        def get_current_total_tax_set(self):
            return self.current_total_tax_set

        def get_customer_id(self):
            return self.customer_id

        def get_customer(self):
            return self.customer

        def get_customer_locale(self):
            return self.customer_locale

        def get_delivery_status(self):
            return self.delivery_status

        def get_discount_applications(self):
            return self.discount_applications

        def get_discount_codes(self):
            return self.discount_codes

        def get_email(self):
            return self.email

        def get_estimated_taxes(self):
            return self.estimated_taxes

        def get_financial_status(self):
            return self.financial_status

        def get_fulfillments(self):
            return self.fulfillments

        def get_fulfillment_status(self):
            return self.fulfillment_status

        def get_id(self):
            return self.id

        def get_landing_site(self):
            return self.landing_site

        def get_location_id(self):
            return self.location_id

        def get_merchant_of_record_app_id(self):
            return self.merchant_of_record_app_id

        def get_name(self):
            return self.name

        def get_note(self):
            return self.note

        def get_note_attributes(self):
            return self.note_attributes

        def get_number(self):
            return self.number

        def get_order_number(self):
            return self.order_number

        def get_original_total_additional_fees_set(self):
            return self.original_total_additional_fees_set

        def get_original_total_duties_set(self):
            return self.original_total_duties_set

        def get_payment_terms(self):
            return self.payment_terms

        def get_payment_gateway_names(self):
            return self.payment_gateway_names

        def get_phone(self):
            return self.phone

        def get_po_number(self):
            return self.po_number

        def get_presentment_currency(self):
            return self.presentment_currency

        def get_processed_at(self):
            return self.processed_at

        def get_referring_site(self):
            return self.referring_site

        def get_refunds(self):
            return self.refunds

        def get_shipping_address(self):
            return self.shipping_address

        def get_shipping_lines(self):
            return self.shipping_lines

        def get_source_name(self):
            return self.source_name

        def get_source_identifier(self):
            return self.source_identifier

        def get_source_url(self):
            return self.source_url

        def get_subtotal_price(self):
            return self.subtotal_price

        def get_subtotal_price_set(self):
            return self.subtotal_price_set

        def get_tags(self):
            return self.tags

        def get_tax_lines(self):
            return self.tax_lines

        def get_taxes_included(self):
            return self.taxes_included

        def get_test(self):
            return self.test

        def get_total_discounts(self):
            return self.total_discounts

        def get_total_discounts_set(self):
            return self.total_discounts_set

        def get_total_line_items_price(self):
            return self.total_line_items_price

        def get_total_line_items_price_set(self):
            return self.total_line_items_price_set

        def get_total_outstanding(self):
            return self.total_outstanding

        def get_total_price(self):
            return self.total_price

        def get_total_price_set(self):
            return self.total_price_set

        def get_total_shipping_price_set(self):
            return self.total_shipping_price_set

        def get_total_tax(self):
            return self.total_tax

        def get_total_tax_set(self):
            return self.total_tax_set

        def get_total_tip_received(self):
            return self.total_tip_received

        def get_total_weight(self):
            return self.total_weight

        def get_updated_at(self):
            return self.updated_at

        def get_user_id(self):
            return self.user_id

        def get_order_status_url(self):
            return self.order_status_url

        def get_token(self):
            return self.token

        def get_line_items(self):
            return self.line_items

        def get_admin_graphql_api_id(self):
            return self.admin_graphql_api_id

        def get_checkout_id(self):
            return self.checkout_id

        def get_confirmed(self):
            return self.confirmed

        def get_contact_email(self):
            return self.contact_email

        def get_device_id(self):
            return self.device_id

        def get_landing_site_ref(self):
            return self.landing_site_ref

        def get_reference(self):
            return self.reference

        def get_tax_exempt(self):
            return self.tax_exempt

        def get_processed_date(self):
            return self.processed_date

        def get_printed_date(self):
            return self.printed_date

        def get_fulfilled_date(self):
            return self.fulfilled_date

        def get_shipping_label_url(self):
            return self.shipping_label_url

        def get_tracking_info(self):
            return self.tracking_info

        def get_error_description(self):
            return self.error_description

        # Setter methods for each attribute
        def set_app_id(self, value):
            self.app_id = value

        def set_billing_address(self, value):
            self.billing_address = value

        def set_browser_ip(self, value):
            self.browser_ip = value

        def set_buyer_accepts_marketing(self, value):
            self.buyer_accepts_marketing = value

        def set_cancel_reason(self, value):
            self.cancel_reason = value

        def set_cancelled_at(self, value):
            self.cancelled_at = value

        def set_cart_token(self, value):
            self.cart_token = value

        def set_checkout_token(self, value):
            self.checkout_token = value

        def set_client_details(self, value):
            self.client_details = value

        def set_closed_at(self, value):
            self.closed_at = value

        def set_company(self, value):
            self.company = value

        def set_confirmation_number(self, value):
            self.confirmation_number = value

        def set_country_code(self, value):
            self.country_code = value

        def set_created_at(self, value):
            self.created_at = value

        def set_currency(self, value):
            self.currency = value

        def set_current_total_additional_fees_set(self, value):
            self.current_total_additional_fees_set = value

        def set_current_total_discounts(self, value):
            self.current_total_discounts = value

        def set_current_total_discounts_set(self, value):
            self.current_total_discounts_set = value

        def set_current_total_duties_set(self, value):
            self.current_total_duties_set = value

        def set_current_total_price(self, value):
            self.current_total_price = value

        def set_current_total_price_set(self, value):
            self.current_total_price_set = value

        def set_current_subtotal_price(self, value):
            self.current_subtotal_price = value

        def set_current_subtotal_price_set(self, value):
            self.current_subtotal_price_set = value

        def set_current_total_tax(self, value):
            self.current_total_tax = value

        def set_current_total_tax_set(self, value):
            self.current_total_tax_set = value

        def set_customer_id(self, value):
            self.customer_id = value

        def set_customer(self, value):
            self.customer = value

        def set_customer_locale(self, value):
            self.customer_locale = value

        def set_delivery_status(self, value):
            self.delivery_status = value

        def set_discount_applications(self, value):
            self.discount_applications = value

        def set_discount_codes(self, value):
            self.discount_codes = value

        def set_email(self, value):
            self.email = value

        def set_estimated_taxes(self, value):
            self.estimated_taxes = value

        def set_financial_status(self, value):
            self.financial_status = value

        def set_fulfillments(self, value):
            self.fulfillments = value

        def set_fulfillment_status(self, value):
            self.fulfillment_status = value

        def set_id(self, value):
            self.id = value

        def set_landing_site(self, value):
            self.landing_site = value

        def set_location_id(self, value):
            self.location_id = value

        def set_merchant_of_record_app_id(self, value):
            self.merchant_of_record_app_id = value

        def set_name(self, value):
            self.name = value

        def set_note(self, value):
            self.note = value

        def set_note_attributes(self, value):
            self.note_attributes = value

        def set_number(self, value):
            self.number = value

        def set_order_number(self, value):
            self.order_number = value

        def set_original_total_additional_fees_set(self, value):
            self.original_total_additional_fees_set = value

        def set_original_total_duties_set(self, value):
            self.original_total_duties_set = value

        def set_payment_terms(self, value):
            self.payment_terms = value

        def set_payment_gateway_names(self, value):
            self.payment_gateway_names = value

        def set_phone(self, value):
            self.phone = value

        def set_po_number(self, value):
            self.po_number = value

        def set_presentment_currency(self, value):
            self.presentment_currency = value

        def set_processed_at(self, value):
            self.processed_at = value

        def set_referring_site(self, value):
            self.referring_site = value

        def set_refunds(self, value):
            self.refunds = value

        def set_shipping_address(self, value):
            self.shipping_address = value

        def set_shipping_lines(self, value):
            self.shipping_lines = value

        def set_source_name(self, value):
            self.source_name = value

        def set_source_identifier(self, value):
            self.source_identifier = value

        def set_source_url(self, value):
            self.source_url = value

        def set_subtotal_price(self, value):
            self.subtotal_price = value

        def set_subtotal_price_set(self, value):
            self.subtotal_price_set = value

        def set_tags(self, value):
            self.tags = value

        def set_tax_lines(self, value):
            self.tax_lines = value

        def set_taxes_included(self, value):
            self.taxes_included = value

        def set_test(self, value):
            self.test = value

        def set_total_discounts(self, value):
            self.total_discounts = value

        def set_total_discounts_set(self, value):
            self.total_discounts_set = value

        def set_total_line_items_price(self, value):
            self.total_line_items_price = value

        def set_total_line_items_price_set(self, value):
            self.total_line_items_price_set = value

        def set_total_outstanding(self, value):
            self.total_outstanding = value

        def set_total_price(self, value):
            self.total_price = value

        def set_total_price_set(self, value):
            self.total_price_set = value

        def set_total_shipping_price_set(self, value):
            self.total_shipping_price_set = value

        def set_total_tax(self, value):
            self.total_tax = value

        def set_total_tax_set(self, value):
            self.total_tax_set = value

        def set_total_tip_received(self, value):
            self.total_tip_received = value

        def set_total_weight(self, value):
            self.total_weight = value

        def set_updated_at(self, value):
            self.updated_at = value

        def set_user_id(self, value):
            self.user_id = value

        def set_order_status_url(self, value):
            self.order_status_url = value

        def set_token(self, value):
            self.token = value

        def set_line_items(self, value):
            self.line_items = value

        def set_admin_graphql_api_id(self, value):
            self.admin_graphql_api_id = value

        def set_checkout_id(self, value):
            self.checkout_id = value

        def set_confirmed(self, value):
            self.confirmed = value

        def set_contact_email(self, value):
            self.contact_email = value

        def set_device_id(self, value):
            self.device_id = value

        def set_landing_site_ref(self, value):
            self.landing_site_ref = value

        def set_reference(self, value):
            self.reference = value

        def set_tax_exempt(self, value):
            self.tax_exempt = value

        def set_processed_date(self, value):
            self.processed_date = value

        def set_printed_date(self, value):
            self.printed_date = value

        def set_fulfilled_date(self, value):
            self.fulfilled_date = value

        def set_shipping_label_url(self, value):
            self.shipping_label_url = value

        def set_tracking_info(self, value):
            self.tracking_info = value

        def set_error_description(self, value):
            self.error_description = value

    class OrderLineItem():
        def __init__(self):
            self.id = None
            self.order_id = None
            self.attributed_staffs = None
            self.fulfillable_quantity = None
            self.fulfillment_service = None
            self.fulfillment_status = None
            self.grams = None
            self.price = None
            self.price_set = None
            self.product_exists = None
            self.product_id = None
            self.quantity = None
            self.requires_shipping = None
            self.sku = None
            self.title = None
            self.variant_id = None
            self.variant_inventory_management = None
            self.variant_title = None
            self.vendor = None
            self.name = None
            self.gift_card = None
            self.properties = None
            self.taxable = None
            self.tax_lines = None
            self.total_discount = None
            self.total_discount_set = None
            self.discount_allocations = None
            self.duties = None
            self.admin_graphql_api_id = None

        # Getter methods
        def get_id(self):
            return self.id

        def get_order_id(self):
            return self.order_id

        def get_attributed_staffs(self):
            return self.attributed_staffs

        def get_fulfillable_quantity(self):
            return self.fulfillable_quantity

        def get_fulfillment_service(self):
            return self.fulfillment_service

        def get_fulfillment_status(self):
            return self.fulfillment_status

        def get_grams(self):
            return self.grams

        def get_price(self):
            return self.price

        def get_price_set(self):
            return self.price_set

        def get_product_exists(self):
            return self.product_exists

        def get_product_id(self):
            return self.product_id

        def get_quantity(self):
            return self.quantity

        def get_requires_shipping(self):
            return self.requires_shipping

        def get_sku(self):
            return self.sku

        def get_title(self):
            return self.title

        def get_variant_id(self):
            return self.variant_id

        def get_variant_inventory_management(self):
            return self.variant_inventory_management

        def get_variant_title(self):
            return self.variant_title

        def get_vendor(self):
            return self.vendor

        def get_name(self):
            return self.name

        def get_gift_card(self):
            return self.gift_card

        def get_properties(self):
            return self.properties

        def get_taxable(self):
            return self.taxable

        def get_tax_lines(self):
            return self.tax_lines

        def get_total_discount(self):
            return self.total_discount

        def get_total_discount_set(self):
            return self.total_discount_set

        def get_discount_allocations(self):
            return self.discount_allocations

        def get_duties(self):
            return self.duties

        def get_admin_graphql_api_id(self):
            return self.admin_graphql_api_id

        # Setter methods
        def set_id(self, value):
            self.id = value

        def set_order_id(self, value):
            self.order_id = value

        def set_attributed_staffs(self, value):
            self.attributed_staffs = value

        def set_fulfillable_quantity(self, value):
            self.fulfillable_quantity = value

        def set_fulfillment_service(self, value):
            self.fulfillment_service = value

        def set_fulfillment_status(self, value):
            self.fulfillment_status = value

        def set_grams(self, value):
            self.grams = value

        def set_price(self, value):
            self.price = value

        def set_price_set(self, value):
            self.price_set = value

        def set_product_exists(self, value):
            self.product_exists = value

        def set_product_id(self, value):
            self.product_id = value

        def set_quantity(self, value):
            self.quantity = value

        def set_requires_shipping(self, value):
            self.requires_shipping = value

        def set_sku(self, value):
            self.sku = value

        def set_title(self, value):
            self.title = value

        def set_variant_id(self, value):
            self.variant_id = value

        def set_variant_inventory_management(self, value):
            self.variant_inventory_management = value

        def set_variant_title(self, value):
            self.variant_title = value

        def set_vendor(self, value):
            self.vendor = value

        def set_name(self, value):
            self.name = value

        def set_gift_card(self, value):
            self.gift_card = value

        def set_properties(self, value):
            self.properties = value

        def set_taxable(self, value):
            self.taxable = value

        def set_tax_lines(self, value):
            self.tax_lines = value

        def set_total_discount(self, value):
            self.total_discount = value

        def set_total_discount_set(self, value):
            self.total_discount_set = value

        def set_discount_allocations(self, value):
            self.discount_allocations = value

        def set_duties(self, value):
            self.duties = value

        def set_admin_graphql_api_id(self, value):
            self.admin_graphql_api_id = value

    class CustomOrdersManagement():
        def __init__(self):
            self.order_processing_id = None
            self.id = None
            self.name = None
            self.country = None
            self.country_code = None
            self.location_id = None
            self.location_name = None
            self.created_at = None
            self.font = None
            self.custom_text = None
            self.prod_name = None
            self.prod_sku = None
            self.quantity = None
            self.vendor_id = None
            self.vendor_name = None
            self.sent_to_vendor_date = None
            self.sorority_flg = None
            self.sent_by_vendor_flg = None
            self.shipped_date = None
            self.tracking_number = None
            self.tracking_url = None
            # self.tracking_delivered_date = None
            self.received_flg = None
            self.received_date = None
            self.received_by = None
            self.received_notes = None
            self.fulfilled_flg = None
            self.fulfilled_date = None
            self.vendor_processing_time = None
            self.vendor_processing_time_unit = None
            self.total_shipping_time = None
            self.total_shipping_time_unit = None
            self.total_processing_time = None
            self.total_processing_time_unit = None

        # Getters
        def get_order_processing_id(self):
            return self.order_processing_id

        def get_id(self):
            return self.id

        def get_name(self):
            return self.name

        def get_country(self):
            return self.country

        def get_country_code(self):
            return self.country_code

        def get_location_id(self):
            return self.location_id

        def get_location_name(self):
            return self.location_name

        def get_created_at(self):
            return self.created_at

        def get_font(self):
            return self.font

        def get_custom_text(self):
            return self.custom_text

        def get_prod_name(self):
            return self.prod_name

        def get_prod_sku(self):
            return self.prod_sku

        def get_quantity(self):
            return self.quantity

        def get_vendor_id(self):
            return self.vendor_id

        def get_vendor_name(self):
            return self.vendor_name

        def get_sent_to_vendor_date(self):
            return self.sent_to_vendor_date

        def get_sorority_flg(self):
            return self.sorority_flg

        def get_sent_by_vendor_flg(self):
            return self.sent_by_vendor_flg

        def get_shipped_date(self):
            return self.shipped_date

        def get_tracking_number(self):
            return self.tracking_number

        def get_tracking_url(self):
            return self.tracking_url

        # def get_tracking_delivered_date(self):
        #     return self.tracking_delivered_date

        def get_received_flg(self):
            return self.received_flg

        def get_received_date(self):
            return self.received_date

        def get_received_by(self):
            return self.received_by

        def get_received_notes(self):
            return self.received_notes

        def get_fulfilled_flg(self):
            return self.fulfilled_flg

        def get_fulfilled_date(self):
            return self.fulfilled_date

        def get_vendor_processing_time(self):
            return self.vendor_processing_time

        def get_vendor_processing_time_unit(self):
            return self.vendor_processing_time_unit

        def get_total_shipping_time(self):
            return self.total_shipping_time

        def get_total_shipping_time_unit(self):
            return self.total_shipping_time_unit

        def get_total_processing_time(self):
            return self.total_processing_time

        def get_total_processing_time_unit(self):
            return self.total_processing_time_unit

        # Setters
        def set_order_processing_id(self, order_processing_id):
            self.order_processing_id = order_processing_id

        def set_id(self, id):
            self.id = id

        def set_name(self, name):
            self.name = name

        def set_country(self, country):
            self.country = country

        def set_country_code(self, country_code):
            self.country_code = country_code

        def set_location_id(self, location_id):
            self.location_id = location_id

        def set_location_name(self, location_name):
            self.location_name = location_name

        def set_created_at(self, created_at):
            self.created_at = created_at

        def set_font(self, font):
            self.font = font

        def set_custom_text(self, custom_text):
            self.custom_text = custom_text

        def set_prod_name(self, prod_name):
            self.prod_name = prod_name

        def set_prod_sku(self, prod_sku):
            self.prod_sku = prod_sku

        def set_quantity(self, quantity):
            self.quantity = quantity

        def set_vendor_id(self, vendor_id):
            self.vendor_id = vendor_id

        def set_vendor_name(self, vendor_name):
            self.vendor_name = vendor_name

        def set_sent_to_vendor_date(self, sent_to_vendor_date):
            self.sent_to_vendor_date = sent_to_vendor_date

        def set_sorority_flg(self, sorority_flg):
            self.sorority_flg = sorority_flg

        def set_sent_by_vendor_flg(self, sent_by_vendor_flg):
            self.sent_by_vendor_flg = sent_by_vendor_flg

        def set_shipped_date(self, shipped_date):
            self.shipped_date = shipped_date

        def set_tracking_number(self, tracking_number):
            self.tracking_number = tracking_number

        def set_tracking_url(self, tracking_url):
            self.tracking_url = tracking_url

        # def set_tracking_delivered_date(self, tracking_delivered_date):
        #     self.tracking_delivered_date = tracking_delivered_date

        def set_received_flg(self, received_flg):
            self.received_flg = received_flg

        def set_received_date(self, received_date):
            self.received_date = received_date

        def set_received_by(self, received_by):
            self.received_by = received_by

        def set_received_notes(self, received_notes):
            self.received_notes = received_notes

        def set_fulfilled_flg(self, fulfilled_flg):
            self.fulfilled_flg = fulfilled_flg

        def set_fulfilled_date(self, fulfilled_date):
            self.fulfilled_date = fulfilled_date

        def set_vendor_processing_time(self, vendor_processing_time):
            self.vendor_processing_time = vendor_processing_time

        def set_vendor_processing_time_unit(self, vendor_processing_time_unit):
            self.vendor_processing_time_unit = vendor_processing_time_unit

        def set_total_shipping_time(self, total_shipping_time):
            self.total_shipping_time = total_shipping_time

        def set_total_shipping_time_unit(self, total_shipping_time_unit):
            self.total_shipping_time_unit = total_shipping_time_unit

        def set_total_processing_time(self, total_processing_time):
            self.total_processing_time = total_processing_time

        def set_total_processing_time_unit(self, total_processing_time_unit):
            self.total_processing_time_unit = total_processing_time_unit

    # GETTERS
    def get_order_count(self):
        return self.order_count

    def get_total_order_count(self):
        return self.total_order_count

    def get_total_order_inserted_count(self):
        return self.total_order_inserted_count

    def get_total_order_updated_count(self):
        return self.total_order_updated_count

    def get_total_order_deleted_count(self):
        return self.total_order_deleted_count

    def get_total_order_with_errors(self):
        return self.total_order_with_errors

    def get_order_with_error_array(self):
        return self.order_with_error_array

    def get_order_line_item_count(self):
        return self.order_line_item_count

    def get_total_order_line_item_count(self):
        return self.total_order_line_item_count

    def get_total_order_line_item_inserted_count(self):
        return self.total_order_line_item_inserted_count

    def get_total_order_line_item_updated_count(self):
        return self.total_order_line_item_updated_count

    def get_total_order_line_item_deleted_count(self):
        return self.total_order_line_item_deleted_count

    def get_total_order_line_item_with_errors(self):
        return self.total_order_line_item_with_errors

    def get_order_line_item_with_error_array(self):
        return self.order_line_item_with_error_array

    def get_module_name(self):
        return self.module_name

    # SETTERS
    def set_order_count(self, value):
        self.order_count = value

    def set_total_order_count(self, value):
        self.total_order_count = value

    def set_total_order_inserted_count(self, value):
        self.total_order_inserted_count = value

    def set_total_order_updated_count(self, value):
        self.total_order_updated_count = value

    def set_total_order_deleted_count(self, value):
        self.total_order_deleted_count = value

    def set_total_order_with_errors(self, value):
        self.total_order_with_errors = value

    def set_order_line_item_count(self, value):
        self.order_line_item_count = value

    def set_total_order_line_item_count(self, value):
        self.total_order_line_item_count = value

    def set_total_order_line_item_inserted_count(self, value):
        self.total_order_line_item_inserted_count = value

    def set_total_order_line_item_updated_count(self, value):
        self.total_order_line_item_updated_count = value

    def set_total_order_line_item_deleted_count(self, value):
        self.total_order_line_item_deleted_count = value

    def set_total_order_line_item_with_errors(self, value):
        self.total_order_line_item_with_errors = value

    # UTILS
    def append_order_with_error_array(self, order_id):
        self.order_with_error_array.append(order_id)

    def append_order_line_item_with_error_array(self, order_line_item_id):
        self.order_line_item_with_error_array.append(order_line_item_id)

    def clear_order_with_error_array(self):
        self.order_with_error_array = []

    def clear_order_line_item_with_error_array(self):
        self.order_line_item_with_error_array = []

    def clear_counters(self):
        self.set_total_order_count(0)
        self.set_total_order_inserted_count(0)
        self.set_total_order_updated_count(0)
        self.set_total_order_deleted_count(0)
        self.set_total_order_with_errors(0)

    def clear_line_items_counters(self):
        self.set_total_order_line_item_count(0)
        self.set_total_order_line_item_inserted_count(0)
        self.set_total_order_line_item_updated_count(0)
        self.set_total_order_line_item_deleted_count(0)
        self.set_total_order_line_item_with_errors(0)

    def execution_summary(self, is_webhook=False, send_email=False):
        if not is_webhook:
            print('\n\n[DONE] Finished getting orders because it ran the maximum runs.')
            print('\n===========================================================')
            print('[INFO] Resume of Execution:')
            print('===========================================================')
            print(f'[INFO] Total number of Orders:\t\t\t\t{self.get_total_order_count()}')
            print(f'[INFO] Total number of Orders Inserted:\t\t\t{self.get_total_order_inserted_count()}')
            print(f'[INFO] Total number of Orders Updated:\t\t\t{self.get_total_order_updated_count()}')
            print(f'[INFO] Total number of Orders Deleted:\t\t\t{self.get_total_order_deleted_count()}')
            print('\n===========================================================')
            print('[INFO] Errors:')
            print('===========================================================')
            print(f'[INFO] Total number of Orders with error:\t\t{self.get_total_order_with_errors()}')
            print(f'[INFO] Orders with error:\t\t\t\t{self.get_order_with_error_array()}')
            print('===========================================================')
            print('\n[INFO] Execution ended.\n\n')
        else:
            total_errors = self.get_total_order_with_errors() #+ self.get_total_order_line_item_with_errors()

            if total_errors > 0:
                print('===========================================================')
                if self.get_total_order_with_errors():
                    print(f'[ERROR] Orders with error:\t\t{self.get_order_with_error_array()}')
                print('===========================================================')

        if send_email:
            print('Sending email notification...')
            email_body = ""
            email_body += '[DONE] Finished getting orders because it ran the maximum runs.'
            email_body += '\n==========================================================='
            email_body += '\n[INFO] Resume of Execution:'
            email_body += '\n==========================================================='
            email_body += f'\n[INFO] Total number of Orders:\t\t\t\t{self.get_total_order_count()}'
            email_body += f'\n[INFO] Total number of Orders Inserted:\t\t\t{self.get_total_order_inserted_count()}'
            email_body += f'\n[INFO] Total number of Orders Updated:\t\t\t{self.get_total_order_updated_count()}'
            email_body += f'\n[INFO] Total number of Orders Deleted:\t\t\t{self.get_total_order_deleted_count()}'
            email_body += '\n==========================================================='
            email_body += '\n[INFO] Errors:'
            email_body += '\n==========================================================='
            email_body += f'\n[INFO] Total number of Orders with error:\t\t{self.get_total_order_with_errors()}'
            email_body += f'\n[INFO] Orders with error:\t\t\t\t{self.get_order_with_error_array()}'
            email_body += '\n==========================================================='
            if self.utils.get_start_time() != 0 and self.utils.get_end_time() != 0:
                hours, minutes, seconds = self.utils.get_total_time_hms(start_time=self.utils.get_start_time(), end_time=self.utils.get_end_time())
                email_body += f'\n\n[INFO] Initial time: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.utils.get_start_time()))}'
                email_body += f'\n[INFO] Final time: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.utils.get_end_time()))}'
                email_body += f'\n[INFO] Total time taken: {hours} hours, {minutes} minutes, {seconds} seconds\n\n'
            email_body += '\n\n[INFO] Execution ended.\n\n'

            email_subject = "[Orders] Resume of execution"
            email_to = ['xxxxxx@COMPANY_NAME.com']
            email_from = "xxxxxxxx@gmail.com"
            self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_names=None, file_path=None)

        self.clear_counters()
        self.clear_order_with_error_array()

    def print_log(self, log_level, type, function, order_id, item_id, try_count, message):
        final_message = f"[INFO] " if log_level == "info" else f"[ERROR] " if log_level == "error" else f"[WARNING] " if log_level == "warning" else f"[DEBUG] " if log_level == "debug" else f"[CRITICAL] "
        if try_count is not None and try_count != "":
            final_message += f"Trying to "
            final_message += f"insert " if function == "insert" else f"update " if function == "update" else f"delete " if function == "delete" else f"get " if function == "get" else f"process " if function == "process" else f"save " if function == "save" else f"verify " if function == "verify" else f"execute "
            final_message += f"order " if type == "order" else f"line item "
            final_message += f"again...\t\tOrder: {order_id}" if type == "order" else f"again...\tOrder: {order_id}\tItem: {item_id}"
            final_message += f".Try: {try_count}"
        else:
            final_message += f"Order " if type == "order" else f"Line Item "
            final_message += f"Inserted..." if function == "insert" else f"Updated... " if function == "update" else f"Deleted... "
            final_message += f"\t\t\t"
            final_message += f"Order: {order_id}" if type == "order" else f"Order: {order_id}\tItem: {item_id}"
        final_message += f"{message}" if message is not None and message != "" else ""

        print(final_message)

    def print_fulfillment_log(self, count_orders, total_orders_to_fulfill, order_number, order_fulfillment_status, tracking_number, line_item_id, fulfillment_id, fulfillment_status, description):
        print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}{line_item_id}{fulfillment_id}{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}{description}")

    # DATABASE FUNCTIONS
    def get_last_processed_order_id_batch(self):
        print(f"\n[INFO] BEGIN - Getting last processed order id...")
        try:
            # Variables
            columns = ["MAX(LAST_ORDER_ID) AS LAST_ORDER_ID"]
            condition = "1=1"
            result_flag, result_query = super().query_record(super().get_tbl_ORDER_PROCESSING_BATCHES(), columns, condition)
            last_order_id = '0'
            row = None

            if result_flag:
                for row in result_query:
                    last_order_id = row.get("LAST_ORDER_ID")
            else:
                last_order_id = '0'

            return last_order_id
        except Exception as e:
            print(f"[ERROR] get_last_processed_order_id_batch: {str(e)}")
            return last_order_id
        finally:
            print(f"[INFO] END - Got last processed order id... Last Order ID: {last_order_id}")
            print(f"[INFO] Clearing variables...")
            try:
                del columns
                del condition
                del result_query
                del result_flag
                del row
                del last_order_id
            except:
                pass

    def get_last_processed_order_id(self, process_type):
        print(f"[INFO] BEGIN - Getting last processed order id for process type: {process_type}")
        # Variables
        return_flag = False
        result_query = None
        last_processed_order_id = None
        columns = ["MAX(LAST_ID) AS LAST_ID"]
        condition = "1=1"
        condition += f"\nAND PROCESS_TYPE = '{process_type}'"

        try:
            return_flag, result_query = super().query_record(super().get_tbl_ORDER_PROCESSING(), columns, condition)
            
            if return_flag:
                for row in result_query:
                    last_processed_order_id = row.get("LAST_ID")
                print(f"[INFO] Last processed order id: {last_processed_order_id}")
                return last_processed_order_id
            else:
                return None
        except Exception as e:
            print(f"[ERROR] - {e}")
            return None
        finally:
            print(f"[INFO] END - Got last processed order Id")
            print("[INFO] - Cleaning up variables")
            try:
                del return_flag
                del result_query
                del last_processed_order_id
                del columns
                del condition
            except:
                pass

    def get_order_ids_for_processing(self, first_id, last_id, created_at_min, created_at_max, limit):
        print(f"\n[INFO] BEGIN - Getting Order Ids for Processing...")
        try:
            # Variables
            result_query = None
            rows = None
            row = None
            order_ids = []
            last_order_id = None

            columns = ["ID"]
            condition = "1=1"
            condition += f"\nAND ID > '{first_id}'" if first_id is not None and first_id != "" else ""
            condition += f"\nAND ID < '{last_id}'" if last_id is not None and last_id != "" else ""
            condition += f"\nAND CREATED_AT >= '{created_at_min}'" if created_at_min is not None and created_at_min != "" else ""
            condition += f"\nAND CREATED_AT <= '{created_at_max}'" if created_at_max is not None and created_at_max != "" else ""
            condition += f"\nAND FULFILLMENT_STATUS IS NULL"
            condition += f"\nAND CANCELLED_AT IS NULL"
            condition += f"\nAND PROCESSED_DATE IS NULL"
            condition += f"\nAND PRINTED_DATE IS NULL"
            condition += f"\nAND FULFILLED_DATE IS NULL"
            condition += f"\nAND SHIPPING_LABEL_URL IS NULL"
            condition += f"\nAND TRACKING_INFO IS NULL"
            condition += f"\nAND TOTAL_PRICE > 0"
            condition += f"\nAND TAGS NOT LIKE '%XXXXXXXX%'"
            condition += f"\nORDER BY ID ASC"
            condition += f"\nLIMIT {limit}" if limit is not None and limit != "" and limit != "0" else "\nLIMIT 100"

            result_flag, result_query = super().query_record(super().get_tbl_ORDER(), columns, condition)

            if result_flag:
                for row in result_query:
                    last_order_id = row.get("ID")
                    order_ids.append(last_order_id)
            else:
                order_ids = []

            return order_ids, last_order_id
        except Exception as e:
            print(f"[ERROR] get_order_ids_for_processing: {str(e)}")
            return [], None
        finally:
            print(f"[INFO] END - Got {len(order_ids)} Order Ids for Processing... Last Order ID: {last_order_id}")
            print(f"[INFO] Clearing variables...")
            try:
                del columns
                del condition
                del result_query
                del result_flag
                del row
                del order_ids
                del last_order_id
            except:
                pass

    def get_all_custom_orders(self, since_id, created_at):
        print(f"\n[INFO] BEGIN - Getting Custom Orders...")
        columns = ["ID", "NAME", "CREATED_AT", "TAGS", "COUNTRY_CODE"]
        condition = "1=1"
        # condition += f"\nAND ID > '{since_id}'"
        # condition += f"\nAND ID > '4944443736141'"
        # condition += f"\nAND ID <= '4906226647117'"
        condition += f"\nAND NAME IN ('#A3096976', '#A3095605', '#A3095636')"
        # condition += f"\nAND CREATED_AT < DATE_FORMAT('{created_at}', '%Y-%m-%d %H:%i:%s')"
        # condition += f"\nORDER BY ID ASC"
        # condition += f"\nLIMIT 10"
        return_flag = False
        result_query = None

        try:
            return_flag, result_query = super().query_record(super().get_view_UNF_CUSTOMS_ORDERS_VIEW(), columns, condition)

            return return_flag, result_query
        except Exception as e:
            self.utils.send_exception_email(module=self.get_module_name(), function="get_all_custom_orders", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
            print(f"[ERROR] Error while getting custom orders: {str(e)}")
            return False, str(e)
        finally:
            print(f"[INFO] END - Got Custom Orders.")
            print(f"[INFO] Clearing variables...")
            try:
                del columns, condition, result_query, return_flag
            except:
                pass

    def get_all_custom_order_line_items(self, order_id):
        # print(f"\n[INFO] BEGIN - Getting Custom Order Line Items...")
        columns = ["ID", "ORDER_ID", "NAME", "QUANTITY", "PRICE", "SKU", "PRODUCT_ID", "TITLE", "VARIANT_ID", "VARIANT_TITLE", "VENDOR", "PROPERTIES"]
        condition = "1=1"
        condition += f"\nAND ORDER_ID = '{order_id}'"
        condition += f"\nAND TITLE LIKE '%Custom/Personalized%'"
        return_flag = False
        result_query = None

        try:
            return_flag, result_query = super().query_record(super().get_tbl_ORDER_LINE_ITEM(), columns, condition)
            return return_flag, result_query
        except Exception as e:
            print(f"[ERROR] Error while getting custom order line items: {str(e)}")
            return False, None
        finally:
            # print(f"[INFO] END - Got Custom Order Line Items.")
            # print(f"[INFO] Clearing variables...")
            try:
                del columns
                del condition
                del result_query
                del return_flag
            except:
                pass

    def insert_processed_order_id(self, process_type, first_id, last_id, total_orders, status):
        print(f"[INFO] BEGIN - Inserting new processed orders ids for process type: {process_type}")

        columns = ["PROCESS_TYPE", "FIRST_ID", "LAST_ID", "TOTAL_ORDERS", "STATUS"]
        values = []
        row_id = None
        return_flag = False
        rowcount = None
        return_string = None

        if process_type is None or process_type == "":
            return False, "Process Type is required."
        if first_id is None or first_id == "":
            return False, "First ID is required."
        if last_id is None or last_id == "":
            return False, "Last ID is required."

        status = "Created" if status is None or status == "" else status
        total_orders = 0 if total_orders is None or total_orders == "" else int(total_orders)

        try:
            values = [process_type, first_id, last_id, total_orders, status]
            return_flag, rowcount, return_string = super().insert_record(super().get_tbl_ORDER_PROCESSING(), columns, values)
            if return_flag:
                columns = ["ROW_ID"]
                condition = "1=1"
                condition += f"\nAND PROCESS_TYPE = '{process_type}'"
                condition += f"\nAND FIRST_ID = '{first_id}'"
                condition += f"\nAND LAST_ID = '{last_id}'"
                condition += f"\nAND TOTAL_ORDERS = '{total_orders}'"
                condition += f"\nAND STATUS = '{status}'"
                return_flag, result_query = super().query_record(super().get_tbl_ORDER_PROCESSING(), columns, condition)

                if return_flag:
                    for row in result_query:
                        row_id = row.get("ROW_ID")

                    return True, rowcount, return_string, row_id
            else:
                return False, 0, return_string, row_id
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, 0, str(e), row_id
        finally:
            print(f"[INFO] END - Inserted new processed orders ids for process type: {process_type}. Row ID: {row_id}")
            print("[INFO] Cleaning up variables")
            try:
                del columns
                del values
                del row_id
                del return_flag
                del rowcount
                del return_string
            except:
                pass

    def update_processed_order_id(self, row_id, process_type, first_id, last_id, total_orders, status):
        print(f"\n[INFO] BEGIN - Updating processed orders ids...")

        columns = []
        values = []
        return_flag = False
        rowcount = None
        return_string = None
        condition = f"ROW_ID = '{row_id}'"

        if row_id is None or row_id == "":
            return False, 0, "Row ID is required."
        if process_type is None and first_id is None and last_id is None and total_orders is None and status is None:
            return False, 0, "At least one field is required to update."

        if process_type is not None and process_type != "":
            columns.append("PROCESS_TYPE")
            values.append(process_type)
        if first_id is not None and first_id != "":
            columns.append("FIRST_ID")
            values.append(first_id)
        if last_id is not None and last_id != "":
            columns.append("LAST_ID")
            values.append(last_id)
        if total_orders is not None and total_orders != "":
            columns.append("TOTAL_ORDERS")
            values.append(total_orders)
        if status is not None and status != "":
            columns.append("STATUS")
            values.append(status)

        try:
            return_flag, rowcount, return_string = super().update_record(super().get_tbl_ORDER_PROCESSING(), columns, values, condition)
            return return_flag, rowcount, return_string
        except Exception as e:
            print(f"[ERROR] - {e}")
            return False, 0, str(e)
        finally:
            print(f"[INFO] END - Updated processed orders ids.")
            print("[INFO] - Cleaning up variables")
            try:
                del columns
                del values
                del return_flag
                del rowcount
                del return_string
                del condition
            except:
                pass

    # Function to count the number of orders in the database
    def verify_order_exits(self, orderId):
        count = 0
        columns = ["COUNT(*) AS TOTAL"]
        condition = (f"ID = '{orderId}'")

        result_flag, result_query = super().query_record(super().get_tbl_ORDER(), columns, condition)

        if result_flag:
            for row in result_query:
                count = row.get("TOTAL")
        else:
            count = 0

        try:
            del result_query
            del columns
            del result_flag
            del condition
        except:
            pass

        return False if count <= 0 else True

    # Function to count the number of order line items in the database
    def verify_order_line_item_exists(self, orderLineItemId):
        count = 0
        columns = ["COUNT(*) AS TOTAL"]
        condition = f"ID = '{orderLineItemId}'"

        result_flag, result_query = super().query_record(super().get_tbl_ORDER_LINE_ITEM(), columns, condition)

        if result_flag:
            for row in result_query:
                count = row.get("TOTAL")
        else:
            count = 0

        try:
            del result_query
            del columns
            del result_flag
            del condition
        except:
            pass

        return False if count <= 0 else True

    # Function to save the JSONs of order received from Shopify in the staging table (ORDER_STAGING)
    def save_order_on_staging_table(self, order_json):
        row_Inserted_Flag = False
        return_string = "Success"

        sql_header = f"INSERT INTO {super().get_DB_OWNER()}.{super().get_tbl_ORDER_STAGING()}"
        sql_column = f" (ORDER_JSON)"
        sql_value = f" VALUES ('{self.utils.replace_special_chars(self.utils.convert_object_to_json(order_json))}')"
        sql_final_cmd = sql_header + sql_column + sql_value

        row_Inserted_Flag, rowcount, return_string = super().exec_db_cmd(sql_final_cmd)

        try:
            del sql_header
            del sql_column
            del sql_value
            del sql_final_cmd
        except:
            pass

        return row_Inserted_Flag, rowcount, return_string

    # Function to read the staging table (ORDER_STAGING) and create the orders in the final tables (ORDER and ORDER_LINE_ITEM)
    # NOTE: This function has no end. It runs forever on purpose.
    def process_orders_from_staging_table(self, limit, remove_orders=False):
        while True:
            try:
                print(f"\n[INFO] Processing orders from staging table...\tLimit: {limit}")

                # Query Variables
                columns_array = ["ROW_ID", "ORDER_JSON"]
                condition = "1=1"
                condition += f"\nORDER BY ROW_ID ASC"
                condition += f"\nLIMIT {str(limit)}" if limit is not None and limit != "" and limit != "0" else "\nLIMIT 1000"
                result_flag, result_query = super().query_record(super().get_tbl_ORDER_STAGING(), columns_array, condition)

                # Variables
                is_order_upserted_success = None
                total_order_upserted_count = None
                response_description = None
                last_order_id = None
                items_upserted_count = 0
                last_staging_table_row_id = None
                error_message = None
                additional_details = None
                order_json = None
                row = None
                is_item_upserted_success = False
                line_items = None
                items_count = 0

                if result_flag:
                    if len(result_query) > 0:
                        print(f"[INFO] Upserting Orders...")
                        for row in result_query:
                            last_staging_table_row_id = row.get("ROW_ID")
                            order_json = json.loads(row.get("ORDER_JSON"))
                            line_items = order_json.get("line_items", [])
                            items_count = len(line_items)

                            is_order_upserted_success, is_item_upserted_success, total_order_upserted_count, items_upserted_count, last_order_id = self.process_order_and_line_items(order_json=order_json)

                            if remove_orders and is_order_upserted_success and items_count > 0 and items_count == items_upserted_count:
                                print(f"[INFO] Removing order from staging table...\tOrder: {last_order_id}")
                                self.delete_staging_table(id=last_staging_table_row_id)
                    else:
                        self.utils.set_end_time(self.utils.get_current_date_time())
                        print("[INFO] No orders found.")
                        print(f"[INFO] Waiting {self.utils.get_WAIT_TIME()} second(s) to try again...")
                        time.sleep(int(self.utils.get_WAIT_TIME()))
                else:
                    self.utils.set_end_time(self.utils.get_current_date_time())
                    print("[INFO] No orders found.")
                    print(f"[INFO] Waiting {self.utils.get_WAIT_TIME()} second(s) to try again...")
                    time.sleep(int(self.utils.get_WAIT_TIME()))
            except Exception as e:
                print(f"[ERROR] Error while processing orders from staging table: {e}")

                additional_info = f'\n[INFO] limit: {limit}'
                additional_info += f'\n[INFO] last_order_id: {last_staging_table_row_id}'
                additional_info += f'\n[INFO] The function process_orders_from_staging_table() execution kept running on Heroku. Please check the logs for more information.'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_orders_from_staging_table", error_code=None, error_message=str(e), additional_details=additional_info, error_severity=self.utils.get_error_severity(2))

                print(f"[INFO] Waiting {self.utils.get_WAIT_TIME()} second(s) to try again...")
                time.sleep(int(self.utils.get_WAIT_TIME()))
            finally:
                print("[INFO] Clearing variables...")
                try:
                    del result_query
                    del columns_array
                    del columns
                    del result_flag
                    del condition
                    del last_order_id
                    del total_order_upserted_count
                    del items_upserted_count
                    del item
                    del line_item
                    del line_item_updated_count
                    del is_order_upserted_success
                    del response_description
                    del error_message
                    del additional_details
                    del order_json
                    del row
                    del is_item_upserted_success
                    del line_items
                    del items_count
                except:
                    pass

    def verify_staging_table_already_exits(self, order_json):
        staging_table_count = 0
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND JSON_CONTAINS(ORDER_JSON, '{self.utils.replace_special_chars(self.utils.convert_object_to_json(order_json))}')"
        condition += f"\nORDER BY ROW_ID ASC"

        try:
            result_flag, result_query = super().query_record(super().get_tbl_ORDER_STAGING(), columns, condition)

            if result_flag:
                for row in result_query:
                    staging_table_count = row.get("TOTAL")
            else:
                staging_table_count = 0

            return staging_table_count
        except Exception as e:
            return 0
        finally:
            try:
                del result_query
                del columns
                del result_flag
                del condition
                del staging_table_count
            except:
                pass

    # Function to read the entire Orders table (limiting by variable), and get the columns ID and LINE_ITEMS.
    # Then the function will call the upsert_line_items function to save the data into the database table ORDER_LINE_ITEM.
    # The the function will remove the LINE_ITEMS from the ORDER table if the remove_line_items variable is True.
    def generate_order_line_items(self, runCounter, maxRuns, limit, since_id, remove_line_items=False):
        runCounter = runCounter
        maxRuns = maxRuns
        limit = limit
        since_id = since_id
        remove_line_items = remove_line_items
        continue_while = True

        while continue_while:
            try:
                print(f"\n[INFO] Getting all orders line items...\t Run {runCounter} of {maxRuns} - Limit: {limit} - Since Id: {since_id}\n")

                if runCounter <= 1:
                    self.utils.set_start_time(self.utils.get_current_date_time())
                if maxRuns == None or maxRuns == "" or maxRuns == "0":
                    print('[DONE] Finished getting orders because maxRuns is None.')
                    return
                if runCounter == None or runCounter == "" or runCounter == "0":
                    print('[DONE] Finished getting orders because runCounter is None.')
                    return

                columns_array = ["ID", "LINE_ITEMS"]
                condition = "1=1"
                condition += f"\nAND ID > '{since_id}'" if since_id is not None and since_id != "" else ""
                condition += f"\nAND LINE_ITEMS IS NOT NULL"
                condition += f"\nORDER BY ID ASC"
                condition += f"\nLIMIT {str(limit)}" if limit is not None and limit != "" and limit != "0" else "\nLIMIT 1000"
                result_flag, result_query = super().query_record(super().get_tbl_ORDER(), columns_array, condition)

                line_items_count = 0
                counter = 0
                last_order_id = None

                if result_flag:
                    if len(result_query) > 0:
                        for row in result_query:
                            last_order_id = row.get("ID")
                            line_items = row.get("LINE_ITEMS")
                            line_items = json.loads(line_items)
                            line_items_count = len(line_items)
                            counter = 0

                            print(f"[INFO] Upserting order line items...\t\tOrder: {last_order_id}")
                            for item in line_items:
                                line_item = self.OrderLineItem()

                                item_id = item.get("id")
                                line_item.set_id(item_id)
                                line_item.set_order_id(last_order_id)
                                line_item.set_attributed_staffs(item.get("attributed_staffs", ""))
                                line_item.set_fulfillable_quantity(item.get("fulfillable_quantity", ""))
                                line_item.set_fulfillment_service(item.get("fulfillment_service", ""))
                                line_item.set_fulfillment_status(item.get("fulfillment_status", ""))
                                line_item.set_grams(item.get("grams", ""))
                                line_item.set_price(item.get("price", ""))
                                line_item.set_price_set(item.get("price_set", ""))
                                line_item.set_product_exists(item.get("product_exists", ""))
                                line_item.set_product_id(item.get("product_id", ""))
                                line_item.set_quantity(item.get("quantity", ""))
                                line_item.set_requires_shipping(item.get("requires_shipping", ""))
                                line_item.set_sku(item.get("sku", ""))
                                line_item.set_title(item.get("title", ""))
                                line_item.set_variant_id(item.get("variant_id", ""))
                                line_item.set_variant_inventory_management(item.get("variant_inventory_management", ""))
                                line_item.set_variant_title(item.get("variant_title", ""))
                                line_item.set_vendor(item.get("vendor", ""))
                                line_item.set_name(item.get("name", ""))
                                line_item.set_gift_card(item.get("gift_card", ""))
                                line_item.set_properties(item.get("properties", ""))
                                line_item.set_taxable(item.get("taxable", ""))
                                line_item.set_tax_lines(item.get("tax_lines", ""))
                                line_item.set_total_discount(item.get("total_discount", ""))
                                line_item.set_total_discount_set(item.get("total_discount_set", ""))
                                line_item.set_discount_allocations(item.get("discount_allocations", ""))
                                line_item.set_duties(item.get("duties", ""))
                                line_item.set_admin_graphql_api_id(item.get("admin_graphql_api_id", ""))

                                is_upserted_success, line_item_inserted_count, line_item_updated_count, response_description = self.upsert_line_items(order_id=last_order_id,line_item=line_item)

                                if is_upserted_success:
                                    counter += line_item_inserted_count + line_item_updated_count
                                    if line_item_inserted_count > 0:
                                        self.print_log(log_level="info", type="line_item", function="insert", order_id=last_order_id, item_id=item_id, try_count=None, message=f" - {counter} of {line_items_count}")
                                    elif line_item_updated_count > 0:
                                        self.print_log(log_level="info", type="line_item", function="update", order_id=last_order_id, item_id=item_id, try_count=None, message=f" - {counter} of {line_items_count}")
                                    else:
                                        self.print_log(log_level="error", type="line_item", function="process", order_id=last_order_id, item_id=item_id, try_count=None, message=f" - {counter} of {line_items_count}")

                            if line_items_count != counter:
                                error_message = f"[ERROR] Error while upserting line items. Order: {last_order_id}"
                                additional_details = f'[INFO] Line Items JSON: {line_items}'
                                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="generate_order_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                            else:
                                if remove_line_items:
                                    print(f"[INFO] Removing line items from order...\tOrder: {last_order_id}\n")
                                    columns_array = ["LINE_ITEMS"]
                                    values_array = ["NULL"]
                                    condition = f"ID = '{last_order_id}'"
                                    super().update_record(super().get_tbl_ORDER(), columns_array, values_array, condition)

                        if runCounter < maxRuns:
                            runCounter += 1
                            since_id = last_order_id
                            continue_while = True
                        else:
                            print('[DONE] Finished getting orders because it reached the maxRuns.')
                            self.utils.set_end_time(self.utils.get_current_date_time())
                            continue_while = False
                    else:
                        print("[DONE] No orders found.")
                        self.utils.set_end_time(self.utils.get_current_date_time())
                        continue_while = False
                else:
                    print("[DONE] No orders found.")
                    self.utils.set_end_time(self.utils.get_current_date_time())
                    continue_while = False
            except Exception as e:
                print(f"[ERROR] Error while getting all orders line items. Error: {str(e)}")
                
                last_order_id = last_order_id if last_order_id is not None else since_id
                additional_info = f'[INFO] runCounter: {runCounter}'
                additional_info += f'\n[INFO] maxRuns: {maxRuns}'
                additional_info += f'\n[INFO] limit: {limit}'
                additional_info += f'\n[INFO] since_id: {since_id}'
                additional_info += f'\n[INFO] last_order_id: {last_order_id}'
                additional_info += f'\n[INFO] The function generate_order_line_items() execution kept running on Heroku with since_id = {last_order_id}. Please check the logs for more information.'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_all_shopify_orders", error_code=None, error_message=str(e), additional_details=additional_info, error_severity=self.utils.get_error_severity(2))

                if runCounter < maxRuns:
                    runCounter += 1
                    since_id = last_order_id
                    continue_while = True
                else:
                    print('[DONE] Finished getting orders because it reached the maxRuns.')
                    self.utils.set_end_time(self.utils.get_current_date_time())
                    continue_while = False
            finally:
                print("[INFO] Clearing variables...")
                try:
                    del columns_array
                    del condition
                    del result_query
                    del rows
                    del columns
                    del line_items_count
                    del line_items_upserted_count
                    del row
                    del line_items
                    del item
                    del line_item
                    del is_upserted_success
                    del line_item_inserted_count
                    del line_item_updated_count
                    del response_description
                    del counter
                except:
                    pass

    # RESUME: For each order in the Order Class, I verify if the order and the order item already exists in the database
    # If not, I insert the order and the order item into the database
    # If yes, then I update the order and the order item in the database
    def upsert_orders(self, order:Order):
        order_upserted_flag = False # Upsert means: Inserted or Updated
        maximum_insert_try = 100
        keep_trying = True
        count_try = 0
        order_inserted_count = 0
        order_updated_count = 0
        order_id = None
        columns = None
        rows = None
        result_query = None
        result_string = None
        error_message = None
        additional_details = None

        try:
            order_id = order.get_id()
            self.utils.clear_columns_values_arrays()

            self.utils.validate_columns_values("APP_ID", order.get_app_id())
            # self.utils.validate_columns_values("BILLING_ADDRESS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_billing_address())))
            self.utils.validate_columns_values("BROWSER_IP", order.get_browser_ip())
            self.utils.validate_columns_values("BUYER_ACCEPTS_MARKETING", order.get_buyer_accepts_marketing())
            self.utils.validate_columns_values("CANCEL_REASON", self.utils.replace_special_chars(order.get_cancel_reason()))
            self.utils.validate_columns_values("CANCELLED_AT", order.get_cancelled_at())
            self.utils.validate_columns_values("CART_TOKEN", order.get_cart_token())
            self.utils.validate_columns_values("CHECKOUT_TOKEN", order.get_checkout_token())
            self.utils.validate_columns_values("CLIENT_DETAILS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_client_details())))
            self.utils.validate_columns_values("CLOSED_AT", order.get_closed_at())
            self.utils.validate_columns_values("COMPANY", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_company())))
            self.utils.validate_columns_values("CONFIRMATION_NUMBER", order.get_confirmation_number())
            self.utils.validate_columns_values("CREATED_AT", order.get_created_at())
            self.utils.validate_columns_values("COUNTRY_CODE", order.get_country_code())
            self.utils.validate_columns_values("CURRENCY", order.get_currency())
            self.utils.validate_columns_values("CURRENT_TOTAL_ADDITIONAL_FEES_SET", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_current_total_additional_fees_set())))
            self.utils.validate_columns_values("CURRENT_TOTAL_DISCOUNTS", order.get_current_total_discounts())
            self.utils.validate_columns_values("CUSTOMER_ID", order.get_customer_id())
            self.utils.validate_columns_values("CUSTOMER", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_customer())))
            self.utils.validate_columns_values("CUSTOMER_LOCALE", order.get_customer_locale())
            # self.utils.validate_columns_values("DISCOUNT_APPLICATIONS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_discount_applications())))
            # self.utils.validate_columns_values("DISCOUNT_CODES", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_discount_codes())))
            self.utils.validate_columns_values("EMAIL", self.utils.replace_special_chars(order.get_email()))
            self.utils.validate_columns_values("ESTIMATED_TAXES", order.get_estimated_taxes())
            self.utils.validate_columns_values("FINANCIAL_STATUS", order.get_financial_status())
            self.utils.validate_columns_values("FULFILLMENTS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_fulfillments())))
            self.utils.validate_columns_values("FULFILLMENT_STATUS", order.get_fulfillment_status())
            self.utils.validate_columns_values("ID", order.get_id())
            self.utils.validate_columns_values("LANDING_SITE", self.utils.replace_special_chars(order.get_landing_site()))
            # self.utils.validate_columns_values("LINE_ITEMS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_line_items())))
            self.utils.validate_columns_values("LOCATION_ID", order.get_location_id())
            self.utils.validate_columns_values("MERCHANT_OF_RECORD_APP_ID", order.get_merchant_of_record_app_id())
            self.utils.validate_columns_values("NAME", self.utils.replace_special_chars(order.get_name()))
            self.utils.validate_columns_values("NOTE", self.utils.replace_special_chars(order.get_note()))
            self.utils.validate_columns_values("NOTE_ATTRIBUTES", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_note_attributes())))
            self.utils.validate_columns_values("NUMBER", self.utils.replace_special_chars(order.get_number()))
            self.utils.validate_columns_values("ORDER_NUMBER", order.get_order_number())
            self.utils.validate_columns_values("ORIGINAL_TOTAL_ADDITIONAL_FEES_SET", self.utils.replace_special_chars(order.get_original_total_additional_fees_set()))
            self.utils.validate_columns_values("ORIGINAL_TOTAL_DUTIES_SET", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_original_total_duties_set())))
            self.utils.validate_columns_values("PAYMENT_TERMS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_payment_terms())))
            self.utils.validate_columns_values("PAYMENT_GATEWAY_NAMES", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_payment_gateway_names())))
            self.utils.validate_columns_values("PHONE", order.get_phone())
            self.utils.validate_columns_values("PO_NUMBER", order.get_po_number())
            self.utils.validate_columns_values("PRESENTMENT_CURRENCY", order.get_presentment_currency())
            self.utils.validate_columns_values("PROCESSED_AT", order.get_processed_at())
            self.utils.validate_columns_values("REFERRING_SITE", self.utils.replace_special_chars(order.get_referring_site()))
            # self.utils.validate_columns_values("REFUNDS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_refunds())))
            # self.utils.validate_columns_values("SHIPPING_ADDRESS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_shipping_address(), ensure_ascii=False)))
            self.utils.validate_columns_values("SHIPPING_LINES", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_shipping_lines())))
            self.utils.validate_columns_values("SOURCE_NAME", self.utils.replace_special_chars(order.get_source_name()))
            self.utils.validate_columns_values("SOURCE_IDENTIFIER", order.get_source_identifier())
            self.utils.validate_columns_values("SOURCE_URL", self.utils.replace_special_chars(order.get_source_url()))
            self.utils.validate_columns_values("SUBTOTAL_PRICE", order.get_subtotal_price())
            self.utils.validate_columns_values("SUBTOTAL_PRICE_SET", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_subtotal_price_set())))
            self.utils.validate_columns_values("TAGS", self.utils.replace_special_chars(order.get_tags()))
            self.utils.validate_columns_values("TAX_LINES", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_tax_lines())))
            self.utils.validate_columns_values("TAXES_INCLUDED", order.get_taxes_included())
            self.utils.validate_columns_values("TEST", order.get_test())
            self.utils.validate_columns_values("TOTAL_DISCOUNTS", order.get_total_discounts())
            self.utils.validate_columns_values("TOTAL_DISCOUNTS_SET", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_total_discounts_set())))
            self.utils.validate_columns_values("TOTAL_LINE_ITEMS_PRICE", order.get_total_line_items_price())
            self.utils.validate_columns_values("TOTAL_LINE_ITEMS_PRICE_SET", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_total_line_items_price_set())))
            self.utils.validate_columns_values("TOTAL_OUTSTANDING", order.get_total_outstanding())
            self.utils.validate_columns_values("TOTAL_PRICE", order.get_total_price())
            self.utils.validate_columns_values("TOTAL_PRICE_SET", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_total_price_set())))
            self.utils.validate_columns_values("TOTAL_SHIPPING_PRICE_SET", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_total_shipping_price_set())))
            self.utils.validate_columns_values("TOTAL_TAX", order.get_total_tax())
            self.utils.validate_columns_values("TOTAL_TAX_SET", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_total_tax_set())))
            self.utils.validate_columns_values("TOTAL_TIP_RECEIVED", order.get_total_tip_received())
            self.utils.validate_columns_values("TOTAL_WEIGHT", order.get_total_weight())
            self.utils.validate_columns_values("UPDATED_AT", order.get_updated_at())
            self.utils.validate_columns_values("USER_ID", order.get_user_id())
            self.utils.validate_columns_values("ORDER_STATUS_URL", self.utils.replace_special_chars(order.get_order_status_url()))
            self.utils.validate_columns_values("TOKEN", order.get_token())
            self.utils.validate_columns_values("ADMIN_GRAPHQL_API_ID", order.get_admin_graphql_api_id())
            self.utils.validate_columns_values("CHECKOUT_ID", order.get_checkout_id())
            self.utils.validate_columns_values("CONFIRMED", order.get_confirmed())
            self.utils.validate_columns_values("CONTACT_EMAIL", self.utils.replace_special_chars(order.get_contact_email()))
            self.utils.validate_columns_values("DEVICE_ID", self.utils.replace_special_chars(order.get_device_id()))
            self.utils.validate_columns_values("LANDING_SITE_REF", self.utils.replace_special_chars(order.get_landing_site_ref()))
            self.utils.validate_columns_values("REFERENCE", order.get_reference())
            self.utils.validate_columns_values("TAX_EXEMPT", order.get_tax_exempt())

            if self.verify_order_exits(order_id) == False:
                # print(f"[INFO] Inserting order...\t\t\tOrder: {order_id}")
                order_upserted_flag, order_inserted_count, result_string = super().insert_record(super().get_tbl_ORDER(), self.utils.get_columns_array(), self.utils.get_values_array())

                #print(result_string)
                if order_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                    if "PRIMARY" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            print(f"[INFO] Trying to insert order again...\t\tOrder: {order_id}.Try: {count_try}")
                            super().generate_next_id()
                            order_upserted_flag, order_inserted_count, result_string = super().insert_record(super().get_tbl_ORDER(), self.utils.get_columns_array(), self.utils.get_values_array())

                            if order_upserted_flag == True:
                                print(f"[INFO] Order Inserted...\t\t\tOrder: {order_id}")
                                keep_trying = False
                            else:
                                if "ID" in result_string:
                                    print(f"[INFO] Updating order...\t\t\tOrder: {order_id}")
                                    self.utils.clear_condition()
                                    self.utils.set_condition(f"ID = '{order_id}'")
                                    order_upserted_flag, order_updated_count, result_string = super().update_record(super().get_tbl_ORDER(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                                    if order_upserted_flag == True:
                                        print(f"[INFO] Order Updated...\t\t\t\tOrder: {order_id}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Order NOT Inserted/Maximum tries...\tOrder: {order_id}.Try: {count_try}")
                                            keep_trying = False
                                else:
                                    if count_try >= maximum_insert_try:
                                        print(f"[INFO] Order NOT Inserted/Maximum tries...\tOrder: {order_id}.Try: {count_try}")
                                        keep_trying = False
                    else:
                        print(f"[INFO] Updating order...\t\t\tOrder: {order_id}")
                        self.utils.clear_condition()
                        self.utils.set_condition(f"ID = '{order_id}'")
                        order_upserted_flag, order_updated_count, result_string = super().update_record(super().get_tbl_ORDER(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())
            else:
                # print(f"[INFO] Updating order...\t\t\tOrder: {order_id}")
                self.utils.clear_condition()
                self.utils.set_condition(f"ID = '{order_id}'")
                order_upserted_flag, order_updated_count, result_string = super().update_record(super().get_tbl_ORDER(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                if order_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                    if "ID" in result_string:
                        while keep_trying:
                            count_try = count_try + 1
                            print(f"[INFO] Trying to update order again...\t\tOrder: {order_id}.Try: {count_try}")
                            order_upserted_flag, order_updated_count, result_string = super().update_record(super().get_tbl_ORDER(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                            if order_upserted_flag == True:
                                print(f"[INFO] Order Updated...\t\t\t\tOrder: {order_id}")
                                keep_trying = False
                            else:
                                if count_try >= maximum_insert_try:
                                    print(f"[INFO] Order NOT Updated/Maximum tries...\tOrder: {order_id}.Try: {count_try}")
                                    keep_trying = False

            if order_upserted_flag == False:
                self.append_order_with_error_array(order_id)
                self.set_total_order_with_errors(self.get_total_order_with_errors() + 1)

            self.set_total_order_count(self.get_total_order_count() + order_inserted_count + order_updated_count)
            self.set_total_order_inserted_count(self.get_total_order_inserted_count() + order_inserted_count)
            self.set_total_order_updated_count(self.get_total_order_updated_count() + order_updated_count)
        except Exception as e:
            error_message = f"[ERROR] Error while upserting order. Order id: {order_id} - Error: {str(e)}."
            additional_details = result_string
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="upsert_orders", error_code=500, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
        finally:
            try:
                del maximum_insert_try, keep_trying, count_try, order_id, columns, rows, result_query, error_message, additional_details
            except Exception as e:
                print(f"[INFO] Variable not found: {e}")
                pass

            return order_upserted_flag, self.get_total_order_count(), order_inserted_count, order_updated_count, result_string

    def upsert_line_items(self, order_id, line_item:OrderLineItem):
        line_item_upserted_flag = False # Upsert means: Inserted or Updated
        maximum_insert_try = 100
        keep_trying = True
        count_try = 0
        line_item_inserted_count = 0
        line_item_updated_count = 0
        columns = None
        rows = None
        result_query = None
        result_string = None
        error_message = None
        additional_details = None
        item_id = line_item.get_id()

        if order_id is not None and order_id != "" and order_id != 0 and order_id != "0":
            try:
                if self.verify_order_exits(order_id) == False:
                    return False, 0, 0, "Order not found."
                else:
                    self.utils.clear_columns_values_arrays()
                    self.utils.validate_columns_values("ID", item_id)
                    self.utils.validate_columns_values("ORDER_ID", order_id)
                    self.utils.validate_columns_values("ATTRIBUTED_STAFFS", self.utils.replace_special_chars(self.utils.convert_object_to_json(line_item.get_attributed_staffs())))
                    self.utils.validate_columns_values("FULFILLABLE_QUANTITY", line_item.get_fulfillable_quantity())
                    self.utils.validate_columns_values("FULFILLMENT_SERVICE", line_item.get_fulfillment_service())
                    self.utils.validate_columns_values("FULFILLMENT_STATUS", line_item.get_fulfillment_status())
                    self.utils.validate_columns_values("GRAMS", line_item.get_grams())
                    self.utils.validate_columns_values("PRICE", line_item.get_price())
                    self.utils.validate_columns_values("PRICE_SET", self.utils.replace_special_chars(self.utils.convert_object_to_json(line_item.get_price_set())))
                    self.utils.validate_columns_values("PRODUCT_EXISTS", line_item.get_product_exists())
                    self.utils.validate_columns_values("PRODUCT_ID", line_item.get_product_id())
                    self.utils.validate_columns_values("QUANTITY", line_item.get_quantity())
                    self.utils.validate_columns_values("REQUIRES_SHIPPING", line_item.get_requires_shipping())
                    self.utils.validate_columns_values("SKU", line_item.get_sku())
                    self.utils.validate_columns_values("TITLE", self.utils.replace_special_chars(line_item.get_title()))
                    self.utils.validate_columns_values("VARIANT_ID", line_item.get_variant_id())
                    self.utils.validate_columns_values("VARIANT_INVENTORY_MANAGEMENT", line_item.get_variant_inventory_management())
                    self.utils.validate_columns_values("VARIANT_TITLE", self.utils.replace_special_chars(line_item.get_variant_title()))
                    self.utils.validate_columns_values("VENDOR", self.utils.replace_special_chars(line_item.get_vendor()))
                    self.utils.validate_columns_values("NAME", self.utils.replace_special_chars(line_item.get_name()))
                    self.utils.validate_columns_values("GIFT_CARD", line_item.get_gift_card())
                    self.utils.validate_columns_values("PROPERTIES", self.utils.replace_special_chars(self.utils.convert_object_to_json(line_item.get_properties())))
                    self.utils.validate_columns_values("TAXABLE", line_item.get_taxable())
                    self.utils.validate_columns_values("TAX_LINES", self.utils.replace_special_chars(self.utils.convert_object_to_json(line_item.get_tax_lines())))
                    self.utils.validate_columns_values("TOTAL_DISCOUNT", line_item.get_total_discount())
                    self.utils.validate_columns_values("TOTAL_DISCOUNT_SET", self.utils.replace_special_chars(self.utils.convert_object_to_json(line_item.get_total_discount_set())))
                    self.utils.validate_columns_values("DISCOUNT_ALLOCATIONS", self.utils.replace_special_chars(self.utils.convert_object_to_json(line_item.get_discount_allocations())))
                    self.utils.validate_columns_values("DUTIES", self.utils.replace_special_chars(self.utils.convert_object_to_json(line_item.get_duties())))
                    self.utils.validate_columns_values("ADMIN_GRAPHQL_API_ID", line_item.get_admin_graphql_api_id())

                    if self.verify_order_line_item_exists(item_id) == False:
                        # print(f"[INFO] Inserting line item...\t\t\tOrder: {order_id}\tItem: {item_id}")
                        line_item_upserted_flag, line_item_inserted_count, result_string = super().insert_record(super().get_tbl_ORDER_LINE_ITEM(), self.utils.get_columns_array(), self.utils.get_values_array())

                        if line_item_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                            if "PRIMARY" in result_string:
                                while keep_trying:
                                    count_try += 1
                                    print(f"[INFO] Trying to insert line item again...\tOrder: {order_id}\tItem: {item_id}.Try: {count_try}")
                                    super().generate_next_id()
                                    line_item_upserted_flag, line_item_inserted_count, result_string = super().insert_record(super().get_tbl_ORDER_LINE_ITEM(), self.utils.get_columns_array(), self.utils.get_values_array())

                                    if line_item_upserted_flag == True:
                                        print(f"[INFO] Line Item Inserted...\t\t\tOrder: {order_id}\tItem: {item_id}")
                                        keep_trying = False
                                    else:
                                        if "ID" in result_string:
                                            print(f"[INFO] Updating line item...\t\t\tOrder: {order_id}\tItem: {item_id}")
                                            self.utils.clear_condition()
                                            self.utils.set_condition(f"ORDER_ID = '{order_id}'\nAND ID = '{item_id}'")
                                            line_item_upserted_flag, line_item_updated_count, result_string = super().update_record(super().get_tbl_ORDER_LINE_ITEM(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                                            if line_item_upserted_flag == True:
                                                print(f"[INFO] Line Item Updated...\t\t\tOrder: {order_id}\tItem: {item_id}")
                                                keep_trying = False
                                            else:
                                                if count_try >= maximum_insert_try:
                                                    print(f"[INFO] Line Item NOT Inserted/Maximum tries...\tOrder: {order_id}\tItem: {item_id}.Try: {count_try}")
                                                    keep_trying = False
                                        else:
                                            if count_try >= maximum_insert_try:
                                                print(f"[INFO] Line Item NOT Inserted/Maximum tries...\tOrder: {order_id}\tItem: {item_id}.Try: {count_try}")
                                                keep_trying = False
                            else:
                                print(f"[INFO] Updating line item...\t\t\tOrder: {order_id}\tItem: {item_id}")
                                self.utils.clear_condition()
                                self.utils.set_condition(f"ORDER_ID = '{order_id}'\nAND ID = '{item_id}'")
                                line_item_upserted_flag, line_item_updated_count, result_string = super().update_record(super().get_tbl_ORDER_LINE_ITEM(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())
                    else:
                        # print(f"[INFO] Updating line item...\t\t\tOrder: {order_id}\tItem: {item_id}")
                        self.utils.clear_condition()
                        self.utils.set_condition(f"ORDER_ID = '{order_id}'\nAND ID = '{item_id}'")
                        line_item_upserted_flag, line_item_updated_count, result_string = super().update_record(super().get_tbl_ORDER_LINE_ITEM(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                        if line_item_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                            if "ID" in result_string:
                                while keep_trying:
                                    count_try = count_try + 1
                                    print(f"[INFO] Trying to update line item again...\tOrder: {order_id}\tItem: {item_id}.Try: {count_try}")
                                    line_item_upserted_flag, line_item_updated_count, result_string = super().update_record(super().get_tbl_ORDER_LINE_ITEM(), self.utils.get_columns_array(), self.utils.get_values_array(), self.utils.get_condition())

                                    if line_item_upserted_flag == True:
                                        print(f"[INFO] Line Item Updated...\t\t\tOrder: {order_id}\tItem: {item_id}")
                                        keep_trying = False
                                    else:
                                        if count_try >= maximum_insert_try:
                                            print(f"[INFO] Line Item NOT Updated/Maximum tries...\tOrder: {order_id}\tItem: {item_id}.Try: {count_try}")
                                            keep_trying = False

                    if line_item_upserted_flag == False:
                        self.append_order_line_item_with_error_array(item_id)
                        self.set_total_order_line_item_with_errors(self.get_total_order_line_item_with_errors() + 1)

                    self.set_total_order_line_item_count(self.get_total_order_line_item_count() + line_item_inserted_count + line_item_updated_count)
                    self.set_total_order_line_item_inserted_count(self.get_total_order_line_item_inserted_count() + line_item_inserted_count)
                    self.set_total_order_line_item_updated_count(self.get_total_order_line_item_updated_count() + line_item_updated_count)

                    return line_item_upserted_flag, line_item_inserted_count, line_item_updated_count, result_string
            except Exception as e:
                self.append_order_line_item_with_error_array(item_id)
                self.set_total_order_line_item_with_errors(self.get_total_order_line_item_with_errors() + 1)

                error_message = f"[ERROR] Error while upserting order line item. Order id: {order_id} - item id: {item_id} - Error: {str(e)}."
                additional_details = result_string
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="upsert_line_items", error_code=500, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))
                return False, 0, 0, "Error while upserting line items."
            finally:
                try:
                    del maximum_insert_try
                    del keep_trying
                    del count_try
                    del columns
                    del rows
                    del result_query
                    del error_message
                    del additional_details
                    del item_id
                    del line_item_updated_count
                    del line_item_inserted_count
                except Exception as e:
                    print(f"[INFO] Variable not found: {e}")
                    pass
        else:
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="upsert_line_items", error_code=500, error_message="order_id is None or empty.", additional_details=None, error_severity=self.utils.get_error_severity(3))
            try:
                del maximum_insert_try
                del keep_trying
                del count_try
                del columns
                del rows
                del result_query
                del error_message
                del additional_details
                del item_id
                del line_item_inserted_count
                del line_item_updated_count
            except Exception as e:
                print(f"[INFO] Variable not found: {e}")
                pass
            return False, 0, 0, "order_id is None or empty."

    def insert_custom_orders_managemet(self, order:CustomOrdersManagement):
        order_upserted_flag = False # Upsert means: Inserted or Updated
        maximum_insert_try = 100
        keep_trying = True
        count_try = 0
        order_inserted_count = 0
        order_id = None
        columns = None
        rows = None
        result_query = None
        result_string = None
        error_message = None
        additional_details = None
        sorority_flg = False
        sent_by_vendor_flg = False
        received_flg = False
        fulfilled_flg = False

        try:
            order_id = order.get_id()
            sorority_flg = False if order.get_sorority_flg() is None or order.get_sorority_flg() == "" else order.get_sorority_flg()
            sent_by_vendor_flg = False if order.get_sent_by_vendor_flg() is None or order.get_sent_by_vendor_flg() == "" else order.get_sent_by_vendor_flg()
            received_flg = False if order.get_received_flg() is None or order.get_received_flg() == "" else order.get_received_flg()
            fulfilled_flg = False if order.get_fulfilled_flg() is None or order.get_fulfilled_flg() == "" else order.get_fulfilled_flg()

            self.utils.clear_columns_values_arrays()
            self.utils.validate_columns_values("ORDER_PROCESSING_ID", order.get_order_processing_id())
            self.utils.validate_columns_values("ID", order_id)
            self.utils.validate_columns_values("NAME", order.get_name())
            self.utils.validate_columns_values("COUNTRY", order.get_country())
            self.utils.validate_columns_values("COUNTRY_CODE", order.get_country_code())
            self.utils.validate_columns_values("LOCATION_ID", order.get_location_id())
            self.utils.validate_columns_values("LOCATION_NAME", order.get_location_name())
            self.utils.validate_columns_values("CREATED_AT", order.get_created_at())
            self.utils.validate_columns_values("FONT", order.get_font())
            self.utils.validate_columns_values("CUSTOM_TEXT", self.utils.replace_special_chars(order.get_custom_text()))
            self.utils.validate_columns_values("PROD_NAME", self.utils.replace_special_chars(order.get_prod_name()))
            self.utils.validate_columns_values("PROD_SKU", order.get_prod_sku())
            self.utils.validate_columns_values("QUANTITY", order.get_quantity())
            self.utils.validate_columns_values("VENDOR_ID", order.get_vendor_id())
            self.utils.validate_columns_values("VENDOR_NAME", self.utils.replace_special_chars(order.get_vendor_name()))
            self.utils.validate_columns_values("SENT_TO_VENDOR_DATE", order.get_sent_to_vendor_date())
            self.utils.validate_columns_values("SORORITY_FLG", sorority_flg)
            self.utils.validate_columns_values("SENT_BY_VENDOR_FLG", sent_by_vendor_flg)
            self.utils.validate_columns_values("SHIPPED_DATE", order.get_shipped_date())
            self.utils.validate_columns_values("TRACKING_NUMBER", order.get_tracking_number())
            self.utils.validate_columns_values("TRACKING_URL", order.get_tracking_url())
            self.utils.validate_columns_values("RECEIVED_FLG", received_flg)
            self.utils.validate_columns_values("RECEIVED_DATE", order.get_received_date())
            self.utils.validate_columns_values("RECEIVED_BY", order.get_received_by())
            self.utils.validate_columns_values("RECEIVED_NOTES", order.get_received_notes())
            self.utils.validate_columns_values("FULFILLED_FLG", fulfilled_flg)
            self.utils.validate_columns_values("FULFILLED_DATE", order.get_fulfilled_date())
            self.utils.validate_columns_values("FULFILLMENT_STATUS", "NULL")
            self.utils.validate_columns_values("CANCELLED_FLG", "FALSE")
            self.utils.validate_columns_values("VENDOR_PROCESSING_TIME", order.get_vendor_processing_time())
            self.utils.validate_columns_values("VENDOR_PROCESSING_TIME_UNIT", order.get_vendor_processing_time_unit())
            self.utils.validate_columns_values("TOTAL_SHIPPING_TIME", order.get_total_shipping_time())
            self.utils.validate_columns_values("TOTAL_SHIPPING_TIME_UNIT", order.get_total_shipping_time_unit())
            self.utils.validate_columns_values("TOTAL_PROCESSING_TIME", order.get_total_processing_time())
            self.utils.validate_columns_values("TOTAL_PROCESSING_TIME_UNIT", order.get_total_processing_time_unit())

            order_upserted_flag, order_inserted_count, result_string = super().insert_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), self.utils.get_columns_array(), self.utils.get_values_array())

            if order_upserted_flag == False and ("1062" in result_string or "Duplicate entry" in result_string):
                if "PRIMARY" in result_string:
                    while keep_trying:
                        count_try += 1
                        print(f"[INFO] Trying to insert order again...\t\tOrder: {order_id}.Try: {count_try}")
                        super().generate_next_id()
                        order_upserted_flag, order_inserted_count, result_string = super().insert_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), self.utils.get_columns_array(), self.utils.get_values_array())

                        if order_upserted_flag == True:
                            print(f"[INFO] Order Inserted...\t\t\tOrder: {order_id}")
                            keep_trying = False
                        else:
                            if count_try >= maximum_insert_try:
                                print(f"[INFO] Order NOT Inserted/Maximum tries...\tOrder: {order_id}.Try: {count_try}")
                                keep_trying = False

            return order_upserted_flag, order_inserted_count, result_string
        except Exception as e:
            error_message = f"[ERROR] Error while upserting order. Order id: {order_id} - Error: {str(e)}."
            additional_details = result_string
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="insert_custom_orders_managemet", error_code=500, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
            return False, 0, str(e)
        finally:
            # print("[INFO] Clearing variables...")
            try:
                del order_upserted_flag
                del maximum_insert_try
                del keep_trying
                del count_try
                del order_id
                del columns
                del rows
                del result_query
                del error_message
                del additional_details
                del sorority_flg
                del sent_by_vendor_flg
                del received_flg
                del fulfilled_flg
            except Exception as e:
                print(f"[INFO] Variable not found: {e}")
                pass

    def delete_order(self, order_id):
        try:
            count_deleted = 0
            self.utils.clear_condition()
            self.utils.set_condition(f"ID = '{order_id}'")
            is_deleted_flag, count_deleted, resultString = super().delete_record(super().get_tbl_ORDER(), self.utils.get_condition())
            self.set_total_order_deleted_count(self.get_total_order_deleted_count() + count_deleted)
        except Exception as e:
            additional_details = f"order_id: {order_id}"
            additional_details += f"\nis_deleted_flag: {is_deleted_flag}"
            additional_details += f"\ncount_deleted: {count_deleted}"
            additional_details += f"\nresultString: {resultString}"
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="delete_order", error_code=None, error_message=str(e), additional_details=additional_details, error_severity=self.utils.get_error_severity(3))

        return is_deleted_flag, count_deleted, resultString

    def delete_staging_table(self, id):
        try:
            count_deleted = 0
            self.utils.set_condition(f"ROW_ID = '{id}'")
            is_deleted_flag, count_deleted, resultString = super().delete_record(super().get_tbl_ORDER_STAGING(), self.utils.get_condition())
        except Exception as e:
            error_message = f"[ERROR] Error while deleting from staging table. Order id: {id} - Error: {str(e)}."
            additional_details = f"id: {id}"
            additional_details += f"\nis_deleted_flag: {is_deleted_flag}"
            additional_details += f"\ncount_deleted: {count_deleted}"
            additional_details += f"\nresultString: {resultString}"
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="delete_staging_table", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(3))
        finally:
            return is_deleted_flag, count_deleted, resultString

    def process_order_and_line_items(self, order_json):
        last_order_id = None
        order_name = None
        error_message = None
        additional_details = None
        is_order_upserted_success = False
        total_order_upserted_count = 0
        order_inserted_count = 0
        order_updated_count = 0
        response_description = "Success"
        items_upserted_count = 0
        items_count = 0
        is_item_upserted_success = False
        line_item_inserted_count = 0
        line_item_updated_count = 0
        last_order_line_item_id = None
        item = None
        is_custom_order = False
        found_custom_item = False
        custom_product_name = None
        custom_products_array = []
        order_fulfillment_status = "unfulfilled"
        line_items = []
        result_flag = False
        count_products_updated = 0
        order_cancelled_at = None
        refunds = []
        refunded_items = []
        result_string = "Success"
        result_code = None
        billing_address_json = None
        shipping_address_json = None
        discount_codes_json = None
        discount_applications_json = None
        fulfillments_json = None
        refunds_json = None
        result_flag_billing_address = False
        return_code_billing_address = None
        rowcount_billing_address = None
        result_string_billing_address = None
        result_flag_shipping_address = False
        return_code_shipping_address = None
        rowcount_shipping_address = None
        result_string_shipping_address = None
        result_flag_discount_codes = False
        return_code_discount_codes = None
        rowcount_discount_codes = None
        result_string_discount_codes = None
        result_flag_discount_applications = False
        return_code_discount_applications = None
        rowcount_discount_applications = None
        result_string_discount_applications = None
        result_flag_fulfillments = False
        return_code_fulfillments = None
        rowcount_fulfillments = None
        result_string_fulfillments = None
        result_flag_refunds = False
        return_code_refunds = None
        rowcount_refunds = None
        result_string_refunds = None

        try:
            order = self.Order()
            last_order_id = order_json.get("id")

            order.set_id(last_order_id)
            order.set_admin_graphql_api_id(order_json.get("admin_graphql_api_id", ""))
            order.set_app_id(order_json.get("app_id", ""))
            order.set_browser_ip(order_json.get("browser_ip", ""))
            order.set_buyer_accepts_marketing(order_json.get("buyer_accepts_marketing", ""))
            order.set_cancel_reason(order_json.get("cancel_reason", ""))
            order_cancelled_at = order_json.get("cancelled_at", "")
            order.set_cancelled_at(order_cancelled_at)
            order.set_cart_token(order_json.get("cart_token", ""))
            order.set_checkout_id(order_json.get("checkout_id", ""))
            order.set_checkout_token(order_json.get("checkout_token", ""))
            order.set_client_details(order_json.get("client_details", ""))
            order.set_closed_at(order_json.get("closed_at", ""))
            order.set_company(order_json.get("company", ""))
            order.set_confirmation_number(order_json.get("confirmation_number", ""))
            order.set_confirmed(order_json.get("confirmed", ""))
            order.set_contact_email(order_json.get("contact_email", ""))
            order.set_created_at(order_json.get("created_at", ""))
            try:
                order.set_country_code(order_json.get("shipping_address", {}).get("country_code", ""))
            except Exception as e:
                error_message = f"[ERROR] Error while getting country_code from shipping_address. Order id: {order_json.get('id')} - Error: {str(e)}."
                additional_details = f'[INFO] This error is not critical. The order will be saved without the country_code.'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                order.set_country_code("")
            order.set_currency(order_json.get("currency", ""))
            order.set_current_subtotal_price(order_json.get("current_subtotal_price", ""))
            order.set_current_subtotal_price_set(order_json.get("current_subtotal_price_set", ""))
            order.set_current_total_additional_fees_set(order_json.get("current_total_additional_fees_set", ""))
            order.set_current_total_discounts(order_json.get("current_total_discounts", ""))
            order.set_current_total_discounts_set(order_json.get("current_total_discounts_set", ""))
            order.set_current_total_duties_set(order_json.get("current_total_duties_set", ""))
            order.set_current_total_price(order_json.get("current_total_price", ""))
            order.set_current_total_price_set(order_json.get("current_total_price_set", ""))
            order.set_current_total_tax(order_json.get("current_total_tax", ""))
            order.set_current_total_tax_set(order_json.get("current_total_tax_set", ""))
            order.set_customer_locale(order_json.get("customer_locale", ""))
            order.set_device_id(order_json.get("device_id", ""))
            discount_codes_json = order_json.get("discount_codes", [])
            # order.set_discount_codes(discount_codes_json)
            order.set_email(order_json.get("email", ""))
            order.set_estimated_taxes(order_json.get("estimated_taxes", ""))
            order.set_financial_status(order_json.get("financial_status", ""))
            order_fulfillment_status = order_json.get("fulfillment_status", "")
            order.set_fulfillment_status(order_json.get("fulfillment_status", ""))
            order.set_landing_site(order_json.get("landing_site", ""))
            order.set_landing_site_ref(order_json.get("landing_site_ref", ""))
            order.set_location_id(order_json.get("location_id", ""))
            order.set_merchant_of_record_app_id(order_json.get("merchant_of_record_app_id", ""))
            order_name = order_json.get("name", "")
            order.set_name(order_json.get("name", ""))
            order.set_note(order_json.get("note", ""))
            order.set_note_attributes(order_json.get("note_attributes", ""))
            order.set_number(order_json.get("number", ""))
            order.set_order_number(order_json.get("order_number", ""))
            order.set_order_status_url(order_json.get("order_status_url", ""))
            order.set_original_total_additional_fees_set(order_json.get("original_total_additional_fees_set", ""))
            order.set_original_total_duties_set(order_json.get("original_total_duties_set", ""))
            order.set_payment_gateway_names(order_json.get("payment_gateway_names", ""))
            order.set_phone(order_json.get("phone", ""))
            order.set_po_number(order_json.get("po_number", ""))
            order.set_presentment_currency(order_json.get("presentment_currency", ""))
            order.set_processed_at(order_json.get("processed_at", ""))
            order.set_reference(order_json.get("reference", ""))
            order.set_referring_site(order_json.get("referring_site", ""))
            order.set_source_identifier(order_json.get("source_identifier", ""))
            order.set_source_name(order_json.get("source_name", ""))
            order.set_source_url(order_json.get("source_url", ""))
            order.set_subtotal_price(order_json.get("subtotal_price", ""))
            order.set_subtotal_price_set(order_json.get("subtotal_price_set", ""))
            is_custom_order = True if "custom" in str(order_json.get("tags")).lower() else False
            order.set_tags(order_json.get("tags", ""))
            order.set_tax_exempt(order_json.get("tax_exempt", ""))
            order.set_tax_lines(order_json.get("tax_lines", ""))
            order.set_taxes_included(order_json.get("taxes_included", ""))
            order.set_test(order_json.get("test", ""))
            order.set_token(order_json.get("token", ""))
            order.set_total_discounts(order_json.get("total_discounts", ""))
            order.set_total_discounts_set(order_json.get("total_discounts_set", ""))
            order.set_total_line_items_price(order_json.get("total_line_items_price", ""))
            order.set_total_line_items_price_set(order_json.get("total_line_items_price_set", ""))
            order.set_total_outstanding(order_json.get("total_outstanding", ""))
            order.set_total_price(order_json.get("total_price", ""))
            order.set_total_price_set(order_json.get("total_price_set", ""))
            order.set_total_shipping_price_set(order_json.get("total_shipping_price_set", ""))
            order.set_total_tax(order_json.get("total_tax", ""))
            order.set_total_tax_set(order_json.get("total_tax_set", ""))
            order.set_total_tip_received(order_json.get("total_tip_received", ""))
            order.set_total_weight(order_json.get("total_weight", ""))
            order.set_updated_at(order_json.get("updated_at", ""))
            order.set_user_id(order_json.get("user_id", ""))
            billing_address_json = order_json.get("billing_address", {})
            # order.set_billing_address(billing_address)
            try:
                order.set_customer_id(order_json.get("customer", {}).get("id"))
            except Exception as e:
                error_message = f"[ERROR] Error while getting customer id from order_json. Order id: {order_json.get('id')} - Error: {str(e)}."
                additional_details = f'[INFO] This error is not critical. The order will be saved without the customer id.'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                order.set_customer_id("")
            order.set_customer(order_json.get("customer", {}))
            discount_applications_json = order_json.get("discount_applications", [])
            # order.set_discount_applications(discount_applications)
            fulfillments_json = order_json.get("fulfillments", [])
            # order.set_fulfillments(order_json.get("fulfillments", ""))
            try:
                line_items = order_json.get("line_items", [])
            except Exception as e:
                error_message = f"[ERROR] Error while getting line_items from order_json. Order id: {order_json.get('id')} - Error: {str(e)}."
                additional_details = f'[INFO] This error is not critical. The order will be saved without the line_items.'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                line_items = []
            order.set_line_items(line_items)
            order.set_payment_terms(order_json.get("payment_terms", ""))
            refunds_json = order_json.get("refunds", [])
            # order.set_refunds(refunds)
            shipping_address_json = order_json.get("shipping_address", {})
            # order.set_shipping_address(shipping_address)
            order.set_shipping_lines(order_json.get("shipping_lines", ""))

            is_order_upserted_success, total_order_upserted_count, order_inserted_count, order_updated_count, response_description = self.upsert_orders(order)

            if is_order_upserted_success:
                if order_inserted_count > 0:
                    self.print_log(log_level="info", type="order", function="insert", order_id=last_order_id, item_id=None, try_count=None, message=None)
                elif order_updated_count > 0:
                    self.print_log(log_level="info", type="order", function="update", order_id=last_order_id, item_id=None, try_count=None, message=None)
                else:
                    self.print_log(log_level="error", type="order", function="process", order_id=last_order_id, item_id=None, try_count=None, message=None)

                if billing_address_json is not None and billing_address_json != {}:
                    try:
                        result_flag_billing_address, return_code_billing_address, rowcount_billing_address, result_string_billing_address = self.billing_address.upsert_billing_address_api(last_order_id, billing_address_json)
                    except:
                        pass

                if shipping_address_json is not None and shipping_address_json != {}:
                    try:
                        result_flag_shipping_address, return_code_shipping_address, rowcount_shipping_address, result_string_shipping_address = self.shipping_address.upsert_shipping_address_api(last_order_id, shipping_address_json)
                    except:
                        pass

                if discount_codes_json is not None and discount_codes_json != []:
                    try:
                        result_flag_discount_codes, return_code_discount_codes, rowcount_discount_codes, result_string_discount_codes = self.discount_cds.upsert_discount_code_api(last_order_id, discount_codes_json)
                    except:
                        pass

                if discount_applications_json is not None and discount_applications_json != []:
                    try:
                        result_flag_discount_applications, return_code_discount_applications, rowcount_discount_applications, result_string_discount_applications = self.discount_apps.upsert_discount_application_api(last_order_id, discount_applications_json)
                    except:
                        pass

                if fulfillments_json is not None and fulfillments_json != []:
                    try:
                        result_flag_fulfillments, return_code_fulfillments, rowcount_fulfillments, result_string_fulfillments = self.fulfill.upsert_order_fulfillment_api(fulfillments_json)
                    except:
                        pass

                if refunds_json is not None and refunds_json != []:
                    try:
                        result_flag_refunds, return_code_refunds, rowcount_refunds, result_string_refunds = self.refunds.upsert_refund_api(refunds_json)
                    except:
                        pass

                if line_items is not None and line_items != []:
                    items_upserted_count = 0
                    items_count = len(line_items)
                    for item in line_items:
                        line_item = self.OrderLineItem()
                        last_order_line_item_id = item.get("id", "")
                        line_item.set_id(last_order_line_item_id)
                        line_item.set_order_id(last_order_id)
                        line_item.set_attributed_staffs(item.get("attributed_staffs", ""))
                        line_item.set_fulfillable_quantity(item.get("fulfillable_quantity", ""))
                        line_item.set_fulfillment_service(item.get("fulfillment_service", ""))
                        line_item.set_fulfillment_status(item.get("fulfillment_status", ""))
                        line_item.set_grams(item.get("grams", ""))
                        line_item.set_price(item.get("price", ""))
                        line_item.set_price_set(item.get("price_set", ""))
                        line_item.set_product_exists(item.get("product_exists", ""))
                        line_item.set_product_id(item.get("product_id", ""))
                        line_item.set_quantity(item.get("quantity", ""))
                        line_item.set_requires_shipping(item.get("requires_shipping", ""))
                        line_item.set_sku(item.get("sku", ""))
                        line_item.set_title(item.get("title", ""))
                        line_item.set_variant_id(item.get("variant_id", ""))
                        line_item.set_variant_inventory_management(item.get("variant_inventory_management", ""))
                        line_item.set_variant_title(item.get("variant_title", ""))
                        line_item.set_vendor(item.get("vendor", ""))
                        if "custom" in item.get("name").lower():
                            custom_product_name = item.get("name")
                            custom_products_array.append(custom_product_name)
                            found_custom_item = True
                        line_item.set_name(item.get("name", ""))
                        line_item.set_gift_card(item.get("gift_card", ""))
                        line_item.set_properties(item.get("properties", ""))
                        line_item.set_taxable(item.get("taxable", ""))
                        line_item.set_tax_lines(item.get("tax_lines", ""))
                        line_item.set_total_discount(item.get("total_discount", ""))
                        line_item.set_total_discount_set(item.get("total_discount_set", ""))
                        line_item.set_discount_allocations(item.get("discount_allocations", ""))
                        line_item.set_duties(item.get("duties", ""))
                        line_item.set_admin_graphql_api_id(item.get("admin_graphql_api_id", ""))

                        is_item_upserted_success, line_item_inserted_count, line_item_updated_count, response_description = self.upsert_line_items(order_id=order.get_id(), line_item=line_item)

                        if is_item_upserted_success:
                            items_upserted_count += line_item_inserted_count + line_item_updated_count
                            if line_item_inserted_count > 0:
                                self.print_log(log_level="info", type="line_item", function="insert", order_id=last_order_id, item_id=last_order_line_item_id, try_count=None, message=f" - {items_upserted_count} of {items_count}")
                            elif line_item_updated_count > 0:
                                self.print_log(log_level="info", type="line_item", function="update", order_id=last_order_id, item_id=last_order_line_item_id, try_count=None, message=f" - {items_upserted_count} of {items_count}")
                            else:
                                self.print_log(log_level="error", type="line_item", function="process", order_id=last_order_id, item_id=last_order_line_item_id, try_count=None, message=f" - {items_upserted_count} of {items_count}")
                        else:
                            error_message = f"[ERROR] Error while processing line item: {response_description}"
                            additional_details = f'[INFO] Line Item JSON: {item}'
                            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                else:
                    error_message = f"[ERROR] No line items found for order: {last_order_id}"
                    additional_details = f'[INFO] Order JSON: {order_json}'
                    self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
            else:
                error_message = f"[ERROR] Error while processing order: {response_description}"
                additional_details = f'[INFO] Order JSON: {order_json}'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))

            print(f"[INFO] {items_upserted_count} of {items_count} item(s) was/were upserted...\tOrder: {last_order_id}")

            if is_custom_order and found_custom_item:
                print(f"[INFO] Order has custom items...\t\tOrder: {last_order_id}")
                if order_fulfillment_status == "fulfilled" or order_fulfillment_status == "partial":
                    print(f"[INFO] Order has items to fulfill...\t\tOrder: {last_order_id}")
                    try:
                        for product in custom_products_array:
                            print(f"[INFO] Updating Custom Fulfillment...\t\tOrder: {last_order_id}\tProd: {product.replace('Custom/Personalized ', '')}")
                            result_code, result_flag, count_products_updated, result_string = self.update_customs_fulfillment_status(order_id=last_order_id, fulfillments=fulfillments_json, product_name=product)
                            if result_flag:
                                if count_products_updated > 0:
                                    print(f"[INFO] Custom fulfillment status updated...\tOrder: {last_order_id}\tProd: {product.replace('Custom/Personalized ', '')}")
                                else:
                                    print(f"[INFO] Custom not fulfilled yet...\t\tOrder: {last_order_id}\tProd: {product.replace('Custom/Personalized ', '')}")
                            else:
                                if result_string is not None and result_string != "Success":
                                    print(f"[ERROR] Error while updating customs fulfillment status: {result_string}")
                                    error_message = f"[ERROR] Error while updating customs fulfillment status: {result_string}"
                                    additional_details = f'[INFO] Order JSON: {order_json}'
                                    self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                                    pass
                        print(f"[INFO] Finished updating customs fulfillment...\tOrder: {last_order_id}")
                    except Exception as e:
                        print(f"[ERROR] Error while updating customs fulfillment status: {e}")
                        error_message = f"[ERROR] Error while updating customs fulfillment status: {e}"
                        additional_details = f'[INFO] Order JSON: {order_json}'
                        self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                        pass

                if order_cancelled_at is not None and order_cancelled_at != "":
                    print(f"[INFO] Order cancelled... Cancelling customs...\tOrder: {last_order_id}")
                    try:
                        result_code, result_flag, rowcount, result_string = self.cancel_or_refund_custom_item(order_id=last_order_id, product_name=None, cancel_reason="cancelled")
                        if result_flag:
                            if rowcount > 0:
                                print(f"[INFO] Custom cancelled...\t\t\tOrder: {last_order_id}")
                        else:
                            if result_string is not None and result_string != "Success":
                                print(f"[ERROR] Error while updating customs cancel: {result_string}")
                                error_message = f"[ERROR] Error while updating customs cancel: {result_string}"
                                additional_details = f'[INFO] Order JSON: {order_json}'
                                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                                pass
                    except Exception as e:
                        print(f"[ERROR] Error while processing custom cancel: {e}")
                        error_message = f"[ERROR] Error while processing custom cancel: {e}"
                        additional_details = f'[INFO] Order JSON: {order_json}'
                        self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                        pass
                else:
                    if refunds is not None and refunds != []:
                        print(f"[INFO] Order has items to refund...\t\tOrder: {last_order_id}")
                        try:
                            for product in custom_products_array:
                                result_code, result_flag, rowcount, result_string = self.verify_if_has_custom_item_to_refund(order_id=last_order_id, refunds=refunds, product_name=product)

                                if result_flag:
                                    if rowcount > 0:
                                        print(f"[INFO] Custom refunded...\t\t\tOrder: {last_order_id}\tProd: {product.replace('Custom/Personalized ', '')}")
                                else:
                                    if result_string is not None and result_string != "Success":
                                        print(f"[ERROR] Error while updating refund of custom item {product}: {result_string}")
                                        error_message = f"[ERROR] Error while updating customs refund: {result_string}"
                                        additional_details = f'[INFO] Order JSON: {order_json}'
                                        self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                                        pass
                            print(f"[INFO] Finished refunding Customs...\tOrder: {last_order_id}")
                        except Exception as e:
                            print(f"[ERROR] Error while processing refunds: {e}")
                            error_message = f"[ERROR] Error while processing refunds: {e}"
                            additional_details = f'[INFO] Order JSON: {order_json}'
                            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                            pass

            return is_order_upserted_success, is_item_upserted_success, total_order_upserted_count, items_upserted_count, last_order_id
        except Exception as e:
            print(f"[ERROR] Error while processing order: {e}")
            error_message = f"[ERROR] Error while processing order: {e}"
            additional_details = f'[INFO] Order JSON: {order_json}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_order_and_line_items", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
            return is_order_upserted_success, is_item_upserted_success, total_order_upserted_count, items_upserted_count, last_order_id
        finally:
            # print(f"\n[INFO] Cleaning up variables...")
            try:
                del last_order_id, order_name, error_message, additional_details, is_order_upserted_success, total_order_upserted_count, order_inserted_count, order_updated_count, response_description, items_upserted_count, items_count, is_item_upserted_success, line_item_inserted_count, line_item_updated_count, last_order_line_item_id, item, is_custom_order, found_custom_item, custom_product_name, custom_products_array, order_fulfillment_status, line_items, result_flag, count_products_updated, order_cancelled_at, refunds, refunded_items, result_string, result_code, billing_address_json, shipping_address_json, discount_codes_json, discount_applications_json, fulfillments_json, refunds_json, result_flag_billing_address, return_code_billing_address, rowcount_billing_address, result_string_billing_address, result_flag_shipping_address, return_code_shipping_address, rowcount_shipping_address, result_string_shipping_address, result_flag_discount_codes, return_code_discount_codes, rowcount_discount_codes, result_string_discount_codes, result_flag_discount_applications, return_code_discount_applications, rowcount_discount_applications, result_string_discount_applications, result_flag_fulfillments, return_code_fulfillments, rowcount_fulfillments, result_string_fulfillments, result_flag_refunds, return_code_refunds, rowcount_refunds, result_string_refunds
            except Exception as e:
                print(f"[INFO] Variable not found: {e}")
                pass

    def update_customs_fulfillment_status(self, order_id, fulfillments, product_name):
        item_fulfilled_flag = False
        item_fulfillment_status = "unfulfilled"
        fulfillment_created_at = None
        condition = None
        item = None
        result_flag = False
        rowcount = 0
        result_string = "Success"
        count_products_updated = 0
        product_replace = product_name.replace("null", "None")
        item_name = None
        item_name_replace = None
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND ID = '{order_id}'"
        condition += f"\nAND PROD_NAME = '{self.utils.replace_special_chars(product_replace)}'"
        item_found = False
        fulfillment_status = None
        fulfillment_cancelled = False
        order_name = None

        try:
            print(f"[INFO] Verifying Customs Management Table...\tOrder: {order_id} Prod: {product_replace.replace('Custom/Personalized ', '')}")
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                print(f"[INFO] Custom found...\t\t\t\tOrder: {order_id}")
                if result_query[0].get("TOTAL") > 0:
                    print(f"[INFO] Verifying if it is fulfilled...\t\tOrder: {order_id}")
                    if len(fulfillments) > 0:
                        print(f"[INFO] Found {len(fulfillments)} fulfillments...\t\t\tOrder: {order_id}")
                        print(f"[INFO] Sorting fulfillments by created_at...\tOrder: {order_id}")
                        fulfillments = sorted(fulfillments, key=lambda x: x.get('created_at'))
                        for fulfill in fulfillments:
                            order_name = fulfill.get('name')
                            fulfillment_status = fulfill.get('status')
                            fulfillment_created_at = fulfill.get('created_at').split('T')[0] + ' ' + fulfill.get('created_at').split('T')[1].split('-')[0]
                            line_items = fulfill.get('line_items')
                            print(f"\n[INFO] {order_name} fulfillment is {str(fulfillment_status)} on {fulfillment_created_at}...")
                            if fulfillment_status == "success" or fulfillment_status == "cancelled":
                                fulfillment_cancelled = True if fulfillment_status == "cancelled" else False
                                print(f"[INFO] Searching for the Custom...\t\tOrder: {order_id} Prod: {product_replace.replace('Custom/Personalized ', '')}")
                                if len(line_items) > 0:
                                    for item in line_items:
                                        item_name = item.get('name')
                                        item_name_replace = item_name.replace("null", "None")
                                        print(f"[INFO] Item: {item_name_replace}")
                                        if item_name_replace == product_replace:
                                            print(f"[INFO] Custom found in the Fulfillments...\tOrder: {order_id}")
                                            item_found = True
                                            item_fulfillment_status = item.get('fulfillment_status')
                                            if item_fulfillment_status == "fulfilled":
                                                item_fulfilled_flag = True
                                                print(f"[INFO] Custom is fulfilled...\t\t\tOrder: {order_id}")
                                                if fulfillment_cancelled:
                                                    item_fulfilled_flag = False
                                                    print(f"[INFO] Fulfillment is cancelled...\t\tOrder: {order_id}")
                                                    print(f"[INFO] Cancelling custom Fulfillment...\t\tOrder: {order_id}")
                                            else:
                                                item_fulfilled_flag = False
                                                print(f"[INFO] Custom not fulfilled... Ignoring...\tOrder: {order_id}")
                                            break
                                        else:
                                            print(f"[INFO] Item doesn't match...\t\t\tOrder: {order_id}")
                                            print(f"[INFO] Checking next item...\t\t\tOrder: {order_id}")
                                    print(f"[INFO] Finished checking items...\t\tOrder: {order_id}")
                                    print(f"[INFO] Checking if it has more fulfillments...\tOrder: {order_id}")
                                else:
                                    print(f"[INFO] No line items found in the fulfillment...\tOrder: {order_id}")
                                    print(f"[INFO] Checking if it has more fulfillments...\tOrder: {order_id}")
                            else:
                                print(f"[INFO] Checking if it has more fulfillments...\tOrder: {order_id}")
                        print(f"[INFO] Finished checking fulfillments...\tOrder: {order_id}")
                        if not item_found:
                            item_fulfilled_flag = False
                            print(f"[INFO] Custom not found in the Fulfillments...\tOrder: {order_id}")
                    else:
                        item_found = False
                        item_fulfilled_flag = False
                        print(f"[INFO] No fulfillments found for the order...\tOrder: {order_id}")
                else:
                    print(f"[INFO] Custom not found in the Customs Management table...\tOrder: {order_id}")
                    return 404, result_flag, 0, "Custom not found in the Customs Management table."
            else:
                print(f"[INFO] Custom not found in the Customs Management table...\tOrder: {order_id}")
                return 404, result_flag, 0, "Custom not found in the Customs Management table."

            print(f"\n[INFO] Updating Custom Fulfillment...\t\tOrder: {order_id}")
            # print(f"[INFO] FULFILLED_FLG: {"TRUE" if item_fulfilled_flag and not fulfillment_cancelled else "FALSE"}...")
            # print(f"[INFO] FULFILLMENT_STATUS: {item_fulfillment_status if item_fulfilled_flag and not fulfillment_cancelled else "NULL"}...")
            # print(f"[INFO] FULFILLED_DATE: {f"DATE_FORMAT('{fulfillment_created_at}', '%Y-%m-%d %H:%i:%s')" if item_fulfilled_flag and not fulfillment_cancelled else "NULL"}...")
            self.utils.clear_columns_values_arrays()
            # self.utils.validate_columns_values("RECEIVED_FLG", "TRUE")
            # self.utils.validate_columns_values("RECEIVED_DATE", f"DATE_FORMAT('{fulfillment_created_at}', '%Y-%m-%d %H:%i:%s')")
            self.utils.validate_columns_values("FULFILLED_FLG", "TRUE" if item_fulfilled_flag and not fulfillment_cancelled else "FALSE")
            self.utils.validate_columns_values("FULFILLMENT_STATUS", item_fulfillment_status if item_fulfilled_flag and not fulfillment_cancelled else "NULL")
            self.utils.validate_columns_values("FULFILLED_DATE", f"DATE_FORMAT('{fulfillment_created_at}', '%Y-%m-%d %H:%i:%s')" if item_fulfilled_flag and not fulfillment_cancelled else "NULL")
            self.utils.validate_columns_values("TOTAL_PROCESSING_TIME", "DATEDIFF(FULFILLED_DATE, CREATED_AT)" if item_fulfilled_flag and not fulfillment_cancelled else "0")
            self.utils.validate_columns_values("TOTAL_PROCESSING_TIME_UNIT", "days")
            condition = "1=1"
            condition += f"\nAND ID = '{order_id}'"
            condition += f"\nAND PROD_NAME = '{self.utils.replace_special_chars(product_replace)}'"

            result_flag, rowcount, result_string = super().update_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

            if result_flag:
                print(f"[INFO] Custom Fulfillment Updated...\t\tOrder: {order_id}\tProd: {product_replace.replace('Custom/Personalized ', '')}")
                count_products_updated += rowcount
                return 200, result_flag, count_products_updated, result_string
            else:
                if result_string is not None and result_string != "Success":
                    print(f"[ERROR] Error while updating custom fulfillment.\t\tOrder: {order_id} - Error: {result_string}")
                    return 404, result_flag, 0, result_string
                else:
                    return 400, result_flag, 0, result_string
        except Exception as e:
            print(f"[ERROR] Error while updating custom fulfillment: {e}")
            return False, 0, str(e)
        finally:
            try:
                del item_fulfilled_flag, item_fulfillment_status, fulfillment_created_at, condition, item, result_flag, rowcount, result_string, count_products_updated, product_replace, item_name, item_name_replace, columns, condition, item_found, fulfillment_status, fulfillment_cancelled
            except Exception as e:
                pass

    def verify_if_has_custom_item_to_refund(self, order_id, refunds, product_name):
        refund = None
        refund_items = None
        item_info = None
        item_name = None
        item_name_replace = None
        fulfillment_status = None
        line_item = None
        result_code = None
        result_flag = False
        rowcount = 0
        result_string = "Success"

        try:
            print(f"[INFO] Verifying if has custom to refund...\tOrder: {order_id} Prod: {product_name.replace('Custom/Personalized ', '')}")
            for refund in refunds:
                refund_items = refund.get("refund_line_items")
                for item_info in refund_items:
                    line_item = item_info.get("line_item")
                    item_name = line_item.get("name")
                    item_name_replace = item_name.replace("null", "None")
                    fulfillment_status = line_item.get("fulfillment_status")
                    if item_name_replace == product_name:
                        print(f"[INFO] Custom found in the refunds...\t\tOrder: {order_id} Prod: {product_name.replace('Custom/Personalized ', '')}")
                        result_code, result_flag, rowcount, result_string = self.cancel_or_refund_custom_item(order_id=order_id, product_name=product_name, cancel_reason="refunded")
                        return result_code, result_flag, rowcount, result_string

            return 404, result_flag, 0, "Custom not found in the refunds."
        except Exception as e:
            print(f"[ERROR] Error while verifying if has custom item to refund: {e}")
            return 500, False, 0, str(e)
        finally:
            try:
                del refund, refund_items, item_info, item_name, item_name_replace, fulfillment_status, line_item, result_code, result_flag, rowcount, result_string
            except Exception as e:
                pass

    def cancel_or_refund_custom_item(self, order_id, product_name, cancel_reason):
        product_replace = None
        if product_name is not None and product_name != "":
            product_replace = self.utils.replace_special_chars(product_name.replace("null", "None"))
        result_flag = False
        rowcount = 0
        result_string = "Success"
        columns = ["COUNT(*) AS TOTAL"]
        condition = "1=1"
        condition += f"\nAND ID = '{order_id}'"
        condition += f"\nAND PROD_NAME = '{product_replace}'" if product_replace is not None and product_replace != "" else ""

        try:
            print(f"[INFO] Verifying Customs Management table...\tOrder: {order_id} {("Prod: " + product_replace.replace('Custom/Personalized ', '')) if product_replace is not None and product_replace != "" else ""}")
            result_flag, result_query = super().query_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), columns, condition)

            if result_flag:
                if result_query[0].get("TOTAL") > 0:
                    self.utils.clear_columns_values_arrays()
                    self.utils.validate_columns_values("FULFILLED_FLG", "FALSE")
                    self.utils.validate_columns_values("FULFILLMENT_STATUS", cancel_reason)
                    self.utils.validate_columns_values("FULFILLED_DATE", "NULL")
                    self.utils.validate_columns_values("CANCELLED_FLG", "TRUE")
                    self.utils.validate_columns_values("VENDOR_PROCESSING_TIME", "0")
                    self.utils.validate_columns_values("TOTAL_SHIPPING_TIME", "0")
                    self.utils.validate_columns_values("TOTAL_PROCESSING_TIME", "0")
                    self.utils.validate_columns_values("VENDOR_PROCESSING_TIME_UNIT", "days")
                    self.utils.validate_columns_values("TOTAL_SHIPPING_TIME_UNIT", "days")
                    self.utils.validate_columns_values("TOTAL_PROCESSING_TIME_UNIT", "days")

                    print(f"[INFO] Cancelling Custom...\t\t\tOrder: {order_id} {("Prod: " + product_replace.replace('Custom/Personalized ', '')) if product_replace is not None and product_replace != "" else ""}")
                    result_flag, rowcount, result_string = super().update_record(super().get_tbl_CUSTOM_ORDERS_MANAGEMENT(), self.utils.get_columns_array(), self.utils.get_values_array(), condition)

                    if result_flag:
                        print(f"[INFO] Custom Cancelled...\t\t\tOrder: {order_id} {("Prod: " + product_replace.replace('Custom/Personalized ', '')) if product_replace is not None and product_replace != "" else ""}")
                        return 200, result_flag, rowcount, result_string
                    else:
                        if result_string is not None and result_string != "Success":
                            print(f"[ERROR] Error while cancelling custom.\tOrder: Error: {result_string}")
                        return 404, result_flag, 0, result_string
                else:
                    print(f"[INFO] Custom not found...\t\t\tOrder: {order_id} {("Prod: " + product_replace.replace('Custom/Personalized ', '')) if product_replace is not None and product_replace != "" else ""}")
                    return 404, result_flag, 0, "Custom not found in the Customs Management table."
            else:
                print(f"[INFO] Custom not found in the Customs Management table...\tOrder: {order_id}")
                return 404, result_flag, 0, "Custom not found in the Customs Management table."
        except Exception as e:
            print(f"[ERROR] Error while cancelling custom: {e}")
            return 500, False, 0, str(e)
        finally:
            try:
                del product_replace, result_flag, rowcount, result_string, columns, condition
            except Exception as e:
                pass

    # Function to get all orders from shopify and save into the database - Starting Function
    def get_all_shopify_orders(self, runCounter, maxRuns, fields, limit, status, created_at_min, created_at_max, processed_at_min, processed_at_max, since_id, fulfillment_status, api_version):
        continue_flag = True
        last_order_id = since_id

        while continue_flag:
            # Variables
            item = None
            line_items = None
            line_items = None
            error_message = None
            additional_details = None
            return_flag = False
            orders = None
            response_description = "Success"
            response_code = 200
            order_data = None
            is_order_upserted_success = False
            is_item_upserted_success = False
            total_order_upserted_count = 0
            items_upserted_count = 0
            additional_info = None

            if runCounter <= 1:
                self.utils.set_start_time(self.utils.get_current_date_time())
            if maxRuns == None or maxRuns == "" or maxRuns == "0":
                print('[DONE] Finished getting orders because maxRuns is None.')
                continue_flag = False
            if runCounter == None or runCounter == "" or runCounter == "0":
                print('[DONE] Finished getting orders because runCounter is None.')
                continue_flag = False
            if limit == None or limit == "" or limit == "0":
                print('[DONE] Finished getting orders because limit is None.')
                continue_flag = False

            print(f'\n[INFO] Getting orders: {runCounter} of {maxRuns}')

            try:
                return_flag, orders, response_description, response_code = self.shopify.get_shopify_list_of_orders(fields=fields, limit=limit, status=status, created_at_min=created_at_min, created_at_max=created_at_min, processed_at_min=created_at_min, processed_at_max=created_at_min, since_id=last_order_id, fulfillment_status=fulfillment_status, api_version=api_version)

                if len(orders) == 0:
                    del orders
                    print('[DONE] Finished getting orders because it found 0.')
                    self.utils.set_end_time(self.utils.get_current_date_time())
                    self.execution_summary(is_webhook=False, send_email=True)
                    continue_flag = False
                else:
                    print(f'[INFO] Total of orders: {len(orders)}\n')
                    print(f"[INFO] Upserting Orders...")
                    for order_data in orders:
                        is_order_upserted_success, is_item_upserted_success, total_order_upserted_count, items_upserted_count, last_order_id = self.process_order_and_line_items(order_json=order_data)

                    if runCounter < maxRuns:
                        runCounter += 1
                        print(f'[INFO] Getting more orders from Shopify. Run number: {runCounter} of {maxRuns}')
                        continue_flag = True
                        # return self.get_all_shopify_orders(runCounter=runCounter, maxRuns=maxRuns, fields=fields, limit=limit, status=status, created_at_min=created_at_min, created_at_max=created_at_max, processed_at_min=processed_at_min, processed_at_max=processed_at_max, since_id=last_order_id, fulfillment_status=fulfillment_status, api_version=api_version)
                    else:
                        print('[DONE] Finished getting orders because it reached the maxRuns.')
                        self.utils.set_end_time(self.utils.get_current_date_time())
                        self.execution_summary(is_webhook=False, send_email=True)
                        continue_flag = False
            except Exception as e:
                self.utils.set_end_time(self.utils.get_current_date_time())

                last_order_id = last_order_id if last_order_id is not None else since_id
                additional_info = f'[INFO] runCounter: {runCounter}'
                additional_info += f'\n[INFO] maxRuns: {maxRuns}'
                additional_info += f'\n[INFO] fields: {fields}'
                additional_info += f'\n[INFO] limit: {limit}'
                additional_info += f'\n[INFO] status: {status}'
                additional_info += f'\n[INFO] created_at_min: {created_at_min}'
                additional_info += f'\n[INFO] created_at_max: {created_at_max}'
                additional_info += f'\n[INFO] processed_at_min: {processed_at_min}'
                additional_info += f'\n[INFO] processed_at_max: {processed_at_max}'
                additional_info += f'\n[INFO] since_id: {since_id}'
                additional_info += f'\n[INFO] fulfillment_status: {fulfillment_status}'
                additional_info += f'\n[INFO] api_version: {api_version}'
                additional_info += f'\n[INFO] last_order_id: {last_order_id}'
                additional_info += f'\n[INFO] The function get_all_shopify_orders() execution kept running on Heroku with since_id = {last_order_id}. Please check the logs for more information.'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_all_shopify_orders", error_code=None, error_message=str(e), additional_details=additional_info, error_severity=self.utils.get_error_severity(2))

                if runCounter < maxRuns:
                    runCounter += 1
                    print(f'[INFO] Getting more orders from Shopify. Run number: {runCounter} of {maxRuns}')
                    continue_flag = True
                    # return self.get_all_shopify_orders(runCounter=runCounter, maxRuns=maxRuns, fields=fields, limit=limit, status=status, created_at_min=created_at_min, created_at_max=created_at_max, processed_at_min=processed_at_min, processed_at_max=processed_at_max, since_id=last_order_id, fulfillment_status=fulfillment_status, api_version=api_version)
                else:
                    print('[DONE] Finished getting orders because it reached the maxRuns.')
                    self.utils.set_end_time(self.utils.get_current_date_time())
                    self.execution_summary(is_webhook=False, send_email=True)
                    continue_flag = False
            finally:
                print(f"[INFO] Cleaning up variables...")
                try:
                    del order_data
                    del orders
                    del return_flag
                    del response_description
                    del response_code
                    del error_message
                    del additional_details
                    del item
                    del line_items
                    del additional_info
                    del is_order_upserted_success
                    del is_item_upserted_success
                    del total_order_upserted_count
                    del items_upserted_count
                except Exception as e:
                    print(f"[INFO] Variable not found: {e}")
                    pass

    # Function to get an specific order from shopify and save into the database - Starting Function
    def get_specific_order(self, orderId, fields, api_version):
        print(f'[INFO] Getting order: {orderId}')

        print("[INFO] Instantiating Variables...")
        # Variables
        response_description = "Success"
        status_code = 200
        is_upserted_success = False
        order_inserted_count = 0
        additional_details = None
        return_flag = None
        order_data = None
        staging_table_count = 0

        try:
            return_flag, order_data, response_description, status_code = self.shopify.get_shopify_specific_order(orderId=orderId, fields=fields, api_version=api_version)

            if return_flag == True:
                if len(order_data) == 0:
                    return False, f'[ERROR] No order found with ID: {orderId}.', status_code
                else:
                    staging_table_count = self.verify_staging_table_already_exits(order_json=order_data)
                    if staging_table_count < 1:
                        is_upserted_success, order_inserted_count, response_description = self.save_order_on_staging_table(order_data)
                    else:
                        print(f'[INFO] Order already exists in the staging table. No JSON difference for Order id: {orderId}')
                        return False, f'[INFO] Order already exists in staging table. Order id: {orderId}', status_code

                    if is_upserted_success == False:
                        additional_details = f'[INFO] orderId: {orderId}'
                        additional_details += f'\n[INFO] fields: {fields}'
                        additional_details += f'\n[INFO] api_version: {api_version}'
                        additional_details += f'\n[INFO] response_description: {response_description}'
                        self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_specific_order", error_code=status_code, error_message=f"[ERROR] Error while upserting order.\tOrder id: {orderId}", additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
                        return False, f"[ERROR] Error while upserting order.\tOrder id: {orderId}", 400
                    else:
                        return True, response_description, status_code
            else:
                return False, response_description, status_code
        except Exception as e:
            additional_details = f'[INFO] orderId: {orderId}'
            additional_details += f'\n[INFO] fields: {fields}'
            additional_details += f'\n[INFO] api_version: {api_version}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_specific_order", error_code="400", error_message=str(e), additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
            return False, f"[ERROR] Error while processing order.\tOrder id: {orderId}", 400
        finally:
            try:
                del additional_details
                # del order
                del order_data
                del return_flag
                del response_description
                del status_code
                del is_upserted_success
                del order_inserted_count
            except:
                print(f"[INFO] Variable not found: {e}")
                pass

    # Function to get all orders with phone cases from shopify and save into the database
    def get_all_valid_orders_with_phone_cases(self, phone_case_ids, since_id, limit, country_code):
        print(f"\n[INFO] Getting all valid orders with phone cases...")

        print("[INFO] Instantiating Variables...")
        # Variables
        phone_cases_orders = []
        found_phone_cases = False
        count_orders = 0
        count_items = 0
        orders_length = 0
        items_length = 0
        i = 0
        order_result_query = None
        item_result_query = None
        items_ids_condition = ""
        first_order_id = None
        last_order_id = None
        order_result_flag = None
        item_result_flag = None
        result_flag_shipping_address = False
        result_query_shipping_address = None
        shipp_address = None

        # Query Variables
        order_columns = ["ID", "NAME", "CREATED", "CREATED_AT", "EMAIL", "TAGS", "COUNTRY_CODE", "CONCAT(JSON_UNQUOTE(JSON_EXTRACT(CUSTOMER, '$.first_name')), \" \", JSON_UNQUOTE(JSON_EXTRACT(CUSTOMER, '$.last_name'))) AS CUSTOMER", "SHIPPING_ADDRESS", "SHIPPING_LABEL_URL"]
        line_item_columns = ["ID", "ORDER_ID", "PRODUCT_ID", "NAME", "TITLE", "SKU", "VARIANT_ID", "VARIANT_TITLE", "FULFILLABLE_QUANTITY"]

        order_condition = "1=1"
        order_condition += f"\nAND ID > '{since_id}'" if since_id is not None and since_id != "" else ""
        # order_condition += f"\nAND NAME IN ('#A2852559')"
        order_condition += f"\nAND CLOSED_AT IS NULL"
        order_condition += f"\nAND CANCELLED_AT IS NULL"
        if country_code is not None and country_code != "":
            if country_code == 'US':
                order_condition += f"\nAND COUNTRY_CODE = 'US'"
                order_condition += f"\nAND PROCESSED_DATE IS NOT NULL"
                order_condition += f"\nAND FULFILLED_DATE IS NULL"
                order_condition += f"\nAND SHIPPING_LABEL_URL IS NOT NULL"
                order_condition += f"\nAND TRACKING_INFO IS NOT NULL"
            # else:
            #     order_condition += f"\nAND COUNTRY_CODE = '{country_code}'" if country_code is not None and country_code != "" else ""
        order_condition += f"\nAND PRINTED_DATE IS NULL"
        order_condition += f"\nAND (FULFILLMENT_STATUS IS NULL OR FULFILLMENT_STATUS = 'partial')"
        order_condition += f"\nAND (TAGS LIKE '%XXXXXXXXXX%' AND (TAGS NOT LIKE '%XXXXXXXXXX%' AND TAGS NOT LIKE '%XXXXXXXXXX%' AND TAGS NOT LIKE '%XXXXXXXXXX%' AND TAGS NOT LIKE '%XXXXXXXXXX%' AND TAGS NOT LIKE '%XXXXXXXXXX%' AND TAGS NOT LIKE '%XXXXXXXXXX%' AND TAGS NOT LIKE '%XXXXXXXXXX%'{ " AND TAGS NOT LIKE '%XXXXXXXXXX%' AND TAGS NOT LIKE '%XXXXXXXXXX%'" if country_code == 'US' else "" }))"
        order_condition += f"\nAND CREATED <= DATE_SUB(NOW(), INTERVAL 120 MINUTE)"
        order_condition += f"\nORDER BY CREATED_AT ASC"
        order_condition += f"\nLIMIT {limit}" if limit is not None and limit != "" and limit != "0" else ""

        if len(phone_case_ids) > 0:
            items_ids_condition += f"\nAND PRODUCT_ID IN ("
            for case_id in phone_case_ids:
                if i == 0:
                    items_ids_condition += f"\n\t'{case_id}'"
                else:
                    items_ids_condition += f",\n\t'{case_id}'"
                i += 1
            items_ids_condition += f"\n)"

        try:
            print(f"[INFO] Querying for orders with Phone Cases... Limit: {limit}")
            # print(f"[INFO] Query Condition: {order_condition}")
            order_result_flag, order_result_query = super().query_record(super().get_tbl_ORDER(), order_columns, order_condition)

            if order_result_flag:
                i = 0
                orders_length = len(order_result_query)
                print(f"[INFO] Got a total of {orders_length} Phone Case orders...")
                print(f"\n[INFO] Orders:")

                for row_order in order_result_query:
                    i += 1
                    found_phone_cases = False

                    order_id = row_order.get("ID")

                    order_data = {
                        "order_id": order_id,
                        "order_name": row_order.get("NAME"),
                        "created_at": f"{row_order.get("CREATED_AT")}",
                        "customer_name": row_order.get("CUSTOMER"),
                        "customer_email": row_order.get("EMAIL"),
                        "tags": row_order.get("TAGS"),
                        "country_code": row_order.get("COUNTRY_CODE"),
                        "shipping_label_url": row_order.get("SHIPPING_LABEL_URL"), # "https://api.shipengine.com/v1/labels/se-12345678.pdf
                        "line_items": [],
                        "shipping_address": []
                    }

                    try:
                        shipp_address = row_order.get("SHIPPING_ADDRESS")
                        if shipp_address is not None and shipp_address != "":
                            order_data['shipping_address'] = self.utils.convert_json_to_object(shipp_address)
                        else:
                            result_flag_shipping_address, result_query_shipping_address = self.shipping_address.get_specific_shipping_address(order_id)
                            if result_flag_shipping_address:
                                order_data['shipping_address'] = result_query_shipping_address
                    except Exception as e:
                        print(f"[ERROR] Error while getting shipping address: {e}")
                        pass

                    line_item_condition = "1=1"
                    if len(phone_case_ids) > 0:
                        line_item_condition += items_ids_condition
                    line_item_condition += f"\nAND ORDER_ID = '{order_id}'"
                    line_item_condition += f"\nAND (FULFILLMENT_STATUS IS NULL OR FULFILLMENT_STATUS = 'partial')"
                    line_item_condition += f"\nAND FULFILLABLE_QUANTITY > 0"

                    item_result_flag, item_result_query = super().query_record(super().get_tbl_ORDER_LINE_ITEM(), line_item_columns, line_item_condition)
                    if item_result_flag:
                        items_length = len(item_result_query)
                        count_items += items_length
                        found_phone_cases = True
                        for row_item in item_result_query:
                            item_data = {
                                "id": row_item.get("ID"),
                                "order_id": row_item.get("ORDER_ID"),
                                "product_id": row_item.get("PRODUCT_ID"),
                                "name": row_item.get("NAME"),
                                "title": row_item.get("TITLE"),
                                "sku": row_item.get("SKU"),
                                "variant_id": row_item.get("VARIANT_ID"),
                                "variant_title": row_item.get("VARIANT_TITLE"),
                                "fulfillable_quantity": row_item.get("FULFILLABLE_QUANTITY")
                            }
                            order_data['line_items'].append(item_data)

                    print(f"[INFO] {i}...{orders_length} - Order: {row_order.get("NAME")} - Total of Phone Cases: {items_length}")
                    if found_phone_cases:
                        if count_orders < 1:
                            first_order_id = order_id
                        last_order_id = order_id
                        count_orders += 1
                        phone_cases_orders.append(order_data)

                print(f"[INFO] Total of valid Orders: {count_orders} out of {orders_length}")
                print(f"[INFO] Total of Phone Cases: {count_items}")

                return True, phone_cases_orders, count_orders, count_items, first_order_id, last_order_id
            else:
                return False, None, count_orders, count_items, first_order_id, last_order_id
        except Exception as e:
            error_message = f"[ERROR] Error while getting all valid orders with phone cases. Error: {str(e)}."
            additional_details = f'[INFO] phone_case_ids: {phone_case_ids}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_all_valid_orders_with_phone_cases", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
            return False, None, count_orders, count_items, first_order_id, last_order_id
        finally:
            # Clearing Variables
            print("[INFO] Clearing variables...")
            try:
                del phone_cases_orders
                del found_phone_cases
                del count_orders
                del count_items
                del orders_length
                del columns
                del order_columns
                del row_order
                del row_item
                del rows_order
                del rows_item
                del order_result_query
                del line_item_columns
                del line_item_condition
                del row_order
                del row_item
                del order_data
                del line_items
                del item
                del item_prod_id
                del item_data
                del order_condition
                del last_order_id
                del order_result_flag
                del item_result_flag
            except:
                pass

    # Called by the function process_phone_case_orders
    def match_phone_cases_with_images(self, phone_cases_orders):
        print(f"\n[INFO] BEGIN - Matching Phone Cases with Images...")

        print("[INFO] Instantiating Variables...")
        #Variables
        orders_counter = 0
        items_counter = 0
        total_items = 0
        phone_cases_orders_length = len(phone_cases_orders)
        line_items_length = 0
        phone_cases_array = []
        phone_case_products_info = []
        phone_case_data = {}
        phone_case_filtered = None
        columns = None
        row = None
        result_flag = False
        columns_array = ["PHONE_CASE_ID", "HAS_VARIANT_FLG", "VARIANT_NAME", "PHONE_CASE_FILE_PATH"]
        condition = "1=1"

        try:
            result_flag, result_query = super().query_record(super().get_tbl_PHONE_CASE_IMAGES(), columns_array, condition)
            if result_flag:
                for row in result_query:
                    phone_case_data = {
                        "phone_case_id": row.get('PHONE_CASE_ID'),
                        "has_variant_flg": row.get('HAS_VARIANT_FLG'),
                        "variant_title": row.get('VARIANT_NAME'),
                        "phone_case_file_path": row.get('PHONE_CASE_FILE_PATH')
                    }
                    phone_case_products_info.append(phone_case_data)

            for phone_cases in phone_cases_orders:
                orders_counter += 1
                items_counter = 0
                items = phone_cases.get('line_items')
                line_items_length = len(items)

                for item in items:
                    total_items += 1
                    items_counter += 1
                    product_id = item.get('product_id')
                    variant_title = item.get('variant_title')
                    variant_title_splited = variant_title.split("/")[0].strip()

                    phone_case_filtered = [phone_case for phone_case in phone_case_products_info if phone_case["phone_case_id"] == str(product_id)]
                    if len(phone_case_filtered) > 1:
                        phone_case_filtered = [phone_case for phone_case in phone_case_products_info if phone_case["phone_case_id"] == str(product_id) and phone_case["variant_title"] == str(variant_title_splited)]

                    if len(phone_case_filtered) > 0:
                        for i in phone_case_filtered:
                            order_data = {
                                "order_id": phone_cases.get('order_id'),
                                "order_name": phone_cases.get('order_name'),
                                "created_at": phone_cases.get('created_at'),
                                "country_code": phone_cases.get('country_code'),
                                "item_name": item.get('name'),
                                "item_title": item.get('title'),
                                "variant_title": i.get('variant_title'),
                                "phone_case_file_path": i.get('phone_case_file_path'),
                                "fulfillable_quantity": item.get('fulfillable_quantity')
                            }
                            print(f"[INFO] {str(orders_counter)}...{str(phone_cases_orders_length)} Phone Case(s) Matched... Order: {phone_cases.get('order_name')} - Item {items_counter} of {line_items_length}: {item.get('name')}")
                            phone_cases_array.append(order_data)

            print(f"[INFO] Total of {total_items} phone cases matched...")
            print(f"[INFO] END - Finished Matching Phone Cases with Images...")

            return phone_cases_array
        except Exception as e:
            error_message = f"[ERROR] Error while matching phone cases with images. Error: {str(e)}."
            additional_details = f'[INFO] phone_cases_orders: {phone_cases_orders}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="match_phone_cases_with_images", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
            return []
        finally:
            print("[INFO] Clearing variables...")
            try:
                del orders_counter
                del items_counter
                del total_items
                del phone_cases_orders_length
                del line_items_length
                del phone_cases_array
                del phone_case_products_info
                del phone_case_data
                del phone_case_filtered
                del columns
                del row
                del columns_array
                del condition
                del result_flag
            except:
                pass

    # Called by the function process_phone_case_orders
    def update_orders_processing_info(self, orders_array, country_code):
        print(f"\n[INFO] BEGIN - Updating Orders Processing Info...")
        order_columns = ["PRINTED_DATE"]
        order_values = []
        order_condition = "1=1"
        counter = 0
        len_orders = len(orders_array)
        country_code = country_code if country_code is not None and country_code != "" else "ALL"

        try:
            order_values.append("DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')")
            if country_code != 'US':
                order_columns.append("PROCESSED_DATE")
                order_values.append("DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')")

            order_condition += f"\nAND ID IN ("
            for order in orders_array:
                counter += 1
                print(f"[INFO] {counter}...{len_orders} Updating Order: {order.get('order_name')}")
                if counter == 1:
                    order_condition += f"\n\t'{order.get('order_id')}'"
                else:
                    order_condition += f",\n\t'{order.get('order_id')}'"
            order_condition += f"\n)"

            # print(f'[INFO] Order Condition: {order_condition}')
            print(f"[INFO] Updating Orders Processing Info...")
            return super().update_record(super().get_tbl_ORDER(), order_columns, order_values, order_condition)
        except Exception as e:
            print(f"[ERROR] Error while updating orders processing info: {e}")
            return False, 0, str(e)
        finally:
            print(f"[INFO] END - Finished Updating Orders Processing Info...")
            try:
                del order_columns, order_values, order_condition
            except:
                pass

    # Function to process Phone Cases Orders - Starting function
    def process_phone_case_orders(self, limit, country_code=None):
        print(f"\n[INFO] BEGIN - Processing phone case orders..." if country_code is None else f"\n[INFO] BEGIN - Processing phone case orders for country code: {country_code}")

        print("[INFO] Instantiating Variables...")
        # Variables
        country_code = str(country_code).upper() if country_code is not None and country_code != "" else "ALL"
        print(f"[INFO] Country Code: {country_code}")
        debug_mode = True if self.utils.get_DEBUG_MODE() == "TRUE" else False
        last_batch_order_id = self.phone_cases.get_last_phone_case_batch_order_id(country_code=country_code) if not debug_mode else "4860667887692"
        last_batch_number = 0
        actual_batch_number = 0
        limit = self.utils.get_PHONE_CASE_LIMIT() if limit is None else limit
        current_date = self.utils.get_current_date_time()
        phone_cases_orders = []
        phone_cases_array = []
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.dirname(current_dir)
        output_file_path = os.path.join(base_dir, 'files/pdfs/phone_cases_output/')
        pdf_file_name = None
        slip_file_name = None
        small_slip_file_name = None
        file_names = []
        page_width_cm = 111.76
        page_height_cm = 19.80
        image_width_cm = 8.50
        image_height_cm = 16.80
        space_between_images_cm = 1
        phone_case_ids = None
        result_flag = None
        count_orders = 0
        count_items = 0
        first_order_id = 0
        last_order_id = 0
        batch_obj = None
        batch_inserted_flag = False
        batch_updated_flag = False
        batch_inserted_count = 0
        batch_updated_count = 0
        result_string = None
        pdf_generated_flag = False
        slips_generated_flag = False
        small_slips_generated_flag = False
        full_pdf_file = None
        full_slip_file = None
        full_small_slip_file = None
        results_pdf = None
        results_slips = None
        result_pdf_flag = False
        result_slips_flag = False
        order_updated_flag = False
        rowcount = 0
        result_pdf_url = None
        result_slips_url = None
        return_message = {
            "code": 200,
            "message": "Success",
            "batch_number": 0,
            "phone_cases_pdf": "",
            "phone_cases_slips": ""
        }
        printing_history = None
        batch_row_id = None
        order_array = []

        try:
            phone_case_ids = self.phone_cases.get_all_phone_cases_ids()
            result_flag, phone_cases_orders, count_orders, count_items, first_order_id, last_order_id = self.get_all_valid_orders_with_phone_cases(phone_case_ids=phone_case_ids, since_id=last_batch_order_id, limit=limit, country_code=country_code)
            # print(f"[INFO] Phone cases orders: {phone_cases_orders}")

            if result_flag and count_orders > 0:
                batch_obj = self.phone_cases.PhoneCaseBatch()
                batch_obj.set_FIRST_ORDER_ID(first_order_id)
                batch_obj.set_LAST_ORDER_ID(last_order_id)
                batch_obj.set_TOTAL_ORDERS(count_orders)
                batch_obj.set_TOTAL_CASES(count_items)
                batch_obj.set_COUNTRY_CODE(country_code)

                if not debug_mode:
                    batch_inserted_flag, batch_inserted_count, result_string = self.phone_cases.insert_phone_case_batch(batch_obj)
                    actual_batch_number, batch_row_id = self.phone_cases.get_last_phone_case_batch_number(country_code=country_code)
                else:
                    batch_inserted_flag = True
                    actual_batch_number = 45

                actual_batch_number = int(actual_batch_number)
                pdf_file_name = f"phone_cases_batch_{actual_batch_number}.pdf"
                slip_file_name = f"phone_cases_slips_batch_{actual_batch_number}.pdf"
                small_slip_file_name = f"phone_cases_small_slips_batch_{actual_batch_number}.pdf"
                file_names = [pdf_file_name, slip_file_name, small_slip_file_name]

                if batch_inserted_flag:
                    phone_cases_array = self.match_phone_cases_with_images(phone_cases_orders)
                    # print(f"phone_cases_array: {phone_cases_array}")
                    pdf_generated_flag, full_pdf_file = self.utils.generate_phone_cases_pdf(batch_number=actual_batch_number, output_file_path=output_file_path, output_file_name=pdf_file_name, page_width_cm=page_width_cm, page_height_cm=page_height_cm, image_width_cm=image_width_cm, image_height_cm=image_height_cm, phone_cases_array=phone_cases_array, space_between_images_cm=space_between_images_cm)
                    if pdf_generated_flag and not debug_mode:
                        results_pdf = self.utils.upload_files_to_s3(file_directory=self.utils.get_base_directory() + "/files/pdfs/phone_cases_output/", file_names=[pdf_file_name], bucket_name="COMPANY_NAME-phone-cases-processing-pdfs")
                    if country_code == 'US':
                        slips_generated_flag, full_slip_file = self.utils.generate_slips(orders_array=phone_cases_orders, type="phone_case", output_file_path=output_file_path, output_file_name=slip_file_name, batch_number=actual_batch_number, page_width_cm=10.16, page_height_cm=15.24)
                        if slips_generated_flag and not debug_mode:
                            results_slips = self.utils.upload_files_to_s3(file_directory=self.utils.get_base_directory() + "/files/pdfs/phone_cases_output/", file_names=[slip_file_name], bucket_name="COMPANY_NAME-phone-cases-processing-slips")
                    else:
                        small_slips_generated_flag, full_small_slip_file = self.utils.generate_small_size_slips(orders_array=phone_cases_orders, type="phone_case", output_file_path=output_file_path, output_file_name=small_slip_file_name, batch_number=actual_batch_number, page_width_cm=5.08, page_height_cm=3.81)
                        if not debug_mode:
                            results_slips = self.utils.upload_files_to_s3(file_directory=self.utils.get_base_directory() + "/files/pdfs/phone_cases_output/", file_names=[small_slip_file_name], bucket_name="COMPANY_NAME-phone-cases-processing-slips")

                    if not debug_mode:
                        for result_pdf in results_pdf:
                            result_pdf_flag = result_pdf.get('success')
                            if result_pdf_flag == True:
                                result_pdf_url = result_pdf.get('url')
                                batch_obj.set_PDFS_URL(result_pdf_url)
                                self.utils.delete_files(sheet_file_path=None, pdf_file_path=output_file_path, sheet_file_names=[], pdf_file_names=[pdf_file_name])

                    if not debug_mode:
                        for result_slip in results_slips:
                            result_slips_flag = result_slip.get('success')
                            if result_slips_flag == True:
                                result_slips_url = result_slip.get('url')
                                batch_obj.set_SLIPS_PDFS_URL(result_slips_url)
                                self.utils.delete_files(sheet_file_path=None, pdf_file_path=output_file_path, sheet_file_names=[], pdf_file_names=[slip_file_name if country_code == 'US' else small_slip_file_name])

                    if debug_mode:
                        result_pdf_flag = True
                        result_slips_flag = True

                    if result_pdf_flag and result_slips_flag:
                        if not debug_mode:
                            batch_updated_flag, batch_updated_count, result_string = self.phone_cases.update_phone_case_batch(phone_case_batch_number=actual_batch_number, phone_case_batch=batch_obj)
                        else:
                            batch_updated_flag = True
                        if batch_updated_flag:
                            if not debug_mode:
                                order_updated_flag, rowcount, return_string = self.update_orders_processing_info(orders_array=phone_cases_orders, country_code=country_code)
                            else:
                                order_updated_flag = True

                            if order_updated_flag:
                                if not debug_mode:
                                    printing_history = self.printing.printingHistory()
                                    printing_history.set_batch_id(batch_row_id)
                                    printing_history.set_batch_number(actual_batch_number)
                                    printing_history.set_pdf_name(pdf_file_name)
                                    printing_history.set_pdf_type("normal_phone_case" if country_code != 'US' else "partial_phone_case")
                                    printing_history.set_pdf_url(result_pdf_url)
                                    printing_history.set_slip_pdf_url(result_slips_url)
                                    printing_history.set_printed_date(f"DATE_FORMAT('{current_date}', '%Y-%m-%d %H:%i:%s')" if printing_history is not None else None)
                                    for order in phone_cases_orders:
                                        order_array.append(order.get('order_id'))
                                    printing_history.set_order_ids(self.utils.convert_object_to_json(order_array))
                                    printing_history.set_total_orders(count_orders)

                                    inserted_flag, inserted_count, result_string = self.printing.insert_printing_history(printing_history)

                                    if inserted_flag:
                                        print(f"[INFO] Printing History inserted successfully.")
                                    else:
                                        self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_phone_case_orders", error_code=None, error_message=result_string, additional_details=None, error_severity=self.utils.get_error_severity(2))

                                return_flag = True
                                status_code = 200
                                return_message["code"] = 200
                                return_message["message"] = "Success"
                                return_message["batch_number"] = actual_batch_number
                                return_message["phone_cases_pdf"] = result_pdf_url
                                return_message["phone_cases_slips"] = result_slips_url
                            else:
                                print(f"[ERROR] Error while updating orders processing info. Error: {return_string}")
                                return_flag = False
                                status_code = 500
                                return_message["code"] = 500
                                return_message["message"] = f"Error while updating orders processing info. Error: {return_string}"
                                return_message["batch_number"] = actual_batch_number
                                return_message["phone_cases_pdf"] = ""
                                return_message["phone_cases_slips"] = ""
                        else:
                            print(f"[ERROR] Error while updating phone case batch. Error: {result_string}")
                            return_flag = False
                            status_code = 500
                            return_message["code"] = 500
                            return_message["message"] = f"Error while updating phone case batch. Error: {result_string}"
                            return_message["batch_number"] = actual_batch_number
                            return_message["phone_cases_pdf"] = ""
                            return_message["phone_cases_slips"] = ""
                    else:
                        print(f"[ERROR] Error while updating phone case batch. Error: {result_string}")
                        return_flag = False
                        status_code = 500
                        return_message["code"] = 500
                        return_message["message"] = f"Error while updating phone case batch. Error: {result_string}"
                        return_message["batch_number"] = actual_batch_number
                        return_message["phone_cases_pdf"] = ""
                        return_message["phone_cases_slips"] = ""
                else:
                    print(f"[ERROR] Error while inserting phone case batch. Error: {result_string}")
                    return_flag = False
                    status_code = 500
                    return_message["code"] = 500
                    return_message["message"] = f"Error while inserting phone case batch. Error: {result_string}"
                    return_message["batch_number"] = actual_batch_number
                    return_message["phone_cases_pdf"] = ""
                    return_message["phone_cases_slips"] = ""
            else:
                print(f"\n[INFO] No phone cases orders found to process...")
                return_flag = False
                status_code = 404
                return_message["code"] = 404
                return_message["message"] = "No phone cases orders found to process."
                return_message["batch_number"] = actual_batch_number
                return_message["phone_cases_pdf"] = ""
                return_message["phone_cases_slips"] = ""

            return return_flag, status_code, return_message
        except Exception as e:
            error_message = f"[ERROR] Error while processing phone case orders. Error: {str(e)}."
            additional_details = f'[INFO] since_id: {last_batch_order_id}'
            additional_details += f'\n[INFO] batch_number: {actual_batch_number}'
            additional_details += f'\n[INFO] limit: {limit}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_phone_case_orders", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
            return_flag = False
            status_code = 500
            return_message["code"] = 500
            return_message["message"] = f"Error while processing phone case orders. Error: {str(e)}"
            return_message["batch_number"] = actual_batch_number
            return_message["phone_cases_pdf"] = ""
            return_message["phone_cases_slips"] = ""
            return return_flag, status_code, return_message
        finally:
            print(f"[INFO] END - Processing phone case orders...")
            print("\n[INFO] Clearing variables...")
            try:
                del last_batch_order_id, last_batch_number, actual_batch_number, limit, current_date, phone_cases_orders, phone_cases_array, current_dir, base_dir, output_file_path, pdf_file_name, slip_file_name, small_slip_file_name, file_names, page_width_cm, page_height_cm, image_width_cm, image_height_cm, space_between_images_cm, phone_case_ids, result_flag, count_orders, count_items, first_order_id, last_order_id, batch_obj, batch_inserted_flag, batch_updated_flag, batch_inserted_count, batch_updated_count, result_string, pdf_generated_flag, slips_generated_flag, small_slips_generated_flag, full_pdf_file, full_slip_file, full_small_slip_file, results_pdf, results_slips, result_pdf_flag, result_slips_flag, order_updated_flag, rowcount, result_pdf_url, result_slips_url, return_message
            except Exception as e:
                pass

    def prepare_orders_for_fullfillment(self, limit):
        # Variables
        first_order_id = None
        last_order_id = None
        one_hour_ago = self.utils.get_x_hours_ago(hours=1)
        order_ids = []
        columns = None
        values = None

        try:
            first_order_id = self.get_last_processed_order_id_batch()
            order_ids, last_order_id = self.get_order_ids_for_processing(first_id=first_order_id, last_id=None, created_at_min=None, created_at_max=one_hour_ago, limit=limit)
            if last_order_id is not None:
                columns = ["FIRST_ORDER_ID", "LAST_ORDER_ID", "TOTAL_ORDERS"]
                values = [first_order_id, last_order_id, len(order_ids)]

        except Exception as e:
            return False, f"[ERROR] Error while getting last processed order id. Error: {str(e)}"
        finally:
            print(f"Clearing variables...")
            try:
                del first_order_id, last_order_id, one_hour_ago, order_ids, columns, values
            except Exception as e:
                print(f"Variable not found: {e}")
                pass

    def get_order_id_by_name(self, order_name):
        # Variables
        order_id = None
        columns = ["ID"]
        condition = f"1=1"
        condition += f"\nAND NAME = '{order_name}'"
        result_flag = False

        try:
            result_flag, result_query = super().query_record(super().get_tbl_ORDER(), columns, condition)
            if result_flag:
                for row in result_query:
                    order_id = row.get("ID")
            return order_id
        except Exception as e:
            return None
        finally:
            try:
                del order_id
                del columns
                del condition
                del result_flag
            except Exception as e:
                print(f"Variable not found: {e}")
                pass

    def get_order_data_by_name(self, order_name, order_columns = ["ID"]):
        # Variables
        order_id = None
        if "ID" not in order_columns:
            columns = ["ID"]+order_columns
        else:
            columns = order_columns
        condition = "1=1"
        condition += f"\nAND NAME = '{order_name}'"
        result_flag = False

        try:
            result_flag, result_query = super().query_record(super().get_tbl_ORDER(), columns, condition)
            if result_flag:
                for row in result_query:
                    order_id = row.get("ID")
            return result_flag, order_id, result_query
        except Exception as e:
            return None
        finally:
            try:
                del order_id
                del columns
                del condition
                del result_flag
                gc.collect()
            except Exception as e:
                print(f"Order name not found: {e}")
                pass

    def add_tags_to_orders(self, file_path, file_name, new_tags_string):
        """add_tags_to_orders: Add new TAGs to list of orders in a CSV file.
        Usage:
            add_tags_to_orders(file_path, file_name, new_tags_string)

        Args:
            file_path (String): CSV File path
            file_name (String): CSV file with one column of order numbers.
            new_tags_string (string): String with new tags to be added separated by space, comma, semicolon, pipe (vertical slash) or forward slash.
        """
        result_flag_csv = True
        result_data_csv = None
        result_message_csv = None
        order_columns = ["ID", "TAGS"]
        new_tag_list = [tag.strip() for tag in re.sub("[,|/;]+", ",", string = new_tags_string).split(",")]
        payload = None
        tags_array = None
        result_flag = None
        order_id = None
        result_query = None
        order_name = None
        old_tags = None
        errors = []

        print(f"[INFO] BEGIN - Add Tags to Orders...")
        try:
            result_flag_csv, result_data_csv, result_message_csv = self.utils.read_csv_file(file_path=file_path, file_name=file_name)
        except Exception as e:
            print(f"[ERROR] CSV file error - {e}.")
            return result_flag_csv, result_data_csv, result_message_csv
        
        try:
            if result_flag_csv:
                for order in result_data_csv:
                    order_name = list(order.values())[0]
                    result_flag, order_id, result_query = self.get_order_data_by_name(order_name, order_columns)

                    if result_flag:
                        for o in result_query:
                            old_tags = o.get('TAGS')
                            break

                        tags_array = (old_tags.split(',') if old_tags is not None else []) + new_tag_list
                        print(f"[INFO] Adding {", ".join(tag for tag in new_tag_list[:-1])}, and {new_tag_list[-1]} tags to order {order_id}.")

                        payload = self.utils.convert_object_to_json({
                            "order": {
                                "id": order_id,
                                "tags": tags_array
                            }
                        })
                        result_flag, order, response_description, code = self.shopify.post_shopify_update_order(order_id, payload, api_version=self.utils.get_SHOPIFY_API_VERSION())
                        if code != 201 and code != 200:
                            print(f"[ERROR] Including {", ".join(tag for tag in new_tag_list[:-1])}, and {new_tag_list[-1]} tags to order {order}. Status code: {code}")
                            errors.append({
                                "order": order_name,
                                "tags": tags_array, 
                                "error" : response_description    
                            })
                        print(f"[INFO] New tags added to order {order_id}.")
                if len(errors) > 0:
                    email_to = ['xxxxxxx@COMPANY_NAME.com', 'xxxxxxx@COMPANY_NAME.com']
                    email_body = "Orders with error while adding tags:"
                    email_body += "\nOrders:"
                    email_body += f"\n{str(errors)}"
                    email_subject = "[ADDING TAGS] Errors"
                    email_from = 'xxxxxxxx@gmail.com'
                    try:
                        self.utils.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_path=file_path, file_names=[file_name])
                    except Exception as e:
                        self.utils.send_exception_email(module=self.get_module_name(), function="send_processed_customs_orders_emails", error=str(e), additional_info=None, start_time=self.get_start_time(), end_time=self.utils.get_current_date_time())
                        print(f"[ERROR] - {e}")
                        return False
                return result_flag_csv, result_data_csv, result_message_csv
            else:
                raise Exception(f"File empty or not found") 
        except Exception as e:

            print(f"[ERROR] Tags NOT added. Orders not altered. {e}.")
            return result_flag_csv, result_data_csv, result_message_csv
        finally:
            self.utils.delete_files(sheet_file_path=file_path, pdf_file_path=None, sheet_file_names=[file_name], pdf_file_names=None)
            print(f"[INFO] END - Finished Add Tags to Orders...")
            print("\n[INFO] Clearing variables...")
            try:
                del result_flag_csv, result_data_csv, result_message_csv, order_columns, new_tag_list, payload, tags_array, result_flag, order_id, result_query, order_name, old_tags
                gc.collect()
            except Exception as e:
                pass

    def cancel_open_or_in_progress_order_fulfillments(self, count_orders, total_orders_to_fulfill, order_id, order_number, items, order_fulfillment_status, tracking_number, message, new_location_name, api_version=None):
        # print(f"\n[INFO] BEGIN - Canceling Orders with Fulfillment in Progress...")
        fulfillment_id = None
        fulfillment_status = None
        assigned_location = None
        curr_fulfill_location = None
        fulfillment_order_return_flag = False
        fulfillment = None
        response_description = "Success"
        status_code = 200
        fulfillment_orders = None
        fulfill = None
        fulfillment_order_cancel_req_return_flag = False
        fulfillment_order_cancel_return_flag = False
        fulfillment_cancel_req = None
        fulfillment_cancel = None
        return_flag = False
        fulfillment_line_items = None
        found_item = False
        item = None
        line_item_id = None
        fulfillment_line_item_id = None
        line_item_variant_id = None
        fulfillment_line_item_variant_id = None
        fulfillment_dict = None
        fulfill_item = None

        try:
            fulfillment_order_return_flag, fulfillment, response_description, status_code = self.shopify.get_shopify_fulfillment_orders(order_id=order_id, include_financial_summaries=None, include_order_reference_fields=None, api_version=api_version)

            if fulfillment_order_return_flag:
                fulfillment_orders = fulfillment.get("fulfillment_orders")

                for fulfill in fulfillment_orders:
                    assigned_location = fulfill.get("assigned_location")
                    fulfillment_id = fulfill.get("id")
                    fulfillment_status = fulfill.get("status")
                    curr_fulfill_location = assigned_location.get("name")
                    fulfillment_line_items = fulfill.get("line_items")
                    found_item = False

                    if fulfillment_status == "in_progress":
                        if curr_fulfill_location != new_location_name:
                            # Create a dictionary for quick lookups
                            fulfillment_dict = {str(fulfill_item.get("variant_id")): fulfill_item for fulfill_item in fulfillment_line_items}
                            found_item = False

                            for item in items:
                                line_item_variant_id = str(item.get("variant_id"))

                                if line_item_variant_id in fulfillment_dict:
                                    found_item = True
                                    break

                            if found_item:
                                self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tCancel Fulfillment - Requesting Fulfillment Cancel for Location {curr_fulfill_location}...")
                                # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tCancel Fulfillment - Requesting Fulfillment Cancel for Location {curr_fulfill_location}...")
                                fulfillment_order_cancel_req_return_flag, fulfillment_cancel_req, response_description, status_code = self.shopify.post_shopify_fulfillment_cancelation_request(fulfillment_order_id=fulfillment_id, message=message, api_version=api_version)

                                if fulfillment_order_cancel_req_return_flag:
                                    self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tCancel Fulfillment - Cancelling Fulfillment for Location {curr_fulfill_location}...")
                                    # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tCancel Fulfillment - Cancelling Fulfillment for Location {curr_fulfill_location}...")
                                    fulfillment_order_cancel_return_flag, fulfillment_cancel, response_description, status_code = self.shopify.post_shopify_fulfillment_orders_cancel(fulfillment_order_id=fulfillment_id, api_version=api_version)

                                    if fulfillment_order_cancel_return_flag:
                                        self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tCancel Fulfillment - Order Fulfillment canceled successfully...")
                                        # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tCancel Fulfillment - Order Fulfillment canceled successfully...")
                                        return_flag = True
                                    else:
                                        self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tCancel Fulfillment - Error while canceling order fulfillment. Error: {response_description}...")
                                        # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tCancel Fulfillment - Error while canceling order fulfillment. Error: {response_description}...")
                                else:
                                    self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tError while canceling order fulfillment request. Error: {response_description}...")
                                    # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tError while canceling order fulfillment request. Error: {response_description}...")
                        else:
                            self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tCancel Fulfillment - Order is already assigned to {new_location_name}...")
                            # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tCancel Fulfillment - Order is already assigned to {new_location_name}...")
                            return_flag = True
                    elif fulfillment_status == "open":
                        self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tCancel Fulfillment - Order is already open...")
                        # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tCancel Fulfillment - Order is already open...")
                        return_flag = True
                    else:
                        self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tCancel Fulfillment - Order is already fulfilled or canceled...")
                        # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tCancel Fulfillment - Order is already fulfilled or canceled...")
            else:
                self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tCancel Fulfillment - Error while getting order fulfillment. Error: {response_description}")
                # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tCancel Fulfillment - Error while getting order fulfillment. Error: {response_description}")
                return_flag = True

            return return_flag, fulfillment, fulfillment_cancel_req, fulfillment_cancel, response_description, status_code
        except Exception as e:
            print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tCancel Fulfillment - Error: {str(e)}")
            return False, fulfillment, fulfillment_cancel_req, fulfillment_cancel, f"[ERROR] Error while canceling order fulfillment. Error: {str(e)}", status_code
        finally:
            # print(f"[INFO] END - Finished Canceling Orders with Fulfillment in Progress...")
            # print("\n[INFO] Clearing variables...")
            try:
                del fulfillment_id, fulfillment_status, assigned_location, curr_fulfill_location, fulfillment_order_return_flag, fulfillment, response_description, status_code, fulfillment_orders, fulfill, fulfillment_order_cancel_req_return_flag, fulfillment_order_cancel_return_flag, fulfillment_cancel_req, fulfillment_cancel, return_flag, found_item, fulfillment_line_items, item, line_item_id, fulfillment_line_item_id, line_item_variant_id, fulfillment_line_item_variant_id
            except Exception as e:
                pass

    def move_fulfillment_location(self, count_orders, total_orders_to_fulfill, order_id, order_number, items, order_fulfillment_status, tracking_number, new_location_name, api_version=None):
        item_columns = ["PRODUCT_ID", "NAME", "TITLE", "SKU", "VARIANT_ID", "VARIANT_TITLE", "FULFILLABLE_QUANTITY", "FULFILLMENT_STATUS"]
        items_condition = f"1=1"
        found_location = False
        fulfillment_order_return_flag = False
        fulfillment_locations_return_flag = False
        fulfillment = None
        response_description = "Success"
        status_code = 200
        fulfillment_orders = None
        assigned_location = None
        fulfillment_id = None
        fulfillment_status = None
        curr_fulfill_location = None
        fulfillment_locations = None
        locations_for_move = None
        location = None
        loc_id = None
        loc_name = None
        local = None
        new_fulfill_loc_id = None
        new_fulfill_loc_name = None
        line_items = None
        fulfillment_order_line_items = []
        item = {}
        line_item_id = None
        line_item_quantity = 0
        line_item_fulfillable_quantity = 0
        line_item_variant_id = None
        change_location_flag = False
        change_location_item_flag = False
        can_fulfill = False
        product_name = None
        product_title = None
        prod_fulfillment_status = "unfulfilled"
        return_flag_item = None
        return_query_item = None
        fulfillment_location_move_return_flag = None
        fulfillment_move = None
        fulfillment_line_items = None
        found_item = False
        item = None
        fulfillment_line_item_id = None
        fulfillment_dict = None
        fulfill_item = None
        total_items_to_change_location = 0
        total_items = 0

        try:
            fulfillment_order_return_flag, fulfillment, response_description, status_code = self.shopify.get_shopify_fulfillment_orders(order_id=order_id, include_financial_summaries=None, include_order_reference_fields=None, api_version=api_version)

            if fulfillment_order_return_flag:
                fulfillment_orders = fulfillment.get("fulfillment_orders")

                for fulfill in fulfillment_orders:
                    assigned_location = fulfill.get("assigned_location")
                    fulfillment_id = fulfill.get("id")
                    fulfillment_status = fulfill.get("status")
                    curr_fulfill_location = assigned_location.get("name")
                    fulfillment_line_items = fulfill.get("line_items")
                    found_item = False

                    if fulfillment_status == "open" or fulfillment_status == "in_progress":
                        if curr_fulfill_location != new_location_name:
                            fulfillment_dict = {str(fulfill_item.get("variant_id")): fulfill_item for fulfill_item in fulfillment_line_items}
                            found_item = False

                            for item in items:
                                line_item_variant_id = str(item.get("variant_id"))

                                if line_item_variant_id in fulfillment_dict:
                                    found_item = True
                                    break

                            if found_item:
                                print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{order_fulfillment_status if len(order_fulfillment_status) >= 11 else order_fulfillment_status + '\t'}\tMove Fulfill Location - Getting Fulfillment Locations...")
                                fulfillment_locations_return_flag, fulfillment_locations, response_description, status_code = self.shopify.get_shopify_fulfillment_locations_for_move(fulfillment_order_id=fulfillment_id, api_version=None)

                                if fulfillment_locations_return_flag:
                                    locations_for_move = fulfillment_locations.get("locations_for_move")
                                    location = None
                                    loc_id = None
                                    loc_name = None
                                    found_location = False

                                    if locations_for_move is not None and locations_for_move != []:
                                        for local in locations_for_move:
                                            location = local.get("location")
                                            loc_id = location.get("id")
                                            loc_name = location.get("name")
                                            if loc_name == new_location_name:
                                                found_location = True
                                                break

                                        if found_location:
                                            new_fulfill_loc_id = loc_id
                                            new_fulfill_loc_name = loc_name
                                            line_items = fulfill.get("line_items")
                                            fulfillment_order_line_items = []
                                            item = {}
                                            line_item_id = None
                                            line_item_quantity = 0
                                            line_item_fulfillable_quantity = 0
                                            fulfillment_line_item_variant_id = None
                                            change_location_flag = False
                                            can_fulfill = False
                                            total_items = 0

                                            for line_item in line_items:
                                                total_items += 1
                                                line_item_id = line_item.get("id")
                                                line_item_quantity = line_item.get("quantity")
                                                line_item_fulfillable_quantity = line_item.get("fulfillable_quantity")
                                                fulfillment_line_item_variant_id = str(line_item.get("variant_id"))

                                                if line_item_quantity > 0 and line_item_fulfillable_quantity > 0:
                                                    product_name = None
                                                    product_title = None
                                                    prod_fulfillment_status = "unfulfilled"
                                                    line_item_variant_id = None
                                                    change_location_item_flag = False

                                                    for i in items:
                                                        product_name = i.get("name")
                                                        product_title = i.get("title")
                                                        prod_fulfillment_status = i.get("fulfillment_status")
                                                        line_item_variant_id = str(i.get("variant_id"))

                                                        if line_item_variant_id == fulfillment_line_item_variant_id:
                                                            item = {
                                                                "id": line_item_id,
                                                                "quantity": line_item_fulfillable_quantity
                                                            }
                                                            fulfillment_order_line_items.append(item)
                                                            change_location_flag = True
                                                            change_location_item_flag = True
                                                            print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t{line_item_id}\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - Changing Location from '{curr_fulfill_location}' to '{new_fulfill_loc_name}' for product '{product_title}' with fulfillment status '{prod_fulfillment_status}'...")
                                                            break

                                                    if not change_location_item_flag:
                                                        print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t{line_item_id}\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - The Item '{product_title}' was not Found in the fulfillment List of items...")
                                                else:
                                                    print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t{line_item_id}\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - Item not fulfillable...")

                                            total_items_to_change_location = len(fulfillment_order_line_items)

                                            if change_location_flag:
                                                print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - Requesting Fulfillment Location Move from '{curr_fulfill_location}' to '{new_fulfill_loc_name}' for {total_items_to_change_location} out of {total_items} items...")
                                                fulfillment_location_move_return_flag, fulfillment_move, response_description, status_code = self.shopify.post_shopify_fulfillment_location_move(fulfillment_order_id=fulfillment_id, new_location_id=new_fulfill_loc_id, fulfillment_order_line_items=fulfillment_order_line_items, api_version=None)

                                                if fulfillment_location_move_return_flag:
                                                    print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t{line_item_id}\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - Location Changed Successfully")
                                                    can_fulfill = True
                                                else:
                                                    print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t{line_item_id}\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - Location not Changed. Error: {response_description}")
                                            else:
                                                print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t{line_item_id}\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - No Locations found for move...")
                                                can_fulfill = True
                                        else:
                                            can_fulfill = False
                                            print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - No Locations found for move...")
                                    else:
                                        can_fulfill = False
                                        print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{order_fulfillment_status if len(order_fulfillment_status) >= 11 else order_fulfillment_status + '\t'}\tMove Fulfill Location - No Locations found for move...")
                                else:
                                    can_fulfill = False
                                    print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - Error while getting fulfillment locations. Error: {response_description}")
                        else:
                            print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - Order is already assigned to '{new_location_name}'...")
                            can_fulfill = True
                    else:
                        print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tMove Fulfill Location - Order is already fulfilled or canceled...")

            return can_fulfill, fulfillment_move, response_description, status_code
        except Exception as e:
            print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{order_fulfillment_status if len(order_fulfillment_status) >= 11 else order_fulfillment_status + '\t'}\tMove Fulfill Location - Error while moving fulfillment location. Error: {str(e)}")
            return False, f"[ERROR] Error while moving fulfillment location. Error: {str(e)}"
        finally:
            try:
                del item_columns, items_condition, found_location, fulfillment_order_return_flag, fulfillment_locations_return_flag, fulfillment, response_description, status_code, fulfillment_orders, assigned_location, fulfillment_id, fulfillment_status, curr_fulfill_location, fulfillment_locations, locations_for_move, location, loc_id, loc_name, local, new_fulfill_loc_id, new_fulfill_loc_name, line_items, fulfillment_order_line_items, item, line_item_id, line_item_quantity, line_item_fulfillable_quantity, line_item_variant_id, change_location_flag, change_location_item_flag, can_fulfill, product_name, product_title, prod_fulfillment_status, return_flag_item, return_query_item, fulfillment_location_move_return_flag, fulfillment_move, fulfillment_line_items, found_item, fulfillment_line_item_id, fulfillment_dict, fulfill_item, total_items_to_change_location
            except Exception as e:
                print(f"Variable not found: {e}")
                pass

    def match_items_from_csv(self, file_path, file_name):
        order_columns = ["ID", "NAME", "CREATED_AT", "COUNTRY_CODE", "FULFILLMENT_STATUS", "TAGS", "NOTE"]
        item_columns = ["ID", "PRODUCT_ID", "NAME", "TITLE", "SKU", "VARIANT_ID", "VARIANT_TITLE", "FULFILLABLE_QUANTITY", "FULFILLMENT_STATUS"]
        result_flag_csv = False
        result_data_csv = None
        result_message_csv = None
        total_orders_to_fulfill = 0
        count_orders = 0
        count_items = 0
        order_number = None
        tracking_number = None
        order_condition = None
        return_flag_order = False
        return_query_order = None
        order_id = None
        order_created = None
        order_country_code = None
        order_fulfillment_status = None
        order_tags = None
        order_note = None
        line_item_id = None
        line_item_quantity = 0
        line_item_fulfillable_quantity = 0
        fulfillment_line_item_variant_id = None
        line_item_variant_id = None
        can_fulfill = False
        return_flag_cancel_fulfill = False
        fulfillment = None
        fulfillment_cancel_req = None
        fulfillment_cancel = None
        response_description = "Success"
        status_code = None
        fulfillment_move = None
        fulfillment_order_return_flag = None
        fulfillment_orders = None
        assigned_location = None
        fulfillment_id = None
        fulfillment_status = None
        curr_fulfill_location = None
        fulfillment_line_items = None
        fulfillment_order_line_items = None
        item = None
        product_name = None
        product_title = None
        items_condition = None
        return_flag_item = False
        return_query_item = None
        fulfillment_result_flag = False
        fulfillment_result = None
        today = self.utils.get_current_date_time()
        base_dir = self.utils.get_base_directory()
        return_flag = False
        orders_array = []
        order_list = {
            "number": None,
            "carrier_name": None,
            "tracking_number": None,
            "items": []
        }
        items_array = []
        item_list = {
            "name": None,
            "sku": None
        }

        try:
            print(f"[INFO] Reading CSV File...")
            result_flag_csv, result_data_csv, result_message_csv = self.utils.read_csv_file(file_path=file_path, file_name=file_name)

            if result_flag_csv:
                # Verify if the CSV has the columns order_number, tracking_number, carrier_name, item_sku
                if "order_number" not in result_data_csv[0].keys() or "tracking_number" not in result_data_csv[0].keys() or "item_sku" not in result_data_csv[0].keys():
                    print(f"[ERROR] CSV file does not have the required columns. Please make sure the CSV file has the columns 'order_number', 'tracking_number', 'carrier_name', 'item_sku'")
                    return False, None, f"[ERROR] CSV file does not have the required columns. Please make sure the CSV file has the columns 'order_number', 'tracking_number', 'carrier_name', 'item_sku'"

                total_orders_to_fulfill = len(result_data_csv)
                print(f"\n[INFO] Got a total of {total_orders_to_fulfill} Items to fulfill...")

                print(f"[INFO] Matching Orders and Items...")
                print(f"[INFO] Progress\t\tOrder Num\tItem SKU")
                for data in result_data_csv:
                    count_items += 1
                    order_list = {}
                    item_list = {}
                    items_array = []
                    order_number = None
                    tracking_number = None
                    order_number = data.get("order_number")
                    tracking_number = data.get("tracking_number")
                    carrier_name = data.get("carrier_name")
                    item_sku = data.get("item_sku")
                    print(f"[INFO] {count_items}...{total_orders_to_fulfill}{'\t\t' if len(str(count_items) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number}\t{item_sku}")

                    order_condition = f"1=1"
                    order_condition += f"\nAND NAME = '{order_number}'"
                    return_flag_order, return_query_order = super().query_record(super().get_tbl_ORDER(), order_columns, order_condition)
                    for order in return_query_order:
                        order_id = int(order.get("ID"))
                        order_created = order.get("CREATED_AT")
                        order_country_code = order.get("COUNTRY_CODE")
                        order_fulfillment_status = "unfulfilled" if order.get("FULFILLMENT_STATUS") is None else order.get("FULFILLMENT_STATUS")
                        order_tags = order.get("TAGS")
                        order_note = order.get("NOTE")
                        line_item_id = None
                        line_item_quantity = 0
                        line_item_fulfillable_quantity = 0
                        line_item_variant_id = None

                        items_condition = f"1=1"
                        items_condition += f"\nAND ORDER_ID = '{order_id}'"
                        items_condition += f"\nAND SKU = '{item_sku}'"

                        return_flag_item, return_query_item = super().query_record(super().get_tbl_ORDER_LINE_ITEM(), item_columns, items_condition)
                        if return_flag_item:
                            for items in return_query_item:
                                line_item_id = items.get("ID")
                                product_name = items.get("NAME")
                                product_title = items.get("TITLE")
                                line_item_variant_id = items.get("VARIANT_ID")
                                prod_fulfillment_status = "unfulfilled" if items.get("FULFILLMENT_STATUS") is None else items.get("FULFILLMENT_STATUS")

                        if orders_array and any(o.get("order_number") == order_number for o in orders_array):
                            # if order_number is in orders array, then append the item to the existing order
                            for ordr in orders_array:
                                if ordr.get("order_number") == order_number:
                                    item_list = {
                                        "id": line_item_id,
                                        "sku": item_sku,
                                        "name": product_name,
                                        "title": product_title,
                                        "fulfillment_status": prod_fulfillment_status,
                                        "variant_id": line_item_variant_id
                                    }
                                    ordr.get("items").append(item_list)
                                    break
                        else:
                            count_orders += 1
                            item_list = {
                                "id": line_item_id,
                                "sku": item_sku,
                                "name": product_name,
                                "title": product_title,
                                "fulfillment_status": prod_fulfillment_status,
                                "variant_id": line_item_variant_id
                            }
                            items_array.append(item_list)

                            order_list = {
                                "order_id": order_id,
                                "order_number": order_number,
                                "order_created": order_created,
                                "order_country_code": order_country_code,
                                "order_fulfillment_status": order_fulfillment_status,
                                "carrier_name": carrier_name,
                                "tracking_number": tracking_number,
                                "order_tags": order_tags,
                                "order_note": order_note,
                                "items": items_array
                            }
                            orders_array.append(order_list)
                print(f"[INFO] Total Orders: {count_orders}")
                print(f"[INFO] Total Items: {count_items}")
                # print(f"[INFO] Orders Array: {orders_array}")

                return True, orders_array, "Success"
            else:
                print(f"[ERROR] Error while reading CSV file. Error: {result_message_csv}")
                return False, None, f"[ERROR] Error while reading CSV file. Error: {result_message_csv}"
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, None, f"[ERROR] {e}"
        finally:
            try:
                del order_columns, item_columns, result_flag_csv, result_data_csv, result_message_csv, total_orders_to_fulfill, count_orders, count_items, order_number, tracking_number, order_condition, return_flag_order, return_query_order, order_id, order_created, order_country_code, order_fulfillment_status, order_tags, order_note, line_item_id, line_item_quantity, line_item_fulfillable_quantity, fulfillment_line_item_variant_id, line_item_variant_id, can_fulfill, return_flag_cancel_fulfill, fulfillment, fulfillment_cancel_req, fulfillment_cancel, response_description, status_code, fulfillment_move, fulfillment_order_return_flag, fulfillment_orders, assigned_location, fulfillment_id, fulfillment_status, curr_fulfill_location, fulfillment_line_items, fulfillment_order_line_items, item, product_name, product_title, items_condition, return_flag_item, return_query_item, fulfillment_result_flag, fulfillment_result, today, base_dir, return_flag, orders_array, order_list, items_array, item_list
            except Exception as e:
                print(f"Variable not found: {e}")
                pass

    def fulfill_orders(self, file_path, file_name, email_to):
        print(f"\n[INFO] BEGIN - Fulfilling Orders...")
        order_columns = ["ID", "NAME", "CREATED_AT", "COUNTRY_CODE", "FULFILLMENT_STATUS", "TAGS", "NOTE"]
        item_columns = ["ID", "PRODUCT_ID", "NAME", "TITLE", "SKU", "VARIANT_ID", "VARIANT_TITLE", "FULFILLABLE_QUANTITY", "FULFILLMENT_STATUS"]
        result_flag_csv = False
        result_flag_match_items = False
        result_data_csv = None
        result_message_csv = None
        total_orders_to_fulfill = 0
        count_orders = 0
        count_items = 0
        order_number = None
        tracking_number = None
        order_condition = None
        return_flag_order = False
        prod_fulfillment_status = "unfulfilled"
        return_query_order = None
        order_id = None
        order_created = None
        order_country_code = None
        order_fulfillment_status = None
        order_tags = None
        order_note = None
        line_item_id = None
        line_item_fulfillable_quantity = 0
        fulfillment_line_item_variant_id = None
        line_item_variant_id = None
        found_item = False
        can_fulfill = False
        return_flag_cancel_fulfill = False
        fulfillment = None
        fulfillment_cancel_req = None
        fulfillment_cancel = None
        response_description = "Success"
        status_code = None
        fulfillment_move = None
        fulfillment_order_return_flag = None
        fulfillment_orders = None
        assigned_location = None
        fulfillment_id = None
        fulfillment_status = None
        curr_fulfill_location = None
        line_items = None
        fulfillment_line_items = None
        fulfillment_order_line_items = None
        item = None
        product_name = None
        product_title = None
        items_condition = None
        return_flag_item = False
        return_query_item = None
        fulfillment_result_flag = False
        fulfillment_result = None
        today = self.utils.get_current_date_time()
        base_dir = self.utils.get_base_directory()
        return_flag = False
        orders_array = []
        order_list = {
            "number": None,
            "carrier_name": None,
            "tracking_number": None,
            "items": []
        }
        items_array = []
        item_list = {
            "name": None,
            "sku": None
        }
        try:
            return_flag, sheets_file_path, return_code = self.syspref.get_sys_pref("FULFILLMENTS_SHEET_FILES_PATH")
            return_flag, sheet_file_name, return_code = self.syspref.get_sys_pref("FULFILLMENTS_SHEET_FILE_NAME")
            return_flag, sheet_file_title, return_code = self.syspref.get_sys_pref("FULFILLMENTS_SHEET_FILE_TITLE")
        except Exception as e:
            print(f"[ERROR] {e}")
            return False

        sheet_file_name = sheet_file_name.format(str(today).replace(" ", "_").replace(":", "_"))
        output_file_path = f"{base_dir}{sheets_file_path}"
        wb:Workbook = None
        ws = None
        sheet_created_flg = False
        email_from = "COMPANY_NAMEheroku@gmail.com"
        email_subject = f"Automatic Fulfillment Process - {today}"
        email_body = None
        email_list = []
        email = None
        email_files = []

        try:
            result_flag_match_items, orders_array, result_message_csv = self.match_items_from_csv(file_path=file_path, file_name=file_name)

            if result_flag_match_items and orders_array != []:
                try:
                    wb = self.utils.create_excel_sheet_if_not_exists(file_path=output_file_path, file_name=sheet_file_name, sheet_name=sheet_file_title)
                    if not wb:
                        print(f"[ERROR] Error creating sheet {sheet_file_name}")
                    else:
                        sheet_created_flg = True
                        ws = wb.active
                        ws.append(["Progress", "Order Id", "Order Number", "Order Status", "Tracking Number", "Item Id", "Fulfillment Id", "Fulfillment Status", "Product Name", "Product Title", "Fulfillable Quantity", "Fulfillment Status", "Description"])
                except Exception as e:
                    print(f"[ERROR] - {e}")

                total_orders_to_fulfill = len(orders_array)
                print(f"[INFO] Getting Orders information...")
                print(f"[INFO] Progress\t\tOrder Num Order Status\tTracking #\t\tItem Id\t\tFulfillment Id\tStatus\t\tDescription")
                count_orders = 0
                count_items = 0
                for data in orders_array:
                    count_orders += 1
                    order_id = data.get("order_id")
                    order_number = data.get("order_number")
                    order_created = data.get("order_created")
                    order_country_code = data.get("order_country_code")
                    order_fulfillment_status = data.get("order_fulfillment_status")
                    order_tags = data.get("order_tags")
                    order_note = data.get("order_note")
                    tracking_number = data.get("tracking_number")
                    carrier_name = data.get("carrier_name")
                    items_array = data.get("items")
                    line_item_id = None
                    line_item_fulfillable_quantity = 0
                    line_item_variant_id = None

                    if order_fulfillment_status.lower() == "unfulfilled" or order_fulfillment_status.lower() == "partial":
                        can_fulfill = False
                        return_flag_cancel_fulfill, fulfillment, fulfillment_cancel_req, fulfillment_cancel, response_description, status_code = self.cancel_open_or_in_progress_order_fulfillments(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_id=order_id, order_number=order_number, items=items_array, order_fulfillment_status=order_fulfillment_status, tracking_number=tracking_number, message="Automatic Fulfillment Cancel Process.", new_location_name="COMPANY_NAME", api_version=None)

                        if return_flag_cancel_fulfill:
                            can_fulfill, fulfillment_move, response_description, status_code = self.move_fulfillment_location(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_id=order_id, order_number=order_number, items=items_array, order_fulfillment_status=order_fulfillment_status, tracking_number=tracking_number, new_location_name="COMPANY_NAME", api_version=None)

                        if can_fulfill:
                            time.sleep(1)
                            fulfillment_order_return_flag, fulfillment, response_description, status_code = self.shopify.get_shopify_fulfillment_orders(order_id=order_id, include_financial_summaries=None, include_order_reference_fields=None, api_version=None)

                            if fulfillment_order_return_flag:
                                fulfillment_orders = fulfillment.get("fulfillment_orders")

                                for fulfill in fulfillment_orders:
                                    assigned_location = fulfill.get("assigned_location")
                                    fulfillment_id = fulfill.get("id")
                                    fulfillment_status = fulfill.get("status")
                                    curr_fulfill_location = assigned_location.get("name")
                                    fulfillment_order_line_items = []

                                    if fulfillment_status == "open" or fulfillment_status == "in_progress":
                                        if curr_fulfill_location == "COMPANY_NAME":
                                            fulfillment_line_items = fulfill.get("line_items")
                                            fulfillment_order_line_items = []
                                            item = {}
                                            line_item_id = None
                                            line_item_fulfillable_quantity = 0
                                            fulfillment_line_item_variant_id = None
                                            found_item = False

                                            fulfillment_dict = {str(fulfill_item.get("variant_id")): fulfill_item for fulfill_item in fulfillment_line_items}
                                            found_item = False

                                            for item in items_array:
                                                line_item_variant_id = str(item.get("variant_id"))

                                                if line_item_variant_id in fulfillment_dict:
                                                    found_item = True
                                                    break

                                            if found_item:
                                                for line_item in fulfillment_line_items:
                                                    line_item_id = str(line_item.get("id"))
                                                    line_item_fulfillable_quantity = line_item.get("fulfillable_quantity")
                                                    fulfillment_line_item_variant_id = str(line_item.get("variant_id"))
                                                    product_name = None
                                                    product_title = None
                                                    line_item_variant_id = None
                                                    prod_fulfillment_status = "unfulfilled"

                                                    if line_item_fulfillable_quantity > 0:
                                                        for i in items_array:
                                                            product_name = i.get("name")
                                                            product_title = i.get("title")
                                                            prod_fulfillment_status = i.get("fulfillment_status")
                                                            line_item_variant_id = str(i.get("variant_id"))

                                                            if line_item_variant_id == fulfillment_line_item_variant_id:
                                                                self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id=line_item_id, fulfillment_id=f"\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tFulfill Order - Fulfilling Product '{product_title}' with fulfillment status '{prod_fulfillment_status}'...")
                                                                # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t{line_item_id}\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tFulfill Order - Fulfilling Product '{product_title}' with fulfillment status '{prod_fulfillment_status}'...")
                                                                item = {
                                                                    "id": line_item_id,
                                                                    "quantity": line_item_fulfillable_quantity
                                                                }
                                                                fulfillment_order_line_items.append(item)
                                                                if sheet_created_flg:
                                                                    ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, line_item_id, fulfillment_id, fulfillment_status, product_name, product_title, line_item_fulfillable_quantity, prod_fulfillment_status, f"Fulfill Order - Fulfilling Product '{product_title}' with fulfillment status '{prod_fulfillment_status}'..."])
                                                                break
                                                    else:
                                                        self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id=line_item_id, fulfillment_id=f"\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tFulfill Order - Product {product_title} with fulfillment status '{prod_fulfillment_status}' is not Fulfillable...")
                                                        # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t{line_item_id}\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tFulfill Order - Product {product_title} with fulfillment status '{prod_fulfillment_status}' is not Fulfillable...")
                                                        if sheet_created_flg:
                                                            ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, line_item_id, fulfillment_id, fulfillment_status, product_name, product_title, line_item_fulfillable_quantity, prod_fulfillment_status, f"Fulfill Order - Product {product_title} with fulfillment status '{prod_fulfillment_status}' is not Fulfillable..."])
                                        else:
                                            self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tFulfill Order - Location not set to COMPANY_NAME...")
                                            # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tFulfill Order - Location not set to COMPANY_NAME...")
                                            if sheet_created_flg:
                                                ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", fulfillment_id, fulfillment_status, "", "", "", "", f"Fulfill Order - Location not set to COMPANY_NAME..."])
                                    else:
                                        self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tFulfill Order - Order is already fulfilled or canceled...")
                                        # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tFulfill Order - Order is already fulfilled or canceled...")
                                        if sheet_created_flg:
                                            ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", fulfillment_id, fulfillment_status, "", "", "", "", f"Fulfill Order - Order is already fulfilled or canceled"])

                                    if fulfillment_order_line_items != []:
                                        fulfillment_result_flag, fulfillment_result, response_description, status_code = self.shopify.post_shopify_fulfillments(fulfillment_order_id=fulfillment_id, fulfillment_order_line_items=fulfillment_order_line_items, quantity=line_item_fulfillable_quantity, message=None, notify_customer=False, origin_address=None, tracking_number=tracking_number, tracking_url=None, carrier_name=None, api_version=None)
                                        if fulfillment_result_flag:
                                            self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tFulfill Order - Fulfilled Successfully")
                                            # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tFulfill Order - Fulfilled Successfully")
                                            if sheet_created_flg:
                                                ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", fulfillment_id, fulfillment_status, "", "", "", "", "Fulfill Order - Items Fulfilled Successfully"])
                                        else:
                                            self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tFulfill Order - Error while fulfilling order. Error: {response_description}")
                                            # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tFulfill Order - Error while fulfilling order. Error: {response_description}")
                                            if sheet_created_flg:
                                                ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", fulfillment_id, fulfillment_status, "", "", "", "", f"Fulfill Order - Error while fulfilling order. Error: {response_description}"])
                                    else:
                                        self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tFulfill Order - No Items to Fulfill")
                                        # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tFulfill Order - No Items to Fulfill")
                                        if sheet_created_flg:
                                            ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", fulfillment_id, fulfillment_status, "", "", "", "", "Fulfill Order - No Items to Fulfill"])
                            else:
                                self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id=f"\t\t\t{fulfillment_id}", fulfillment_status=f"\t{fulfillment_status}", description=f"\tFulfill Order - {response_description}")
                                # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t{fulfillment_id}\t{fulfillment_status if len(fulfillment_status) >= 11 else fulfillment_status + '\t'}\tFulfill Order - {response_description}")
                                if sheet_created_flg:
                                    ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", "", "", "", "", "", "", f"Fulfill Order - {response_description}"])
                        else:
                            self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id="", fulfillment_status="", description=f"\t\t\t\t\t\t\tFulfill Order - {response_description}")
                            # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t\t\t\t\tFulfill Order - {response_description}")
                            if sheet_created_flg:
                                ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", "", "", "", "", "", "", f"Fulfill Order - {response_description}"])
                    else:
                        self.print_fulfillment_log(count_orders=count_orders, total_orders_to_fulfill=total_orders_to_fulfill, order_number=order_number, order_fulfillment_status=order_fulfillment_status, tracking_number=f"\t{tracking_number}", line_item_id="", fulfillment_id="", fulfillment_status="", description=f"\t\t\t\t\t\t\tFulfill Order - Order is already fulfilled or canceled")
                        # print(f"[INFO] {count_orders}...{total_orders_to_fulfill}{'\t\t' if len(str(count_orders) + str(total_orders_to_fulfill)) < 6 else '\t'}{order_number} {order_fulfillment_status}\t{tracking_number if len(tracking_number) >= 16 else tracking_number + '\t'}\t\t\t\t\t\t\tFulfill Order - Order is already fulfilled or canceled")
                        if sheet_created_flg:
                            ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", "", "", "", "", "", "", f"Fulfill Order - Order is already fulfilled or canceled"])
                        else:
                            if sheet_created_flg:
                                ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", "", "", "", "", "", "", f"Fulfill Order - Order not found for Order Number: {order_number}"])
            else:
                print(f"[ERROR] Error while matching items. Error: {result_message_csv}")
                if sheet_created_flg:
                    ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", "", "", "", "", "", "", f"Fulfill Order - Error while matching items. Error: {result_message_csv}"])

            if sheet_created_flg:
                wb.save(output_file_path + sheet_file_name)
                print(f"[INFO] Sheet {sheet_file_name} saved successfully")

        except Exception as e:
            print(f"[ERROR] Error while fulfilling orders. Error: {str(e)}")
            if sheet_created_flg:
                ws.append([f"{count_orders}...{total_orders_to_fulfill}", order_id, order_number, order_fulfillment_status, tracking_number, "", "", "", "", "", "", "", f"Fulfill Order - Error while fulfilling orders. Error: {str(e)}"])
                wb.save(output_file_path + sheet_file_name)
                print(f"[INFO] Sheet {sheet_file_name} saved successfully")
                self.utils.send_exception_email(module=self.get_module_name(), function="fulfill_orders", error=str(e), additional_info=None, start_time=0, end_time=0)
        finally:
            print(f"[INFO] Sending email to {email_to}...")
            email_body = f"Hello,\n\nThis is an automated email regarding the Order Fulfillment Process."
            email_body += f"\n\nAttached is the result of the Order Fulfillment Process, sent on {today}."
            email_body += f"\n\nA Total of {total_orders_to_fulfill} orders was/were fulfilled."
            
            if not result_flag_match_items:
                email_body += f"\n\nError while matching items. Error:\n{result_message_csv}"

            for email in str(email_to.replace(' ', '')).split(','):
                email_list.append(email)

            if sheet_created_flg:
                email_files.append(sheet_file_name)
                result_flag, result_string = self.utils.send_email(email_from=email_from, email_to=email_list, email_subject=email_subject, email_body=email_body, file_path=output_file_path, file_names=email_files)
            else:
                result_flag, result_string = self.utils.send_email(email_from=email_from, email_to=email_list, email_subject=email_subject, email_body=email_body, file_path=None, file_names=None)

            self.utils.delete_files(sheet_file_path=output_file_path, pdf_file_path=None, sheet_file_names=email_files, pdf_file_names=None)
            self.utils.delete_files(sheet_file_path=file_path, pdf_file_path=None, sheet_file_names=[file_name], pdf_file_names=None)

            print(f"[INFO] END - Finished Fulfilling Orders...")
            print("\n[INFO] Clearing variables...")
            try:
                del result_flag_match_items, result_data_csv, result_message_csv, total_orders_to_fulfill, count_orders, count_items, order_number, tracking_number, order_condition, return_flag_order, prod_fulfillment_status, return_query_order, order_id, order_created, order_country_code, order_fulfillment_status, order_tags, order_note, line_item_id, line_item_fulfillable_quantity, fulfillment_line_item_variant_id, line_item_variant_id, found_item, can_fulfill, return_flag_cancel_fulfill, fulfillment, fulfillment_cancel_req, fulfillment_cancel, response_description, status_code, fulfillment_move, fulfillment_order_return_flag, fulfillment_orders, assigned_location, fulfillment_id, fulfillment_status, curr_fulfill_location, line_items, fulfillment_line_items, fulfillment_order_line_items, item, product_name, product_title, items_condition, return_flag_item, return_query_item, fulfillment_result_flag, fulfillment_result, today, base_dir, return_flag, orders_array, order_list, items_array, item_list, sheets_file_path, sheet_file_name, sheet_file_title, output_file_path, wb, ws, sheet_created_flg, email_from, email_subject, email_body, email_list, email, email_files, result_flag
            except Exception as e:
                print(f"Variable not found: {e}")
                pass

    # Microservice Functions
    def get_specific_order_from_db(self, order_id, order_name):
        # Variables
        order_json = {}
        line_items = []
        order_columns = ['*']
        line_items_columns = ['*']
        order_condition = f"1=1"
        line_items_condition = f"1=1"
        order_result_query = None
        line_items_result_query = None
        item = None
        response_description = "Success"
        status_code = 200
        created_at = None
        updated_at = None
        closed_at = None
        processed_date = None
        printed_date = None
        fulfilled_date = None
        billing_address = None
        client_details = None
        company = None
        current_total_additional_fees_set = None
        customer = None
        discount_applications = None
        discount_codes = None
        fulfillments = None
        note_attributes = None
        original_total_additional_fees_set = None
        original_total_duties_set = None
        payment_terms = None
        payment_gateway_names = None
        refunds = None
        shipping_address = None
        shipping_lines = None
        subtotal_price_set = None
        tax_lines = None
        total_discounts_set = None
        total_line_items_price_set = None
        total_price = None
        total_price_set = None
        total_tax_set = None
        total_shipping_price_set = None
        tracking_info = None
        item_price_set = None
        item_total_discount_set = None
        item_discount_allocations = None
        item_tax_lines = None
        order_result_flag = False
        line_items_result_flag = False

        if order_id is None or order_id == "":
            if order_name is None or order_name == "":
                return False, {}, f"Order ID or Name is required", 400
            else:
                order_name = "#" + order_name if "#" not in order_name else order_name
                order_id = self.get_order_id_by_name(order_name=order_name)
                if order_id is None:
                    return False, {}, f"Order ID not found for Order Name: {order_name}", 400

        try:
            order_condition += f"\nAND ID = '{order_id}'" if order_id is not None and order_id != "" else ""
            order_condition += f"\nAND NAME = '{order_name}'" if order_name is not None and order_name != "" else ""
            order_result_flag, order_result_query = super().query_record(super().get_tbl_ORDER(), order_columns, order_condition)

            if order_result_flag:
                line_items_condition += f"\nAND ORDER_ID = '{order_id}'"
                line_items_result_flag, line_items_result_query = super().query_record(super().get_tbl_ORDER_LINE_ITEM(), line_items_columns, line_items_condition)

                if line_items_result_flag:
                    for row_order in order_result_query:
                        order_name = row_order.get("NAME")
                        created_at = row_order.get("CREATED_AT").strftime("%Y-%m-%d %H:%M:%S") if row_order.get("CREATED_AT") is not None else None
                        updated_at = row_order.get("UPDATED_AT").strftime("%Y-%m-%d %H:%M:%S") if row_order.get("UPDATED_AT") is not None else None
                        closed_at = row_order.get("CLOSED_AT").strftime("%Y-%m-%d %H:%M:%S") if row_order.get("CLOSED_AT") is not None else None
                        processed_date = row_order.get("PROCESSED_DATE").strftime("%Y-%m-%d %H:%M:%S") if row_order.get("PROCESSED_DATE") is not None else None
                        printed_date = row_order.get("PRINTED_DATE").strftime("%Y-%m-%d %H:%M:%S") if row_order.get("PRINTED_DATE") is not None else None
                        fulfilled_date = row_order.get("FULFILLED_DATE").strftime("%Y-%m-%d %H:%M:%S") if row_order.get("FULFILLED_DATE") is not None else None
                        billing_address = self.utils.convert_json_to_object(row_order.get("BILLING_ADDRESS")) if row_order.get("BILLING_ADDRESS") is not None else None
                        client_details = self.utils.convert_json_to_object(row_order.get("CLIENT_DETAILS")) if row_order.get("CLIENT_DETAILS") is not None else None
                        company = self.utils.convert_json_to_object(row_order.get("COMPANY")) if row_order.get("COMPANY") is not None else None
                        current_total_additional_fees_set = self.utils.convert_json_to_object(row_order.get("CURRENT_TOTAL_ADDITIONAL_FEES_SET")) if row_order.get("CURRENT_TOTAL_ADDITIONAL_FEES_SET") is not None else None
                        customer = self.utils.convert_json_to_object(row_order.get("CUSTOMER")) if row_order.get("CUSTOMER") is not None else None
                        discount_applications = self.utils.convert_json_to_object(row_order.get("DISCOUNT_APPLICATIONS")) if row_order.get("DISCOUNT_APPLICATIONS") is not None else None
                        discount_codes = self.utils.convert_json_to_object(row_order.get("DISCOUNT_CODES")) if row_order.get("DISCOUNT_CODES") is not None else None
                        fulfillments = self.utils.convert_json_to_object(row_order.get("FULFILLMENTS")) if row_order.get("FULFILLMENTS") is not None else None
                        note_attributes = self.utils.convert_json_to_object(row_order.get("NOTE_ATTRIBUTES")) if row_order.get("NOTE_ATTRIBUTES") is not None else None
                        original_total_additional_fees_set = self.utils.convert_json_to_object(row_order.get("ORIGINAL_TOTAL_ADDITIONAL_FEES_SET")) if row_order.get("ORIGINAL_TOTAL_ADDITIONAL_FEES_SET") is not None else None
                        original_total_duties_set = self.utils.convert_json_to_object(row_order.get("ORIGINAL_TOTAL_DUTIES_SET")) if row_order.get("ORIGINAL_TOTAL_DUTIES_SET") is not None else None
                        payment_terms = self.utils.convert_json_to_object(row_order.get("PAYMENT_TERMS")) if row_order.get("PAYMENT_TERMS") is not None else None
                        payment_gateway_names = self.utils.convert_json_to_object(row_order.get("PAYMENT_GATEWAY_NAMES")) if row_order.get("PAYMENT_GATEWAY_NAMES") is not None else None
                        refunds = self.utils.convert_json_to_object(row_order.get("REFUNDS")) if row_order.get("REFUNDS") is not None else None
                        shipping_address = self.utils.convert_json_to_object(row_order.get("SHIPPING_ADDRESS")) if row_order.get("SHIPPING_ADDRESS") is not None else None
                        shipping_lines = self.utils.convert_json_to_object(row_order.get("SHIPPING_LINES")) if row_order.get("SHIPPING_LINES") is not None else None
                        subtotal_price_set = self.utils.convert_json_to_object(row_order.get("SUBTOTAL_PRICE_SET")) if row_order.get("SUBTOTAL_PRICE_SET") is not None else None
                        tax_lines = self.utils.convert_json_to_object(row_order.get("TAX_LINES")) if row_order.get("TAX_LINES") is not None else None
                        total_discounts_set = self.utils.convert_json_to_object(row_order.get("TOTAL_DISCOUNTS_SET")) if row_order.get("TOTAL_DISCOUNTS_SET") is not None else None
                        total_line_items_price_set = self.utils.convert_json_to_object(row_order.get("TOTAL_LINE_ITEMS_PRICE_SET")) if row_order.get("TOTAL_LINE_ITEMS_PRICE_SET") is not None else None
                        total_price_set = self.utils.convert_json_to_object(row_order.get("TOTAL_PRICE_SET")) if row_order.get("TOTAL_PRICE_SET") is not None else None
                        total_tax_set = self.utils.convert_json_to_object(row_order.get("TOTAL_TAX_SET")) if row_order.get("TOTAL_TAX_SET") is not None else None
                        tracking_info = self.utils.convert_json_to_object(row_order.get("TRACKING_INFO")) if row_order.get("TRACKING_INFO") is not None else None
                        total_shipping_price_set = self.utils.convert_json_to_object(row_order.get("TOTAL_SHIPPING_PRICE_SET")) if row_order.get("TOTAL_SHIPPING_PRICE_SET") is not None else None

                        order_json = {
                            "id": row_order.get("ID"),
                            "name": row_order.get("NAME"),
                            "app_id": row_order.get("APP_ID"),
                            "billing_address": billing_address,
                            "browser_ip": row_order.get("BROWSER_IP"),
                            "buyer_accepts_marketing": row_order.get("BUYER_ACCEPTS_MARKETING"),
                            "cancel_reason": row_order.get("CANCEL_REASON"),
                            "cancelled_at": row_order.get("CANCELLED_AT"),
                            "cart_token": row_order.get("CART_TOKEN"),
                            "checkout_token": row_order.get("CHECKOUT_TOKEN"),
                            "client_details": client_details,
                            "created_at": created_at,
                            "updated_at": updated_at,
                            "closed_at": closed_at,
                            "company": company,
                            "confirmation_number": row_order.get("CONFIRMATION_NUMBER"),
                            "country_code": row_order.get("COUNTRY_CODE"),
                            "currency": row_order.get("CURRENCY"),
                            "current_total_additional_fees_set": current_total_additional_fees_set,
                            "current_total_discounts": row_order.get("CURRENT_TOTAL_DISCOUNTS"),
                            "customer_id": row_order.get("CUSTOMER_ID"),
                            "customer": customer,
                            "customer_locale": row_order.get("CUSTOMER_LOCALE"),
                            "delivery_status": row_order.get("DELIVERY_STATUS"),
                            "discount_applications": discount_applications,
                            "discount_codes": discount_codes,
                            "email": row_order.get("EMAIL"),
                            "estimated_taxes": row_order.get("ESTIMATED_TAXES"),
                            "financial_status": row_order.get("FINANCIAL_STATUS"),
                            "fulfillments": fulfillments,
                            "fulfillment_status": row_order.get("FULFILLMENT_STATUS"),
                            "line_items": [],
                            "LANDING_SITE": row_order.get("LANDING_SITE"),
                            "location_id": row_order.get("LOCATION_ID"),
                            "merchant_of_record_app_id": row_order.get("MERCHANT_OF_RECORD_APP_ID"),
                            "note": row_order.get("NOTE"),
                            "note_attributes": note_attributes,
                            "number": row_order.get("NUMBER"),
                            "order_number": row_order.get("ORDER_NUMBER"),
                            "original_total_additional_fees_set": original_total_additional_fees_set,
                            "original_total_duties_set": original_total_duties_set,
                            "payment_terms": payment_terms,
                            "payment_gateway_names": payment_gateway_names,
                            "phone": row_order.get("PHONE"),
                            "po_number": row_order.get("PO_NUMBER"),
                            "presentment_currency": row_order.get("PRESENTMENT_CURRENCY"),
                            "referring_site": row_order.get("REFERRING_SITE"),
                            "refunds": refunds,
                            "shipping_address": shipping_address,
                            "shipping_lines": shipping_lines,
                            "source_name": row_order.get("SOURCE_NAME"),
                            "source_identifier": row_order.get("SOURCE_IDENTIFIER"),
                            "source_url": row_order.get("SOURCE_URL"),
                            "subtotal_price": row_order.get("SUBTOTAL_PRICE"),
                            "subtotal_price_set": subtotal_price_set,
                            "tags": row_order.get("TAGS"),
                            "tax_lines": tax_lines,
                            "taxes_included": row_order.get("TAXES_INCLUDED"),
                            "test": row_order.get("TEST"),
                            "total_discounts": row_order.get("TOTAL_DISCOUNTS"),
                            "total_discounts_set": total_discounts_set,
                            "total_line_items_price": row_order.get("TOTAL_LINE_ITEMS_PRICE"),
                            "total_line_items_price_set": total_line_items_price_set,
                            "total_outstanding": row_order.get("TOTAL_OUTSTANDING"),
                            "total_price": row_order.get("TOTAL_PRICE"),
                            "total_price_set": total_price_set,
                            "total_shipping_price_set": total_shipping_price_set,
                            "total_tax": row_order.get("TOTAL_TAX"),
                            "total_tax_set": total_tax_set,
                            "total_tip_received": row_order.get("TOTAL_TIP_RECEIVED"),
                            "total_weight": row_order.get("TOTAL_WEIGHT"),
                            "user_id": row_order.get("USER_ID"),
                            "order_status_url": row_order.get("ORDER_STATUS_URL"),
                            "token": row_order.get("TOKEN"),
                            "admin_graphql_api_id": row_order.get("ADMIN_GRAPHQL_API_ID"),
                            "checkout_id": row_order.get("CHECKOUT_ID"),
                            "confirmed": row_order.get("CONFIRMED"),
                            "contact_email": row_order.get("CONTACT_EMAIL"),
                            "device_id": row_order.get("DEVICE_ID"),
                            "landing_site_ref": row_order.get("LANDING_SITE_REF"),
                            "reference": row_order.get("REFERENCE"),
                            "tax_exempt": row_order.get("TAX_EXEMPT"),
                            "processed_date": processed_date,
                            "printed_date": printed_date,
                            "fulfilled_date": fulfilled_date,
                            "shipping_label_url": row_order.get("SHIPPING_LABEL_URL"),
                            "tracking_info": tracking_info,
                            "error_description": row_order.get("ERROR_DESCRIPTION")
                        }

                    for row_item in line_items_result_query:
                        print(f"row_item: {row_item}")
                        item_price_set = self.utils.convert_json_to_object(row_item.get("PRICE_SET")) if row_item.get("PRICE_SET") is not None else None
                        item_total_discount_set = self.utils.convert_json_to_object(row_item.get("TOTAL_DISCOUNT_SET")) if row_item.get("TOTAL_DISCOUNT_SET") is not None else None
                        item_discount_allocations = self.utils.convert_json_to_object(row_item.get("DISCOUNT_ALLOCATIONS")) if row_item.get("DISCOUNT_ALLOCATIONS") is not None else None
                        item_tax_lines = self.utils.convert_json_to_object(row_item.get("TAX_LINES")) if row_item.get("TAX_LINES") is not None else None

                        item = {
                            "id": row_item.get("ID"),
                            "product_id": row_item.get("PRODUCT_ID"),
                            "name": row_item.get("NAME"),
                            "title": row_item.get("TITLE"),
                            "sku": row_item.get("SKU"),
                            "variant_id": row_item.get("VARIANT_ID"),
                            "variant_title": row_item.get("VARIANT_TITLE"),
                            "quantity": row_item.get("QUANTITY"),
                            "attributed_staffs": row_item.get("ATTRIBUTED_STAFFS"),
                            "fulfillment_service": row_item.get("FULFILLMENT_SERVICE"),
                            "fulfillment_status": row_item.get("FULFILLMENT_STATUS"),
                            "grams": row_item.get("GRAMS"),
                            "price": row_item.get("PRICE"),
                            "price_set": item_price_set,
                            "product_exists": row_item.get("PRODUCT_EXISTS"),
                            "requires_shipping": row_item.get("REQUIRES_SHIPPING"),
                            "variant_inventory_management": row_item.get("VARIANT_INVENTORY_MANAGEMENT"),
                            "vendor": row_item.get("VENDOR"),
                            "gift_card": row_item.get("GIFT_CARD"),
                            "properties": row_item.get("PROPERTIES"),
                            "taxable": row_item.get("TAXABLE"),
                            "tax_lines": item_tax_lines,
                            "total_discount": row_item.get("TOTAL_DISCOUNT"),
                            "total_discount_set": item_total_discount_set,
                            "discount_allocations": item_discount_allocations,
                            "duties": row_item.get("DUTIES"),
                            "admin_graphql_api_id": row_item.get("ADMIN_GRAPHQL_API_ID")
                        }
                        line_items.append(item)

                    order_json["line_items"] = line_items

                    return True, order_json, response_description, status_code
                else:
                    return False, {}, f"No line items found for the order with ID: {order_id} and name: {order_name}", 404
            else:
                return False, {}, f"No order found with ID: {order_id} and name: {order_name}", 404
        except Exception as e:
            return False, {}, f"Error while getting order with ID: {order_id} and name: {order_name} - Error: {str(e)}", 500
        finally:
            try:
                del order_json, line_items, columns_order, columns_items, order_columns, line_items_columns, order_condition, line_items_condition, order_result_query, line_items_result_query, item, response_description, status_code, billing_address, client_details, company, current_total_additional_fees_set, customer, discount_applications, discount_codes, fulfillments, note_attributes, original_total_additional_fees_set, original_total_duties_set, payment_terms, payment_gateway_names, refunds, shipping_address, shipping_lines, subtotal_price_set, tax_lines, total_discounts_set, total_line_items_price_set, total_price, total_price_set, total_tax_set, tracking_info, item_price_set, item_total_discount_set, item_discount_allocations, total_shipping_price_set, item_tax_lines
            except Exception as e:
                print(f"Variable not found: {e}")
                pass

    def get_list_of_orders(self, since_id, order_number, fields, product_name, product_variant, order_status, created_at_min, created_at_max, refunded_flag, prod_sku, country_code, received_back_date_min, received_back_date_max, tracking_number, is_fulfilled_flag, fulfilled_date_min, fulfilled_date_max, is_cancelled_flag, fulfillment_status, limit="50", order_by="asc"):
        """
        Retrieves a list of orders from the ORDER_DETAILS_VIEW based on the specified filters.
        View Columns:
            ORDER_NAME
            CREATED_AT
            ORDER_FULFILLMENT
            ORDER_STATUS
            ORDER_CANCELLED
            PRODUCT_ID
            ITEM_TITLE
            VARIANT_TITLE
            ITEM_SKU
            ITEM_QUANTITY
            ITEM_FULFILLMENT
            REFUNDED
            COUNTRY_CODE
            PROPERTIES
            TAGS

        Args:
            since_id (str): The minimum order name for retrieval. (View Column: ORDER_NAME)
            order_number (str): The order number to filter. (View Column: ORDER_NAME)
            fields (str): The fields to retrieve (Can be: ORDER_NAME, CREATED_AT, ORDER_FULFILLMENT, ORDER_STATUS, ORDER_CANCELLED, PRODUCT_ID, ITEM_TITLE, VARIANT_TITLE, ITEM_SKU, ITEM_FULFILLMENT, REFUNDED, COUNTRY_CODE, PROPERTIES, TAGS)
            created_at_min (str): The minimum creation date for filtering. (View Column: CREATED_AT)
            created_at_max (str): The maximum creation date for filtering. (View Column: CREATED_AT)
            fulfillment_status (str): The fulfillment status to filter. (View Column: ORDER_FULFILLMENT)
            order_status (str): The order status filter with 'Yes' ou 'No' string. (View Column: ORDER_STATUS)
            is_cancelled_flag (bool): Filter for cancelled flag. (View Column: ORDER_CANCELLED)
            product_id (str): The product ID to filter. (View Column: PRODUCT_ID)
            product_name (str): The product name to filter. (View Column: ITEM_TITLE)
            product_variant (str): The product variant to filter. (View Column: VARIANT_TITLE)
            prod_sku (str): The product SKU to filter. (View Column: ITEM_SKU)
            ITEM_FULFILLMENT
            refunded_flag (bool): Refunded flag to filter as 'true' or 'false'. (View Column: REFUNDED)
            COUNTRY_CODE
            PROPERTIES
            TAGS
            limit (str): The maximum number of records to retrieve. Default is "50".
            order_by (str): The order of retrieval. Default is "asc".
    
            received_back_date_min (str): The minimum received back date for filtering.
            received_back_date_max (str): The maximum received back date for filtering.
            is_fulfilled_flag (bool): Filter for fulfilled flag.
            fulfilled_date_min (str): The minimum fulfilled date for filtering.
            fulfilled_date_max (str): The maximum fulfilled date for filtering.
            

        Returns:
            tuple: (bool, list or str, int) indicating success, the list of orders or error message, and HTTP status code.
        """
        print(f"\n[INFO] BEGIN - Getting list of orders from ORDER_DETAILS_VIEW.")
        orders_list = []
        order = {}
        columns = []

        if order_number is not None and order_number != "":
            order_number = f"#{str(order_number).upper()}" if "#" not in order_number else str(order_number).upper()
        if is_cancelled_flag is not None:
            is_cancelled_flag = 'TRUE' if str(is_cancelled_flag).lower() == 'true' else 'FALSE'
        if is_fulfilled_flag is not None:
            is_fulfilled_flag = 'TRUE' if str(is_fulfilled_flag).lower() == 'true' else 'FALSE'
        if fulfillment_status is not None:
            fulfillment_status = str(fulfillment_status).lower() if str(fulfillment_status).lower() != "unfulfilled" else "IS NULL"
        if refunded_flag is not None:
            refunded_flag = 'refunded' if str(refunded_flag).lower() == 'true' else ''
        if order_status is not None:
            order_status = 'Yes' if str(order_status).lower() == 'yes' else 'No'

        if fields is not None:
            fields = fields.replace(" ", "").upper()
            if ',' in fields:
                columns = [field.strip() for field in fields.split(',')]
            else:
                columns.append(fields.strip())
        else:
            columns = ['*']
        columns = ['*'] if len(columns) == 0 else columns

        condition = "1=1"
        condition += (f"\nAND ORDER_NAME >= '{since_id}'" if order_by.lower() == "asc" else f"\nAND ORDER_NAME <= '{since_id}'") if since_id is not None else ""
        condition += f"\nAND ORDER_NAME LIKE '%{order_number}%'" if order_number is not None else ""
        condition += f"\nAND ITEM_TITLE LIKE '%{product_name}%'" if product_name is not None else ""
        condition += f"\nAND VARIANT_TITLE LIKE '%{product_variant}%'" if product_variant is not None else ""
        condition += f"\nAND CREATED_AT >= '{created_at_min}'" if created_at_min is not None else ""
        condition += f"\nAND CREATED_AT <= '{created_at_max}'" if created_at_max is not None else ""
        condition += f"\nAND ITEM_SKU LIKE '%{prod_sku}%'" if prod_sku is not None else ""
        condition += f"\nAND COUNTRY_CODE LIKE '%{country_code}%'" if country_code is not None else ""
        condition += f"\nAND ORDER_STATUS = {order_status}" if order_status is not None else ""
        condition += f"\nAND ORDER_CANCELLED = {is_cancelled_flag}" if is_cancelled_flag is not None else ""
        condition += f"\nAND REFUNDED = '{refunded_flag}'" if refunded_flag is not None else ""
        condition += f"\nORDER BY ORDER_NAME ASC" if order_by.lower() == "asc" else f"\nORDER BY ORDER_NAME DESC"

        # Terminar de alterar as conditions de acordo com as colunas da view.
        # ORDER_FULFILLMENT


        # PRODUCT_ID


        # ITEM_QUANTITY
        # ITEM_FULFILLMENT
        # REFUNDED
        # COUNTRY_CODE
        # TAGS


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
            result_flag, result_query = super().query_record("COMPANY_NAME.ORDER_DETAILS_VIEW", columns, condition)
            if result_flag:
                print(f"[INFO] Got {len(result_query)} orders from ORDER_DETAILS_VIEW.")
                for row in result_query:
                    order = {}
                    for column in columns:
                        order[column] = row[column]
                    orders_list.append(order)
                return True, orders_list, 200
            else:
                print(f"[ERROR] Error getting orders from ORDER_DETAILS_VIEW. Error: {result_query}")
                return False, result_query, 404
        except Exception as e:
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="get_list_of_orders", error_code=500, error_message=str(e), additional_details=None, error_severity=self.utils.get_error_severity(3))
            print(f"[ERROR] - {e}")
            return False, str(e), 500
        finally:
            print("[INFO] Cleaning up variables")
            try:
                del orders_list, order, columns, condition, result_flag, result_query, row
            except:
                pass

    # WEBHOOK FUNCTIONS
    def webhook_save_order(self, order_json):
        response_description = "Success"
        status_code = 200
        staging_table_count = 0

        if len(order_json) == 0:
            return False, f'[ERROR] Empty JSON.', 400
        else:
            try:
                staging_table_count = self.verify_staging_table_already_exits(order_json=order_json)
                if staging_table_count < 1:
                    is_upserted_success, order_inserted_count, response_description = self.save_order_on_staging_table(order_json)
                else:
                    print(f'[INFO] Order already exists in the staging table. No JSON difference for Order id: {order_json.get("id")}')
                    return False, f'[INFO] Order already exists in the staging table. Order id: {order_json.get("id")}', status_code

                self.execution_summary(is_webhook=True, send_email=False)
                if is_upserted_success == False:
                    additional_details = f'Response Description: {response_description}'
                    additional_details += f'\n[INFO] order_json: {order_json}'
                    order_id = None
                    
                    try:
                        order_id = order_json.get("id")
                    except Exception as e:
                        try:
                            order_id = order_json['order'].get("id")
                        except Exception as e:
                            pass

                    self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="webhook_save_order", error_code='500', error_message=f"[ERROR] Error while upserting order. Order id: {order_id}.", additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
                    return is_upserted_success, f"[ERROR] Error while upserting order. Order id: {order_id}", 500
                else:
                    return is_upserted_success, response_description, status_code
            except Exception as e:
                additional_details = f'[INFO] order_json: {order_json}'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="webhook_save_order", error_code="500", error_message=str(e), additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
                return False, f'[ERROR] Error while saving order. Error: {str(e)}', 500

    def webhook_delete_order(self, order_json):
        if len(order_json) == 0:
            print(f'[ERROR] Empty JSON.')
        else:
            try:
                order_id = order_json.get("id")
                result_command, rowcount, response_description = self.delete_order(order_id)

                if result_command == True and rowcount > 0:
                    print(f'[INFO] Order deleted: {order_id}')
                    return result_command, f'[INFO] Order deleted: {order_id} - {response_description}', 200
                else:
                    additional_details = f'[INFO] order_json: {order_json}'
                    self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="webhook_delete_order", error_code="500", error_message=response_description, additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
                    return result_command, f'[INFO] Order NOT deleted: {order_id} - {response_description}', 500
            except Exception as e:
                additional_details = f'[INFO] order_json: {order_json}'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="webhook_delete_order", error_code="500", error_message=str(e), additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
                return result_command, {response_description}, 500

        self.execution_summary(is_webhook=True, send_email=False)

    def webhook_edit_order(self, order_json):
        if len(order_json) == 0:
            return False, f'[ERROR] Empty JSON.', 400
        else:
            try:
                orders = order_json.get('order_edit', [])

                is_upserted_success, response_description, status_code = self.get_specific_order(orderId=orders.get("order_id", ""), fields=None, api_version="2023-10")

                if is_upserted_success:
                    return True, response_description, status_code
                else:
                    additional_details = f'[INFO] order_json: {order_json}'
                    self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="webhook_edit_order", error_code=status_code, error_message=response_description, additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
                    self.execution_summary(is_webhook=True, send_email=False)
                    return False, response_description, status_code
            except Exception as e:
                additional_details = f'[INFO] order_json: {order_json}'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="webhook_edit_order", error_code="500", error_message=str(e), additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
                return False, f'[ERROR] Error while editing order. Error: {str(e)}', 500

    async def webhook_order_edit(self, order_json):
        if len(order_json) == 0:
            return False, f'[ERROR] Empty JSON.', 400
        else:
            try:
                orders = order_json.get('order_edit', [])

                return_flag, order_data, response_description, status_code = self.shopify.get_shopify_specific_order(orderId=orders.get("order_id", ""), fields=None, api_version=self.utils.get_SHOPIFY_API_VERSION())
                if return_flag == True:
                    is_upserted_success, response_description, status_code = await self.mq.queue_message(virtual_host="orders", queue_name="order", message_to_queue=json.dumps(order_data).encode("utf-8"))
                else:
                    is_upserted_success = False

                if is_upserted_success:
                    return True, response_description, status_code
                else:
                    additional_details = f'[INFO] order_json: {order_json}'
                    self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="webhook_edit_order", error_code=status_code, error_message=response_description, additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
                    self.execution_summary(is_webhook=True, send_email=False)
                    return False, response_description, status_code
            except Exception as e:
                additional_details = f'[INFO] order_json: {order_json}'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="webhook_edit_order", error_code="500", error_message=str(e), additional_details=additional_details, error_severity=self.utils.get_error_severity(2))
                return False, f'[ERROR] Error while editing order. Error: {str(e)}', 500

    async def get_order_from_mq(self):
        response_description = "Success"
        status_code = 200
        response = None
        return_flag = False

        try:
            return_flag, response, status_code = await self.mq.get_message(virtual_host="orders", queue_name="order")

            if return_flag:
                return True, json.loads(response), response_description, status_code
            else:
                return False, response, response, status_code
        except Exception as e:
            print(f"[ERROR] Error while getting order from MQ: {e}")
            return False, response, f"{str(e)}", 400
        finally:
            try:
                del response
                del columns
                del result_query
                del response_description
                del status_code
            except:
                pass

    async def process_orders_from_mq(self):
        while True:
            try:
                # Variables
                is_order_upserted_success = None
                total_order_upserted_count = 0
                response_description = None
                line_items = None
                last_order_id = None
                last_order_line_item_id = None
                items_count = 0
                items_upserted_count = 0
                additional_details = None
                order_json = None
                is_item_upserted_success = False
                return_flag = False
                status_code = 200

                return_flag, order_json, response_description, status_code = await self.get_order_from_mq()

                if return_flag:
                    print(f"\n[INFO] Processing order from MQ...\t\tOrder: {order_json.get("id")}")
                    try:
                        order_json = order_json['order']
                    except:
                        pass
                    is_order_upserted_success, is_item_upserted_success, total_order_upserted_count, items_upserted_count, last_order_id = self.process_order_and_line_items(order_json=order_json)

                else:
                    self.utils.set_end_time(self.utils.get_current_date_time())
                    print("[INFO] No orders found.")
                    print(f"[INFO] Waiting {self.utils.get_WAIT_TIME()} second(s) to try again...")
                    time.sleep(int(self.utils.get_WAIT_TIME()))
            except Exception as e:
                print(f"[ERROR] Error while processing orders from staging table: {e}")

                additional_details = f'\n[INFO] last_order_id: {last_order_id}'
                additional_details += f'\n[INFO] The function process_orders_from_staging_table() execution kept running on Heroku. Please check the logs for more information.'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_orders_from_staging_table", error_code=None, error_message=str(e), additional_details=additional_details, error_severity=self.utils.get_error_severity(2))

                print(f"[INFO] Waiting {self.utils.get_WAIT_TIME()} second(s) to try again...")
                time.sleep(int(self.utils.get_WAIT_TIME()))
            finally:
                print("[INFO] Clearing variables...")
                try:
                    del line_items, last_order_id, last_order_line_item_id, total_order_upserted_count, items_count, items_upserted_count, line_item, line_item_updated_count, is_order_upserted_success, response_description, additional_details, order_json, is_item_upserted_success, status_code
                    gc.collect()
                except:
                    pass
