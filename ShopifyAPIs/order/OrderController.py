from database.DataController import DataController
from utils.UtilsController import *
from utils.LogsController import *
from apis.inbound.ShopifyController import *
from mq.MQController import *

class OrderController(DataController):
    # CONSTRUCTOR
    def __init__(self):
        sys.setrecursionlimit(10**6)
        super().__init__()
        self.utils = UtilsController()
        self.shopify = ShopifyController()
        self.log = LogsController()
        self.mq = MQController()

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
            email_to = ['xxx']
            email_from = "xxx"
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

    # DATABASE FUNCTIONS
    def get_last_processed_order_id(self):
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
            print(f"[ERROR] get_last_processed_order_id: {str(e)}")
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

        sql_header = f"""INSERT INTO {super().get_DB_OWNER()}.{super().get_tbl_ORDER_STAGING()}"""
        sql_column = f""" (ORDER_JSON)"""
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
                        self.utils.set_end_time(time.time())
                        print("[INFO] No orders found.")
                        print(f"[INFO] Waiting {self.utils.get_WAIT_TIME()} second(s) to try again...")
                        time.sleep(int(self.utils.get_WAIT_TIME()))
                else:
                    self.utils.set_end_time(time.time())
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
                    self.utils.set_start_time(time.time())
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
                            self.utils.set_end_time(time.time())
                            continue_while = False
                    else:
                        print("[DONE] No orders found.")
                        self.utils.set_end_time(time.time())
                        continue_while = False
                else:
                    print("[DONE] No orders found.")
                    self.utils.set_end_time(time.time())
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
                    self.utils.set_end_time(time.time())
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
            self.utils.validate_columns_values("BILLING_ADDRESS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_billing_address())))
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
            self.utils.validate_columns_values("DISCOUNT_APPLICATIONS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_discount_applications())))
            self.utils.validate_columns_values("DISCOUNT_CODES", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_discount_codes())))
            self.utils.validate_columns_values("EMAIL", self.utils.replace_special_chars(order.get_email()))
            self.utils.validate_columns_values("ESTIMATED_TAXES", order.get_estimated_taxes())
            self.utils.validate_columns_values("FINANCIAL_STATUS", order.get_financial_status())
            self.utils.validate_columns_values("FULFILLMENTS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_fulfillments())))
            self.utils.validate_columns_values("FULFILLMENT_STATUS", order.get_fulfillment_status())
            self.utils.validate_columns_values("ID", order.get_id())
            self.utils.validate_columns_values("LANDING_SITE", self.utils.replace_special_chars(order.get_landing_site()))
            self.utils.validate_columns_values("LINE_ITEMS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_line_items())))
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
            self.utils.validate_columns_values("REFUNDS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_refunds())))
            self.utils.validate_columns_values("SHIPPING_ADDRESS", self.utils.replace_special_chars(self.utils.convert_object_to_json(order.get_shipping_address(), ensure_ascii=False)))
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
                del maximum_insert_try
                del keep_trying
                del count_try
                del order_id
                del columns
                del rows
                del result_query
                del error_message
                del additional_details
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
        error_message = None
        additional_details = None
        is_order_upserted_success = False
        total_order_upserted_count = 0
        order_inserted_count = 0
        order_updated_count = 0
        response_description = "Success"
        line_items = None
        items_upserted_count = 0
        items_count = 0
        is_item_upserted_success = False
        line_item_inserted_count = 0
        line_item_updated_count = 0
        last_order_line_item_id = None
        item = None

        try:
            order = self.Order()
            last_order_id = order_json.get("id")

            order.set_id(last_order_id)
            order.set_admin_graphql_api_id(order_json.get("admin_graphql_api_id", ""))
            order.set_app_id(order_json.get("app_id", ""))
            order.set_browser_ip(order_json.get("browser_ip", ""))
            order.set_buyer_accepts_marketing(order_json.get("buyer_accepts_marketing", ""))
            order.set_cancel_reason(order_json.get("cancel_reason", ""))
            order.set_cancelled_at(order_json.get("cancelled_at", ""))
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
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_orders_from_staging_table", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
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
            order.set_discount_codes(order_json.get("discount_codes", ""))
            order.set_email(order_json.get("email", ""))
            order.set_estimated_taxes(order_json.get("estimated_taxes", ""))
            order.set_financial_status(order_json.get("financial_status", ""))
            order.set_fulfillment_status(order_json.get("fulfillment_status", ""))
            order.set_landing_site(order_json.get("landing_site", ""))
            order.set_landing_site_ref(order_json.get("landing_site_ref", ""))
            order.set_location_id(order_json.get("location_id", ""))
            order.set_merchant_of_record_app_id(order_json.get("merchant_of_record_app_id", ""))
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
            order.set_billing_address(order_json.get("billing_address", ""))
            try:
                order.set_customer_id(order_json.get("customer", {}).get("id"))
            except Exception as e:
                error_message = f"[ERROR] Error while getting customer id from order_json. Order id: {order_json.get('id')} - Error: {str(e)}."
                additional_details = f'[INFO] This error is not critical. The order will be saved without the customer id.'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_orders_from_staging_table", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                order.set_customer_id("")
            order.set_customer(order_json.get("customer", {}))
            order.set_discount_applications(order_json.get("discount_applications", ""))
            order.set_fulfillments(order_json.get("fulfillments", ""))
            try:
                line_items = order_json.get("line_items", [])
            except Exception as e:
                error_message = f"[ERROR] Error while getting line_items from order_json. Order id: {order_json.get('id')} - Error: {str(e)}."
                additional_details = f'[INFO] This error is not critical. The order will be saved without the line_items.'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_orders_from_staging_table", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                line_items = []
            order.set_line_items(line_items)
            order.set_payment_terms(order_json.get("payment_terms", ""))
            order.set_refunds(order_json.get("refunds", ""))
            order.set_shipping_address(order_json.get("shipping_address", {}))
            order.set_shipping_lines(order_json.get("shipping_lines", ""))

            is_order_upserted_success, total_order_upserted_count, order_inserted_count, order_updated_count, response_description = self.upsert_orders(order)

            if is_order_upserted_success:
                if order_inserted_count > 0:
                    self.print_log(log_level="info", type="order", function="insert", order_id=last_order_id, item_id=None, try_count=None, message=None)
                elif order_updated_count > 0:
                    self.print_log(log_level="info", type="order", function="update", order_id=last_order_id, item_id=None, try_count=None, message=None)
                else:
                    self.print_log(log_level="error", type="order", function="process", order_id=last_order_id, item_id=None, try_count=None, message=None)

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
                            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_orders_from_staging_table", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
                else:
                    error_message = f"[ERROR] No line items found for order: {last_order_id}"
                    additional_details = f'[INFO] Order JSON: {order_json}'
                    self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_orders_from_staging_table", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
            else:
                error_message = f"[ERROR] Error while processing order: {response_description}"
                additional_details = f'[INFO] Order JSON: {order_json}'
                self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_orders_from_staging_table", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))

            print(f"[INFO] {items_upserted_count} of {items_count} item(s) was/were upserted...\tOrder: {last_order_id}")

            return is_order_upserted_success, is_item_upserted_success, total_order_upserted_count, items_upserted_count, last_order_id
        except Exception as e:
            print(f"[ERROR] Error while processing order: {e}")
            error_message = f"[ERROR] Error while processing order: {e}"
            additional_details = f'[INFO] Order JSON: {order_json}'
            self.log.log_error(repository_name=self.utils.get_REPOSITORY_NAME(), module_name=self.get_module_name(), function_name="process_orders_from_staging_table", error_code=None, error_message=error_message, additional_details=additional_details, error_severity=self.utils.get_error_severity(6))
            return is_order_upserted_success, is_item_upserted_success, total_order_upserted_count, items_upserted_count, last_order_id
        finally:
            print(f"Cleaning up variables...")
            try:
                del last_order_id
                del error_message
                del additional_details
                del is_order_upserted_success
                del total_order_upserted_count
                del order_inserted_count
                del order_updated_count
                del response_description
                del line_items
                del items_upserted_count
                del items_count
                del is_item_upserted_success
                del line_item_inserted_count
                del line_item_updated_count
                del last_order_line_item_id
                del item
            except Exception as e:
                print(f"[INFO] Variable not found: {e}")
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
                self.utils.set_start_time(time.time())
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
                    self.utils.set_end_time(time.time())
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
                        self.utils.set_end_time(time.time())
                        self.execution_summary(is_webhook=False, send_email=True)
                        continue_flag = False
            except Exception as e:
                self.utils.set_end_time(time.time())

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
                    self.utils.set_end_time(time.time())
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

    def prepare_orders_for_fullfillment(self, limit):
        # Variables
        first_order_id = None
        last_order_id = None
        one_hour_ago = self.utils.get_x_hours_ago(hours=1)
        order_ids = []
        columns = None
        values = None

        try:
            first_order_id = self.get_last_processed_order_id()
            order_ids, last_order_id = self.get_order_ids_for_processing(first_id=first_order_id, last_id=None, created_at_min=None, created_at_max=one_hour_ago, limit=limit)
            if last_order_id is not None:
                columns = ["FIRST_ORDER_ID", "LAST_ORDER_ID", "TOTAL_ORDERS"]
                values = [first_order_id, last_order_id, len(order_ids)]

        except Exception as e:
            return False, f"[ERROR] Error while getting last processed order id. Error: {str(e)}"
        finally:
            print(f"Clearing variables...")
            try:
                del first_order_id
                del last_order_id
                del one_hour_ago
                del order_ids
                del columns
                del values
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
                del order_json
                del line_items
                del columns_order
                del columns_items
                del order_columns
                del line_items_columns
                del order_condition
                del line_items_condition
                del order_result_query
                del line_items_result_query
                del item
                del response_description
                del status_code
                del billing_address
                del client_details
                del company
                del current_total_additional_fees_set
                del customer
                del discount_applications
                del discount_codes
                del fulfillments
                del note_attributes
                del original_total_additional_fees_set
                del original_total_duties_set
                del payment_terms
                del payment_gateway_names
                del refunds
                del shipping_address
                del shipping_lines
                del subtotal_price_set
                del tax_lines
                del total_discounts_set
                del total_line_items_price_set
                del total_price
                del total_price_set
                del total_tax_set
                del tracking_info
                del item_price_set
                del item_total_discount_set
                del item_discount_allocations
                del total_shipping_price_set
                del item_tax_lines
            except Exception as e:
                print(f"Variable not found: {e}")
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
                    self.utils.set_end_time(time.time())
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
                    del line_items
                    del last_order_id
                    del last_order_line_item_id
                    del total_order_upserted_count
                    del items_count
                    del items_upserted_count
                    del line_item
                    del line_item_updated_count
                    del is_order_upserted_success
                    del response_description
                    del additional_details
                    del order_json
                    del is_item_upserted_success
                    del status_code
                except:
                    pass
