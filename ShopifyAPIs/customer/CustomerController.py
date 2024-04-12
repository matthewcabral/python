from database.DataController import DataController
from utils.UtilsController import UtilsController

class CustomerController(DataController):
    def __init__(self):
        super().__init__()
        self.utils = UtilsController()

    class Customer():
        def __init__(self):
            self.addresses = None
            self.created_at = None
            self.default_address = None
            self.email = None
            self.email_marketing_consent = None
            self.first_name = None
            self.id = None
            self.last_name = None
            self.last_order_id = None
            self.last_order_name = None
            self.metafield = None
            self.multipass_identifier = None
            self.note = None
            self.orders_count = None
            self.phone = None
            self.sms_marketing_consent = None
            self.state = None
            self.tags = None
            self.tax_exempt = None
            self.tax_exemptions = None
            self.total_spent = None
            self.updated_at = None
            self.verified_email = None

        def get_addresses(self):
            return self.addresses

        def get_created_at(self):
            return self.created_at

        def get_default_address(self):
            return self.default_address

        def get_email(self):
            return self.email

        def get_email_marketing_consent(self):
            return self.email_marketing_consent

        def get_first_name(self):
            return self.first_name

        def get_id(self):
            return self.id

        def get_last_name(self):
            return self.last_name

        def get_last_order_id(self):
            return self.last_order_id

        def get_last_order_name(self):
            return self.last_order_name

        def get_metafield(self):
            return self.metafield

        def get_multipass_identifier(self):
            return self.multipass_identifier

        def get_note(self):
            return self.note

        def get_orders_count(self):
            return self.orders_count

        def get_phone(self):
            return self.phone

        def get_sms_marketing_consent(self):
            return self.sms_marketing_consent

        def get_state(self):
            return self.state

        def get_tags(self):
            return self.tags

        def get_tax_exempt(self):
            return self.tax_exempt

        def get_tax_exemptions(self):
            return self.tax_exemptions

        def get_total_spent(self):
            return self.total_spent

        def get_updated_at(self):
            return self.updated_at

        def get_verified_email(self):
            return self.verified_email

        def set_addresses(self, value):
            self.addresses = value

        def set_created_at(self, value):
            self.created_at = value

        def set_default_address(self, value):
            self.default_address = value

        def set_email(self, value):
            self.email = value

        def set_email_marketing_consent(self, value):
            self.email_marketing_consent = value

        def set_first_name(self, value):
            self.first_name = value

        def set_id(self, value):
            self.id = value

        def set_last_name(self, value):
            self.last_name = value

        def set_last_order_id(self, value):
            self.last_order_id = value

        def set_last_order_name(self, value):
            self.last_order_name = value

        def set_metafield(self, value):
            self.metafield = value

        def set_multipass_identifier(self, value):
            self.multipass_identifier = value

        def set_note(self, value):
            self.note = value

        def set_orders_count(self, value):
            self.orders_count = value

        def set_phone(self, value):
            self.phone = value

        def set_sms_marketing_consent(self, value):
            self.sms_marketing_consent = value

        def set_state(self, value):
            self.state = value

        def set_tags(self, value):
            self.tags = value

        def set_tax_exempt(self, value):
            self.tax_exempt = value

        def set_tax_exemptions(self, value):
            self.tax_exemptions = value

        def set_total_spent(self, value):
            self.total_spent = value

        def set_updated_at(self, value):
            self.updated_at = value

        def set_verified_email(self, value):
            self.verified_email = value

