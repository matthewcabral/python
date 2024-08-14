import os
import mysql.connector  # MySQL Database on Amazon AWS
import psycopg2         # PostgreSQL Database on Heroku
import pandas as pd
import json
import string
from psycopg2 import errors
from utils.UtilsController import *
from mysql.connector import IntegrityError

utils = None

class Controller:
    """Class Controller: Manages database connections and table names for a variety of databases.
    
    This class handles connections to both MySQL and PostgreSQL databases using credentials 
    stored in environment variables. It also maintains a list of table names used in the 
    application.
    """
    conn = None

    # Database connection variables saved on the OS environment variables
    def __init__(self):
        self.utils = UtilsController()

        self.DB_DRIVER = os.getenv('DB_DRIVER')
        self.DB_NAME = os.getenv("DB_NAME")
        self.DB_USER = os.getenv("DB_USER")
        self.DB_PASSWORD = os.getenv("DB_PASSWORD")
        self.DB_HOST = os.getenv("DB_HOST")
        self.DB_PORT = os.getenv("DB_PORT")
        self.DB_OWNER = os.getenv("DB_OWNER")

        self.tbl_ID = "ROW_ID"
        self.tbl_PROD = "PRODUCT"
        self.tbl_PROD_INV = "PROD_INV"
        self.tbl_PROD_VARIANT_INV = "PROD_VARIANT_INV"
        self.tbl_CARTS = "CARTS"
        self.tbl_PDF = "PDFS"
        self.tbl_ORDER = "ORDER"
        self.tbl_ORDER_LINE_ITEM = "ORDER_LINE_ITEM"
        self.tbl_ORDER_RANGE = "ORDER_RANGE"
        self.tbl_PHONE_CASE_IMAGES = "PHONE_CASE_IMAGES"
        self.tbl_LOGS = "LOGS"
        self.tbl_WEBHOOK_LOGS = "WEBHOOK_LOGS"
        self.tbl_ORDER_STAGING = "ORDER_STAGING"
        self.tbl_SYS_PREF = "SYS_PREF"
        self.tbl_PHONE_CASE_BATCH = "PHONE_CASE_BATCH"
        self.tbl_ORDER_PROCESSING_BATCHES = "ORDER_PROCESSING_BATCHES"
        self.tbl_DAILY_CURRENCY = "DAILY_CURRENCY"
        self.tbl_ORDER_PROCESSING = "ORDER_PROCESSING"
        self.tbl_CUSTOM_PROD_FONTS = "CUSTOM_PROD_FONTS"
        self.tbl_CUSTOM_PROD_VARIANT_FONTS = "CUSTOM_PROD_VARIANT_FONTS"
        self.tbl_LOCATIONS = "LOCATION"
        self.tbl_CUSTOM_ORDERS_MANAGEMENT = "CUSTOM_ORDERS_MANAGEMENT"
        self.tbl_REPORTS_MANAGEMENT = "REPORTS_MANAGEMENT"
        self.tbl_REPORTS_DATA = "REPORTS_DATA"
        self.tbl_PRINTING_HISTORY = "PRINTING_HISTORY"
        self.tbl_CUSTOM_FONTS = "CUSTOM_FONTS"
        self.tbl_CURRENCIES = "DAILY_CURRENCY"
        self.tbl_VENDOR = "VENDOR"
        self.tbl_BILLING_ADDRESS = "BILLING_ADDRESS"
        self.tbl_SHIPPING_ADDRESS = "SHIPPING_ADDRESS"
        self.tbl_CONTACT = "CONTACT"
        self.tbl_DISCOUNT_APPLICATIONS = "DISCOUNT_APPLICATIONS"
        self.tbl_DISCOUNT_CODES = "DISCOUNT_CODES"
        self.tbl_FULFILLMENTS = "FULFILLMENTS"
        self.tbl_FULFILLMENT_LINE_ITEMS = "FULFILLMENTS_LINE_ITEMS"
        self.tbl_REFUNDS = "REFUNDS"
        self.tbl_REFUNDS_LINE_ITEMS = "REFUNDS_LINE_ITEMS"
        self.tbl_REFUNDS_DISCOUNTS = "REFUNDS_DISCOUNTS"
        self.view_UNF_CUSTOMS_ORDERS_VIEW = "UNF_CUSTOMS_ORDERS_VIEW"
        self.tbl_ORDER_DETAILS_VIEW = "ORDER_DETAILS_VIEW"
        self.tbl_VENDOR_LOCATION = "VENDOR_LOCATION"
        self.view_VENDOR_LOCATION_VIEW = "VENDOR_LOCATION_VIEW"

        self.conn = None

    def get_DB_DRIVER(self):
        return self.DB_DRIVER

    def get_DB_NAME(self):
        return self.DB_NAME

    def get_DB_USER(self):
        return self.DB_USER

    def get_DB_PASSWORD(self):
        return self.DB_PASSWORD

    def get_DB_HOST(self):
        return self.DB_HOST

    def get_DB_PORT(self):
        return self.DB_PORT

    def get_DB_OWNER(self):
        return self.DB_OWNER

    def get_tbl_ID(self):
        return self.tbl_ID

    def get_tbl_PROD(self):
        return self.tbl_PROD

    def get_tbl_CARTS(self):
        return self.tbl_CARTS

    def get_tbl_PDF(self):
        return self.tbl_PDF

    def get_tbl_ORDER(self):
        return self.tbl_ORDER

    def get_tbl_ORDER_LINE_ITEM(self):
        return self.tbl_ORDER_LINE_ITEM

    def get_tbl_ORDER_RANGE(self):
        return self.tbl_ORDER_RANGE

    def get_tbl_PHONE_CASE_IMAGES(self):
        return self.tbl_PHONE_CASE_IMAGES

    def get_tbl_LOGS(self):
        return self.tbl_LOGS

    def get_tbl_WEBHOOK_LOGS(self):
        return self.tbl_WEBHOOK_LOGS

    def get_tbl_ORDER_STAGING(self):
        return self.tbl_ORDER_STAGING

    def get_tbl_SYS_PREF(self):
        return self.tbl_SYS_PREF

    def get_tbl_PHONE_CASE_BATCH(self):
        return self.tbl_PHONE_CASE_BATCH

    def get_tbl_ORDER_PROCESSING_BATCHES(self):
        return self.tbl_ORDER_PROCESSING_BATCHES

    def get_tbl_DAILY_CURRENCY(self):
        return self.tbl_DAILY_CURRENCY

    def get_tbl_ORDER_PROCESSING(self):
        return self.tbl_ORDER_PROCESSING

    def get_tbl_CUSTOM_PROD_FONTS(self):
        return self.tbl_CUSTOM_PROD_FONTS

    def get_tbl_CUSTOM_PROD_VARIANT_FONTS(self):
        return self.tbl_CUSTOM_PROD_VARIANT_FONTS

    def get_tbl_LOCATIONS(self):
        return self.tbl_LOCATIONS

    def get_tbl_CUSTOM_ORDERS_MANAGEMENT(self):
        return self.tbl_CUSTOM_ORDERS_MANAGEMENT

    def get_tbl_REPORTS_MANAGEMENT(self):
        return self.tbl_REPORTS_MANAGEMENT

    def get_tbl_REPORTS_DATA(self):
        return self.tbl_REPORTS_DATA

    def get_tbl_PROD_INV(self):
        return self.tbl_PROD_INV

    def get_tbl_PROD_VARIANT_INV(self):
        return self.tbl_PROD_VARIANT_INV

    def get_tbl_PRINTING_HISTORY(self):
        return self.tbl_PRINTING_HISTORY

    def get_tbl_CUSTOM_FONTS(self):
        return self.tbl_CUSTOM_FONTS

    def get_tbl_CURRENCIES(self):
        return self.tbl_CURRENCIES

    def get_tbl_VENDOR(self):
        return self.tbl_VENDOR

    def get_tbl_BILLING_ADDRESS(self):
        return self.tbl_BILLING_ADDRESS

    def get_tbl_SHIPPING_ADDRESS(self):
        return self.tbl_SHIPPING_ADDRESS

    def get_tbl_CONTACT(self):
        return self.tbl_CONTACT

    def get_tbl_DISCOUNT_APPLICATIONS(self):
        return self.tbl_DISCOUNT_APPLICATIONS

    def get_tbl_DISCOUNT_CODES(self):
        return self.tbl_DISCOUNT_CODES

    def get_tbl_FULFILLMENTS(self):
        return self.tbl_FULFILLMENTS

    def get_tbl_FULFILLMENT_LINE_ITEMS(self):
        return self.tbl_FULFILLMENT_LINE_ITEMS

    def get_tbl_REFUNDS(self):
        return self.tbl_REFUNDS

    def get_tbl_REFUNDS_LINE_ITEMS(self):
        return self.tbl_REFUNDS_LINE_ITEMS

    def get_tbl_REFUNDS_DISCOUNTS(self):
        return self.tbl_REFUNDS_DISCOUNTS

    def get_view_UNF_CUSTOMS_ORDERS_VIEW(self):
        return self.view_UNF_CUSTOMS_ORDERS_VIEW

    def get_tbl_VENDOR_LOCATION(self):
        return self.tbl_VENDOR_LOCATION

    def get_view_VENDOR_LOCATION_VIEW(self):
        return self.view_VENDOR_LOCATION_VIEW
