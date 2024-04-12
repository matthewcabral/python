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
        self.tbl_CARTS = "CARTS"
        self.tbl_PDF = "PDFS"
        self.tbl_ORDER = "ORDER"
        self.tbl_ORDER_LINE_ITEM = "ORDER_LINE_ITEM"
        self.tbl_ORDER_STATUS = "ORDER_STATUS"
        self.tbl_PHONE_CASE_IMAGES = "PHONE_CASE_IMAGES"
        self.tbl_LOGS = "LOGS"
        self.tbl_WEBHOOK_LOGS = "WEBHOOK_LOGS"
        self.tbl_ORDER_STAGING = "ORDER_STAGING"
        self.tbl_SYS_PREF = "SYS_PREF"
        self.tbl_PHONE_CASE_BATCH = "PHONE_CASE_BATCH"
        self.tbl_ORDER_PROCESSING_BATCHES = "ORDER_PROCESSING_BATCHES"

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

    def get_tbl_ORDER_STATUS(self):
        return self.tbl_ORDER_STATUS

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
