import time
import os
import tempfile
import smtplib
import httplib2
import platform
import subprocess
import requests
import json
import sys
import pytz
import io
import inspect

from typing import List
from datetime import datetime, timedelta
from dotenv import load_dotenv
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import *
from reportlab.lib.utils import *
from reportlab.graphics.barcode import code128
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from googleapiclient import discovery
from fastapi import APIRouter, HTTPException, Body, Response, status
from pydantic import BaseModel, Field
from typing import Optional
from memory_profiler import profile

class UtilsController():

    def __init__(self):
        load_dotenv()

        self.REPOSITORY_NAME = self.load_env_variable('REPOSITORY_NAME')
        self._PYTHONPATH = self.load_env_variable('PYTHONPATH')

        self.SHOPIFY_API_VERSION = self.load_env_variable('SHOPIFY_API_VERSION')
        self.SHOPIFY_PRIV_KEY = self.load_env_variable('SHOPIFY_PRIV_KEY')
        self.SHOPIFY_PUB_KEY = self.load_env_variable('SHOPIFY_PUB_KEY')

        self.AMAZON_ACCESS_KEY = self.load_env_variable('AMAZON_ACCESS_KEY')
        self.AMAZON_ACCESS_SECRET = self.load_env_variable('AMAZON_ACCESS_SECRET')
        self.AMAZON_REGION = self.load_env_variable('AMAZON_REGION')
        self.AMAZON_BUCKET_NAME = self.load_env_variable('AMAZON_BUCKET_NAME')
        self.AMAZON_CDN_URL = self.load_env_variable('AMAZON_CDN_URL')

        self.MACHOOL_PRIV_KEY = self.load_env_variable('MACHOOL_PRIV_KEY')
        self.MACHOOL_PUB_KEY = self.load_env_variable('MACHOOL_PUB_KEY')
        self.FARETRADE_API_KEY = self.load_env_variable('FARETRADE_API_KEY')
        self.CANADA_POST_AUTHORIZATION = self.load_env_variable('CANADA_POST_AUTHORIZATION')

        self.EMAIL_ADDRESS = self.load_env_variable('EMAIL_ADDRESS')
        self.EMAIL_PASSWORD = self.load_env_variable('EMAIL_PASSWORD')

        self.GOOGLE_SHEET_DEV_KEY = self.load_env_variable('GOOGLE_SHEET_DEV_KEY')
        self.GOOGLE_SHEET_DEV_KEY_2 = self.load_env_variable('GOOGLE_SHEET_DEV_KEY_2')
        self.SPREAD_SHEET_ID = self.load_env_variable('SPREAD_SHEET_ID')
        self.SPREAD_SHEET_ID_2 = self.load_env_variable('SPREAD_SHEET_ID_2')

        self.RUNCOUNTER = self.load_env_variable('ORDER_RUN_COUNTER')
        self.MAXRUNS = self.load_env_variable('ORDER_MAX_RUNS')
        self.FIELDS = self.load_env_variable('ORDER_FIELDS')
        self.LIMIT = self.load_env_variable('ORDER_LIMIT')
        self.STATUS = self.load_env_variable('ORDER_STATUS')
        self.SINCE_ID = self.load_env_variable('ORDER_SINCE_ID')
        self.FULFILLMENT_STATUS = self.load_env_variable('ORDER_FULFILLMENT_STATUS')
        self.CREATED_AT_MIN = self.load_env_variable('ORDER_CREATED_AT_MIN')
        self.CREATED_AT_MAX = self.load_env_variable('ORDER_CREATED_AT_MAX')
        self.PROCESSED_AT_MIN = self.load_env_variable('ORDER_PROCESSED_AT_MIN')
        self.PROCESSED_AT_MAX = self.load_env_variable('ORDER_PROCESSED_AT_MAX')
        self.REMOVE_ORDERS = self.load_env_variable('REMOVE_ORDERS')
        self.REMOVE_LINE_ITEMS = self.load_env_variable('REMOVE_LINE_ITEMS')
        self.WAIT_TIME = self.load_env_variable('STAGING_WAIT_TIME')
        self.PHONE_CASE_LIMIT = self.load_env_variable('PHONE_CASE_LIMIT')

        self.MENU_OPTION = self.load_env_variable('MENU_OPTION')

        self.columns_array = []
        self.values_array = []
        self.condition = ""
        self.start_time = 0
        self.end_time = 0

    def get_total_time_hms(self, start_time, end_time):
        time = end_time - start_time
        hours = int(time / 3600)
        minutes = int((time - hours * 3600) / 60)
        seconds = int(time - hours * 3600 - minutes * 60)
        return hours, minutes, seconds

    def get_error_message(self, code):
        error_response = ""
        error_messages = {
            400: "Bad Request",
            401: "Unauthorized",
            402: "Payment Required",
            403: "Forbidden",
            404: "Not Found",
            405: "Method Not Allowed",
            406: "Not Acceptable",
            407: "Proxy Authentication Required",
            408: "Request Timeout",
            409: "Conflict",
            410: "Gone",
            411: "Length Required",
            412: "Precondition Failed",
            413: "Request Entity Too Large",
            414: "Request-URI Too Long",
            415: "Unsupported Media Type",
            416: "Requested Range Not Satisfiable",
            417: "Expectation Failed",
            418: "I\'m a teapot",
            419: "Page Expired (Laravel Framework)",
            420: "Method Failure (Spring Framework)",
            421: "Misdirected Request",
            422: "Unprocessable Entity",
            423: "Locked",
            424: "Failed Dependency",
            426: "Upgrade Required",
            428: "Precondition Required",
            429: "Too Many Requests",
            431: "Request Header Fields Too Large",
            440: "Login Time-out",
            444: "Connection Closed Without Response",
            449: "Retry With",
            450: "Blocked by Windows Parental Controls",
            451: "Unavailable For Legal Reasons",
            494: "Request Header Too Large",
            495: "SSL Certificate Error",
            496: "SSL Certificate Required",
            497: "HTTP Request Sent to HTTPS Port",
            498: "Invalid Token (Esri)",
            499: "Client Closed Request",
            500: "Internal Server Error",
            501: "Not Implemented",
            502: "Bad Gateway",
            503: "Service Unavailable",
            504: "Gateway Timeout",
            505: "HTTP Version Not Supported",
            506: "Variant Also Negotiates",
            507: "Insufficient Storage",
            508: "Loop Detected",
            509: "Bandwidth Limit Exceeded",
            510: "Not Extended",
            511: "Network Authentication Required",
            520: "Unknown Error",
            521: "Web Server Is Down",
            522: "Connection Timed Out",
            523: "Origin Is Unreachable",
            524: "A Timeout Occurred",
            525: "SSL Handshake Failed",
            526: "Invalid SSL Certificate",
            527: "Railgun Listener to Origin Error",
            530: "Origin DNS Error",
            598: "Network Read Timeout Error"
        }

        if code in error_messages:
            error_response += error_messages[code]
            return error_response
        else:
            return False

    def get_error_severity(self, code):
        error_response = ""
        error_messages = {
            0: "Emergency",
            1: "Alert",
            2: "Critical",
            3: "Error",
            4: "Warning",
            5: "Notification",
            6: "Informational",
            7: "Debugging"
        }

        if code in error_messages:
            error_response += error_messages[code]
            return error_response
        else:
            return False

    def replace_special_chars(self, value):
        replacements = {
            "'": "''",
            "\t": "",
            "\n": "",
            "\\t": "",
            "\\\t": "",
            "\\n": "",
            "\\\n": "",
            "\r": "", # BUG FIX - Order Examples: 4793006489677
            "\\r": "", # BUG FIX - Order Examples: 4793006489677
            "\\\r": "", # BUG FIX - Order Examples: 4793006489677
            "\\\\": "-", # BUG FIX - Order Examples: 4746625318989, 4775205699661
            '\"\\\\\"': '""',
            '\\\"': ""
        }

        for i in range(1):
            if value is not None:
                try:
                    for old, new in replacements.items():
                        value = value.replace(old, new)
                except Exception as e:
                    pass

        return value

    def send_email(self, email_from, email_to, email_subject, email_body, file_names, file_path):
        print("\n[INFO] BEGIN - Sending Email...")

        print("[INFO] Instantiating Variables...")
        # Variables
        sender_email = self.get_EMAIL_ADDRESS()
        sender_password = self.get_EMAIL_PASSWORD()
        message = MIMEMultipart()
        message["From"] = email_from
        message["To"] = ', '.join(email_to)
        message["Subject"] = email_subject
        message.attach(MIMEText(email_body, "plain"))
        attachment = None
        file_name = None
        part = None
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        server = None

        try:
            print("[INFO] Checking if it has files to be sent...")
            if file_names is not None and file_names != []:
                print("[INFO] Attaching files to be sent...")
                if file_path is not None:
                    for file_name in file_names:
                        attachment = open(file_path + file_name, 'rb')
                        print(f"[INFO] File attached... File Name: {file_name}")
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(attachment.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', f'attachment; filename = {file_name}')
                        message.attach(part)
                else:
                    print("[ERROR] File path is None.")

            print("[INFO] Sending email...")
            try:
                server = smtplib.SMTP(smtp_server, smtp_port)
                server.starttls()
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, email_to, message.as_string())
                print("[INFO] Email Sent...")
            except Exception as e:
                print(f"[ERROR] Error while Sending Email: {e}")
        except Exception as e:
            print(f"[ERROR] Error while Sending Email: {e}")
        finally:
            print("[INFO] Clearing Variables...")
            try:
                del sender_email
                del sender_password
                del message
                del attachment
                del file_name
                del part
                del smtp_server
                del smtp_port
                del server
            except:
                pass
            print("[INFO] END - Finished sending email...")

    def send_exception_email(self, module, function, error, additional_info, start_time, end_time):
        email_subject = f"[{module}] Error Exception - Crash Report"
        email_to = ['xxxx']
        email_from = "xxxx"

        email_body = f'[INFO] Error Exception on Module: {module}, funtion: {function}:'
        email_body += f'\n\n[ERROR DETAILS]'
        email_body += f'\n[INFO] Error Description: {error}'
        email_body += f'\n\n[EXECUTION DETAILS]'

        if start_time != 0 and end_time != 0:
            hours, minutes, seconds = self.get_total_time_hms(start_time=start_time, end_time=end_time)
            email_body += f'\n[INFO] Initial time: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))}'
            email_body += f'\n[INFO] Final time: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))}'
            email_body += f'\n[INFO] Total time taken: {hours} hours, {minutes} minutes, {seconds} seconds'

        email_body += f'\n\n[ADDITIONAL INFORMATION]'
        email_body += f'\n[INFO] Function: {function}'
        email_body += f'\n{additional_info}\n\n' if additional_info is not None else ''

        self.send_email(email_from=email_from, email_to=email_to, email_subject=email_subject, email_body=email_body, file_names=None, file_path=None)

    def convert_date_format(self, date_string, input_format, output_format):
        date = datetime.strptime(date_string, input_format)

        return date.strftime(output_format)

    def get_current_date(self):
        return time.strftime("%Y-%m-%d", time.localtime(time.time()))

    def get_current_date_time(self):
        toronto_tz = pytz.timezone('America/Toronto')
        now = datetime.datetime.now(toronto_tz)
        return now.strftime("%Y-%m-%d %H:%M:%S")

    def get_x_hours_ago(self, hours):
        toronto_tz = pytz.timezone('America/Toronto')
        now = datetime.datetime.now(toronto_tz)
        x_hours_ago = now - timedelta(hours=hours)
        return x_hours_ago.strftime("%Y-%m-%d %H:%M:%S")

    def verify_is_processing(self, orderTags):
        tags = orderTags.split(',')

        is_processing = False

        for tag in tags:
            tagStrip = tag.strip()
            if tagStrip == 'processing_ship_done_canada':
                is_processing = True
                break

        return is_processing, orderTags

    def print_file(self, file_path):
        try:
            if platform.system() == 'Windows':
                os.startfile(file_path, 'print')
            elif platform.system() == 'Darwin':
                subprocess.run(['lp', file_path])
            elif platform.system() == 'Linux':
                subprocess.run(['lp', file_path])
            else:
                print("Unsupported operating system.")
        except Exception as e:
            print(f"Error printing PDF: {e}")

    def read_document(self, text_path):
        try:
            with open(text_path, 'r') as file:
                content = file.read()
        except Exception as e:
            return f"Error printing text: {e}"

        return content

    def get_invalid_adresses(self):
        invalid_address = {}

        print('[INFO] Getting invalid addresses.')
        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
        service = discovery.build('sheets', 'v4', http=httplib2.Http(), discoveryServiceUrl=discoveryUrl, developerKey=self.get_GOOGLE_SHEET_DEV_KEY())

        spreadsheetId = self.get_SPREAD_SHEET_ID()
        rangeName = 'A1:A9999999'
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=rangeName).execute()
        values = result.get('values', [])

        if not values:
            return invalid_address, '[ERROR] No data found in the spreadsheet.'
        else:
            for row in values:
                for cel in row:
                    invalid_address[cel] = True
                    if cel.startswith('#'):
                        output_string = cel[1:]
                        invalid_address[output_string] = True
                    else:
                        invalid_address[('#' + cel)] = True

        return invalid_address, '[DONE] Finished getting invalid addresses.'

    def generate_image_pdf(self, output_file_path, output_file_name, page_width_cm, page_height_cm, image_width_cm, image_height_cm, images_path, image_names, space_between_images_cm):
        print("[INFO] Generating PDF...")

        file_path = os.path.join(output_file_path, output_file_name)
        c = canvas.Canvas(filename=file_path, pagesize=letter, pageCompression=None, invariant=None, verbosity=0, encrypt=None, cropMarks=None, pdfVersion=None, enforceColorSpace=None, initialFontName=None, initialFontSize=None, initialLeading=None, cropBox=None, artBox=None, trimBox=None, bleedBox=None, lang=None)
        c.setPageSize((page_width_cm * 28.3465, page_height_cm * 28.3465))  # Convert centimeters to points (1 cm = 28.3465 points)

        x_start = 0
        y_start = 0
        cumulative_width = 0
        cumulative_height = 0

        for image_name in image_names:
            image_path = os.path.join(images_path, image_name)
            
            if not os.path.exists(image_path):
                raise FileNotFoundError(f"Image file not found: {image_path}")

            if cumulative_width + image_width_cm * 28.3465 > (page_width_cm * 28.3465):
                x_start = 0
                cumulative_width = 0
                
                cumulative_height += image_height_cm * 28.3465 + space_between_images_cm * 28.3465
                y_start = cumulative_height
                
                if y_start + image_height_cm * 28.3465 + space_between_images_cm * 28.3465 > (page_height_cm * 28.3465):
                    c.showPage()
                    y_start = 0
                    cumulative_height = 0
            else:
                x_start = cumulative_width

            c.drawImage(image_path, x_start, y_start, width=image_width_cm * 28.3465, height=image_height_cm * 28.3465, preserveAspectRatio=True)
            cumulative_width += (image_width_cm * 28.3465) + (space_between_images_cm * 28.3465)

        c.save()
        return '[DONE] PDF generated.'

    def generate_phone_cases_pdf(self, batch_number, output_file_path, output_file_name, page_width_cm, page_height_cm, image_width_cm, image_height_cm, phone_cases_array, space_between_images_cm):
        print(f"\n[INFO] BEGIN - Generating Phone Case PDF(s)...")

        print("[INFO] Instantiating Variables...")
        # Variables
        order_name = None
        order_id = None
        item_name = None
        x_start = 20
        y_start = 10
        cumulative_width = 20
        cumulative_height = 10
        inches = 28.3465
        array_counter = 0
        phone_cases_array_length = len(phone_cases_array)

        try:
            print("[INFO] Creating PDF File...")
            print(f"[INFO] PDF Output File: {output_file_path + output_file_name}")
            file_path = os.path.join(output_file_path, output_file_name)
            pdf = canvas.Canvas(filename=file_path, pagesize=letter)
            pdf.setPageSize((page_width_cm * inches, page_height_cm * inches))

            print(f"[INFO] Looping through phone_cases_array... Size: {str(phone_cases_array_length)} items.")
            print(f"[INFO] Drawing Phone Cases on PDF...")
            for phone_case in phone_cases_array:
                array_counter += 1

                image_link = phone_case.get('phone_case_file_path')
                order_name = phone_case.get('order_name')
                order_id = phone_case.get('order_id')
                item_name = phone_case.get('item_name')

                barcode39 = code128.Code128(order_name, barHeight = 1 * inches, barWidth=1.5)
                codes = [barcode39]

                # Download image from the link
                response = requests.get(image_link)
                if response.status_code == 200:
                    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
                        temp_file.write(response.content)
                        temp_file.flush()

                        # Calculate position and size for the image
                        if cumulative_width + image_width_cm * inches > (page_width_cm * inches):
                            x_start = 10
                            cumulative_width = 10
                            cumulative_height += image_height_cm * inches + space_between_images_cm * inches
                            y_start = cumulative_height

                            if y_start + image_height_cm * inches + space_between_images_cm * inches > (page_height_cm * inches):
                                pdf.showPage()
                                y_start = 10
                                cumulative_height = 10
                        else:
                            x_start = cumulative_width

                        # Draw the barcode on the pdf
                        for code in codes:
                            code.drawOn(pdf, x_start + 20, 1 * inches) # Horizontal position, vertical position

                        pdf.setFontSize(10)

                        # Draw the Order Name on the pdf
                        pdf.saveState()
                        pdf.translate(x_start + (8.50 * inches), 0.6 * inches)
                        pdf.scale(-1, 1)
                        pdf.drawString(0, 0, str(order_name))
                        pdf.restoreState()

                        # Draw the Item Name on the pdf
                        pdf.saveState()
                        pdf.translate(x_start + (8.50 * inches), 0.2 * inches)
                        pdf.scale(-1, 1)
                        pdf.drawString(0, 0, str(item_name))
                        pdf.restoreState()

                        if batch_number != None and batch_number != "":
                            # Draw the Item BATCH Number on the pdf
                            pdf.saveState()
                            pdf.translate(x_start + (4 * inches), 0.6 * inches)
                            pdf.scale(-1, 1)
                            pdf.drawString(0, 0, f'BATCH {batch_number}')
                            pdf.restoreState()

                        # Draw the image on the canvas
                        pdf.drawImage(temp_file.name, x_start, y_start + (2.2 * inches), width=image_width_cm * inches, height=image_height_cm * inches, preserveAspectRatio=True)
                        cumulative_width += (image_width_cm * inches) + (space_between_images_cm * inches)
                        print(f"[INFO] {str(array_counter)}...{str(phone_cases_array_length)} Phone Case(s) Drawed... Order: {order_name} - Item: {item_name}")
                else:
                    print(f"Failed to download image from {image_link}")

            print("[INFO] Saving PDF...")
            pdf.save()
        except Exception as e:
            print(f"[ERROR] Error generating PDF: {e}")
            return f"[ERROR] Error generating PDF: {e}"
        finally:
            print("[INFO] Clearing variables...")
            try:
                del file_path
                del order_name
                del order_id
                del item_name
                del x_start
                del y_start
                del cumulative_width
                del cumulative_height
                del inches
                del pdf
            except:
                pass

            print(f"[INFO] END - Phone Case PDF(s) generated.")

    def convert_json_to_object(self, json_string):
        return json.loads(json_string)

    def convert_object_to_json(self, json_string, **kwargs):
        return json.dumps(obj=json_string, ensure_ascii=False)

    def generate_slips(self, orders_array, type, output_file_path, output_file_name, batch_number, page_width_cm, page_height_cm):
        print(f"\n[INFO] BEGIN - Generating Slip PDF(s)...")

        print("[INFO] Instantiating Variables...")
        # Variables
        inches = 28.3465
        file_path = None
        pdf = None
        order_count = 0
        item_count = 0
        type_slip = "PHONE CASE" if type == "phone_case" else "OTHER"
        y_position = 0
        order_number = None
        country_code = None
        order_date = None
        line_items = None
        item_name = None
        customer_name = None
        orders_length = len(orders_array)
        total_items = 0
        current_date = time.strftime("%Y-%m-%d", time.localtime(time.time()))

        try:
            print("[INFO] Creating PDF File...")
            print(f"[INFO] PDF Output File: {output_file_path + output_file_name}")
            file_path = os.path.join(output_file_path, output_file_name)
            pdf = canvas.Canvas(filename=file_path, pagesize=letter)
            pdf.setPageSize((page_width_cm * inches, page_height_cm * inches))

            print(f"[INFO] Looping through orders_array... Size: {str(len(orders_array))} orders.")
            if len(orders_array) > 0:
                for order in orders_array:
                    order_count += 1
                    item_count = 0
                    order_number = order.get('order_name')
                    country_code = order.get('country_code')
                    order_date = order.get('created_at')
                    line_items = order.get('line_items')
                    customer_name = order.get('customer_name')

                    # LOGO
                    pdf.setFont("Helvetica-Bold", 16)
                    pdf.drawString(x=3*inches, y=14*inches, text=f'XXXXX XXXXX')
                    pdf.line(x1=0*inches, y1=13.5*inches, x2=10.16*inches, y2=13.5*inches) # horizontal line

                    # BARCODE and ORDER NUMBER
                    barcode = code128.Code128(order_number, barHeight=60, barWidth=1)
                    codes = [barcode]
                    for code in codes:
                        code.drawOn(pdf, x=0.1*inches, y=11*inches)
                    pdf.drawString(x=1.15*inches, y=10.5*inches, text=f'{order_number}')

                    # LINES
                    pdf.line(x1=5*inches, y1=13.5*inches, x2=5*inches, y2=10*inches) # Vertical line
                    pdf.line(x1=5*inches, y1=12.3*inches, x2=page_width_cm*inches, y2=12.3*inches) # horizontal line
                    pdf.line(x1=5*inches, y1=11.2*inches, x2=page_width_cm*inches, y2=11.2*inches) # horizontal line
                    pdf.line(x1=0*inches, y1=10*inches, x2=10.16*inches, y2=10*inches) # horizontal line

                    pdf.setFont("Helvetica", 10)
                    pdf.drawString(x=5.15*inches, y=13.1*inches, text='COUNTRY CODE:')
                    pdf.drawString(x=5.15*inches, y=11.9*inches, text='SENT ON:')
                    pdf.drawString(x=5.15*inches, y=10.8*inches, text='SLIP NUMBER:')

                    pdf.setFont("Helvetica-Bold", 20)
                    pdf.drawString(x=5.15*inches, y=12.4*inches, text=f'{country_code}')
                    pdf.setFont("Helvetica", 16)
                    pdf.drawString(x=5.15*inches, y=11.3*inches, text=f'{current_date}')
                    pdf.drawString(x=5.15*inches, y=10.2*inches, text=f'#{order_count}')

                    # Batch information
                    pdf.setFont("Helvetica-Bold", 24)
                    pdf.drawString(x=2.2*inches, y=9*inches, text=type_slip.upper())
                    if batch_number != None and batch_number != "":
                        pdf.drawString(x=2.5*inches, y=8*inches, text=f'BATCH #{batch_number}')
                    pdf.line(x1=0*inches, y1=7.6*inches, x2=10.16*inches, y2=7.6*inches)

                    # Order Info
                    pdf.setFont("Helvetica-Bold", 16)
                    pdf.drawString(x=3*inches, y=6.8*inches, text='ORDER INFO:')
                    pdf.setFont("Helvetica-Bold", 12)
                    pdf.drawString(x=1*inches, y=6.2*inches, text=f'CUSTOMER NAME: ')
                    pdf.drawString(x=1.3*inches, y=5.6*inches, text=f'CREATED AT: ')
                    pdf.setFont("Helvetica", 12)
                    pdf.drawString(x=5*inches, y=6.2*inches, text=f'{customer_name}')
                    pdf.drawString(x=4.4*inches, y=5.6*inches, text=f'{order_date}')
                    pdf.line(x1=0*inches, y1=5.3*inches, x2=10.16*inches, y2=5.3*inches)

                    pdf.setFont("Helvetica-Bold", 16)
                    pdf.drawString(x=3.2*inches, y=4.6*inches, text=f'{type_slip.upper() + "S" if len(line_items) > 1 else type_slip.upper()}:')

                    pdf.setFont('Helvetica', 12)
                    y_position = 4.1*inches
                    for item in line_items:
                        total_items += 1
                        item_count += 1
                        item_name = item.get('name')

                        pdf.drawString(x=0.1*inches, y=y_position, text=item_name)
                        y_position = y_position-0.5*inches

                        print(f"[INFO] {str(order_count)}...{str(orders_length)} Order(s) Drawed... Order: {order_number} - Item {item_count} of {len(line_items)}: {item_name}")

                    pdf.showPage()
                print("[INFO] Saving PDF...")
                print(f"[INFO] Total Orders: {order_count} - Total Items: {total_items}")
                pdf.save()
                return True, f"[DONE] PDF generated."
        except Exception as e:
            print(f"[ERROR] Error generating PDF: {e}")
            return False, f"[ERROR] Error generating PDF: {e}"
        finally:
            print("[INFO] Clearing variables...")
            try:
                del inches
                del file_path
                del pdf
                del order_count
                del item_count
                del type_slip
                del y_position
                del order_number
                del country_code
                del order_date
                del item
                del line_items
                del item_name
                del orders_length
                del total_items
                del current_date
            except:
                pass

            print(f"[INFO] END - Slip PDF(s) generated.")

    def load_env_variable(self, VARIABLE_NAME):
        variable = None
        try:
            variable = None if os.getenv(VARIABLE_NAME) == "None" else os.getenv(VARIABLE_NAME)
        except:
            variable = None
        finally:
            return variable

    def get_current_directory(self):
        caller_frame = inspect.stack()[1]
        caller_module = inspect.getmodule(caller_frame[0])
        if caller_module:
            return os.path.dirname(os.path.abspath(caller_module.__file__))
        else:
            return None

    def get_base_directory(self):
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def get_PYTHONPATH(self):
        return self._PYTHONPATH

    def get_SHOPIFY_PRIV_KEY(self):
        return self.SHOPIFY_PRIV_KEY

    def get_SHOPIFY_PUB_KEY(self):
        return self.SHOPIFY_PUB_KEY

    def get_AMAZON_ACCESS_KEY(self):
        return self.AMAZON_ACCESS_KEY

    def get_AMAZON_ACCESS_SECRET(self):
        return self.AMAZON_ACCESS_SECRET

    def get_AMAZON_REGION(self):
        return self.AMAZON_REGION

    def get_AMAZON_BUCKET_NAME(self):
        return self.AMAZON_BUCKET_NAME

    def get_AMAZON_CDN_URL(self):
        return self.AMAZON_CDN_URL

    def get_MACHOOL_PRIV_KEY(self):
        return self.MACHOOL_PRIV_KEY

    def get_MACHOOL_PUB_KEY(self):
        return self.MACHOOL_PUB_KEY

    def get_FARETRADE_API_KEY(self):
        return self.FARETRADE_API_KEY

    def get_CANADA_POST_AUTHORIZATION(self):
        return self.CANADA_POST_AUTHORIZATION

    def get_EMAIL_ADDRESS(self):
        return self.EMAIL_ADDRESS

    def get_EMAIL_PASSWORD(self):
        return self.EMAIL_PASSWORD

    def get_GOOGLE_SHEET_DEV_KEY(self):
        return self.GOOGLE_SHEET_DEV_KEY

    def get_GOOGLE_SHEET_DEV_KEY_2(self):
        return self.GOOGLE_SHEET_DEV_KEY_2

    def get_SPREAD_SHEET_ID(self):
        return self.SPREAD_SHEET_ID

    def get_SPREAD_SHEET_ID_2(self):
        return self.SPREAD_SHEET_ID_2

    def get_columns_array(self):
        return self.columns_array

    def get_values_array(self):
        return self.values_array

    def get_condition(self):
        return self.condition

    def set_condition(self, value):
        self.condition = value

    def clear_condition(self):
        self.condition = ""

    def append_columns_array(self, columns):
        self.columns_array.append(columns)

    def append_values_array(self, values):
        self.values_array.append(values)

    def append_columns_values_array(self, columns, values):
        self.append_columns_array(columns)
        self.append_values_array(values)

    def clear_columns_values_arrays(self):
        self.columns_array = []
        self.values_array = []

    def validate_columns_values(self, table_column, value):
        if value is not None and value != "":
            self.append_columns_values_array(table_column, value)
        else:
            self.append_columns_values_array(table_column, "NULL")

    def get_start_time(self):
        return self.start_time

    def get_end_time(self):
        return self.end_time

    def set_start_time(self, value):
        self.start_time = value

    def set_end_time(self, value):
        self.end_time = value

    def get_REPOSITORY_NAME(self):
        return self.REPOSITORY_NAME

    def get_RUNCOUNTER(self):
        return self.RUNCOUNTER

    def get_MAXRUNS(self):
        return self.MAXRUNS

    def get_FIELDS(self):
        return self.FIELDS

    def get_LIMIT(self):
        return self.LIMIT

    def get_STATUS(self):
        return self.STATUS

    def get_CREATED_AT_MIN(self):
        return self.CREATED_AT_MIN

    def get_CREATED_AT_MAX(self):
        return self.CREATED_AT_MAX

    def get_PROCESSED_AT_MIN(self):
        return self.PROCESSED_AT_MIN

    def get_PROCESSED_AT_MAX(self):
        return self.PROCESSED_AT_MAX

    def get_SINCE_ID(self):
        return self.SINCE_ID

    def get_FULFILLMENT_STATUS(self):
        return self.FULFILLMENT_STATUS

    def get_SHOPIFY_API_VERSION(self):
        return self.SHOPIFY_API_VERSION

    def get_REMOVE_LINE_ITEMS(self):
        return self.REMOVE_LINE_ITEMS

    def get_REMOVE_ORDERS(self):
        return self.REMOVE_ORDERS

    def get_WAIT_TIME(self):
        return self.WAIT_TIME if self.WAIT_TIME is not None else "0"

    def get_PHONE_CASE_LIMIT(self):
        return self.PHONE_CASE_LIMIT

    def get_MENU_OPTION(self):
        return self.MENU_OPTION