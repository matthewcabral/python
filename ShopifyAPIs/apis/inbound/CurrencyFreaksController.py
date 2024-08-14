"""
Module: CurrencyFreaksController

This module provides a controller class `CurrencyFreaksController` that interacts with the 
CurrencyFreaks API to fetch the latest currency exchange rates. It utilizes the `UtilsController` 
for utility functions, such as retrieving API keys and handling errors, and the `LogsController` for
logging operations.

Classes:
    - CurrencyFreaksController: A controller class for managing interactions with the 
      CurrencyFreaks API.

Methods:
    - __init__: Initializes the controller with utility and logging instances.
    - get_module_name: Returns the name of the module.
    - get_currencyfreaks_lastest_rates: Fetches the latest currency rates from the CurrencyFreaks 
      API, handles potential errors, and returns the results along with a status flag and 
      descriptive messages.

Exceptions:
    - The module handles exceptions that may occur during API requests, including HTTP errors and 
      other unexpected issues, by logging the errors and returning appropriate error codes and 
      messages.

Usage:
    The `CurrencyFreaksController` is designed to be used within a broader application where 
    currency rates are needed. The `get_currencyfreaks_lastest_rates` method can be called to 
    retrieve and process the most recent exchange rates from the CurrencyFreaks service.
"""
from utils.UtilsController import *
from utils.LogsController import *

class CurrencyFreaksController:
    def __init__(self):
        self.utils = UtilsController()
        self.log = LogsController()
        self.module_name = "CurrencyFreaksController"

    def get_module_name(self):
        return self.module_name

    def get_currencyfreaks_lastest_rates(self):
        # Variables
        response_description = "Success"
        error_code = 200
        error_message = None
        base_currency = None
        date_rates = None
        rates = None
        res = None
        endpoint = f'https://api.currencyfreaks.com/v2.0/rates/latest?apikey={self.utils.get_CURRENCYFREAKS_API_KEY()}'

        try:
            res = requests.get(endpoint)
            res.raise_for_status()
            res = res.json()
            base_currency = res.get('base')
            date_rates = res.get('date')
            rates = res.get('rates')

            return True, base_currency, date_rates, rates, response_description, error_code, error_message
        except Exception as e:
            try:
                code = res.status_code
            except:
                code = 500
            response_description = self.utils.get_error_message(code)
            error_code = code
            error_message = str(e)

            return False, base_currency, date_rates, rates, response_description, error_code, error_message
        finally:
            print("[INFO] Cleaning up variables")
            try:
                del response_description
                del error_code
                del error_message
                del base_currency
                del date_rates
                del rates
                del endpoint
                del res
            except:
                pass
