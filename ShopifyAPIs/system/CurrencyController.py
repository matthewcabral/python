
from utils.UtilsController import *
from utils.LogsController import *
from database.DataController import *

class CurrencyController(DataController):
    def __init__(self):
        super().__init__()

    def get_rates(self):
        print(f"\n[INFO] BEGIN - Getting all currencies...")
        columns = ["RATES"]
        condition = "1=1"
        condition += f"\nAND DATE(CREATED) = '{self.utils.get_current_date()}'"
        return_code = 200
        rates = None

        try:
            result_flag, result_query = super().query_record(super().get_tbl_CURRENCIES(), columns, condition)

            if result_flag:
                for row in result_query:
                    rates = row.get("RATES")
                return True, return_code, self.utils.convert_json_to_object(rates)
            else:
                return False, 500, rates
        except Exception as e:
            print(f"[ERROR] {e}")
            return False, 500, str(e)
        finally:
            print(f"[INFO] END - Finished getting all custom currencies")
            print("[INFO] Cleaning up variables")
            try:
                del columns, condition, return_code, rates
            except:
                pass