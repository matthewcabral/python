from order.OrderController import *
from utils.UtilsController import *
from utils.LogsController import *
from apis.inbound.CanadaPostController import *
from product.PhoneCasesController import *
from product.CustomsProductController import *
from database.DataController import *
from routines.RoutinesController import *
from workers.OrderProcessingController import *
from apis.inbound.ShopifyController import *


class Main:
    def __init__(self):
        sys.dont_write_bytecode = True
        load_dotenv()
        self.order = OrderController()
        self.utils = UtilsController()
        self.log = LogsController()
        self.phone = PhoneCasesController()
        self.customs = CustomsProductController()
        self.db = DataController()
        self.sp = ShopifyController()
        self.opc = OrderProcessingController()
        self.routines = RoutineController()
        self.repository_name = self.utils.get_REPOSITORY_NAME()
        self.routine_to_run = str(sys.argv[1]) if len(sys.argv) > 1 else "NULL"
        self.__main__()

    def __main__(self):
        self.utils.set_start_time(self.utils.get_current_date_time())

        if self.repository_name == "company_name-order-webhooks":
            asyncio.run(self.async_process_orders_from_mq())
        elif self.repository_name == "shopify-get-all-orders":
            self.order.get_all_shopify_orders(
                runCounter=int(self.utils.get_RUNCOUNTER()),
                maxRuns=int(self.utils.get_MAXRUNS()),
                fields=self.utils.get_FIELDS(),
                limit=self.utils.get_LIMIT(),
                status=self.utils.get_STATUS(),
                created_at_min=self.utils.get_CREATED_AT_MIN(),
                created_at_max=self.utils.get_CREATED_AT_MAX(),
                processed_at_min=self.utils.get_PROCESSED_AT_MIN(),
                processed_at_max=self.utils.get_PROCESSED_AT_MAX(),
                since_id=self.utils.get_SINCE_ID(),
                fulfillment_status=self.utils.get_FULFILLMENT_STATUS(),
                api_version=self.utils.get_SHOPIFY_API_VERSION()
            )
        elif self.repository_name == "company_name-upsert-orders":
            self.order.process_orders_from_staging_table(
                limit=self.utils.get_LIMIT(),
                remove_orders=self.utils.get_REMOVE_ORDERS()
            )
        elif self.repository_name == "company_name-order-processing":
            self.opc.process_orders()
        elif self.repository_name == "company_name-order-routines":
            if self.routine_to_run == "process_custom_orders":
                self.routines.process_custom_orders()
            if self.routine_to_run == "update_reports":
                self.routines.update_reports()
            else:
                print("[ERROR] Invalid routine name")
        else:
            print("[ERROR] Invalid repository name")

        self.utils.set_end_time(self.utils.get_current_date_time())
        hours, minutes, seconds = self.utils.get_total_time_hms(start_time=self.utils.get_start_time(), end_time=self.utils.get_end_time())
        print(f"\n[INFO] Total time taken: {hours} hours, {minutes} minutes, {seconds} seconds\n\n")

    async def async_process_orders_from_mq(self):
        await self.order.process_orders_from_mq()

if __name__ == "__main__":
    main = Main()

