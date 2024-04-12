from order.OrderController import *
from utils.UtilsController import *
from utils.LogsController import *
from apis.inbound.CanadaPostController import *
from database.DataController import *

class Main:
    def __init__(self):
        load_dotenv()
        self.order = OrderController()
        self.utils = UtilsController()
        self.log = LogsController()
        self.db = DataController()
        self.repository_name = self.utils.get_REPOSITORY_NAME()
        self.__main__()

    def __main__(self):
        self.utils.set_start_time(time.time())

        self.order.process_phone_case_orders(limit=10)
        if self.repository_name == "order-webhooks":
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
        elif self.repository_name == "upsert-orders":
            self.order.process_orders_from_staging_table(
                limit=self.utils.get_LIMIT(),
                remove_orders=self.utils.get_REMOVE_ORDERS()
            )
        else:
            print("[ERROR] Invalid repository name")

        self.utils.set_end_time(time.time())
        hours, minutes, seconds = self.utils.get_total_time_hms(start_time=self.utils.get_start_time(), end_time=self.utils.get_end_time())
        print(f"[INFO] Total time taken: {hours} hours, {minutes} minutes, {seconds} seconds\n\n")

    async def async_process_orders_from_mq(self):
        await self.order.process_orders_from_mq()

if __name__ == "__main__":
    main = Main()
