from api.inbound.OpenAIController import OpenAIController
from utils.UtilsController import UtilsController

class Main:
    def __init__(self):
        self.utils = UtilsController()
        self.chatgpt = OpenAIController()
        self.__main__()

    def __main__(self):
        self.utils.set_start_time(self.utils.get_current_date_time())
        
        # print(self.chatgpt.get_role("sys"))
        # print(self.chatgpt.get_model("4", "mini-o"))

        print(self.chatgpt.post_chat_completitions(
            model=self.chatgpt.get_model("4", "mini-o"),
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Write a short email message to a gay friend wishing a bappy birthday."}
            ],
            temperature=1,
            max_completion_tokens=10,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
            response_format=self.chatgpt.get_response_format("txt")
        ))

        self.utils.set_end_time(self.utils.get_current_date_time())
        hours, minutes, seconds = self.utils.get_total_time_hms(start_time=self.utils.get_start_time(), end_time=self.utils.get_end_time())
        print(f"\n[INFO] Total time taken: {hours} hours, {minutes} minutes, {seconds} seconds\n\n")

if __name__ == "__main__":
    main = Main()