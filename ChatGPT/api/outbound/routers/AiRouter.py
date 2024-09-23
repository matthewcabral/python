from api.inbound.OpenAIController import OpenAIController
from utils.UtilsController import *

class AiRouter:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(AiRouter, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):  # Ensure __init__ is only called once
            self._initialized = True
            self.chatgpt = OpenAIController()
            self.utils = UtilsController()
            self.router = APIRouter()
            self.setup_routes()

    def setup_routes(self):
        @self.router.post('/chat', status_code=200)
        async def chat(
            model,
            request_data : dict,
            temperature=1,
            max_completion_tokens=100,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
            response_format={"type": "text"},
            response: Response = {}
        ):
            try:
                if request_data is None:
                    response.status_code = 400
                    raise HTTPException(status_code=400, detail='Empty JSON.')
                else:
                    # request = self.utils.convert_json_to_object(request_data)
                    messages = request_data.get("messages")

                    return_flag, response_description, response_message, response_dict = self.chatgpt.post_chat_completitions(
                        model=model,
                        messages=messages,
                        temperature=int(temperature),
                        max_completion_tokens=int(max_completion_tokens),
                        top_p=int(top_p),
                        frequency_penalty=int(frequency_penalty),
                        presence_penalty=int(presence_penalty),
                        response_format=response_format
                    )

                    if not return_flag:
                        raise HTTPException(status_code=500, detail=response)
                    else:
                        response.status_code = 200
                        return response_message
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get('/available_models', status_code=200)
        async def get_available_models(response: Response):
            try:
                return {
                    "models": self.chatgpt.get_available_models()
                }
            except HTTPException as http_exc:
                raise http_exc
            except Exception as e:
                response.status_code = 500
                raise HTTPException(status_code=500, detail=str(e))