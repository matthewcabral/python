from openai import OpenAI
from utils.UtilsController import UtilsController

class OpenAIController:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(OpenAIController, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):  # Ensure __init__ is only called once
            self._initialized = True 
            self.utils = UtilsController()
            self.__module_name = "OpenAIController"
            self.__OPENAI_API_KEY = self.utils.load_env_variable('OPENAI_API_KEY')
            self.__OPENAI_ORGANIZATION = self.utils.load_env_variable('OPENAI_ORGANIZATION')
            self.__OPENAI_PROJECT = self.utils.load_env_variable('OPENAI_PROJECT')
            self.client = OpenAI(
                api_key = self.__OPENAI_API_KEY,
                organization=self.__OPENAI_ORGANIZATION,
                project=self.__OPENAI_PROJECT,
            )
            self.__roles = {
                "sys": "system",
                "usr": "user",
                "asst": "assistant",
                "tol": "tool"
            }
            self.__response_format = {
                "txt": "text",
                "json": "json_object",
                "sch": "json_schema"
            }
            self.__models = {
                "3.5": {
                    "turbo": "gpt-3.5-turbo"
                },
                "4": {
                    "mini-o": "gpt-4o-mini",
                    "normal": "gpt-4",
                    "turbo": "gpt-4-turbo",
                    "o": "gpt-4o"
                }
            }

    def get_module_name(self):
        return self.__module_name

    def get_role(self, role_abbreviation="usr") -> str:
        """
        Retrieve the full name of a role based on its abbreviation.

        Args:
            role_abbreviation (str): The abbreviation of the role to look up. 
                                    Defaults to "usr" (user).

        Returns:
            str: The full name of the role (e.g., "system", "user", "assistant", "tool").
        
        Example:
            >>> get_role("asst")
            'assistant'
        """
        return self.__roles.get(role_abbreviation)

    def get_response_format(self, response_format="txt") -> object:
        """
        Retrieve the response format type based on its abbreviation.

        Args:
            response_format (str): The abbreviation of the response format. 
                                Defaults to "txt" (text).

        Returns:
            object: A dictionary with the format type (e.g., {"type": "text"}).

        Example:
            >>> get_response_format("json")
            {'type': 'json_object'}
        """
        return {
            "type": self.__response_format.get(response_format)
        }

    def get_model(self, number="4", version="mini-o") -> str:
        """
        Retrieve the full model name based on its version number and type.

        Args:
            number (str): The model version number ("3.5" or "4"). Defaults to "4".
            version (str): The specific version type of the model. 
                        Defaults to "mini-o" for GPT-4o-mini.

        Returns:
            str: The full model name (e.g., "gpt-4o-mini", "gpt-3.5-turbo").

        Example:
            >>> get_model("4", "turbo")
            'gpt-4-turbo'
        """
        return self.__models.get(number).get(version)

    def post_chat_completitions(self, model, messages, temperature=1, max_completion_tokens=0, top_p=1, frequency_penalty=0, presence_penalty=0, response_format={"type": "text"}) -> tuple[bool, str, object]:
        """
        Sends a request to the OpenAI API to create a chat completion using the specified parameters.

        Args:
            model (str): The model to be used for generating the completion (e.g., "gpt-4", "gpt-3.5-turbo").
            messages (list): A list of message objects representing the conversation history. Each message is a dictionary with a "role" (system, user, assistant) and "content".
            temperature (float, optional): Sampling temperature to control randomness. Defaults to 1.
            max_completion_tokens (int, optional): The maximum number of tokens to generate. Defaults to 0 (unlimited).
            top_p (float, optional): Controls diversity via nucleus sampling. Defaults to 1 (no nucleus sampling).
            frequency_penalty (float, optional): Penalizes new tokens based on their frequency in the generated text. Defaults to 0.
            presence_penalty (float, optional): Penalizes new tokens based on whether they appear in the text so far. Defaults to 0.
            response_format (dict, optional): Specifies the format of the response. Defaults to {"type": "text"}.

        Returns:
            tuple: A tuple containing:
                - bool: `True` if the request was successful, otherwise `False`.
                - str: A success or error message.
                - object: The response from the OpenAI API as a JSON object, or an empty object in case of failure.
        
        Raises:
            Exception: If an error occurs while making the API request, the exception is caught, and its message is returned.

        Example:
            >>> post_chat_completitions(
                    model="gpt-4",
                    messages=[{"role": "user", "content": "Hello"}]
                )
            (True, "Success", {"id": "chatcmpl-...", "choices": [...], ...})
        """
        return_flag = False
        response = "Success"
        response_message = {}
        response_dict = None

        if messages == [] or messages is None:
            return return_flag, "A list of messages is required. Message cannot be null.", response_message
        if model is None and model == "":
            return return_flag, "Model is required.", response_message

        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_completion_tokens=max_completion_tokens,
                top_p=top_p,
                frequency_penalty=frequency_penalty,
                presence_penalty=presence_penalty,
                response_format=response_format
            )

            if response:
                return_flag = True
                print(response)
                response_message = response.choices[0].message.to_dict()
                response_dict = response.to_dict()

            return return_flag, response, response_message, response_dict
        except Exception as e:
            response = str(e)
            return return_flag, response, response_message, response_dict
        finally:
            try:
                del return_flag, response, response_message, response_dict
            except Exception as e:
                print(str(e))
                pass

    def get_available_models(
        self
    ):
        return self.__models
