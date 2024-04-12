import asyncio
import ssl
import aio_pika
from utils.UtilsController import *

class Controller:
    def __init__(self):
        self.utils = UtilsController()
        self.user = self.utils.load_env_variable("MQ_USER")
        self.password = self.utils.load_env_variable("MQ_PASSWORD")
        self.port = self.utils.load_env_variable("MQ_PORT")
        self.domain = self.utils.load_env_variable("MQ_DOMAIN")
        self.certificate = self.utils.load_env_variable("MQ_CERTIFICATE")
        self.timeout = self.utils.load_env_variable("MQ_TIMEOUT") # Default 10 seconds on get_timeout function

    def get_user(self):
        return self.user

    def get_password(self):
        return self.password

    def get_port(self):
        return self.port

    def get_domain(self):
        return self.domain

    def get_url(self):
        return f"amqps://{self.get_user()}:{self.get_password()}@{self.get_domain()}:{self.get_port()}"

    def get_certificate(self):
        return self.certificate

    def get_timeout(self):
        return int(self.timeout) if self.timeout is not None else 10