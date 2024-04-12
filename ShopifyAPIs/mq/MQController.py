from .Controller import *

class MQController(Controller):
    def __init__(self):
        super().__init__()

    async def open_mq_connection(self, virtual_host) -> aio_pika.Connection:
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(cadata=super().get_certificate())
        connection = await aio_pika.connect_robust(f"{super().get_url()}/{virtual_host}", ssl_context=ssl_context)

        return connection

    async def close_mq_connection(self, connection: aio_pika.Connection) -> None:
        async with connection:
            await connection.close()

    async def queue_message(self, virtual_host, queue_name, message_to_queue) -> None:
        connection = await self.open_mq_connection(virtual_host=virtual_host)

        try:
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=10)
                queue = await channel.declare_queue(queue_name, auto_delete=True)
                await channel.default_exchange.publish(
                    aio_pika.Message(body=message_to_queue),
                    routing_key=queue_name,
                    mandatory=True
                )
            return True, "Success", 200
        except aio_pika.exceptions.AMQPError as e:
            return False, f"An AMQP error occurred: {e}", 500
        except Exception as e:
            return False, f"An error occurred: {e}", 500
        finally:
            await self.close_mq_connection(connection)

    async def get_message(self, virtual_host, queue_name):
        connection = await self.open_mq_connection(virtual_host=virtual_host)

        try:
            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(queue_name, auto_delete=True)

                async def wait_for_message():
                    try:
                        message = await queue.get()
                        if message:
                            return message
                        else:
                            raise aio_pika.exceptions.QueueEmpty
                    except aio_pika.exceptions.QueueEmpty:
                        return "QueueEmpty"
                    except Exception as e:
                        return None

                try:
                    message = await asyncio.wait_for(wait_for_message(), timeout=super().get_timeout())
                except Exception as e:
                    return False, f"An error occurred: {e}", 500
                if message is not None and message != "QueueEmpty":
                    message_body = message.body.decode()
                    await message.ack()

                    return True, message_body, 200
                else:
                    return False, "No message received within the timeout period", 404
        except asyncio.TimeoutError:
            return False, "No message received within the timeout period", 404
        except aio_pika.exceptions.AMQPError as e:
            return False, f"An AMQP error occurred: {e}", 500
        except Exception as e:
            return False, f"An error occurred: {e}", 500
        finally:
            await self.close_mq_connection(connection)
