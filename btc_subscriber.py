import os
import platform
import time

from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, ServiceEvent
from solace.messaging.resources.queue import Queue
from solace.messaging.config.retry_strategy import RetryStrategy
from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.config.missing_resources_creation_configuration import MissingResourcesCreationStrategy

if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1"

# Handle received messages
class MessageHandlerImpl(MessageHandler):
    def __init__(self, persistent_receiver: PersistentMessageReceiver):
        self.receiver: PersistentMessageReceiver = persistent_receiver

    def on_message(self, message: InboundMessage):
        payload = message.get_payload_as_string() or message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            payload = payload.decode()
        topic = message.get_destination_name()
        print(f"\n[BTC] Received message on: {topic}")
        print(f"[BTC] Payload: {payload}\n")
        self.receiver.ack(message)

# Error handling classes
class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent): print("\non_reconnected", e.get_message())
    def on_reconnecting(self, e: "ServiceEvent"): print("\non_reconnecting", e.get_message())
    def on_service_interrupted(self, e: "ServiceEvent"): print("\non_service_interrupted", e.get_message())

broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOLACE_HOST') or "tcp://localhost:55555",
    "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.username": os.environ.get('SOLACE_USERNAME') or "admin",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOLACE_PASSWORD') or "admin"
}

messaging_service = MessagingService.builder().from_properties(broker_props)\
                    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                    .build()

messaging_service.connect()
print(f'BTC Subscriber connected? {messaging_service.is_connected}')

service_handler = ServiceEventHandler()
messaging_service.add_reconnection_listener(service_handler)
messaging_service.add_reconnection_attempt_listener(service_handler)
messaging_service.add_service_interruption_listener(service_handler)

queue_name = "BTC-Queue"
durable_exclusive_queue = Queue.durable_exclusive_queue(queue_name)

try:
    persistent_receiver = messaging_service.create_persistent_message_receiver_builder()\
        .with_missing_resources_creation_strategy(MissingResourcesCreationStrategy.CREATE_ON_START)\
        .build(durable_exclusive_queue)
    persistent_receiver.start()
    persistent_receiver.receive_async(MessageHandlerImpl(persistent_receiver))
    print(f'[BTC Subscriber] Bound to Queue [{durable_exclusive_queue.get_name()}]')

    while True:
        time.sleep(1)

except PubSubPlusClientError:
    print(f'\nMake sure queue {queue_name} exists with topic subscription: crypto/bitcoin')

finally:
    if persistent_receiver and persistent_receiver.is_running():
        persistent_receiver.terminate(grace_period=0)
    messaging_service.disconnect()
