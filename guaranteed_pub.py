# Guaranteed Publisher publishing persistent crypto prices using Coinbase API and Solace PubSub+

import os
import platform
import time
import threading
import requests
import json

from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher, MessagePublishReceiptListener
from solace.messaging.resources.topic import Topic

if platform.uname().system == 'Windows':
    os.environ["PYTHONUNBUFFERED"] = "1"  # Disable stdout buffer

lock = threading.Lock()

# === Constants ===
TOPIC_NAME = "crypto/prices"

# === Event Handling Classes ===
class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_reconnecting(self, e: "ServiceEvent"):
        print("\non_reconnecting")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_service_interrupted(self, e: "ServiceEvent"):
        print("\non_service_interrupted")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

class MessageReceiptListener(MessagePublishReceiptListener):
    def __init__(self):
        self._receipt_count = 0

    @property
    def receipt_count(self):
        return self._receipt_count

    def on_publish_receipt(self, publish_receipt: 'PublishReceipt'):
        with lock:
            self._receipt_count += 1
            print(f"\nPublish Receipt #: {self.receipt_count}")

# === Solace Broker Configuration ===
broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOLACE_HOST') or "tcp://localhost:55555",
    "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.username": os.environ.get('SOLACE_USERNAME') or "admin",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOLACE_PASSWORD') or "admin"
}

# === Build Messaging Service ===
messaging_service = MessagingService.builder().from_properties(broker_props)\
    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3))\
    .build()
messaging_service.connect()
print(f'Messaging Service connected? {messaging_service.is_connected}')

# Attach Event Listeners
service_handler = ServiceEventHandler()
messaging_service.add_reconnection_listener(service_handler)
messaging_service.add_reconnection_attempt_listener(service_handler)
messaging_service.add_service_interruption_listener(service_handler)

# === Publisher Setup ===
publisher: PersistentMessagePublisher = messaging_service.create_persistent_message_publisher_builder().build()
publisher.start()

# Delivery Receipt Listener
receipt_listener = MessageReceiptListener()
publisher.set_message_publish_receipt_listener(receipt_listener)

# Topic Destination
topic = Topic.of(TOPIC_NAME)

# Message Builder
outbound_msg_builder = messaging_service.message_builder() \
    .with_property("application", "crypto_publisher") \
    .with_property("language", "Python")

# === Fetch from Coinbase API ===
def fetch_coinbase_prices():
    cryptos = ['BTC', 'ETH']
    prices = {}
    for crypto in cryptos:
        url = f'https://api.coinbase.com/v2/prices/{crypto}-USD/spot'
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                prices[crypto] = float(data['data']['amount'])
            else:
                print(f"Failed to fetch {crypto} price. Status: {response.status_code}")
        except Exception as e:
            print(f"Error fetching {crypto} price: {e}")
    return prices

# === Publishing Loop ===
count = 0
try:
    while True:
        prices = fetch_coinbase_prices()
        if prices:
            message_body = json.dumps(prices)
            outbound_msg = outbound_msg_builder \
                .with_application_message_id(f'crypto_update_{count}') \
                .build(message_body)

            publisher.publish(outbound_msg, topic)
            print(f'Published crypto prices: {message_body} → Topic: [{topic.get_name()}]')
            count += 1

        time.sleep(10)

except KeyboardInterrupt:
    print(f'\nDelivery receipt count: {receipt_listener.receipt_count}')
    print('\nTerminating Publisher...')
    publisher.terminate()
    print('\nDisconnecting Messaging Service...')
    messaging_service.disconnect()
