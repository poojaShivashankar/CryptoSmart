import os
import platform
import time
import threading
import requests
import json

from solace.messaging.messaging_service import MessagingService, RetryStrategy
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher
from solace.messaging.resources.topic import Topic

if platform.uname().system == 'Windows':
    os.environ["PYTHONUNBUFFERED"] = "1"

lock = threading.Lock()

broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOLACE_HOST') or "tcp://localhost:55555",
    "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.username": os.environ.get('SOLACE_USERNAME') or "admin",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOLACE_PASSWORD') or "admin"
}

messaging_service = MessagingService.builder().from_properties(broker_props)\
    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3))\
    .build()

messaging_service.connect()
print(f'Messaging Service connected? {messaging_service.is_connected}')

publisher: PersistentMessagePublisher = messaging_service.create_persistent_message_publisher_builder().build()
publisher.start()

btc_topic = Topic.of("crypto/bitcoin")
eth_topic = Topic.of("crypto/ethereum")

msg_builder = messaging_service.message_builder() \
    .with_property("application", "crypto_publisher") \
    .with_property("language", "Python")

count = 0

try:
    while True:
        btc_res = requests.get('https://api.coinbase.com/v2/prices/BTC-USD/spot')
        eth_res = requests.get('https://api.coinbase.com/v2/prices/ETH-USD/spot')

        if btc_res.status_code == 200 and eth_res.status_code == 200:
            btc_price = btc_res.json()["data"]
            eth_price = eth_res.json()["data"]

            btc_msg = msg_builder.with_application_message_id(f'btc_{count}').build(json.dumps(btc_price))
            eth_msg = msg_builder.with_application_message_id(f'eth_{count}').build(json.dumps(eth_price))

            publisher.publish(btc_msg, btc_topic)
            publisher.publish(eth_msg, eth_topic)

            print(f'Published BTC: {btc_price["amount"]} | ETH: {eth_price["amount"]}')
            count += 1
        else:
            print("Failed to fetch prices")

        time.sleep(10)

except KeyboardInterrupt:
    print('\nTerminating Publisher...')
    publisher.terminate()
    messaging_service.disconnect()
