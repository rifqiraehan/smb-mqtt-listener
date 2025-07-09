import paho.mqtt.client as mqtt
import requests
import json
import os
import sys
from dotenv import load_dotenv
import ssl

load_dotenv()

MQTT_BROKER = os.getenv('MQTT_HOST')
MQTT_PORT = int(os.getenv('MQTT_PORT', 8883))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
MQTT_TOPIC = os.getenv('MQTT_TRANSACTIONS_TOPIC', "bayuraksaka/smartmedbox/transactions")
LARAVEL_SYNC_URL = os.getenv('LARAVEL_SYNC_URL')

if not MQTT_BROKER:
    print("Error: MQTT_HOST environment variable is not set.", file=sys.stderr)
    sys.exit(1)
if not LARAVEL_SYNC_URL:
    print("Error: LARAVEL_SYNC_URL environment variable is not set.", file=sys.stderr)
    sys.exit(1)

def on_connect(client, userdata, flags, rc):
    """
    Callback function that runs when the client connects to the MQTT broker.
    """
    if rc == 0:
        print(f"[{os.getpid()}] Connected to MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC)
        print(f"[{os.getpid()}] Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"[{os.getpid()}] Failed to connect, return code {rc}", file=sys.stderr)
        if rc == 1: print("Connection refused - incorrect protocol version")
        elif rc == 2: print("Connection refused - invalid client identifier")
        elif rc == 3: print("Connection refused - server unavailable")
        elif rc == 4: print("Connection refused - bad username or password")
        elif rc == 5: print("Connection refused - not authorized")
        sys.exit(1)

def on_message(client, userdata, msg):
    """
    Callback function that runs when a message is received on the subscribed topic.
    """
    print(f"[{os.getpid()}] Received message on topic {msg.topic}: {msg.payload.decode()}")
    try:
        data = json.loads(msg.payload.decode())
        print(f"[{os.getpid()}] Parsed data: {data}")

        response = requests.post(LARAVEL_SYNC_URL, json=data, timeout=10)
        response.raise_for_status()
        print(f"[{os.getpid()}] Successfully sent data to Laravel: {response.json()}")

    except json.JSONDecodeError:
        print(f"[{os.getpid()}] Error decoding JSON from MQTT message: {msg.payload.decode()}", file=sys.stderr)
    except requests.exceptions.Timeout:
        print(f"[{os.getpid()}] Timeout while sending data to Laravel at {LARAVEL_SYNC_URL}", file=sys.stderr)
    except requests.exceptions.RequestException as e:
        print(f"[{os.getpid()}] Error sending data to Laravel via HTTP: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(f"[{os.getpid()}] Laravel response status: {e.response.status_code}", file=sys.stderr)
            print(f"[{os.getpid()}] Laravel response body: {e.response.text}", file=sys.stderr)
    except Exception as e:
        print(f"[{os.getpid()}] An unexpected error occurred: {e}", file=sys.stderr)

if __name__ == "__main__":
    print(f"[{os.getpid()}] Starting MQTT Listener...")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)

    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    try:

        client.tls_set(tls_version=ssl.PROTOCOL_TLS)

        print(f"[{os.getpid()}] TLS configured for MQTT connection.")
    except Exception as e:
        print(f"[{os.getpid()}] Error configuring TLS: {e}", file=sys.stderr)
        sys.exit(1)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        print(f"[{os.getpid()}] Attempting to connect to {MQTT_BROKER}:{MQTT_PORT}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        print(f"[{os.getpid()}] MQTT listener stopped by user (Ctrl+C).")
    except Exception as e:

        print(f"[{os.getpid()}] Fatal MQTT connection error during connect() call: {e}", file=sys.stderr)
        sys.exit(1)