import json
import time
import datetime
import random
from awscrt import mqtt
from awsiot import mqtt_connection_builder

from awscrt.mqtt import Connection, Client, QoS

from concurrent.futures import Future
import sys 


ENDPOINT = "a1jc42iddoh7sh-ats.iot.eu-north-1.amazonaws.com"
CLIENT_ID = "MySmartStreetlight"
PATH_TO_CERTIFICATE = "C:\\bulut_projesi_1\\certs\\797c31dc9ba13e31f8b60fabe8f4f34fc3dbd9f18549b075ad39bebe560b14d9-certificate.pem.crt"
PATH_TO_PRIVATE_KEY = "C:\\bulut_projesi_1\\certs\\797c31dc9ba13e31f8b60fabe8f4f34fc3dbd9f18549b075ad39bebe560b14d9-private.pem.key"
PATH_TO_ROOT_CA = "C:\\bulut_projesi_1\\certs\\AmazonRootCA1.pem"
TOPIC = "smartcity/streetlights"

def generate_sensor_data():
    """Simüle edilmiş sensör verisi üretir."""
    status = random.choice(["ON", "OFF"])
    timestamp = datetime.datetime.utcnow().isoformat() + "Z"
    
    data = {
        "deviceId": CLIENT_ID, 
        "status": status,
        "timestamp": timestamp
    }
    return data


def on_connection_interrupted(connection, error, **kwargs):
    print(f"Connection interrupted. error: {error}")


def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print(f"Connection resumed. return_code: {return_code} session_present: {session_present}")
    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to topics...")
        

if __name__ == "__main__":
    print(f"Connecting to {ENDPOINT} with client ID '{CLIENT_ID}'...")

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=ENDPOINT,
        cert_filepath=PATH_TO_CERTIFICATE,
        pri_key_filepath=PATH_TO_PRIVATE_KEY,
        ca_filepath=PATH_TO_ROOT_CA,
        client_id=CLIENT_ID,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        clean_session=False,
        keep_alive_secs=30)

    try:
        connect_future = mqtt_connection.connect()
        connect_future.result() 
        print("Connected to AWS IoT Core!")
    except Exception as e:
        print(f"Failed to connect to AWS IoT Core: {e}")
        sys.exit(1) 

    print("Simüle Edilmiş Cihaz Başlatıldı. Veri AWS IoT Core'a gönderiliyor...")
    print("CTRL+C ile durdurabilirsiniz.")
    
    try:
        publish_count = 0
        while True:
            sensor_data = generate_sensor_data()
            message_json = json.dumps(sensor_data)
            print(f"Publishing message to topic '{TOPIC}': {message_json}")
            
            
            publish_result = mqtt_connection.publish(
                topic=TOPIC,
                payload=message_json,
                qos=mqtt.QoS.AT_LEAST_ONCE) 
            
            
            if isinstance(publish_result, tuple):
                publish_future = publish_result[0]
            else:
                publish_future = publish_result
            publish_future.result()
            publish_count += 1
            print(f"Message {publish_count} published successfully.")
            
            time.sleep(5) 
    except KeyboardInterrupt:
        print("Simülasyon durduruldu. Bağlantı kesiliyor...")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if mqtt_connection:
            print("Disconnecting from AWS IoT Core...")
            disconnect_future = mqtt_connection.disconnect()
            disconnect_future.result()
            print("Disconnected.") 