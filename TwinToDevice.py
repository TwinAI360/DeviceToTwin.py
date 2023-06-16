import RPi.GPIO as GPIO
import time
import json
from azure.storage.blob import BlobServiceClient
from DCMotor import *


#Blob storage connection
connection_string='DefaultEndpointsProtocol=https;AccountName=rgdigitaltwin9fdf;AccountKey=01vFhg8ijBUjs0Piv6bbDlQnRuMoNd6iwfrJdNsAbVLebYEiJ0ck7rJFhGc922hX1Rtv9KHXf7wy+AStsXculQ==;EndpointSuffix=core.windows.net'

def storage_client_init():
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("adttwindatablob")
    blob_client = container_client.get_blob_client("Dht11TwinData.json")
    return blob_client
    
def iothub_twin_to_device_run():
    fan = DCMotor()
    blob_client = storage_client_init()
    coolingsystemactivated = False
    while True:
        #Read JSON file from blob
        dht11data = json.loads(blob_client.download_blob().readall().decode("utf-8")) # read blob content as string
        print("Receiving the message: {}".format(dht11data))
        
        #If ActivateCoolingSystem is true then blink led
        if dht11data["ActivateCoolingSystem"]:
            
            if not coolingsystemactivated:
                print("Activating Cooling System...")
                fan.start()
                coolingsystemactivated = True
                print("Cooling System Activated.")
        else:
            if coolingsystemactivated:
                print("De-Activating Cooling System...")
                fan.stop()
                coolingsystemactivated = False
                print("Cooling System De-Activated.")
        time.sleep(1)

if __name__ == '__main__':
    print("IoT Hub Quickstart #1 - Simulated device")
    print("Press Ctrl-C to exit")
    iothub_twin_to_device_run()

