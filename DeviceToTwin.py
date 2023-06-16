import random
import time
import pandas as pd
from azure.iot.device import IoTHubDeviceClient, Message
from azure.storage.blob import BlobServiceClient
import json
import requests

#Blob storage connection
connection_string="DefaultEndpointsProtocol=https;AccountName=twinaistorage;AccountKey=ET1kVgu4GAUmImCxf9bzv6Zr+akG501gynzapvOsvyfqm/O9rB2NVZ0vCW91wpO9XpGb3H3kYBua+AStpBTILw==;EndpointSuffix=core.windows.net"

CONNECTION_STRING_DHT11 = "HostName=adt-twinai360-iothub.azure-devices.net;DeviceId=DHT11Sensor;SharedAccessKey=Cb12fZdcMelFTCWAsJ76RKU8QFb7VNrOsrx/swPIUAk="
CONNECTION_STRING_BIOREACTOR = "HostName=adt-twinai360-iothub.azure-devices.net;DeviceId=BioReactor;SharedAccessKey=4nafOYNIp0Iy85AliNgEMqJ2eGxwzNDl5E/1wT8fPxA="
CONNECTION_STRING_dOSensor = "HostName=adt-twinai360-iothub.azure-devices.net;DeviceId=dOSensor;SharedAccessKey=/HDBa6gdyeY/EC+gy7+5nae3J7QFAzJZY/qitqmqbVc="
CONNECTION_STRING_pHSensor = "HostName=adt-twinai360-iothub.azure-devices.net;DeviceId=pHSensor;SharedAccessKey=lg9k1a2PKLsyvi0qACpojopN7BaQY3sv0E4Wz7qlDjc="
CONNECTION_STRING_UVSensor = "HostName=adt-twinai360-iothub.azure-devices.net;DeviceId=UVSensor;SharedAccessKey=AwoHIgm/p4cJEUYhrFj/yYEorPePIaZQTYCDLeAJquY="
CONNECTION_STRING_CO2Sensor = "HostName=adt-twinai360-iothub.azure-devices.net;DeviceId=CO2Sensor;SharedAccessKey=3Ve0QCLeg6Un9GM9K8T++DdUOpgIROPrY5iUYU4a99k="

DHT11_MSG_TXT = '{{"Temperature": {temperature},"MinTemperature": {minTemperature}, ' \
                '"MaxTemperature": {maxTemperature},"Humidity": {humidity}}}'
BIOREACTOR_MSG_TXT = '{{"Capacity":{capacity},"CellCount":{cellCount}}}'
dOSensor_MSG_TXT = '{{"dOSensorValue":{dOSensorValue},"dOSensorMinValue":{dOSensorMinValue},' \
                  '"dOSensorMaxValue":{dOSensorMaxValue}}}'
pHSensor_MSG_TXT = '{{"pHSensorValue":{pHSensorValue},"pHSensorMinValue":{pHSensorMinValue},' \
                  '"pHSensorMaxValue":{pHSensorMaxValue}}}'
UVSensor_MSG_TXT = '{{"UVSensorValue":{UVSensorValue},"UVSensorMinValue":{UVSensorMinValue},' \
                  '"UVSensorMaxValue":{UVSensorMaxValue}}}'
CO2Sensor_MSG_TXT = '{{"CO2SensorValue":{CO2SensorValue},"CO2SensorMinValue":{CO2SensorMinValue},' \
                  '"CO2SensorMaxValue":{CO2SensorMaxValue}}}'

def storage_client_init():
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("adttwindatablob")
    blob_client = container_client.get_blob_client("Sensor_Data_for_TwinOps.xlsx")
    return blob_client

def iothub_dht11_client_init():
    # Create an IoT Hub client
    dht11_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING_DHT11)
    return dht11_client

def iothub_bioreactor_client_init():
    # Create an IoT Hub client
    bioreactor_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING_BIOREACTOR)
    return bioreactor_client

def iothub_dOSensor_client_init():
    # Create an IoT Hub client
    dOSensor_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING_dOSensor)
    return dOSensor_client

def iothub_pHSensor_client_init():
    # Create an IoT Hub client
    pHSensor_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING_pHSensor)
    return pHSensor_client

def iothub_UVSensor_client_init():
    # Create an IoT Hub client
    UVSensor_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING_UVSensor)
    return UVSensor_client

def iothub_CO2Sensor_client_init():
    # Create an IoT Hub client
    CO2Sensor_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING_CO2Sensor)
    return CO2Sensor_client

def predict_cell_count(dOSensor, pHSensor, UVSensor, CO2Sensor, DHT11Sensor):
    handler = {
        'ClientCertificateOptions': 'ClientCertificateOption.Manual',
        'ServerCertificateCustomValidationCallback':
            lambda httpRequestMessage, cert, cetChain, policyErrors: True
    }
    url = "https://twinai-ml-workspace.eastus.inference.ml.azure.com/score"
    payload = json.dumps({
        "input_data": {
            "columns": [
                "dOSensor",
                "pHSensor",
                "UVSensor",
                "CO2Sensor",
                "DHT11Sensor"
            ],
            "index": [
                1
            ],
            "data": [
                [
                    dOSensor,
                    pHSensor,
                    UVSensor,
                    CO2Sensor,
                    DHT11Sensor
                ]
            ]
        }
    })
    headers = {
        'azureml-model-deployment': 'cellcountpredictionmodel-4',
        'Authorization': 'Bearer aRYnQfnBDRaPkoftkM5jxXDM5DNmQRYF',
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    # print(response.text.removeprefix('[').removesuffix(']'))
    return response.text.removeprefix('[').removesuffix(']')


def iothub_client_telemetry_sample_run():
    try:
        dht11_client = iothub_dht11_client_init()
        bioreactor_client = iothub_bioreactor_client_init()
        dOSensor_client = iothub_dOSensor_client_init()
        pHSensor_client = iothub_pHSensor_client_init()
        UVSensor_client = iothub_UVSensor_client_init()
        CO2Sensor_client = iothub_CO2Sensor_client_init()

        sheet1 = "Sensor_Data_for_TwinOps"
        sheet2 = "Threshold_Values"

        blob_client = storage_client_init()
        df = pd.read_excel(blob_client.download_blob().readall(), sheet_name=sheet1, engine='openpyxl')
        df2 = pd.read_excel(blob_client.download_blob().readall(), sheet_name=sheet2, engine='openpyxl')

        dOSensorMin = df2['MinValue'][0]
        dOSensorMax = df2['MaxValue'][0]
        pHSensorMin = df2['MinValue'][1]
        pHSensorMax = df2['MaxValue'][1]
        UVSensorMin = df2['MinValue'][2]
        UVSensorMax = df2['MaxValue'][2]
        CO2SensorMin = df2['MinValue'][3]
        CO2SensorMax = df2['MaxValue'][3]
        dht11Min = df2['MinValue'][4]
        dht11Max = df2['MaxValue'][4]
        print("IoT Hub device sending sensors telemetry periodically, press Ctrl-C to exit")
        for x in df.index:
            dht11_msg_formatted = DHT11_MSG_TXT.format(temperature=round(df['DHT11Sensor'][x], 2), minTemperature=dht11Min,
                                                       maxTemperature=dht11Max, humidity=0.00)
            dht11_message = Message(dht11_msg_formatted)
            dht11_client.send_message(dht11_message)
            print(dht11_message)

            dOSensor_msg_formatted = dOSensor_MSG_TXT.format(dOSensorValue=round(df['dOSensor'][x], 2), dOSensorMinValue=dOSensorMin,
                                                           dOSensorMaxValue=dOSensorMax)
            dOSensor_message = Message(dOSensor_msg_formatted)
            dOSensor_client.send_message(dOSensor_message)
            print(dOSensor_msg_formatted)

            pHSensor_msg_formatted = pHSensor_MSG_TXT.format(pHSensorValue=round(df['pHSensor'][x], 2), pHSensorMinValue=pHSensorMin,
                                                           pHSensorMaxValue=pHSensorMax)
            pHSensor_message = Message(pHSensor_msg_formatted)
            pHSensor_client.send_message(pHSensor_message)
            print(pHSensor_msg_formatted)

            UVSensor_msg_formatted = UVSensor_MSG_TXT.format(UVSensorValue=round(df['UVSensor'][x], 2), UVSensorMinValue=UVSensorMin,
                                                           UVSensorMaxValue=UVSensorMax)
            UVSensor_message = Message(UVSensor_msg_formatted)
            UVSensor_client.send_message(UVSensor_message)
            print(UVSensor_msg_formatted)

            CO2Sensor_msg_formatted = CO2Sensor_MSG_TXT.format(CO2SensorValue=round(df['CO2Sensor'][x], 2), CO2SensorMinValue=CO2SensorMin,
                                                           CO2SensorMaxValue=CO2SensorMax)
            CO2Sensor_message = Message(CO2Sensor_msg_formatted)
            CO2Sensor_client.send_message(CO2Sensor_message)
            print(CO2Sensor_msg_formatted)
            cell_count = predict_cell_count(round(df['dOSensor'][x], 2), round(df['pHSensor'][x], 2), round(df['UVSensor'][x], 2), round(df['CO2Sensor'][x], 2), round(df['DHT11Sensor'][x], 2))
            bioreactor_msg_formatted = BIOREACTOR_MSG_TXT.format(capacity=600, cellCount=round(int(float(cell_count)), 0))

            bioreactor_message = Message(bioreactor_msg_formatted)
            bioreactor_client.send_message(bioreactor_message)
            print(bioreactor_msg_formatted)

            time.sleep(1)

    except KeyboardInterrupt:
        print("IoTHubClient sample stopped")


if __name__ == '__main__':
    print("IoT Hub Quickstart #1 - Simulated device")
    print("Press Ctrl-C to exit")
    iothub_client_telemetry_sample_run()