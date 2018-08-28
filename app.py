#!/usr/bin/env python

# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import random
import time
import sys
from iothub_client import IoTHubClient, IoTHubClientError, IoTHubTransportProvider, IoTHubClientResult
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError, DeviceMethodReturnValue
import config as config
from BME280SensorSimulator import BME280SensorSimulator
import RPi.GPIO as GPIO
from Adafruit_BME280 import *
import re

# Camera and Azure
from telemetry import Telemetry
from azure.storage.blob import BlockBlobService 
from azure.storage.blob import ContentSettings 
import picamera 
import  uuid
import subprocess
import os.path
import shlex

# Internal temp probe
import os
import glob
import time
import datetime

# Internal probe settings DS18B20
os.system('modprobe w1-gpio')
os.system('modprobe w1-therm')
 
base_dir = '/sys/bus/w1/devices/'
device_folder = glob.glob(base_dir + '28*')[0]
device_file = device_folder + '/w1_slave'

# HTTP options
# Because it can poll "after 9 seconds" polls will happen effectively
# at ~10 seconds.
# Note that for scalabilty, the default value of minimumPollingTime
# is 25 minutes. For more information, see:
# https://azure.microsoft.com/documentation/articles/iot-hub-devguide/#messaging
TIMEOUT = 241000
MINIMUM_POLLING_TIME = 9

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubClient.send_event_async.
# By default, messages do not expire.
MESSAGE_TIMEOUT = 10000

RECEIVE_CONTEXT = 0
MESSAGE_COUNT = 0
MESSAGE_SWITCH = True
TWIN_CONTEXT = 0
SEND_REPORTED_STATE_CONTEXT = 0
METHOD_CONTEXT = 0
TEMPERATURE_ALERT = 30.0

# global counters
RECEIVE_CALLBACKS = 0
SEND_CALLBACKS = 0
BLOB_CALLBACKS = 0
TWIN_CALLBACKS = 0
SEND_REPORTED_STATE_CALLBACKS = 0
METHOD_CALLBACKS = 0
EVENT_SUCCESS = "success"
EVENT_FAILED = "failed"
WAIT_COUNT = 5

# chose HTTP, AMQP or MQTT as transport protocol
PROTOCOL = IoTHubTransportProvider.MQTT

# String containing Hostname, Device Id & Device Key in the format:
# "HostName=<host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>"
telemetry = Telemetry()

if len(sys.argv) < 2:
    print ( "You need to provide the device connection string as command line arguments." )
    telemetry.send_telemetry_data(None, EVENT_FAILED, "Device connection string is not provided")
    sys.exit(0)

def is_correct_connection_string():
    m = re.search("HostName=.*;DeviceId=.*;", CONNECTION_STRING)
    if m:
        return True
    else:
        return False

CONNECTION_STRING = sys.argv[1]

if not is_correct_connection_string():
    print ( "Device connection string is not correct." )
    telemetry.send_telemetry_data(None, EVENT_FAILED, "Device connection string is not correct.")
    sys.exit(0)

MSG_TXT = "{\"deviceId\": \"Raspberry Pi - Python\",\"externalTemp\": %f,\"humidity\": %f,\"internalTemp\": %f, \"timeOfReading\": \"%s\"}"

GPIO.setmode(GPIO.BCM) # Setup breadboard by GPIO Address
GPIO.setup(config.GPIO_PIN_ADDRESS, GPIO.OUT) # Setup LED
GPIO.setup(config.LAMP_ADDRESS, GPIO.OUT)   # Set up lamp

# DS18B20
def read_temp_raw():
    f = open(device_file, 'r')
    lines = f.readlines()
    f.close()
    return lines

# DS18B20
def read_internal_temp():    
    lines = read_temp_raw()
    while lines[0].strip()[-3:] != 'YES':
        time.sleep(0.2)
        lines = read_temp_raw()
    equals_pos = lines[1].find('t=')
    if equals_pos != -1:
        temp_string = lines[1][equals_pos+2:]
        temp_c = float(temp_string) / 1000.0
        temp_f = temp_c * 9.0 / 5.0 + 32.0
        print (temp_f)
        return temp_f


def receive_message_callback(message, counter):
    global RECEIVE_CALLBACKS
    message_buffer = message.get_bytearray()
    size = len(message_buffer)
    print ( "Received Message, uploading photo and data [%d]:" % counter )
    print ( "    Data: %s & Size=%d" % (message_buffer[:size].decode("utf-8"), size) )
    map_properties = message.properties()
    key_value_pair = map_properties.get_internals()
    print ( "    Properties: %s" % key_value_pair )
    counter += 1
    RECEIVE_CALLBACKS += 1
    print ( "    Total calls received: %d" % RECEIVE_CALLBACKS )
    return IoTHubMessageDispositionResult.ACCEPTED


def send_confirmation_callback(message, result, user_context):
    global SEND_CALLBACKS
    print ( "Confirmation[%d] received for message with result = %s" % (user_context, result) )
    map_properties = message.properties()
    print ( "    message_id: %s" % message.message_id )
    print ( "    correlation_id: %s" % message.correlation_id )
    key_value_pair = map_properties.get_internals()
    print ( "    Properties: %s" % key_value_pair )
    SEND_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % SEND_CALLBACKS )    
    
    
def take_picture():    
    print "take picture method called"
    # Turn on LED  & Lamp
    GPIO.output(config.GPIO_PIN_ADDRESS, GPIO.HIGH)
    GPIO.output(config.LAMP_ADDRESS, GPIO.HIGH)
    
    # Take Picture
    camera = picamera.PiCamera()
    pic_id = "/home/pi/iot-hub-python-raspberrypi-client-app/imgs/" + str(uuid.uuid4()) + '.jpg' 
    camera.capture(pic_id)
    camera.close() #turn camera off
    
    # Upload picture
    block_blob_service = BlockBlobService(
        account_name='yourBlobStorage',
        account_key='your-account-key') 
    block_blob_service.create_blob_from_path(
       'mycontainer', 
       str(pic_id), 
       str(pic_id), 
       content_settings=ContentSettings(content_type='image/jpeg'))
    print "picture uploaded"
    
    #Turn off LED & Lamp
    GPIO.output(config.GPIO_PIN_ADDRESS, GPIO.LOW)
    GPIO.output(config.LAMP_ADDRESS, GPIO.LOW)
    
def take_video():
    print "take video method called"
    # Turn on LED  & Lamp
    GPIO.output(config.GPIO_PIN_ADDRESS, GPIO.HIGH)
    GPIO.output(config.LAMP_ADDRESS, GPIO.HIGH)
    
    camera = picamera.PiCamera()
    guid = str(uuid.uuid4())
    vid_id = "/home/pi/iot-hub-python-raspberrypi-client-app/vids/" + guid
    vid_id_264 = "/home/pi/iot-hub-python-raspberrypi-client-app/vids/" + guid + '.h264'
    vid_id_mp4 = "/home/pi/iot-hub-python-raspberrypi-client-app/vids/" + guid + '.mp4'
    camera.resolution = (640, 480)
    
    camera.start_recording(vid_id_264)
    camera.wait_recording(10)
    camera.stop_recording()
    
    camera.close() #turn camera off
    
    # Convert video
    command = shlex.split("MP4Box -add {f}.h264 {f}.mp4".format(f=vid_id))
    output = subprocess.check_output(command, stderr=subprocess.STDOUT)
    print(output)
        
    # Upload picture
    block_blob_service = BlockBlobService(account_name='yourBlobStorage',
                                      account_key='your-account-key') 
    block_blob_service.create_blob_from_path(
       'myvideocontainer', 
       str(vid_id_mp4), 
       str(vid_id_mp4), 
       content_settings=ContentSettings(content_type='video/mp4'))
    print "video uploaded"
    
    #Turn off LED & Lamp
    GPIO.output(config.GPIO_PIN_ADDRESS, GPIO.LOW)
    GPIO.output(config.LAMP_ADDRESS, GPIO.LOW)
    
    
def device_twin_callback(update_state, payload, user_context):
    global TWIN_CALLBACKS
    print ( "\nTwin callback called with:\nupdateStatus = %s\npayload = %s\ncontext = %s" % (update_state, payload, user_context) )
    TWIN_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % TWIN_CALLBACKS )


def send_reported_state_callback(status_code, user_context):
    global SEND_REPORTED_STATE_CALLBACKS
    print ( "Confirmation for reported state received with:\nstatus_code = [%d]\ncontext = %s" % (status_code, user_context) )
    SEND_REPORTED_STATE_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % SEND_REPORTED_STATE_CALLBACKS )


def device_method_callback(method_name, payload, user_context):
    global METHOD_CALLBACKS,MESSAGE_SWITCH
    print ( "\nMethod callback called with:\nmethodName = %s\npayload = %s\ncontext = %s" % (method_name, payload, user_context) )
    METHOD_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % METHOD_CALLBACKS )
    device_method_return_value = DeviceMethodReturnValue()
    device_method_return_value.response = "{ \"Response\": \"This is the response from the device\" }"
    device_method_return_value.status = 200
    if method_name == "start":
        MESSAGE_SWITCH = True
        print ( "Start sending message\n" )
        device_method_return_value.response = "{ \"Response\": \"Successfully started\" }"
        return device_method_return_value
    if method_name == "stop":
        MESSAGE_SWITCH = False
        print ( "Stop sending message\n" )
        device_method_return_value.response = "{ \"Response\": \"Successfully stopped\" }"
        return device_method_return_value
    if method_name == "TakeVideo":
        take_video()
    if method_name == "TakePicture":
        take_picture()
    if method_name == "TakeTempHumidity":
        take_temp_humidity()
    if method_name == "TakeInternalTemp":
        read_internal_temp()

    return device_method_return_value


def blob_upload_conf_callback(result, user_context):
    global BLOB_CALLBACKS
    print ( "Blob upload confirmation[%d] received for message with result = %s" % (user_context, result) )
    BLOB_CALLBACKS += 1
    print ( "Total calls confirmed: %d" % BLOB_CALLBACKS )


def iothub_client_init():
    # prepare iothub client
    client = IoTHubClient(CONNECTION_STRING, PROTOCOL)
    client.set_option("product_info", "HappyPath_RaspberryPi-Python")
    if client.protocol == IoTHubTransportProvider.HTTP:
        client.set_option("timeout", TIMEOUT)
        client.set_option("MinimumPollingTime", MINIMUM_POLLING_TIME)
    # set the time until a message times out
    client.set_option("messageTimeout", MESSAGE_TIMEOUT)
    # to enable MQTT logging set to 1
    if client.protocol == IoTHubTransportProvider.MQTT:
        client.set_option("logtrace", 0)
    client.set_message_callback(
        receive_message_callback, RECEIVE_CONTEXT)
    if client.protocol == IoTHubTransportProvider.MQTT or client.protocol == IoTHubTransportProvider.MQTT_WS:
        client.set_device_twin_callback(
            device_twin_callback, TWIN_CONTEXT)
        client.set_device_method_callback(
            device_method_callback, METHOD_CONTEXT)
    return client

# global client
client = iothub_client_init()

def print_last_message_time(client):
    try:
        last_message = client.get_last_message_receive_time()
        print ( "Last Message: %s" % time.asctime(time.localtime(last_message)) )
        print ( "Actual time : %s" % time.asctime() )
    except IoTHubClientError as iothub_client_error:
        if iothub_client_error.args[0].result == IoTHubClientResult.INDEFINITE_TIME:
            print ( "No message received" )
        else:
            print ( iothub_client_error )

def take_temp_humidity():
    try:
        global client

        if client.protocol == IoTHubTransportProvider.MQTT:
            print ( "IoTHubClient is reporting state" )
            reported_state = "{\"newState\":\"standBy\"}"
            client.send_reported_state(reported_state, len(reported_state), send_reported_state_callback, SEND_REPORTED_STATE_CONTEXT)

        if not config.SIMULATED_DATA:
            sensor = BME280(address = config.I2C_ADDRESS)
        else:
            sensor = BME280SensorSimulator()

        telemetry.send_telemetry_data(parse_iot_hub_name(), EVENT_SUCCESS, "IoT hub connection is established")

        global MESSAGE_COUNT,MESSAGE_SWITCH
        if MESSAGE_SWITCH:
            # send a few messages every minute
            print ( "IoTHubClient sending %d messages" % MESSAGE_COUNT )
            externalTemperature = sensor.read_temperature()*1.8 + 32
            humidity = sensor.read_humidity()
            internalTemperature = read_internal_temp()
            time_of_reading = str(datetime.datetime.now())

            msg_txt_formatted = MSG_TXT % (
                externalTemperature,
                humidity,
                internalTemperature,
                time_of_reading)

            print (msg_txt_formatted)
            message = IoTHubMessage(msg_txt_formatted)
            # optional: assign ids
            message.message_id = "message_%d" % MESSAGE_COUNT
            message.correlation_id = "correlation_%d" % MESSAGE_COUNT
            # optional: assign properties
            prop_map = message.properties()
            prop_map.add("temperatureAlert", "true" if externalTemperature > TEMPERATURE_ALERT else "false")

            client.send_event_async(message, send_confirmation_callback, MESSAGE_COUNT)
            print ( "TEMP SENT IoTHubClient.send_event_async accepted message [%d] for transmission to IoT Hub." % MESSAGE_COUNT )

            status = client.get_send_status()
            print ( "Send status: %s" % status )
            MESSAGE_COUNT += 1

    except IoTHubError as iothub_error:
        print ( "Unexpected error %s from IoTHub" % iothub_error )
        telemetry.send_telemetry_data(parse_iot_hub_name(), EVENT_FAILED, "Unexpected error %s from IoTHub" % iothub_error)
        return
    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

    print_last_message_time(client)        

def iothub_client_sample_run():
    try:
        global client

        if client.protocol == IoTHubTransportProvider.MQTT:
            print ( "IoTHubClient is reporting state" )
            reported_state = "{\"newState\":\"standBy\"}"
            client.send_reported_state(reported_state, len(reported_state), send_reported_state_callback, SEND_REPORTED_STATE_CONTEXT)

        if not config.SIMULATED_DATA:
            sensor = BME280(address = config.I2C_ADDRESS)
        else:
            sensor = BME280SensorSimulator()

        telemetry.send_telemetry_data(parse_iot_hub_name(), EVENT_SUCCESS, "IoT hub connection is established")
        while True:
            global MESSAGE_COUNT,MESSAGE_SWITCH
            if MESSAGE_SWITCH:
                # send a few messages every minute
                print ( "IoTHubClient sending %d messages" % MESSAGE_COUNT )
                externalTemperature = sensor.read_temperature()*1.8 + 32
                humidity = sensor.read_humidity()
                internalTemperature = read_internal_temp()
                time_of_reading = str(datetime.datetime.now())

                msg_txt_formatted = MSG_TXT % (
                    externalTemperature,
                    humidity,
                    internalTemperature,
                    time_of_reading)

                print (msg_txt_formatted)
                message = IoTHubMessage(msg_txt_formatted)
                # optional: assign ids
                message.message_id = "message_%d" % MESSAGE_COUNT
                message.correlation_id = "correlation_%d" % MESSAGE_COUNT
                # optional: assign properties
                prop_map = message.properties()
                prop_map.add("temperatureAlert", "true" if externalTemperature > TEMPERATURE_ALERT else "false")

                client.send_event_async(message, send_confirmation_callback, MESSAGE_COUNT)
                print ( "IoTHubClient.send_event_async accepted message [%d] for transmission to IoT Hub." % MESSAGE_COUNT )

                status = client.get_send_status()
                print ( "Send status: %s" % status )
                MESSAGE_COUNT += 1
            time.sleep(config.MESSAGE_TIMESPAN / 1000.0)

    except IoTHubError as iothub_error:
        print ( "Unexpected error %s from IoTHub" % iothub_error )
        telemetry.send_telemetry_data(parse_iot_hub_name(), EVENT_FAILED, "Unexpected error %s from IoTHub" % iothub_error)
        return
    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

    print_last_message_time(client)

def led_blink():
    GPIO.output(config.GPIO_PIN_ADDRESS, GPIO.HIGH)
    time.sleep(config.BLINK_TIMESPAN / 1000.0)
    GPIO.output(config.GPIO_PIN_ADDRESS, GPIO.LOW)

def usage():
    print ( "Usage: iothub_client_sample.py -p <protocol> -c <connectionstring>" )
    print ( "    protocol        : <amqp, amqp_ws, http, mqtt, mqtt_ws>" )
    print ( "    connectionstring: <HostName=<host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>>" )

def parse_iot_hub_name():
    m = re.search("HostName=(.*?)\.", CONNECTION_STRING)
    return m.group(1)

if __name__ == "__main__":
    print ( "\nPython %s" % sys.version )
    print ( "IoT Hub Client for Python" )

    iothub_client_sample_run()
