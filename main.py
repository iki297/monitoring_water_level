#!/usr/bin/env python3
#import sshtunnel
import random
import json
import pymysql
import mysql.connector
from paho.mqtt import client as mqtt_client
from datetime import datetime


#///////////////////////////////////ACCESS MYSQL/////////////////////////////
db = mysql.connector.connect(
        user = 'user',
        password = 'password',
        host = '127.0.0.1',
        port = 3306,
        database = 'db',
        #auth_plugin='mysql_native_password'
    )
print("ok konek")
cursor = db.cursor()
db.ping(reconnect=True)

#///////////////////////////////////ACCESS MYSQL/////////////////////////////


broker = 'mqtt'
port = 1883
topic = "topi"c
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'username'
password = 'password'

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        parsejson = json.loads(msg.payload)
#       print(parsejson["device"])
        try:
            if (parsejson["device"] == "primaBot/2209-5/0.8" ):
                #parsejson = json.loads(msg.payload)
                sampling = datetime.fromtimestamp(parsejson["sampling"])
                distance = parsejson["distance"]
                sensor_height = parsejson ["sensor_height"]
                baterai = parsejson["battery"]
                rssi = parsejson["signal_quality"]
                TMA = parsejson["sensor_height"] - (parsejson["distance"] * parsejson["sensor_resolution"])
                #TMA1 = 288 - (parsejson["distance"] * parsejson["sensor_resolution"])
                sql = "INSERT INTO jurolot1 (sampling, distance, tma, sensor_height, rssi, baterai) VALUES (%s, %s, %s, %s, %s, %s)"
                val = (sampling, distance, TMA, sensor_height, rssi, baterai)
                cursor.execute(sql, val)
                db.commit()
                print(cursor.rowcount, "data 1 masuk")
            elif (parsejson["device"] == "primaBot/2209-4/0.8" ):
                #parsejson = json.loads(msg.payload)
                sampling = datetime.fromtimestamp(parsejson["sampling"])
                distance = parsejson["distance"]
                sensor_height = parsejson ["sensor_height"]
                baterai = parsejson["battery"]
                rssi = parsejson["signal_quality"]
                TMA = parsejson["sensor_height"] - (parsejson["distance"] * parsejson["sensor_resolution"])
                #TMA2 = 266 - (parsejson["distance"] * parsejson["sensor_resolution"])
                sql = "INSERT INTO jurolot2 (sampling, distance, tma, sensor_height, rssi, baterai) VALUES (%s, %s, %s, %s, %s, %s)"
                val = (sampling, distance, TMA, sensor_height, rssi, baterai)
                cursor.execute(sql, val)
                db.commit()
                print(cursor.rowcount, "data 2 masuk")
            elif (parsejson["device"] == "primaBot/2301-1/0.8" ):
                #parsejson = json.loads(msg.payload)
                sampling = datetime.fromtimestamp(parsejson["sampling"])
                distance = parsejson["distance"]
                sensor_height = parsejson ["sensor_height"]
                baterai = parsejson["battery"]
                rssi = parsejson["signal_quality"]
                TMA = parsejson["sensor_height"] - (parsejson["distance"] * parsejson["sensor_resolution"])
                tick = parsejson["tick"]
                tipping_factor = parsejson ["tipping_factor"]
                curah_hujan = parsejson["tick"] * 0.2
                sql = "INSERT INTO leuwitereup (sampling, distance, tma, sensor_height, rssi, baterai, tick, tipping_factor, curah_hujan) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                val = (sampling, distance, TMA, sensor_height, rssi, baterai, tick, tipping_factor, curah_hujan)
                cursor.execute(sql, val)
                db.commit()
                print(cursor.rowcount, "data 3 masuk")
        except Error as e:
            print(e)

    client.subscribe(topic)
    client.on_message = on_message
        
    

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()
    subscribe.loop_forever() #tambahan


if __name__ == '__main__':
    run()
