import pymongo
import json
import paho.mqtt.client as mqtt
dbClient = pymongo.MongoClient('mongodb+srv://adel-khouly:adel-khouly9499@clusterofmongoprogect.xy8ec.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
db = dbClient['mongoProject']
collection = db.randomData


# Connection success callback
def on_connect(client, userdata, flags, rc):
    print('Connected with result code '+str(rc))
    client.subscribe('myTest/test')


def on_message(client, userdata, msg):
    obj = json.loads(f"{msg.payload.decode()}")
    print(obj)
    try:
        id = collection.insert_one(obj).inserted_id
        print(id)
    except Exception as err:
        print(err)


client = mqtt.Client()

# Specify callback function
client.on_connect = on_connect
client.on_message = on_message

# Establish a connection
client.connect('broker.emqx.io', 1883, 60)

# # Publish a message
client.publish('emqtt',payload='Hello World',qos=0)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    client.loop_forever()
