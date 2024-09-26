import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime
import random


IITJODHPUR_COORDINATES = {
    "latiude": 26.472059, 
    "longitude": 73.114016
}

DELHI_COORDINATES = {
    "latiude": 28.551141,  
    "longitude": 77.088411
}

#Calculate the increment in latitude and longitude
LATITUDE_INCREMENT = (DELHI_COORDINATES["latiude"] - IITJODHPUR_COORDINATES["latiude"]) / 100
LONGITUDE_INCREMENT = (DELHI_COORDINATES["longitude"] - IITJODHPUR_COORDINATES["longitude"]) / 100

#Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.environ.get("VEHIKLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.environ.get("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.environ.get("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.environ.get("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.environ.get("EMERGENCY_TOPIC", "emergency_data")


startTime = datetime.now()
startLocation = IITJODHPUR_COORDINATES.copy()

def simulate_vehicle_movement():
    '''
    Simulate the movement of the vehicle from IIT Jodhpur to Delhi
    
    Increment the latitude and longitude of the vehicle by a fixed amount and then add a random value to simulate the movement of the vehicle
    '''
    global startTime, startLocation
    # currentTime = datetime.now()
    # timeDifference = currentTime - startTime
    # timeDifferenceInSeconds = timeDifference.total_seconds()
    # timeDifferenceInHours = timeDifferenceInSeconds / 3600

    # currentLatitude = startLocation["lattiude"] + (timeDifferenceInHours * LATITUDE_INCREMENT)
    # currentLongitude = startLocation["longitude"] + (timeDifferenceInHours * LONGITUDE_INCREMENT)

    # return {
    #     "timestamp": currentTime.timestamp(),
    #     "lattitude": currentLatitude,
    #     "longitude": currentLongitude
    # }
    
    startLocation['latitude'] += LATITUDE_INCREMENT
    startLocation['longitude'] += LONGITUDE_INCREMENT
    
    #Adding Random Movements for simulation
    
    startLocation['latitude'] += random.uniform(-0.0005, 0.0005)
    startLocation['longitude'] += random.uniform(-0.0005, 0.0005)
    
    return startLocation
    
    

def generate_vehicle_data(deviceID):
    '''
    Generate vehicle data for a given device ID
    
    Parameters
    ----------
    deviceID : str
        The ID of the device
    
    Returns
    -------
    dict
        The vehicle data containing the device ID and the current location
    '''
    location = simulate_vehicle_movement()

def simulate_journey(producer, deviceID):
    # When travelling
    '''
    Simulate the journey of a vehicle with the given device ID
    
    Publishes vehicle data to the VEHICLE_TOPIC topic every second
    '''
    
    while True:
        vehicleData = generate_vehicle_data(deviceID)


if __name__ == "__main__":
    producerConfig = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka Error: {err}')
    }
    
    producer = SerializingProducer(producerConfig)
    
    try:
        simulate_journey(producer, 'Lamborghini')
    except KeyboardInterrupt:
        print("Keyboard Interrupt - Simulation Ended")
    except Exception as e:
        print(f"Error: {e}")
    



