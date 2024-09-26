import os
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random


IITJODHPUR_COORDINATES = {
    "latitude": 26.472059, 
    "longitude": 73.114016
}

DELHI_COORDINATES = {
    "latitude": 28.551141,  
    "longitude": 77.088411
}

#Calculate the increment in latitude and longitude
LATITUDE_INCREMENT = (DELHI_COORDINATES["latitude"] - IITJODHPUR_COORDINATES["latitude"]) / 100
LONGITUDE_INCREMENT = (DELHI_COORDINATES["longitude"] - IITJODHPUR_COORDINATES["longitude"]) / 100

#Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.environ.get("VEHIKLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.environ.get("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.environ.get("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.environ.get("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.environ.get("EMERGENCY_TOPIC", "emergency_data")

random.seed(42)
startTime = datetime.now()
startLocation = IITJODHPUR_COORDINATES.copy()

def get_next_time():
    global startTime
    startTime+=timedelta(seconds=random.randint(30,60))
    return startTime


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
    return{
        'id':uuid.uuid4(),
        'deviceID': deviceID,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed':random.uniform(10,40),
        'direction':'North-East',
        'make':'BMW',
        'model':'C500',
        'year':2024,
        'fuelType':'Hybrid'
        
        
    }
def generate_gps_data(deviceID,timestamp,vehicleType='private'):
    return{
        'id':uuid.uuid4(),
        'deviceID': deviceID,
        'timestamp': timestamp,
        'speed':random.uniform(0,40),
        'direction':'North-East',
        'vehicleType':vehicleType
    }
    
def generate_traffic_camera_data(deviceID,timestamp,location,cameraID):
    return{
        'id':uuid.uuid4(),
        'deviceID': deviceID,
        'cameraID': cameraID,
        'location': location,
        'timestamp': timestamp,
        'snapshot':'Base64EncodedString'
    }

def generate_weather_data(deviceID,timestamp,location):
    return{
        'id':uuid.uuid4(),
        'deviceID': deviceID,
        'timestamp': timestamp,
        'location': location,
        'temperature':random.uniform(-5,34),
        'weatherCondition':random.choice(['Sunny','Cloudy','Rainy','Storm']),
        'precipitation':random.uniform(0,25),
        'windSpeed':random.uniform(0,100),
        'humidity':random.uniform(0,100),
        'airQualityIndex':random.uniform(0,500)
    
    }

def generate_emergency_incident_data(deviceID,timestamp,location):
    return{
        'id':uuid.uuid4(),
        'deviceID': deviceID,
        'incidentID': uuid.uuid4(),
        'type': random.choice(['Fire','Accident','Earthquake','Flood','Medical','Police','None']),
        'timestamp': timestamp,
        'location': location,
        'status':random.choice(['Active', 'Resolved']),
        'description':'Description of the incident'
    }

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
    
    



def simulate_journey(producer, deviceID):
    # When travelling
    '''
    Simulate the journey of a vehicle with the given device ID
    
    Publishes vehicle data to the VEHICLE_TOPIC topic every second
    '''
    
    while True:
        vehicleData = generate_vehicle_data(deviceID)
        gpsData=generate_gps_data(deviceID,vehicleData['timestamp'])
        traffic_camera_data=generate_traffic_camera_data(deviceID,vehicleData['timestamp'],vehicleData['location'],'Nikon-Cam123')
        weather_data=generate_weather_data(deviceID,vehicleData['timestamp'],vehicleData['location'])
        emergency_incident_data=generate_emergency_incident_data(deviceID,vehicleData['timestamp'],vehicleData['location'])
        
        print(f"Publishing Vehicle Data: {vehicleData}")
        print(f"Publishing GPS Data: {gpsData}")
        print(f"Publishing Traffic Camera Data: {traffic_camera_data}")
        print(f"Publishing Weather Data: {weather_data}")
        print(f"Publishing Emergency Incident Data: {emergency_incident_data}")
        break


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
    



