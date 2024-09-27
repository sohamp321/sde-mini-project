import os
import time
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random
from loguru import logger


IITJODHPUR_COORDINATES = {
    "latitude": 26.472059, 
    "longitude": 73.114016
}

DELHI_COORDINATES = {
    "latitude": 28.551141,  
    "longitude": 77.088411
}

LATITUDE_INCREMENT = (DELHI_COORDINATES["latitude"] - IITJODHPUR_COORDINATES["latitude"]) / 100
LONGITUDE_INCREMENT = (DELHI_COORDINATES["longitude"] - IITJODHPUR_COORDINATES["longitude"]) / 100

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
    '''
    Get the next time for the vehicle data
    '''
    global startTime
    startTime += timedelta(seconds=random.randint(1, 30))
    return startTime 

def generate_gps_data(deviceID, timestamp, vehicleType = 'private'):
    return {
        'id' : uuid.uuid4(),
        'deviceID': deviceID,
        'timestamp': timestamp,
        'speed': random.uniform(20, 120),
        'direction' : 'North-East',
        'vehicleType' : vehicleType
    }
    
def generate_traffic_camera_data(deviceID, timestamp, location, cameraID):
    # ? Can make it more complex by adding more camera or more cars 
    return {
        'id' : uuid.uuid4(),
        'deviceID': deviceID,
        'cameraID': cameraID,
        'timestamp': timestamp,
        'location' : location,
        # ! Can update this with a url from a storage bucket
        'snapshot': 'Base64EncodedString',
        'trafficIntensity': random.uniform(0, 100),
        'trafficType' : 'Normal',
        'location' : 'Delhi'
    }
    
def generate_weather_data(deviceID, timestamp, location):
    return {
        'id' : uuid.uuid4(),
        'deviceID': deviceID,
        'location': location,
        'timestamp': timestamp,
        # ? Can get these from a weather API
        'temperature': random.uniform(30, 50),
        'weatherCondition' : random.choice(['Sunny', 'Cloudy', 'Humid']),
        'humidity': random.randint(0, 100),
        'precipitation': random.uniform(0, 100),
        'windSpeed': random.uniform(0, 100),
        'airQualityIndex': random.uniform(0, 500)
    }
def generate_emergency_incident_data(deviceID, timestamp, location):
    return {
        'id' : uuid.uuid4(),
        'deviceID': deviceID,
        'incidentID' : uuid.uuid4(),
        'incidentType': random.choice(['Accident', 'Fire', 'Medical Emergency']),
        'location': location,
        'timestamp': timestamp,
        'severity': random.choice(['Low', 'Medium', 'High']),
        'description': 'Incident Description',
        'status': random.choice(["Active", "Resolved"])
    }

def simulate_vehicle_movement():
    '''
    Simulate the movement of the vehicle from IIT Jodhpur to Delhi
    
    Increment the latitude and longitude of the vehicle by a fixed amount and then add a random value to simulate the movement of the vehicle
    '''
    global startTime, startLocation
    
    startLocation['latitude'] += LATITUDE_INCREMENT
    startLocation['longitude'] += LONGITUDE_INCREMENT
    
    #Adding Random Movements for simulation
    
    startLocation['latitude'] += random.uniform(-0.0005, 0.0005)
    startLocation['longitude'] += random.uniform(-0.0005, 0.0005)
    
    return startLocation
    
    
def generate_vehicle_data(deviceID):
    '''
    Simulates Vehicle Data obtained from the sensor
    '''
    location = simulate_vehicle_movement()
    
    return {
        'id' : uuid.uuid4(),
        'deviceID': deviceID,
        'timestamp': get_next_time().isoformat(),
        'location' : (location['latitude'], location['longitude']),
        'speed': random.uniform(20, 120),
        'direction' : 'North-East',
        'make' : 'Lamborghini',
        'model' : 'Huracan',
        'year' : 2021,
        'fuelType' : 'Petrol',
    }
    
def json_serializer(data):
    if isinstance(data,uuid.UUID):
        return str(data)
    raise TypeError(f"Type {type(data)} not serializable")

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    # ? Edge Case might be when the UUID is not generated properly
    key = str(data['id'])
    producer.produce(
        topic=topic, 
        key=key.encode('utf-8'),
        value=json.dumps(data, default=json_serializer).encode('utf-8'), 
        on_delivery=delivery_report)
    
    producer.flush()

def simulate_journey(producer, deviceID):
    # When travelling
    while True:
        vehicleData = generate_vehicle_data(deviceID)
        logger.info(f"Vehicle Data: {vehicleData}")
        #get the data at a certain time so we add timestamp to every function
        gpsData = generate_gps_data(deviceID, vehicleData['timestamp'])
        logger.info(f"GPS Data: {gpsData}")
        trafficCameraData = generate_traffic_camera_data(deviceID, vehicleData['timestamp'], vehicleData['location'], 'camera1')
        logger.info(f"Traffic Camera Data: {trafficCameraData}")
        weatherData = generate_weather_data(deviceID, vehicleData['timestamp'], vehicleData['location'])
        logger.info(f"Weather Data: {weatherData}")
        emergencyIncidentData = generate_emergency_incident_data(deviceID, vehicleData['timestamp'], vehicleData['location'])
        logger.info(f"Emergency Incident Data: {emergencyIncidentData}")
        
        if(vehicleData['location'][0] >= DELHI_COORDINATES['latitude'] and vehicleData['location'][1] <= DELHI_COORDINATES['longitude']):
            logger.success("Reached Destination. Ending Simulation")
            break
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicleData)
        produce_data_to_kafka(producer, GPS_TOPIC, gpsData)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, trafficCameraData)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weatherData)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergencyIncidentData)
        
        time.sleep(5)


if __name__ == "__main__":
    producerConfig = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: logger.error(f'Kafka Error: {err}')
    }

    
    producer = SerializingProducer(producerConfig)
    
    try:
        simulate_journey(producer, 'Lamborghini')
    except KeyboardInterrupt:
        logger.error("Keyboard Interrupt - Simulation Ended")
    except Exception as e:
        logger.error(f"Error: {e}")
    



