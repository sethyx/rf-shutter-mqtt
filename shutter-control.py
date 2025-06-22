#!/usr/bin/python

import json
import logging
import os
import queue
import sys
import threading
import time
import traceback

import paho.mqtt.client as mqtt
from tinydb import Query, Storage, TinyDB, where
from tinydb.storages import MemoryStorage

from rf import RFDevice

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # To print to stdout as well
    ]
)

logger = logging.getLogger(__name__)
# Dual TinyDB Storage
class HybridStorage(Storage):
    def __init__(self, file_path):
        self.lock = threading.Lock()
        self.memory_db = TinyDB(storage=MemoryStorage)
        self.file_db = TinyDB(file_path)
        # read file and update memory at init
        data = self.file_db.storage.read()
        self.memory_db.storage.write(data)
        logger.info("Database init")

    def read(self):
        with self.lock:
            data = self.memory_db.storage.read()
        logger.debug("Database memory read")
        return data

    def write(self, data):
        with self.lock:
            self.memory_db.storage.write(data)
        logger.debug("Database memory write")
        
    def persist(self):
        with self.lock:
            data = self.memory_db.storage.read()
            self.file_db.storage.write(data)
        logger.debug("Database persist")

    def close(self):
        self.persist()
        logger.debug(f"Database closed: {self.file_path}")

# Command Queue Manager
class CommandQueueManager:
    def __init__(self):
        self.command_queue = queue.Queue()
        self.lock = threading.Lock()
        self.worker_thread = threading.Thread(target=self._process_commands)
        self.worker_thread.daemon = True
        self.worker_thread.start()
        logger.info("CommandQueueManager initialized and worker thread started")

    def add_command(self, command):
        with self.lock:
            self.command_queue.put((command))
        logger.debug(f"Added command to queue")

    def _process_commands(self):
        while True:
            command = self.command_queue.get()
            if command:
                logger.debug(f"Processing command")
                try:
                    command.execute()
                except Exception as e:
                    logger.error(f"Failed to execute command: {e}")
                self.command_queue.task_done()

# Shutter Control Class
class ShutterControl:
    def __init__(self, shutter_id, rf_device, client, topic, db, command_queue):
        Shutter = Query()
        self.db = db
        db_data = self.db.search(Shutter.shutter_id == shutter_id)[0]
        self.topic = topic
        self.shutter_id = shutter_id
        self.command_codes = db_data.get('command_codes')
        self.shutter_name = db_data.get('shutter_name')
        self.rf_device = rf_device
        self.calibration_time = db_data.get('calibration_time')
        self.current_position = db_data.get('position')
        self.current_state = db_data.get('state')
        self.group_shutter = db_data.get('group_shutter')
        self.group_members = db_data.get('group_members')
        self.mqtt_client = client
        self.command_queue = command_queue
        self.moving = False
        self.lock = threading.Lock()
        self.move_start_time = time.time()
        self.move_start_position = self.current_position
        logger.info(f"Initialized ShutterControl for {self.shutter_name} with ID {shutter_id}")

    def calculate_position_from_elapsed(self, elapsed):
        logger.debug(f"vars: {self.move_start_position}, {self.calibration_time}, {elapsed}, {self.current_state}")
        if self.current_state == 'closing':
            new_position = self.move_start_position - (elapsed / self.calibration_time) * 100
            new_position = 0.0 if new_position < 0 else new_position
        elif self.current_state == 'opening':
            new_position = self.move_start_position + (elapsed / self.calibration_time) * 100
            new_position = 100.0 if new_position > 100 else new_position
        else:
            new_position = self.current_position
        logger.debug(f"calculated new position: {new_position}")
        return new_position
    
    def state_from_position(self, position):
        return 'closed' if position == 0.0 else 'open'

    def move_to_position(self, target_position, sendrf):
        if target_position == self.current_position:
            logger.debug(f"{self.shutter_name}: Already at position {target_position}")
            return
        
        if target_position < self.current_position:
            direction = 'CLOSE'
            move_time = self.calibration_time * ((self.current_position - target_position) / 100)
        else:
            direction = 'OPEN'
            move_time = self.calibration_time * ((target_position - self.current_position) / 100)

        logger.info(f"{self.shutter_name}: Moving to position {target_position}, direction {direction} in time {move_time}s")

        try:
            if sendrf:
                self.send_rf_command(direction)
            with self.lock:
                self.move_start_time = time.time()
                self.move_start_position = self.current_position
                self.moving = True
            self.current_state = 'closing' if direction == 'CLOSE' else 'opening'
            self.update_state()
            while True:
                now = time.time()
                with self.lock:
                    move_start_time = self.move_start_time
                    moving = self.moving
                elapsed = now - move_start_time
                logger.info(f"{self.shutter_name}: In move_to_position loop, {direction}, move time: {move_time}, elapsed: {elapsed}")
                if moving == False:
                    break
                if elapsed > move_time:
                    if sendrf:
                        self.send_rf_command('STOP')
                    
                    with self.lock:
                        self.moving = False
                    break
                self.current_position = self.calculate_position_from_elapsed(elapsed)
                self.update_state()
                time.sleep(0.5)
            
            self.current_position = self.calculate_position_from_elapsed(time.time() - self.move_start_time)
            self.current_state = self.state_from_position(self.current_position)
            
            self.update_state(True)
            logger.info(f"{self.shutter_name}: Reached position {self.current_position}")
        except Exception as e:
            logger.error(f"Error while moving {self.shutter_name}: {e}")

    def handle_command(self, command, sendrf=True):
        if command == 'CLOSE':
            target_position = 0
            move_time = self.calibration_time * ((self.current_position - target_position) / 100)
        elif command == 'OPEN':
            target_position = 100
            move_time = self.calibration_time * ((target_position - self.current_position) / 100)
        else:
            move_time = 0

        try:
            if sendrf:
                self.send_rf_command(command)
            if command == 'STOP':
                with self.lock:
                    self.moving = False
                return
            else:
                with self.lock:
                    self.move_start_time = time.time()
                    self.move_start_position = self.current_position
                    self.moving = True
                self.current_state = 'closing' if command == 'CLOSE' else 'opening'
                self.update_state()
                while True:
                    now = time.time()
                    with self.lock:
                        move_start_time = self.move_start_time
                        moving = self.moving
                    elapsed = now - move_start_time
                    logger.info(f"{self.shutter_name}: In command loop, {command}, move time: {move_time}, elapsed: {elapsed}")
                    if moving == False:
                        break
                    if elapsed > move_time:
                        with self.lock:
                            self.moving = False
                        self.update_state()
                        break
                    self.current_position = self.calculate_position_from_elapsed(elapsed)
                    self.update_state()
                    time.sleep(0.5)

            self.current_position = self.calculate_position_from_elapsed(time.time() - self.move_start_time)
            self.current_state = self.state_from_position(self.current_position)
            self.update_state(True)
            logger.info(f"{self.shutter_name}: End of command execution, {command}")
        except Exception as e:
            logger.error(f"Error while moving {self.shutter_name}: {e}")
        

    def send_rf_command(self, command):
        # Use the RFDevice instance to send the command
        def command_execution():
            try:
                rf_code = self.command_codes.get(command)
                logger.debug(f"Sending RF command: {command} / {rf_code} for {self.shutter_name}")
                self.rf_device.control(rf_code)
            except Exception as e:
                logger.error(f"Error while sending RF command {self.shutter_name}: {e}")

        self.command_queue.add_command(Command(command_execution))

    def update_state(self, persist=False):
        # Update the state in TinyDB and send MQTT updates to Home Assistant
        shutter_data = {'state': self.current_state, 'position': self.current_position}
        self.db.update(shutter_data, where('shutter_id') == self.shutter_id)
        if persist:
            self.db.storage.persist()
        self.mqtt_client.publish(f'{self.topic}/shutter/{self.shutter_id}/state', json.dumps(shutter_data), retain=True)
        logger.debug(f"{self.shutter_name}: Updated state to Home Assistant: {shutter_data}")

# Command Wrapper
class Command:
    def __init__(self, execute_fn):
        self.execute_fn = execute_fn

    def execute(self):
        self.execute_fn()

# MQTT Home Assistant Auto Discovery
def setup_mqtt_discovery(client, topic, shutter_id, shutter_name):
    discovery_payload = {
        "name": shutter_name,
        "device_class": "shutter",
        "command_topic": f"{topic}/shutter/{shutter_id}/set",
        "state_topic": f"{topic}/shutter/{shutter_id}/state",
        "position_topic": f"{topic}/shutter/{shutter_id}/state",
        "set_position_topic": f"{topic}/shutter/{shutter_id}/set_position",
        "availability_topic": f"{topic}/system/availability",
        "value_template": "{{ value_json.state }}",
        "state_closed": "closed",
        "state_open": "open",
        "position_template": "{{ value_json.position }}",
        "position_open": 100,
        "position_closed": 0,
        "payload_available": "online",
        "payload_not_available": "offline"
    }
    client.publish(f'homeassistant/cover/{topic}/{shutter_id}/config', json.dumps(discovery_payload), retain=True)
    logger.info(f"Published MQTT discovery config for {shutter_id} / {shutter_name}")

# MQTT Callbacks
def on_message(client, userdata, msg):
    def handle_message():
        try:
            payload = msg.payload.decode("utf-8")
            topic = msg.topic
            sendrf = False if 'norf' in topic else True
            if 'set_position' in topic:
                position = int(payload)
                shutter_id = topic.split('/')[2]
                control_object = f"shutter_control_{shutter_id}"
                shutter_control = userdata[control_object]
                set_topic = shutter_control.topic
                if shutter_control.group_shutter:
                    for member in shutter_control.group_members:
                        client.publish(f'{set_topic}/shutter/{member}/set_position_norf', position)
                shutter_control.move_to_position(position, sendrf)
                logger.info(f"Received MQTT command to set position: {position}")
            elif 'set' in topic:
                shutter_id = topic.split('/')[2]
                control_object = f"shutter_control_{shutter_id}"
                shutter_control = userdata[control_object]
                set_topic = shutter_control.topic
                if shutter_control.group_shutter:
                    for member in shutter_control.group_members:
                        client.publish(f'{set_topic}/shutter/{member}/set_norf', payload)
                shutter_control.handle_command(payload, sendrf)
                logger.info(f"Received MQTT command to send command: {payload}")

        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(f"Error handling MQTT message: {e}")
            
    threading.Thread(target=handle_message).start()

def on_connect(client, userdata, flags, rc, properties=None):
    """Callback for when the client connects to the MQTT broker."""
    MQTT_TOPIC = os.environ.get("MQTT_TOPIC")
    if rc == 0:
        logger.info("Successfully connected to MQTT broker")
        # Subscribe to all shutter command topics using a wildcard
        client.subscribe(f'{MQTT_TOPIC}/shutter/#')
        
        # Announce that the service is online. Retain=True ensures the message is saved by the broker.
        client.publish(f"{MQTT_TOPIC}/system/availability", "online", retain=True)
        logger.info(f"Published 'online' to availability topic and subscribed to command topics.")
    else:
        logger.error(f"Failed to connect, return code {rc}")

# Main application logic
def main():
    logger.info("Starting shutter control application")
    MQTT_HOST = os.environ.get("MQTT_HOST")
    MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
    MQTT_USER = os.environ.get("MQTT_USER")
    MQTT_PASS = os.environ.get("MQTT_PASS", "")
    MQTT_TOPIC = os.environ.get("MQTT_TOPIC")
    DB_PATH = os.environ.get("DB_PATH", '/data/shutter_storage.json')

    if (MQTT_HOST is None or MQTT_PORT is None or MQTT_USER is None or MQTT_TOPIC is None):
        logger.error("Please provide MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_TOPIC in environment.")
        sys.exit(1)

    # Setup MQTT and TinyDB
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt_client.will_set(topic=f"{MQTT_TOPIC}/system/availability", payload="offline", retain=True)

    # Assign the on_connect callback
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    logger.info("Connected to MQTT broker")

    # Initialize RF Device
    rf_device = RFDevice()
    logger.info("RF device initialized")

    # Initialize TinyDB with hybrid storage
    hybrid_storage = HybridStorage(DB_PATH)
    db = TinyDB(storage=lambda: hybrid_storage)
    logger.info("Initialized TinyDB with hybrid storage")

    # Create command queue manager
    command_queue = CommandQueueManager()

    # Load shutters from TinyDB
    shutters = db.all()
    for shutter in shutters:
        shutter_control = ShutterControl(
            shutter['shutter_id'],
            rf_device,
            mqtt_client,
            MQTT_TOPIC,
            db.table('_default'),
            command_queue
        )

        # Set up Home Assistant MQTT discovery
        setup_mqtt_discovery(mqtt_client, MQTT_TOPIC, shutter['shutter_id'], shutter['shutter_name'])
        shutter_control.update_state()

        # Store the shutter control object in the userdata for MQTT callbacks
        control_object_name = f"shutter_control_{shutter['shutter_id']}"
        userdata = mqtt_client.user_data_get()
        if userdata == None:
            userdata = {}
        userdata[control_object_name] = shutter_control
        mqtt_client.user_data_set(userdata)
        

    mqtt_client.subscribe(f'{MQTT_TOPIC}/shutter/#')
    # Start MQTT loop
    mqtt_client.loop_start()
    logger.info("MQTT loop started")
    while True:
        time.sleep(0.5)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(traceback.format_exc())
        logger.critical(f"Unhandled exception in main: {e}")
