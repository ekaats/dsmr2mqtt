import os
import json

from datetime import datetime
from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V5

from paho.mqtt import client as mqtt_client

from pathlib import Path


def is_docker():
    cgroup = Path("/proc/self/cgroup")
    return (
        Path("/.dockerenv").is_file()
        or cgroup.is_file()
        and "docker" in cgroup.read_text()
    )


if is_docker() == True:
    # ENVIRONMENT VARIABLES
    MQTT_HOST = os.environ.get("MQTT_HOST", "mqtt")
    MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
    MQTT_CLIENTID = os.environ.get("MQTT_CLIENTID", "dsmr2mqtt")
    MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "mqtt")
    MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "")
    DSMR_PORT = os.environ.get("DSMR_PORT", "/dev/ttyUSB0")
    DSMR_VERSION = os.environ.get("DSMR_VERSION", 5)
    REPORT_INTERVAL = int(os.environ.get("REPORT_INTERVAL", 15))
    GAS_CURRENT_CONSUMPTION_REPORT_INTERVAL = int(
        os.environ.get("GAS_CURRENT_CONSUMPTION_REPORT_INTERVAL", 60)
    )
    LASTREADING_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%s")
else:
    import json

    with open("settings.json") as settings_file:
        settings = json.load(settings_file)
        MQTT_HOST = settings.get("mqtt_host", "mqtt")
        MQTT_PORT = settings.get("mqtt_port", 1883)
        MQTT_CLIENTID = settings.get("mqtt_client-id", "dsmr2mqtt")
        MQTT_USERNAME = settings.get("mqtt_username", None)
        MQTT_PASSWORD = settings.get("mqtt_password", None)
        DSMR_PORT = settings.get("dsmr_port", "/dev/ttyUSB0")
        DSMR_VERSION = settings.get("dsmr_version", 5)
        REPORT_INTERVAL = settings.get("reportinterval", 15)
        LASTREADING_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%s")

print("MQTT Host:       ", MQTT_HOST)
print("MQTT Port:       ", MQTT_PORT)
print("MQTT Client ID:  ", MQTT_CLIENTID)
print("DSMR_PORT:       ", DSMR_PORT)
print("Report interval: ", REPORT_INTERVAL, "s")

current_date = datetime.today()

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2, MQTT_CLIENTID)
    if MQTT_USERNAME is not None:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.connect(MQTT_HOST, MQTT_PORT)
    return client


def process(topic, value):


    if topic == "dsmr/reading/timestamp":
        LASTREADING_TIMESTAMP = str(value)

    if topic == "dsmr/consumption/gas/delivered":

        client.publish(
            "dsmr/consumption/gas/read_at",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        client.publish(
            "dsmr/consumption/gas/currently_delivered",

        )

    client.publish(topic, str(value))


def publish(telegram):
    for attr, value in telegram:
        match attr:
            case "P1_MESSAGE_HEADER":
                process("dsmr/meter-stats/dsmr_version", value=value.value)
            case "P1_MESSAGE_TIMESTAMP":
                process("dsmr/reading/timestamp", value=str(value))
            case "EQUIPMENT_IDENTIFIER":
                process("dsmr/meter-stats/dsmr_meter_id", value=value.value)
            case "ELECTRICITY_USED_TARIFF_1":
                # Meter Reading electricity delivered to client (low tariff) in 0,001 kWh
                process("dsmr/reading/electricity_delivered_1", value=value.value)

            case "ELECTRICITY_USED_TARIFF_2":
                # Meter Reading electricity delivered to client (normal tariff) in 0,001 kWh
                process("dsmr/reading/electricity_delivered_2", value=value.value)

            case "ELECTRICITY_DELIVERED_TARIFF_1":
                # Meter Reading electricity delivered by client (low tariff) in 0,001 kWh
                process("dsmr/reading/electricity_returned_1", value=value.value)

            case "ELECTRICITY_DELIVERED_TARIFF_2":
                # Meter Reading electricity delivered by client (normal tariff) in 0,001 kWh
                process("dsmr/reading/electricity_returned_2", value=value.value)

            case "ELECTRICITY_ACTIVE_TARIFF":
                # Tariff indicator electricity. The tariff indicator can be used to switch tariff
                # dependent loads e.g boilers. This is responsibility of the P1 user
                process("dsmr/meter-stats/electricity_tariff", value=value.value)

            case "CURRENT_ELECTRICITY_USAGE":
                # Actual electricity power delivered (+P) in 1 Watt resolution
                process(
                    "dsmr/reading/electricity_currently_delivered", value=value.value
                )
            case "CURRENT_ELECTRICITY_DELIVERY":
                # Actual electricity power received (-P) in 1 Watt resolution
                process(
                    "dsmr/reading/electricity_currently_returned", value=value.value
                )
            case "LONG_POWER_FAILURE_COUNT":
                process("dsmr/meter-stats/long_power_failure_count", value=value.value)
            case "SHORT_POWER_FAILURE_COUNT":
                process("dsmr/meter-stats/power_failure_count", value=value.value)
            case "VOLTAGE_SAG_L1_COUNT":
                process("dsmr/meter-stats/voltage_sag_count_l1", value=value.value)
            case "VOLTAGE_SAG_L2_COUNT":
                process("dsmr/meter-stats/voltage_sag_count_l2", value=value.value)
            case "VOLTAGE_SAG_L3_COUNT":
                process("dsmr/meter-stats/voltage_sag_count_l3", value=value.value)
            case "VOLTAGE_SWELL_L1_COUNT":
                process("dsmr/meter-stats/voltage_swell_count_l1", value=value.value)
            case "VOLTAGE_SWELL_L2_COUNT":
                process("dsmr/meter-stats/voltage_swell_count_l2", value=value.value)
            case "VOLTAGE_SWELL_L3_COUNT":
                process("dsmr/meter-stats/voltage_swell_count_l3", value=value.value)
            case "TEXT_MESSAGE_CODE":
                pass  # Not used
            case "TEXT_MESSAGE":
                pass  # Not used
            case "DEVICE_TYPE":
                process("dsmr/meter-stats/dsmr_meter_type", value=value.value)
            case "INSTANTANEOUS_VOLTAGE_L1":
                process("dsmr/reading/phase_voltage_l1", value=value.value)
            case "INSTANTANEOUS_VOLTAGE_L2":
                process("dsmr/reading/phase_voltage_l2", value=value.value)
            case "INSTANTANEOUS_VOLTAGE_L3":
                process("dsmr/reading/phase_voltage_l3", value=value.value)
            case "INSTANTANEOUS_CURRENT_L1":
                process("dsmr/reading/phase_power_current_l1", value=value.value)
            case "INSTANTANEOUS_CURRENT_L2":
                process("dsmr/reading/phase_power_current_l2", value=value.value)
            case "INSTANTANEOUS_CURRENT_L3":
                process("dsmr/reading/phase_power_current_l3", value=value.value)
            case "INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE":
                process("dsmr/reading/phase_currently_delivered_l1", value=value.value)
            case "INSTANTANEOUS_ACTIVE_POWER_L2_POSITIVE":
                process("dsmr/reading/phase_currently_delivered_l2", value=value.value)
            case "INSTANTANEOUS_ACTIVE_POWER_L3_POSITIVE":
                process("dsmr/reading/phase_currently_delivered_l3", value=value.value)
            case "INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE":
                process("dsmr/reading/phase_currently_returned_l1", value=value.value)
            case "INSTANTANEOUS_ACTIVE_POWER_L2_NEGATIVE":
                process("dsmr/reading/phase_currently_returned_l2", value=value.value)
            case "INSTANTANEOUS_ACTIVE_POWER_L3_NEGATIVE":
                process("dsmr/reading/phase_currently_returned_l3", value=value.value)
            case "EQUIPMENT_IDENTIFIER_GAS":
                process("dsmr/meter-stats/gas_meter_id", value=value.value)
            case "HOURLY_GAS_METER_READING":
                process("dsmr/consumption/gas/delivered", value=value.value)
            case "POWER_EVENT_FAILURE_LOG":
                # A list of dicts with each power failure event. Not used at the moment
                pass
            case _:
                raise Exception("Not Implemented")
                """
                These MQTT topics are used in the HA DSMR reader,
                but don't seem to have a good DSMR counterpart
                "dsmr/reading/extra_device_delivered"
                
                "dsmr/day-consumption/electricity1_cost"
                "dsmr/day-consumption/electricity2_cost"
                "dsmr/day-consumption/electricity_cost_merged"
                "dsmr/day-consumption/gas_cost"
                "dsmr/day-consumption/total_cost"
                "dsmr/day-consumption/energy_supplier_price_electricity_delivered_1"
                "dsmr/day-consumption/energy_supplier_price_electricity_delivered_2"
                "dsmr/day-consumption/energy_supplier_price_electricity_returned_2"
                "dsmr/day-consumption/energy_supplier_price_gas"
                "dsmr/day-consumption/fixed_cost"
                
                "dsmr/meter-stats/rejected_telegrams"
                
                "dsmr/current-month/electricity1"
                "dsmr/current-month/electricity2"
                "dsmr/current-month/electricity1_returned"
                "dsmr/current-month/electricity2_returned"
                "dsmr/current-month/electricity_merged"
                "dsmr/current-month/electricity_returned_merged"
                "dsmr/current-month/electricity1_cost"
                "dsmr/current-month/electricity2_cost"
                "dsmr/current-month/electricity_cost_merged"
                "dsmr/current-month/gas"
                "dsmr/current-month/gas_cost"
                "dsmr/current-month/fixed_cost"
                "dsmr/current-month/total_cost"
                
                "dsmr/current-year/electricity1"
                "dsmr/current-year/electricity2"
                "dsmr/current-year/electricity1_returned"
                "dsmr/current-year/electricity2_returned"
                "dsmr/current-year/electricity_merged"
                "dsmr/current-year/electricity_returned_merged"
                "dsmr/current-year/electricity1_cost"
                "dsmr/current-year/electricity2_cost"
                "dsmr/current-year/electricity_cost_merged"
                "dsmr/current-year/gas"
                "dsmr/current-year/gas_cost"
                "dsmr/current-year/fixed_cost"
                "dsmr/current-year/total_cost"
                
                "dsmr/consumption/quarter-hour-peak-electricity/average_delivered"
                "dsmr/consumption/quarter-hour-peak-electricity/read_at_start"
                "dsmr/consumption/quarter-hour-peak-electricity/read_at_end"
                
                """


client = connect_mqtt()
lastrun = datetime(2000, 1, 1)
# DSMR connection
serial_reader = SerialReader(
    device=DSMR_PORT,
    serial_settings=SERIAL_SETTINGS_V5,
    telegram_specification=telegram_specifications.V5,
)

# init stats counter

try:
    serial_obj = serial_reader.read_as_object()
except UnicodeDecodeError:
    serial_obj = None

if serial_obj is not None:
    for telegram in serial_obj:

        if (datetime.now() - lastrun).seconds >= REPORT_INTERVAL:
            # reset daily stats on midnight
            if datetime.now().hour == 0:
                current_date = datetime.combine(datetime.today(), datetime.min.time())

            if datetime.now().day == 1:
                pass # todo: report on current month

            if datetime.now().month == 1:
                pass # todo: report on current year

            lastrun = datetime.now()
            publish(telegram=telegram)
