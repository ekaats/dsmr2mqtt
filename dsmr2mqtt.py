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
    READINGS_PERISTENCE_DATA_PATH = os.environ.get(
        "READINGS_PERISTENCE_DATA_PATH", "/data/readings.json"
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
        READINGS_PERISTENCE_DATA_PATH = settings.get(
            "persistence_data_path", "data/readings.json"
        )
        LASTREADING_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%s")

print("MQTT Host:       ", MQTT_HOST)
print("MQTT Port:       ", MQTT_PORT)
print("MQTT Client ID:  ", MQTT_CLIENTID)
print("DSMR_PORT:       ", DSMR_PORT)
print("Report interval: ", REPORT_INTERVAL, "s")

current_date = datetime.combine(datetime.today(), datetime.min.time())


class ConsumptionStats:

    def __init__(
        self,
        electricity_used_tariff_low,
        electricity_used_tariff_high,
        electricity_delivered_tariff_low,
        electricity_delivered_tariff_high,
        gas_used,
    ):
        self.name = "Energy Consumption Statistics"
        self.electricity_used_tariff_low = float(electricity_used_tariff_low)
        self.electricity_used_tariff_high = float(electricity_used_tariff_high)
        self.electricity_used_today_tariff_low = float(0)
        self.electricity_used_today_tariff_high = float(0)
        self.electricity_delivered_tariff_low = float(electricity_delivered_tariff_low)
        self.electricity_delivered_tariff_high = float(
            electricity_delivered_tariff_high
        )
        self.electricity_delivered_today_tariff_low = float(0)
        self.electricity_delivered_today_tariff_high = float(0)
        self.gas_used = gas_used
        self.gas_used_today = 0
        self.gas_last_reading = 0
        self.gas_current_delivery = 0
        self.last_gas_current_consumption_report_timestamp = datetime.combine(
            datetime.today(), datetime.min.time()
        )

    def update_gas_consumption(self, gas):
        gas_reading = float(gas)
        self.gas_used_today = round(gas_reading - self.gas_used, 3)

        if (
            datetime.now() - self.last_gas_current_consumption_report_timestamp
        ).total_seconds() > GAS_CURRENT_CONSUMPTION_REPORT_INTERVAL:
            if self.gas_last_reading > 0:
                self.gas_current_delivery = round(
                    (gas_reading - self.gas_last_reading)
                    * (3600 / GAS_CURRENT_CONSUMPTION_REPORT_INTERVAL),
                    3,
                )
                self.last_gas_current_consumption_report_timestamp = datetime.now()

            self.gas_last_reading = gas_reading

    def update_electricity_consumption(self, tariff, reading):
        if tariff == "0001":
            self.electricity_used_today_tariff_low = round(
                (float(reading) - self.electricity_used_tariff_low), 3
            )

        if tariff == "0002":
            self.electricity_used_today_tariff_high = round(
                (float(reading) - self.electricity_used_tariff_high), 3
            )

    def update_electricity_delivery(self, tariff, reading):
        if tariff == "0001":
            self.electricity_delivered_today_tariff_low = round(
                (float(reading) - self.electricity_delivered_tariff_low), 3
            )

        if tariff == "0002":
            self.electricity_delivered_today_tariff_high = round(
                (float(reading) - self.electricity_delivered_tariff_high), 3
            )

    def gas_today(self):
        return self.gas_used_today

    def gas_currently_delivered(self):
        return self.gas_current_delivery

    def electricity_consumption_today(self):
        return round(
            self.electricity_used_today_tariff_high
            + self.electricity_used_today_tariff_low,
            3,
        )

    def electricity_delivered_today(self):
        return round(
            self.electricity_delivered_today_tariff_high
            + self.electricity_delivered_today_tariff_low,
            3,
        )

    def reset_daily_stats(self):
        self.electricity_used_today_tariff_low = float(0)
        self.electricity_used_today_tariff_high = float(0)
        self.electricity_delivered_today_tariff_low = float(0)
        self.electricity_delivered_today_tariff_high = float(0)
        self.gas_used_today = float(0)


class DataPersistence:
    def __init__(self) -> None:
        self.data = self.load_datafile()

    def get_value(self, key):
        return self.data[key]

    def set_value(self, key, value):
        self.data[key] = value

    def load_datafile(self):
        f = open(READINGS_PERISTENCE_DATA_PATH)
        file_data = json.load(f)
        f.close()

        return file_data

    def write_datafile(self):
        with open(READINGS_PERISTENCE_DATA_PATH, "w", encoding="utf-8") as outfile:
            self.data["file_date"] = str(datetime.now())
            json.dump(self.data, outfile)


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

    try:
        if topic == "dsmr/reading/timestamp":
            LASTREADING_TIMESTAMP = str(value)

        if topic == "dsmr/consumption/gas/delivered":
            stats.update_gas_consumption(str(value))
            client.publish(
                "dsmr/consumption/gas/read_at",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            client.publish(
                "dsmr/consumption/gas/currently_delivered",
                stats.gas_currently_delivered(),
            )
            client.publish("dsmr/day-consumption/gas", stats.gas_today())

            # also periodically update electricity totals
            client.publish(
                "dsmr/day-consumption/electricity_merged",
                stats.electricity_consumption_today(),
            )
            client.publish(
                "dsmr/day-consumption/electricity_returned_merged",
                stats.electricity_delivered_today(),
            )

        if topic == "dsmr/reading/electricity_delivered_1":
            stats.update_electricity_consumption("0001", str(value))
            client.publish(
                "dsmr/day-consumption/electricity1",
                stats.electricity_used_today_tariff_low,
            )

        if topic == "dsmr/reading/electricity_delivered_2":
            stats.update_electricity_consumption("0002", str(value))
            client.publish(
                "dsmr/day-consumption/electricity2",
                stats.electricity_used_today_tariff_high,
            )

        if topic == "dsmr/reading/electricity_returned_1":
            stats.update_electricity_delivery("0001", str(value))
            client.publish(
                "dsmr/day-consumption/electricity1_returned",
                stats.electricity_delivered_today_tariff_low,
            )

        if topic == "dsmr/reading/electricity_returned_2":
            stats.update_electricity_delivery("0002", str(value))
            client.publish(
                "dsmr/day-consumption/electricity2_returned",
                stats.electricity_delivered_today_tariff_high,
            )

        client.publish(topic, str(value))

    except KeyError:
        print(f"{topic} has no value")


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
                process("dsmr/reading/electricity_delivered_1", value=value.value)
            case "ELECTRICITY_USED_TARIFF_2":
                process("dsmr/reading/electricity_delivered_2", value=value.value)
            case "ELECTRICITY_DELIVERED_TARIFF_1":
                process("dsmr/reading/electricity_returned_1", value=value.value)
            case "ELECTRICITY_DELIVERED_TARIFF_2":
                process("dsmr/reading/electricity_returned_2", value=value.value)
            case "ELECTRICITY_ACTIVE_TARIFF":
                process("dsmr/meter-stats/electricity_tariff", value=value.value)
            case "CURRENT_ELECTRICITY_USAGE":
                process(
                    "dsmr/reading/electricity_currently_delivered", value=value.value
                )
            case "CURRENT_ELECTRICITY_DELIVERY":
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
                pass
            case "INSTANTANEOUS_ACTIVE_POWER_L2_POSITIVE":
                pass
            case "INSTANTANEOUS_ACTIVE_POWER_L3_POSITIVE":
                pass
            case "INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE":
                pass
            case "INSTANTANEOUS_ACTIVE_POWER_L2_NEGATIVE":
                pass
            case "INSTANTANEOUS_ACTIVE_POWER_L3_NEGATIVE":
                pass
            case "EQUIPMENT_IDENTIFIER_GAS":
                process("dsmr/meter-stats/gas_meter_id", value=value.value)
            case "HOURLY_GAS_METER_READING":
                process("dsmr/consumption/gas/delivered", value=value.value)
            case "POWER_EVENT_FAILURE_LOG":
                # A list of dicts with each power failure event. Not used at the moment
                pass
            case "EQUIPMENT_IDENTIFIER_GAS":
                process("dsmr/meter-stats/gas_meter_id", value=value)
            case "HOURLY_GAS_METER_READING":
                process("dsmr/consumption/gas/delivered", value=value)
            case _:
                raise Exception("Not Implemented")


client = connect_mqtt()
lastrun = datetime(2000, 1, 1)
# DSMR connection
serial_reader = SerialReader(
    device=DSMR_PORT,
    serial_settings=SERIAL_SETTINGS_V5,
    telegram_specification=telegram_specifications.V5,
)

# init stats counter
stats_persist = DataPersistence()
stats = ConsumptionStats(
    stats_persist.get_value("electricity_low_value"),
    stats_persist.get_value("electricity_high_value"),
    stats_persist.get_value("electricity_delivered_low_value"),
    stats_persist.get_value("electricity_delivered_high_value"),
    stats_persist.get_value("gas_meter_value"),
)

for telegram in serial_reader.read_as_object():
    if (datetime.now() - lastrun).seconds >= REPORT_INTERVAL:
        # reset daily stats on midnight
        if datetime.combine(datetime.today(), datetime.min.time()) > current_date:

            stats_persist.write_datafile()
            stats.reset_daily_stats()
            current_date = datetime.combine(datetime.today(), datetime.min.time())

        lastrun = datetime.now()
        publish(telegram)
