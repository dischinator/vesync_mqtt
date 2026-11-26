import asyncio
import json
import logging
import logging.config
import re
import sys
import time
from pyvesync import VeSync
from configparser import ConfigParser
import paho.mqtt.client as mqtt

# Globale Konstanten
logger = logging.getLogger(__name__)

# Home Assistant: Sensoren, die automatisch angelegt werden, sobald das
# entsprechende Statusfeld einen Wert liefert (Key = Feld aus device.state.to_dict()).
HA_SENSORS = {
    "filter_life": {
        "name": "Filterlebensdauer",
        "unit_of_measurement": "%",
        "state_class": "measurement",
        "icon": "mdi:air-filter",
        "entity_category": "diagnostic",
    },
    "pm25": {
        "name": "PM2.5",
        "device_class": "pm25",
        "unit_of_measurement": "µg/m³",
        "state_class": "measurement",
    },
    "pm1": {
        "name": "PM1",
        "device_class": "pm1",
        "unit_of_measurement": "µg/m³",
        "state_class": "measurement",
    },
    "pm10": {
        "name": "PM10",
        "device_class": "pm10",
        "unit_of_measurement": "µg/m³",
        "state_class": "measurement",
    },
    "aq_percent": {
        "name": "Luftqualität",
        "unit_of_measurement": "%",
        "state_class": "measurement",
        "icon": "mdi:air-filter",
    },
    "air_quality_level": {
        "name": "Luftqualitätsstufe",
        "device_class": "enum",
        "options": ["excellent", "good", "moderate", "poor", "unknown"],
        "value_template": (
            "{% set m = {1: 'excellent', 2: 'good', 3: 'moderate', 4: 'poor'} %}"
            "{{ m.get(value | int(0), 'unknown') }}"
        ),
        "icon": "mdi:air-filter",
    },
    "humidity": {
        "name": "Luftfeuchtigkeit",
        "device_class": "humidity",
        "unit_of_measurement": "%",
        "state_class": "measurement",
    },
    "co2": {
        "name": "CO2",
        "device_class": "carbon_dioxide",
        "unit_of_measurement": "ppm",
        "state_class": "measurement",
    },
}

class VesyncToMqtt:

    fanbuffer = dict()
    Config = ConfigParser()
    manager = None
    mqtt_client = None
    polling_interval = 60
    mqtt_base_topic = "vesync"
    loop = None
    status_lock_sec = 200
    ha_enabled = True
    ha_discovery_prefix = "homeassistant"

    def __init__(self):
        self.logger = logging.getLogger("VesyncToMqtt")
        # (device.cid, statusfeld) -> Zeitpunkt des letzten Befehls
        self.lock_status_updates = {}
        # Home Assistant: bereits veröffentlichte Discovery-Configs (Topic -> Payload)
        # und zuletzt gesendete Statuswerte (Topic -> Payload), um sie nach einem
        # HA-Neustart erneut senden zu können. Nur im asyncio-Thread verwenden.
        self.ha_published = {}
        self.last_payloads = {}
        self.init_config()
        self.init_interval()
        self.init_homeassistant()
        # MQTT wird erst nach erfolgreichem VeSync-Login in create() initialisiert,
        # damit eingehende Kommandos nicht auf einen leeren Manager treffen.
        self.logger.debug("VesyncToMqtt-Instanz synchron initialisiert.")

    def init_config(self):
        try:
            self.Config.read("vesync_mqtt.ini")
        except Exception as e:
            self.logger.error("Fehler beim Lesen der Konfigurationsdatei: %s", e)

    def init_interval(self):
        # Polling-Intervall
        try:
            self.polling_interval = self.Config.getint("vesync", "polling_interval_sec")
        except:
            self.logger.warning(
                "Polling-Intervalle nicht in INI gefunden oder ungültig. Verwende Default-Werte."
            )

        # Sperrzeit für Status-Updates nach einem Befehl (API liefert neue Werte verzögert)
        self.status_lock_sec = self.Config.getint(
            "vesync", "status_lock_sec", fallback=self.status_lock_sec
        )

    def init_homeassistant(self):
        """Liest die Home-Assistant-Einstellungen (MQTT Discovery)."""
        self.ha_enabled = self.Config.getboolean(
            "homeassistant", "enabled", fallback=self.ha_enabled
        )
        self.ha_discovery_prefix = self.Config.get(
            "homeassistant", "discovery_prefix", fallback=self.ha_discovery_prefix
        ).strip("/")

    def init_mqtt(self):
        """Initialisiert und verbindet den synchronen MQTT-Client."""
        try:
            host = self.Config.get("mqtt", "host")
            port = self.Config.getint("mqtt", "port")
            user = self.Config.get("mqtt", "username")
            pwd = self.Config.get("mqtt", "password")
            self.mqtt_base_topic = self.Config.get("mqtt", "base_topic").strip('/')
            use_tls = self.Config.getboolean("mqtt", "tls", fallback=False)
            ca_certs = self.Config.get("mqtt", "ca_certs", fallback=None)

            self.mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

            self.mqtt_client.username_pw_set(user, pwd)
            self.mqtt_client.on_connect = self.on_mqtt_connect
            self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
            self.mqtt_client.on_message = self.on_mqtt_message
            # Exceptions in Callbacks dürfen den Netzwerk-Thread nicht beenden
            self.mqtt_client.suppress_exceptions = True
            self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)

            # TLS mit regulärer Zertifikatsprüfung (z. B. Let's Encrypt).
            # Ohne ca_certs werden die System-CAs verwendet; der Hostname wird
            # gegen das Zertifikat geprüft, daher muss "host" der Zertifikatsname sein.
            if use_tls:
                self.mqtt_client.tls_set(ca_certs=ca_certs)

            # LWT
            self.mqtt_client.will_set(
                topic=f"{self.mqtt_base_topic}/availability",
                payload="offline",
                qos=1,
                retain=True,
            )

            self.logger.info("Verbinde mit MQTT-Broker: %s:%d", host, port)
            # connect_async: Verbindungsaufbau im Loop-Thread, paho versucht es bei
            # nicht erreichbarem Broker (auch beim Start) automatisch erneut.
            self.mqtt_client.connect_async(host, port, 60)
            self.mqtt_client.loop_start() # Startet separaten Thread für MQTT-Kommunikation
        except Exception as e:
            self.logger.critical("Fehler bei der MQTT-Initialisierung: %s", e)
            self.mqtt_client = None

    def on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        # Prüfe, ob der ReasonCode-Wert 0 ist (Erfolg)
        if reason_code == 0:
            self.logger.info("MQTT-Verbindung erfolgreich hergestellt.")
            self.subscribe_control_topics()

            # update LWT
            self.mqtt_client.publish(
                topic=f"{self.mqtt_base_topic}/availability",
                payload="online",
                qos=1,
                retain=True,
            )

            # Home Assistant Discovery (neu) senden, z. B. nach Broker-Neustart
            self.schedule_ha_resync()
        else:
            self.logger.error("MQTT-Verbindung fehlgeschlagen, Grund: %s", reason_code)

    def on_mqtt_disconnect(self, client, userdata, disconnect_reason_code, properties, *args):
        # Logik: Wir verwenden disconnect_reason_code, das der V2-Standard ist
        self.logger.warning("MQTT-Verbindung getrennt. Grundcode: %s", disconnect_reason_code)

    def subscribe_control_topics(self):
        """Abonniert das Wildcard Control-Topic für alle möglichen Geräte."""
        if not self.mqtt_client or not self.mqtt_client.is_connected():
            self.logger.warning("MQTT-Client nicht verbunden, kann Wildcard-Topic nicht abonnieren.")
            return

        # Wildcard-Topic: basetopic/+/set/#
        # '+/' ersetzt den Gerätenamen
        # '# ' am Ende fängt alle Kommandos (on, off, mode, fan_speed, etc.)
        wildcard_topic = f"{self.mqtt_base_topic}/+/set/#"

        result, mid = self.mqtt_client.subscribe(wildcard_topic, qos=1) # QOS=1 für Steuerbefehle

        if result == mqtt.MQTT_ERR_SUCCESS:
            self.logger.info("Wildcard Control-Topic '%s' erfolgreich abonniert.", wildcard_topic)
        else:
            self.logger.error("Fehler beim Abonnieren des Wildcard-Topics: %d", result)

        if self.ha_enabled:
            # Birth-Message von Home Assistant: nach HA-Neustart Discovery erneut senden
            ha_status_topic = f"{self.ha_discovery_prefix}/status"
            result, mid = self.mqtt_client.subscribe(ha_status_topic, qos=1)
            if result == mqtt.MQTT_ERR_SUCCESS:
                self.logger.info("Home-Assistant-Status-Topic '%s' abonniert.", ha_status_topic)
            else:
                self.logger.error("Fehler beim Abonnieren von '%s': %d", ha_status_topic, result)

    def on_mqtt_message(self, client, userdata, message):
        """Behandelt eingehende MQTT-Steuernachrichten und prüft die Geräteexistenz."""
        topic = message.topic

        try:
            try:
                payload = message.payload.decode('utf-8').strip()
            except UnicodeDecodeError:
                self.logger.warning("Ungültiger (nicht UTF-8) Payload auf %s ignoriert.", topic)
                return

            if self.ha_enabled and topic == f"{self.ha_discovery_prefix}/status":
                if payload == "online":
                    self.logger.info("Home Assistant ist online, sende Discovery erneut.")
                    self.schedule_ha_resync()
                return

            self.logger.debug("MQTT-Kommando empfangen: %s -> %s", topic, payload)

            # Topic-Struktur: basetopic/DEVICE_NAME/set/COMMAND
            parts = topic.split('/')

            # Mindestanforderung: basetopic, DEVICE_NAME, set, COMMAND
            if len(parts) < 4 or parts[-2] != "set":
                self.logger.warning("Unerwartetes Topic-Format ignoriert: %s", topic)
                return

            device_name_topic = parts[-3] # Dies ist der Platzhalter, den wir prüfen müssen
            command = parts[-1]

            if self.loop is None or self.manager is None:
                self.logger.warning("VeSync noch nicht initialisiert, Kommando %s ignoriert.", topic)
                return

            # Gerätesuche und Ausführung laufen im asyncio-Thread, damit
            # manager.devices nicht parallel zu manager.update() gelesen wird.
            asyncio.run_coroutine_threadsafe(
                self.execute_command(device_name_topic, command, payload), self.loop
            )
            self.logger.debug("Befehl %s für Gerät %s an asyncio-Schleife übergeben.", command, device_name_topic)

        except Exception as e:
            self.logger.error("Fehler bei der Verarbeitung des MQTT-Kommandos %s: %s", topic, e)

    async def execute_command(self, device_name_topic, command, payload):
        """Führt ein Kommando im asyncio-Thread aus und sendet bei Erfolg Feedback."""
        try:
            # Finde das VeSync-Gerät und prüfe die Existenz
            # device_name_topic muss mit d.device_name.replace(" ", "_") übereinstimmen
            device = next((d for d in self.manager.devices
                           if d.device_name.replace(" ", "_").lower() == device_name_topic.lower()), None)

            if not device:
                self.logger.warning("Gerät '%s' in VeSync-Liste nicht gefunden.", device_name_topic)
                return

            self.logger.info("Führe Befehl '%s' -> '%s' für %s aus.", command, payload, device.device_name)

            # feedback: Statusfelder, die nach erfolgreichem Befehl gesendet und gesperrt werden
            if command == "power" and payload in ("on", "off"):
                success = await (device.turn_on() if payload == "on" else device.turn_off())
                feedback = {"device_status": payload}
            elif command == "display" and payload in ("on", "off"):
                success = await (device.turn_on_display() if payload == "on" else device.turn_off_display())
                feedback = {"display_state": payload}
            elif command == "mode" and payload in self._device_modes(device):
                success = await device.set_mode(payload)
                feedback = {"mode": payload}
            elif command == "fan_level" and payload == "0":
                # Home Assistant sendet bei 0 % Lüfterstufe 0 -> ausschalten
                success = await device.turn_off()
                feedback = {"device_status": "off"}
            elif command == "fan_level" and payload in self._device_fan_levels(device):
                success = await device.set_fan_speed(int(payload))
                # fan_level setzen impliziert manuellen Modus und eingeschaltetes Gerät
                feedback = {"fan_level": payload, "mode": "manual", "device_status": "on"}
            elif (
                command == "nightlight"
                and payload in [str(m) for m in (getattr(device, "nightlight_modes", None) or [])]
            ):
                success = await device.set_nightlight_mode(payload)
                feedback = {"nightlight_status": payload}
            else:
                self.logger.warning(
                    "Unbekannter Befehl '%s' -> '%s' für %s", command, payload, device.device_name
                )
                return

            # pyvesync meldet Fehler per Rückgabewert False, nicht per Exception
            if not success:
                self.logger.error(
                    "VeSync-Befehl '%s' -> '%s' für %s fehlgeschlagen.", command, payload, device.device_name
                )
                return

            device_name = device.device_name.replace(" ", "_")
            now = int(time.time())
            for key, value in feedback.items():
                # Sperre gegen in der API noch nicht aktualisierte Werte, pro Gerät und Feld
                self.lock_status_updates[(device.cid, key)] = now

                topic = f"{self.mqtt_base_topic}/{device_name}/{key}"
                self.last_payloads[topic] = str(value)
                if self.mqtt_client and self.mqtt_client.is_connected():
                    self.mqtt_client.publish(topic, str(value), qos=0, retain=False)
                    self.logger.debug("Feedback-Publish: %s -> %s", topic, value)

        except Exception as e:
            self.logger.error(
                "Fehler bei der Ausführung des VeSync-Befehls %s für %s: %s", command, device_name_topic, e
            )

    # ------------------------------------------------------------------
    # Home Assistant MQTT Discovery
    # Die bestehenden Topics (basetopic/GERÄT/FELD bzw. basetopic/GERÄT/set/KOMMANDO)
    # werden als State- und Command-Topics verwendet und bleiben unverändert.
    # ------------------------------------------------------------------

    @staticmethod
    def _device_fan_levels(device):
        levels = getattr(device, "fan_levels", None) or [1, 2, 3, 4]
        return [str(int(level)) for level in levels]

    @staticmethod
    def _device_modes(device):
        modes = getattr(device, "modes", None) or ["auto", "manual"]
        return [str(mode) for mode in modes]

    @staticmethod
    def _is_purifier(device):
        return hasattr(device, "fan_levels") and hasattr(device, "modes")

    def _ha_build_configs(self, device, state_dict):
        """Erzeugt die Discovery-Configs (Topic -> Payload-Dict) für ein Gerät."""
        device_name = device.device_name.replace(" ", "_")
        base = f"{self.mqtt_base_topic}/{device_name}"
        node_id = "vesync_" + re.sub(r"[^a-zA-Z0-9_-]", "_", str(device.cid))

        device_info = {
            "identifiers": [node_id],
            "name": device.device_name,
            "manufacturer": "Levoit / VeSync",
            "model": device.device_type,
        }
        if getattr(device, "current_firm_version", None):
            device_info["sw_version"] = str(device.current_firm_version)
        if getattr(device, "mac_id", None):
            device_info["connections"] = [["mac", str(device.mac_id)]]

        common = {
            "device": device_info,
            "origin": {"name": "vesync_mqtt"},
            "availability_mode": "all",
            "availability": [
                {"topic": f"{self.mqtt_base_topic}/availability"},
                {
                    "topic": f"{base}/connection_status",
                    "payload_available": "online",
                    "payload_not_available": "offline",
                },
            ],
        }

        configs = {}

        def add(component, object_id, payload):
            payload = {**common, "unique_id": f"{node_id}_{object_id}", **payload}
            configs[f"{self.ha_discovery_prefix}/{component}/{node_id}/{object_id}/config"] = payload

        if self._is_purifier(device):
            fan = {
                "name": None,  # Entity heißt wie das Gerät
                "state_topic": f"{base}/device_status",
                "command_topic": f"{base}/set/power",
                "payload_on": "on",
                "payload_off": "off",
            }
            levels = [int(level) for level in self._device_fan_levels(device)]
            if levels:
                fan.update({
                    "percentage_state_topic": f"{base}/fan_level",
                    "percentage_command_topic": f"{base}/set/fan_level",
                    "speed_range_min": min(levels),
                    "speed_range_max": max(levels),
                })
            # "manual" entspricht in HA einer gesetzten Lüfterstufe ohne Preset
            presets = [m for m in self._device_modes(device) if m != "manual"]
            if presets:
                fan.update({
                    "preset_mode_state_topic": f"{base}/mode",
                    "preset_mode_command_topic": f"{base}/set/mode",
                    "preset_modes": presets,
                    "preset_mode_value_template": (
                        "{% if value in " + json.dumps(presets) + " %}{{ value }}"
                        "{% else %}None{% endif %}"
                    ),
                })
            add("fan", "fan", fan)

            if state_dict.get("display_state") is not None:
                add("switch", "display", {
                    "name": "Display",
                    "icon": "mdi:monitor",
                    "entity_category": "config",
                    "state_topic": f"{base}/display_state",
                    "command_topic": f"{base}/set/display",
                    "payload_on": "on",
                    "payload_off": "off",
                    "state_on": "on",
                    "state_off": "off",
                })

            nightlight_modes = [str(m) for m in (getattr(device, "nightlight_modes", None) or [])]
            if nightlight_modes and getattr(device, "supports_nightlight", False):
                add("select", "nightlight", {
                    "name": "Nachtlicht",
                    "icon": "mdi:lightbulb-night",
                    "entity_category": "config",
                    "state_topic": f"{base}/nightlight_status",
                    "command_topic": f"{base}/set/nightlight",
                    "options": nightlight_modes,
                })

        for key, sensor in HA_SENSORS.items():
            if state_dict.get(key) is not None:
                add("sensor", key, {"state_topic": f"{base}/{key}", **sensor})

        return configs

    def ha_publish_discovery(self):
        """Sendet neue/geänderte Discovery-Configs. Liefert True, wenn etwas gesendet wurde."""
        if not self.ha_enabled or not self.manager:
            return False
        if not self.mqtt_client or not self.mqtt_client.is_connected():
            return False

        sent = False
        for device in self.manager.devices:
            try:
                state = getattr(device, "state", None)
                state_dict = state.to_dict() if state is not None else {}
                configs = self._ha_build_configs(device, state_dict)
            except Exception as e:
                self.logger.error(
                    "Fehler beim Erstellen der HA-Discovery für %s: %s", device.device_name, e
                )
                continue

            for topic, payload in configs.items():
                payload_json = json.dumps(payload, ensure_ascii=False)
                if self.ha_published.get(topic) == payload_json:
                    continue
                self.mqtt_client.publish(topic, payload_json, qos=1, retain=True)
                self.ha_published[topic] = payload_json
                self.logger.debug("HA-Discovery: %s", topic)
                sent = True
        return sent

    async def ha_resync(self):
        """Discovery komplett neu senden und letzte Statuswerte wiederholen."""
        self.ha_published.clear()
        if not self.ha_publish_discovery():
            return
        self.logger.info("Home-Assistant-Discovery gesendet.")
        # HA abonniert die State-Topics erst nach Empfang der Config
        await asyncio.sleep(2)
        if self.mqtt_client and self.mqtt_client.is_connected():
            for topic, payload in list(self.last_payloads.items()):
                self.mqtt_client.publish(topic, payload, qos=0, retain=False)

    def schedule_ha_resync(self):
        """Thread-sicher aus dem MQTT-Thread aufrufbar."""
        if not self.ha_enabled or self.loop is None or self.manager is None:
            return
        asyncio.run_coroutine_threadsafe(self.ha_resync(), self.loop)

    async def _async_init(self):
        """Asynchrone Initialisierung: VeSync-Login durchführen."""
        self.logger.info("Starte VeSync-Login...")
        self.manager = VeSync(
            username=self.Config.get("vesync", "username"),
            password=self.Config.get("vesync", "password"),
        )
        await self.manager.login()

        if not self.manager.enabled:
            self.logger.critical(
                "VeSync-Login fehlgeschlagen. Bitte Zugangsdaten prüfen."
            )
            return False

        self.logger.info("VeSync-Login erfolgreich.")
        await self.manager.get_devices()
        return True

    async def update_devices_data(self):
        """Aktualisiert Gerätestatus und sendet Statusvariablen an MQTT."""
        if not self.manager or not self.manager.enabled:
            self.logger.error(
                "VeSync Manager ist nicht initialisiert oder nicht angemeldet. Überspringe Aktualisierung."
            )
            return

        if not self.mqtt_client or not self.mqtt_client.is_connected():
            self.logger.warning(
                "MQTT-Client ist nicht verbunden. Status-Update wird nur lokal durchgeführt, keine Veröffentlichung."
            )

        self.logger.info("Starte Gerätestatus-Aktualisierung...")

        await self.manager.update()

        # Home Assistant: neue Entities (z. B. erstmals gelieferte Sensorwerte) anmelden
        try:
            if self.ha_publish_discovery():
                self.logger.info("Neue Home-Assistant-Discovery-Configs gesendet.")
                # HA abonniert die State-Topics erst nach Empfang der Config
                await asyncio.sleep(2)
        except Exception as e:
            self.logger.error("Fehler bei der Home-Assistant-Discovery: %s", e)

        updated_count = 0
        published_count = 0

        for device in self.manager.devices:
            updated_count += 1
            device_name = device.device_name.replace(" ", "_")

            if hasattr(device, "state") and device.state is not None:
                try:
                    state_dict = device.state.to_dict()
                    for key, value in state_dict.items():

                        if (
                            value is None
                            or "_set_" in key
                            # Falls die Variable "lock_status_updates" gesetzt wurde und das Feld in dieser Liste enthalten
                            # ist, wird das Feld nicht gesendet für eine gewisse zeit, da die API erst nach einer weile den geänderten wert
                            # liefert
                            or (
                                int(time.time()) - self.lock_status_updates.get((device.cid, key), 0)
                                < self.status_lock_sec
                            )
                        ):
                            continue

                        # Erstelle das MQTT-Topic: vesync/GERÄTENAME/STATUSFELD
                        topic = f"{self.mqtt_base_topic}/{device_name}/{key}"

                        # Sicherstellen, dass der Payload ein String ist
                        payload = str(value)
                        self.last_payloads[topic] = payload

                        if self.mqtt_client and self.mqtt_client.is_connected():
                            self.mqtt_client.publish(
                                topic, payload, qos=0, retain=False
                            )
                            self.logger.debug("MQTT-Publish: %s -> %s", topic, payload)
                            published_count += 1

                except Exception as e:
                    self.logger.error(
                        "Fehler beim Serialisieren/Veröffentlichen des Status von Gerät %s: %s",
                        device.device_name,
                        e,
                    )
            else:
                self.logger.warning(
                    "Gerät %s hat keinen verfügbaren Status.", device.device_name
                )

        self.logger.info(
            "Aktualisierung abgeschlossen. %d Geräte verarbeitet. %d MQTT-Nachrichten gesendet.",
            updated_count,
            published_count,
        )

    async def polling_loop(self, interval, task_func, task_name):
        """Generische Schleife für die Endlosschleife mit festem Intervall."""
        self.logger.info(
            "Starte %s-Schleife. Intervall: %d Sekunden.", task_name, interval
        )
        while True:
            try:
                await task_func()
            except Exception as e:
                self.logger.error("Fehler in der %s-Schleife: %s", task_name, e)

            await asyncio.sleep(interval)

    async def start_polling(self):
        """Startet beide Polling-Schleifen parallel."""

        status_task = asyncio.create_task(
            self.polling_loop(
                self.polling_interval, self.update_devices_data, "Status-Update"
            )
        )

        await asyncio.gather(status_task)

    @classmethod
    async def create(cls):
        instance = cls()

        # EVENT LOOP vom Haupt-Thread holen
        current_loop = asyncio.get_running_loop()
        instance.loop = current_loop  # <<< HIER WIRD DIE LOOP GESPEICHERT für on_mqtt_message

        if await instance._async_init():
            # Erst jetzt verbinden/abonnieren: Manager und Geräteliste sind bereit
            instance.init_mqtt()
            if instance.mqtt_client is None:
                return None
            return instance
        return None


async def main():
    # Logging Konfiguration laden
    try:
        defaults = {"sys.stdout": sys.stdout, "sys.stderr": sys.stderr}
        logging.config.fileConfig(
            "vesync_mqtt.ini", disable_existing_loggers=False, defaults=defaults
        )
        logger.info("Logging erfolgreich aus vesync_mqtt.ini konfiguriert.")
    except Exception as e:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        logger.error(
            "FEHLER beim Laden der Logging-Konfiguration aus vesync_mqtt.ini: %s", e
        )
        logger.warning("Verwende Basis-Logging-Konfiguration.")

    # Hauptlogik starten
    logger.info("Initialisiere VesyncToMqtt...")
    vesync_to_mqtt = await VesyncToMqtt.create()

    if vesync_to_mqtt:
        logger.info(
            "VesyncToMqtt-Instanz vollständig initialisiert und bereit. Starte Polling-Tasks..."
        )
        await vesync_to_mqtt.start_polling()
    else:
        logger.critical("Initialisierung fehlgeschlagen. Polling wird nicht gestartet.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Programm durch Benutzer unterbrochen (Ctrl+C). Beende.")
    except Exception as e:
        logger.critical("Unbehandelter Fehler in asyncio.run: %s", e)
