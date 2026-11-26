# vesync_mqtt

Bridge zwischen der VeSync-Cloud (Levoit-Luftreiniger u. a.) und einem MQTT-Broker.
Der Dienst fragt die Gerätestatus regelmäßig über [pyvesync](https://github.com/webdjoe/pyvesync) ab,
veröffentlicht sie als MQTT-Topics und nimmt Steuerbefehle per MQTT entgegen.
Optional werden die Geräte per MQTT Discovery automatisch in Home Assistant angelegt.

## Funktionen

- Regelmäßiges Polling aller Geräte des VeSync-Kontos (Intervall konfigurierbar)
- Ein Topic pro Statusfeld: `<base_topic>/<Gerätename>/<Feld>`
- Steuerung per `<base_topic>/<Gerätename>/set/<Kommando>`
- Verfügbarkeit (Last Will): `<base_topic>/availability` = `online` / `offline` (retained)
- Home Assistant MQTT Discovery (Lüfter, Display-Schalter, Nachtlicht, Sensoren)
- TLS mit Zertifikatsprüfung, automatischer Reconnect zum Broker
- Docker-/Podman-Image mit gehärteter Compose-Konfiguration

## Voraussetzungen

- Python 3.11 (oder Container-Laufzeit: Docker/Podman)
- VeSync-Konto (Zugangsdaten der App)
- MQTT-Broker

Abhängigkeiten ([requirements.txt](requirements.txt)): `paho-mqtt 2.1.x`, `pyvesync 3.3.x`

## Konfiguration

```sh
cp vesync_mqtt.ini.sample vesync_mqtt.ini
```

Danach Zugangsdaten eintragen. Die Datei wird beim Start aus dem aktuellen Arbeitsverzeichnis gelesen
und enthält neben den Einstellungen auch die Logging-Konfiguration. `*.ini` ist in `.gitignore` eingetragen.

| Sektion | Schlüssel | Bedeutung | Default |
|---|---|---|---|
| `vesync` | `username`, `password` | Login des VeSync-Kontos | – |
| `vesync` | `polling_interval_sec` | Abfrageintervall in Sekunden | `60` |
| `vesync` | `status_lock_sec` | Sperrzeit für Statuswerte nach einem Befehl (die API liefert geänderte Werte verzögert) | `200` |
| `mqtt` | `host`, `port` | Broker-Adresse | – |
| `mqtt` | `username`, `password` | Broker-Login | – |
| `mqtt` | `base_topic` | Basis-Topic | `vesync` |
| `mqtt` | `tls` | TLS aktivieren; `host` muss dann dem Namen im Zertifikat entsprechen | `false` |
| `mqtt` | `ca_certs` | Optionale eigene CA-Datei (sonst System-CAs) | – |
| `homeassistant` | `enabled` | MQTT Discovery aktivieren | `true` |
| `homeassistant` | `discovery_prefix` | Discovery-Präfix von Home Assistant | `homeassistant` |

Das Log-Level wird in `[logger_VesyncToMqtt]` eingestellt (`INFO` bzw. `DEBUG` für ausführliche Ausgabe).

## Start

### Lokal

```sh
python -m venv venv
. venv/bin/activate
pip install -r requirements.txt
python vesync_mqtt.py
```

### Container (Podman/Docker)

```sh
podman build -t localhost/vesync-mqtt:latest .
podman compose up -d
```

Hinweise zur [docker-compose.yaml](docker-compose.yaml):

- Die `vesync_mqtt.ini` wird schreibgeschützt nach `/app/vesync_mqtt.ini` gemountet und muss für UID `10001` lesbar sein.
- Der Container läuft unprivilegiert, mit schreibgeschütztem Root-Dateisystem, ohne Capabilities und mit Ressourcenlimits.
- Da das Dateisystem schreibgeschützt ist, im Container den `fileHandler` aus der Logging-Konfiguration entfernen
  (aus `[handlers] keys` und `[logger_root] handlers`) und nur auf stdout loggen. Logs dann per `podman logs vesync_mqtt`.

## MQTT-Topics

Gerätenamen werden mit `_` statt Leerzeichen verwendet (z. B. `Wohnzimmer Luftreiniger` → `Wohnzimmer_Luftreiniger`).
Beim Empfang von Befehlen wird Groß-/Kleinschreibung ignoriert.

### Status

`<base_topic>/<Gerätename>/<Feld>` – alle Felder aus dem Gerätestatus von pyvesync, z. B.:

| Feld | Beispielwerte |
|---|---|
| `device_status` | `on`, `off` |
| `connection_status` | `online`, `offline` |
| `mode` | `auto`, `manual`, `sleep`, … |
| `fan_level` | `1` … `4` |
| `display_state` | `on`, `off` |
| `filter_life` | Prozent |
| `pm25`, `pm1`, `pm10`, `air_quality_level`, `humidity`, `co2` | je nach Gerät |

Die Status-Nachrichten werden nicht retained gesendet. Nach einem Befehl werden die betroffenen Felder sofort mit dem neuen
Wert veröffentlicht und für `status_lock_sec` Sekunden nicht durch Polling-Werte überschrieben.

### Befehle

`<base_topic>/<Gerätename>/set/<Kommando>` mit Payload:

| Kommando | Payload | Wirkung |
|---|---|---|
| `power` | `on` / `off` | Gerät ein-/ausschalten |
| `display` | `on` / `off` | Display ein-/ausschalten |
| `mode` | ein vom Gerät unterstützter Modus (z. B. `auto`, `manual`, `sleep`) | Modus setzen |
| `fan_level` | unterstützte Stufe (z. B. `1`–`4`); `0` schaltet aus | Lüfterstufe setzen (impliziert `manual` und `on`) |
| `nightlight` | ein vom Gerät unterstützter Nachtlicht-Modus | Nachtlicht setzen |

Beispiel:

```sh
mosquitto_pub -h mqtt.example.com -t vesync/Wohnzimmer_Luftreiniger/set/fan_level -m 3
```

## Home Assistant

Bei `enabled=true` werden Discovery-Configs (retained) unter `<discovery_prefix>/<component>/vesync_<cid>/<objekt>/config` gesendet:

- **Fan** (Luftreiniger): Ein/Aus, Lüfterstufe als Prozent, Modi als Presets
- **Switch** „Display“, sofern das Gerät einen Display-Status liefert
- **Select** „Nachtlicht“, sofern unterstützt
- **Sensoren** für Filterlebensdauer, PM2.5/PM1/PM10, Luftqualität, Luftqualitätsstufe, Luftfeuchtigkeit und CO2 –
  jeweils erst, wenn das Gerät einen Wert dafür liefert

Die Entities sind verfügbar, wenn sowohl die Bridge (`<base_topic>/availability`) als auch das Gerät
(`connection_status`) online sind. Nach einem Neustart von Home Assistant (`<discovery_prefix>/status` = `online`)
oder des Brokers werden Discovery und die letzten Statuswerte automatisch erneut gesendet.
Die bestehenden Topics bleiben dabei unverändert und können weiterhin direkt genutzt werden.
