# Verwende ein offizielles Python-Image als Basis
FROM python:3.13-slim

# Python-Optimierung
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY vesync_mqtt.py .

# Unprivilegierter User, kein Login-Shell
RUN useradd -r -u 10001 -s /usr/sbin/nologin app
USER 10001

CMD ["python", "vesync_mqtt.py"]
