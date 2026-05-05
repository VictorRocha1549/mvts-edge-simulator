import pika
import json
import time
import random
from datetime import datetime

# Configuración de conexión al servidor Docker de RabbitMQ
RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'telemetry.gps'
VEHICLE_ID = "CAMION_001"

# Coordenadas iniciales (Simulando la mina)
lat_actual = 27.9150
lon_actual = -110.9000

def iniciar_simulacion():
    print(f"🔄 Conectando al servidor RabbitMQ en {RABBITMQ_HOST}...")
    try:
        # Se conecta al puerto 5672 de Docker
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
    except Exception as e:
        print(f"❌ Error: ¿Está encendido Docker? Detalle: {e}")
        return

    # Asegura que la "fila" donde formaremos los mensajes exista
    channel.queue_declare(queue=QUEUE_NAME)
    
    print("✅ Conexión exitosa.")
    print("🚚 Simulador de Camión Minero Iniciado. (Presiona CTRL+C para detener)\n")

    global lat_actual, lon_actual

    try:
        while True: # Ciclo infinito (El camión está encendido)
            # 1. Simular el movimiento sumando un número aleatorio a las coordenadas
            lat_actual += random.uniform(-0.0005, 0.0005)
            lon_actual += random.uniform(-0.0005, 0.0005)

            # 2. Armar el Contrato de Datos (El JSON exacto que espera Víctor en Java)
            payload = {
                "vehicle_id": VEHICLE_ID,
                "latitude": round(lat_actual, 6),
                "longitude": round(lon_actual, 6),
                "status": "en_ruta",
                "timestamp": datetime.now().isoformat()
            }

            # 3. Convertir a texto y enviar
            mensaje = json.dumps(payload)
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=mensaje
            )

            print(f"[📡] GPS Enviado: {mensaje}")

            # 4. Esperar 3 segundos antes del siguiente envío (Evita saturar la PC)
            time.sleep(3)

    except KeyboardInterrupt:
        print("\n🛑 Simulación detenida.")
    finally:
        if connection.is_open:
            connection.close()

if __name__ == '__main__':
    iniciar_simulacion()