import pika
import json
import time
import requests
import threading

# --- CONFIGURACIÓN ---
RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'telemetry.gps'
BACKEND_URL = 'http://localhost:8080/api/semaforos'

START = (27.923859, -110.9042787)
END = (27.9243607, -110.9169012)

def obtener_estado_semaforo(id_semaforo):
    try:
        response = requests.get(BACKEND_URL)
        if response.status_code == 200:
            for s in response.json():
                if s['id'] == id_semaforo:
                    return s['estado']
    except:
        pass
    return "VERDE"

# Ahora la función recibe un "delay" para no salir todos al mismo tiempo
def simular_camion(id_camion, conductor, material, delay):
    time.sleep(delay) # Espera su turno para salir de la mina
    print(f"🟢 {id_camion} iniciando ruta...")

    # Pika (RabbitMQ) requiere una conexión independiente por cada hilo (camión)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    SEMAFOROS_CHECKPOINTS = [
        {"id": 1, "pct": 0.33, "pasado": False},
        {"id": 2, "pct": 0.66, "pasado": False}
    ]

    pasos = 100 
    for i in range(pasos + 1):
        pct = i / pasos
        curr_lat = START[0] + (END[0] - START[0]) * pct
        curr_lng = START[1] + (END[1] - START[1]) * pct

        # Lógica del semáforo
        for sem in SEMAFOROS_CHECKPOINTS:
            if not sem["pasado"] and pct >= sem["pct"]:
                estado = obtener_estado_semaforo(sem["id"])
                while estado == "ROJO":
                    payload = {
                        "vehicle_id": id_camion,
                        "latitude": curr_lat,
                        "longitude": curr_lng,
                        "status": "detenido_por_semaforo",
                        "semaforo_id": sem["id"],
                        "speed": 0,
                        "weight": 40.5,
                        "driver": conductor,
                        "material": material,
                        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                    }
                    channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=json.dumps(payload))
                    print(f"⚠️ {id_camion} detenido en Semáforo {sem['id']} (ROJO)")
                    time.sleep(2)
                    estado = obtener_estado_semaforo(sem["id"])
                
                sem["pasado"] = True 

        # En movimiento
        payload = {
            "vehicle_id": id_camion,
            "latitude": curr_lat,
            "longitude": curr_lng,
            "status": "en_ruta",
            "speed": 45,
            "weight": 40.5,
            "driver": conductor,
            "material": material,
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        }
        channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=json.dumps(payload))
        time.sleep(1)

    # Llegada
    payload_llegada = {
        "vehicle_id": id_camion,
        "latitude": END[0],
        "longitude": END[1],
        "status": "llegada", 
        "speed": 0,          
        "weight": 40.5,
        "driver": conductor,
        "material": material,
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    }
    channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=json.dumps(payload_llegada))
    print(f"🏁 {id_camion} llegó a su destino y descargó {material}.")
    
    connection.close()

if __name__ == "__main__":
    print("🚀 Iniciando simulador de flota...")
    
    # Configuramos 5 camiones: ID, Conductor, Material, Segundos de retraso en salir
    flota = [
        ("CAMION-01", "Víctor A.", "Cobre", 0),
        ("CAMION-02", "Juan P.", "Oro", 4),
        ("CAMION-03", "María L.", "Plata", 8),
        ("CAMION-04", "Carlos R.", "Roca Estéril", 12),
        ("CAMION-05", "Ana G.", "Cobre", 16)
    ]

    hilos = []
    
    # Arrancamos todos los camiones al mismo tiempo (el 'delay' interno hace que salgan escalonados)
    for c in flota:
        hilo = threading.Thread(target=simular_camion, args=c)
        hilos.append(hilo)
        hilo.start()

    # Esperamos a que todos terminen
    for h in hilos:
        h.join()
        
    print("🛑 Simulación de flota terminada.")