# data_generator.py
import os
import json
import time
import random
from datetime import datetime, timedelta, timezone

# -----------------------------
#  Define landing folders
# -----------------------------
base_path = "streams"
sensors = ["bridge_temperature", "bridge_vibration", "bridge_tilt"]

# Create folders if they don't exist
for sensor in sensors:
    os.makedirs(os.path.join(base_path, sensor), exist_ok=True)

# -----------------------------
#  Define bridges
# -----------------------------
bridge_ids = [1, 2, 3, 4, 5]  # sample bridge IDs

# -----------------------------
# Event generation function
# -----------------------------
def generate_event(sensor_type, bridge_id):
    # Random lag up to 60 seconds to simulate late arrivals
    now = datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 60))

    # Generate sensor values
    if sensor_type == "bridge_temperature":
        value = round(random.uniform(-10, 40), 2)
    elif sensor_type == "bridge_vibration":
        value = round(random.uniform(0, 5), 2)
    else:  # tilt
        value = round(random.uniform(0, 90), 2)

    return {
        "event_time": now.isoformat(),
        "bridge_id": bridge_id,
        "sensor_type": sensor_type.split("_")[1],  # temperature|vibration|tilt
        "value": value,
        "ingest_time": datetime.now(timezone.utc).isoformat()
    }

# -----------------------------
# Main loop to emit events
# -----------------------------
def main():
    file_index = 0
    print("Starting data generator. Press Ctrl+C to stop...")
    while True:
        for sensor in sensors:
            for bridge_id in bridge_ids:
                event = generate_event(sensor, bridge_id)
                filename = f"{int(time.time())}_{bridge_id}_{file_index}.json"
                file_path = os.path.join(base_path, sensor, filename)
                with open(file_path, "w") as f:
                    json.dump(event, f)
                file_index += 1
        time.sleep(30)  # emit every 30 seconds

if __name__ == "__main__":
    main()
