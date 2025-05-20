from flask import Flask, request, jsonify
import os
import tempfile
import logging
import time
import json
from pymavlink import mavutil
from Battery import process_battery_data

app = Flask(__name__)

# Configure logging to file and console
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler()
    ]
)

# Set maximum file size (e.g., 200MB)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB limit

# Directory to store processed data
DATA_STORAGE_DIR = "processed_data"
if not os.path.exists(DATA_STORAGE_DIR):
    os.makedirs(DATA_STORAGE_DIR)

@app.errorhandler(413)
def handle_large_file(e):
    return jsonify({"error": "File too large, max 200MB"}), 413

def safe_remove_file(file_path, retries=1, delay=0.5):
    """Attempt to delete a file with retries to handle Windows file lock issues."""
    for attempt in range(retries):
        try:
            os.remove(file_path)
            logging.info(f"Deleted temp file: {file_path}")
            return True
        except Exception as e:
            logging.warning(f"Failed to delete temp file (attempt {attempt + 1}): {str(e)}")
            time.sleep(delay)
    logging.error(f"Could not delete temp file after {retries} attempts: {file_path}")
    return False

def save_processed_data(key, data):
    """Save processed data to a JSON file with the given key."""
    try:
        file_path = os.path.join(DATA_STORAGE_DIR, f"{key}.json")
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        logging.info(f"Saved processed data with key: {key}")
        return True
    except Exception as e:
        logging.error(f"Failed to save processed data with key {key}: {str(e)}")
        return False

@app.route('/upload', methods=['POST'])
def upload_bin():
    logging.info("Received upload request")
    
    # Check for file in request
    if 'file' not in request.files:
        logging.error("No file part in request")
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        logging.error("No selected file")
        return jsonify({"error": "No selected file"}), 400

    # Generate a unique key (e.g., timestamp or user-provided key)
    key = request.args.get('key', default=str(int(time.time())), type=str)
    
    # Save uploaded file to a temp location
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as temp_file:
            file.save(temp_file.name)
            log_path = temp_file.name
            logging.info(f"Saved file to: {log_path}")
    except Exception as e:
        logging.error(f"Failed to save file: {str(e)}")
        return jsonify({"error": f"Failed to save file: {str(e)}"}), 500

    try:
        # Initialize message_types set
        message_types = set()
        
        # Open the binary log to collect message types
        mlog = mavutil.mavlink_connection(log_path, dialect="ardupilotmega")
        logging.info(f"Opened MAVLink connection for: {log_path}")

        while True:
            msg = mlog.recv_match(blocking=False)
            if msg is None:
                break
            msg_type = msg.get_type()
            message_types.add(msg_type)
            logging.debug(f"Processing message type: {msg_type}")

        # Close the MAVLink connection
        mlog.close()
        logging.info("Closed MAVLink connection")

        # Process battery data using Battery.py
        result = process_battery_data(log_path)
        
        # Clean up temp file with retries
        safe_remove_file(log_path)

        # Check if no battery data was found
        if not result["battery_data"]:
            return jsonify({
                "error": "No BAT messages found in the log",
                "message_types": sorted(message_types)
            }), 400

        # Get optional query parameters for limiting data
        limit = request.args.get('limit', default=None, type=int)
        full_data = request.args.get('full', default='false', type=str).lower() == 'true'

        # Prepare response data based on parameters
        response_data = result["battery_data"] if full_data or limit is None else result["battery_data"][:limit]

        # Prepare the data to store
        stored_data = {
            "battery_data": response_data,
            "max_voltage": round(result["max_voltage"], 2) if result["max_voltage"] is not None else None,
            "min_voltage": round(result["min_voltage"], 2) if result["min_voltage"] is not None else None,
            "mean_voltage": round(result["mean_voltage"], 2) if result["mean_voltage"] is not None else None,
            "message_types": sorted(message_types),
            "timestamp": time.time()
        }

        # Save the processed data with the key
        if not save_processed_data(key, stored_data):
            logging.error("Failed to save processed data")
            return jsonify({"error": "Failed to save processed data"}), 500

        # Return response with the key
        return jsonify({
            "key": key,
            "battery_data": response_data,
            "max_voltage": stored_data["max_voltage"],
            "min_voltage": stored_data["min_voltage"],
            "mean_voltage": stored_data["mean_voltage"],
            "message_types": stored_data["message_types"]
        })

    except Exception as e:
        logging.error(f"Error processing file: {str(e)}", exc_info=True)
        safe_remove_file(log_path)
        return jsonify({"error": f"Error processing file: {str(e)}"}), 500

@app.route('/data/<key>', methods=['GET'])
def get_stored_data(key):
    """Retrieve stored data by key."""
    try:
        file_path = os.path.join(DATA_STORAGE_DIR, f"{key}.json")
        if not os.path.exists(file_path):
            logging.error(f"Data not found for key: {key}")
            return jsonify({"error": "Data not found"}), 404

        with open(file_path, 'r') as f:
            data = json.load(f)
        logging.info(f"Retrieved data for key: {key}")
        return jsonify(data)
    except Exception as e:
        logging.error(f"Error retrieving data for key {key}: {str(e)}")
        return jsonify({"error": f"Error retrieving data: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)