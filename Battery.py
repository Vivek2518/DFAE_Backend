from pymavlink import mavutil
import logging
import statistics

def process_battery_data(log_path):
    """Process battery data from a MAVLink binary log file."""
    try:
        # Open the binary log
        mlog = mavutil.mavlink_connection(log_path, dialect="ardupilotmega")
        logging.info(f"Opened MAVLink connection for battery processing: {log_path}")

        battery_data = []
        voltages = []  # Store voltages for max/min/mean calculation

        while True:
            msg = mlog.recv_match(blocking=False)
            if msg is None:
                break
            msg_type = msg.get_type()
        
            # Collect BAT data
            if msg_type == "BAT":
                logging.info(f"BAT message: {msg.to_dict()}")
                voltage = msg.Volt
                voltages.append(voltage)
                battery_data.append({
                    "timestamp": msg.TimeUS / 1e6,  # Convert microseconds to seconds
                    "Volt": msg.Volt,  # Battery voltage in volts
                    "VoltR": msg.VoltR if hasattr(msg, 'VoltR') else None,  # Remaining or reference voltage
                    "Curr": msg.Curr,  # Current in amps
                    "CurrTot": msg.CurrTot if hasattr(msg, 'CurrTot') else None,  # Total current consumed (mAh)
                    "EnrgTot": msg.EnrgTot if hasattr(msg, 'EnrgTot') else None,  # Total energy consumed
                    "Temp": msg.Temp if hasattr(msg, 'Temp') else None,  # Temperature in °C
                    "Res": msg.Res if hasattr(msg, 'Res') else None,  # Internal resistance
                    "RemPct": msg.RemPct if hasattr(msg, 'RemPct') else None,  # Remaining capacity percentage
                    "H": msg.H if hasattr(msg, 'H') else None,  # Health flag
                    "SH": msg.SH if hasattr(msg, 'SH') else None  # Secondary health/state flag
                })

        # Close the MAVLink connection
        mlog.close()
        logging.info("Closed MAVLink connection for battery processing")

        # Calculate battery metrics
        max_voltage = max(voltages) if voltages else None
        min_voltage = min(voltages) if voltages else None
        mean_voltage = statistics.mean(voltages) if voltages else None

        # Log summary of collected data
        logging.info(f"Collected {len(battery_data)} battery data points")
        logging.info(f"Max Voltage: {max_voltage}V, Min Voltage: {min_voltage}V, Mean Voltage: {mean_voltage}V")

        return {
            "battery_data": battery_data,
            "max_voltage": max_voltage,
            "min_voltage": min_voltage,
            "mean_voltage": mean_voltage
        }

    except Exception as e:
        logging.error(f"Error processing battery data: {str(e)}", exc_info=True)
        raise