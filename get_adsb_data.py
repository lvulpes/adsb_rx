import requests
import os
import datetime
import sqlite3
import json
import sys

# Configuration, global parameters
DB_FILE = "adsb.db"

def get_adsb_feed():
    headers = {
    }
    # Get ADS-B data for aircraft within 10NM of Winterthur
    # response = requests.get('https://adsbexchange.com/api/aircraft/lat/47.4999/lon/8.7262/dist/10/', headers = headers)
    URL_ADSB_ONE = 'https://api.adsb.one/v2/point/47.4999/8.7262/10'
    response = requests.get(URL_ADSB_ONE, headers = headers)
    return response.json()

def hex_list_2_dict(hexes: list) -> dict:
    """ Take a list of dicts from API and make it a dict with hexes as keys. """
    hex_dict = {x['hex']: x for x in hexes}
    return hex_dict

def get_db_connection():
    """
    Establishes a robust connection to the SQLite database.
    Checks for file existence and enables foreign key constraints.
    """
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file '{DB_FILE}' not found.")
        print("Please run the 'create_database.py' script first.")
        return None
    
    try:
        conn = sqlite3.connect(DB_FILE)
        # Enforce foreign key constraints, which is off by default in SQLite
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None

def process_aircraft_data(aircraft_list):
    """
    Connects to the database and updates it with a list of aircraft data.
    """
    connection = get_db_connection()
    if not connection:
        print('Returning without doing anything')
        return # Exit if connection failed

    try:
        cursor = connection.cursor()
        current_timestamp = int(datetime.datetime.timestamp(datetime.datetime.now()))

        for aircraft in aircraft_list:
            icao = aircraft.get('hex')
            if not icao:
                continue

            flight = aircraft.get('flight', 'N/A')
            squawk = aircraft.get('squawk', 'N/A')
            if flight: # Ensure flight is a string before stripping
                flight = flight.strip()

            cursor.execute("""
                INSERT INTO aircraft (icao24, flight, first_seen, last_seen, squawk)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(icao24) DO UPDATE SET
                    flight = excluded.flight,
                    squawk = excluded.squawk,
                    last_seen = excluded.last_seen;
            """, (icao, flight, current_timestamp, current_timestamp, squawk))
            
            cursor.execute("""
                INSERT INTO positions (icao24, timestamp, lat, lon, altitude, ground_speed, track)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            """, (
                icao,
                current_timestamp,
                aircraft.get('lat'),
                aircraft.get('lon'),
                aircraft.get('alt_baro'),
                aircraft.get('ground_speed'),
                aircraft.get('track')
            ))
        
        connection.commit()
        print(f"Successfully processed {len(aircraft_list)} aircraft records.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()

def cleanup_old_aircraft(timeout_seconds=3600):
    """
    Removes aircraft and their position data if they haven't been seen
    for the specified duration.
    """
    connection = get_db_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor()
        cleanup_timestamp = int(datetime.datetime.timestamp(datetime.datetime.now())) - timeout_seconds

        cursor.execute("SELECT icao24 FROM aircraft WHERE last_seen < ?", (cleanup_timestamp,))
        stale_aircraft_tuples = cursor.fetchall()

        if not stale_aircraft_tuples:
            print(f"Cleanup: No aircraft found that haven't been seen for {timeout_seconds} seconds.")
            return

        stale_aircraft_ids = [item[0] for item in stale_aircraft_tuples]
        placeholders = ', '.join('?' for _ in stale_aircraft_ids)

        cursor.execute(f"DELETE FROM positions WHERE icao24 IN ({placeholders})", stale_aircraft_ids)
        positions_deleted = cursor.rowcount

        cursor.execute(f"DELETE FROM aircraft WHERE icao24 IN ({placeholders})", stale_aircraft_ids)
        aircraft_deleted = cursor.rowcount

        connection.commit()
        print(f"Cleanup: Removed {aircraft_deleted} stale aircraft and their {positions_deleted} position records.")

    except sqlite3.Error as e:
        print(f"Database error during cleanup: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()

def print_db_contents():
    """Connects to the DB and prints the contents of all tables for verification."""
    connection = get_db_connection()
    if not connection:
        return
    
    try:
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        
        print("\n--- Current Database Contents ---")
        
        print("\n[ aircraft table ]")
        cursor.execute("SELECT icao24, flight, first_seen, last_seen, squawk FROM aircraft")
        rows = cursor.fetchall()
        if not rows:
            print("...empty...")
        else:
            print(f"{'ICAO24':<10} | {'Flight':<11} | {'Squawk': <10} | {'First Seen':<20} | {'Last Seen'}")
            print("-" * 80)
            for row in rows:
                fs = datetime.datetime.fromtimestamp(row['first_seen']).strftime('%Y-%m-%d %H:%M:%S')
                ls = datetime.datetime.fromtimestamp(row['last_seen']).strftime('%Y-%m-%d %H:%M:%S')
                print(f"{row['icao24']:<10} | {row['flight']:<11} | {row['squawk']: <10} | {fs:<12} | {ls}")

        print("\n[ positions table (last 5 entries) ]")
        cursor.execute("SELECT id, icao24, timestamp, lat, lon, altitude FROM positions ORDER BY id DESC LIMIT 5")
        rows = cursor.fetchall()
        if not rows:
            print("...empty...")
        else:
            print(f"{'ID':<5} | {'ICAO24':<10} | {'Timestamp':<20} | {'Lat':<10} | {'Lon':<10} | {'Altitude'}")
            print("-" * 80)
            for row in rows:
                ts = datetime.datetime.fromtimestamp(row['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                print(f"{row['id']:<5} | {row['icao24']:<10} | {ts:<20} | {row['lat']:<10} | {row['lon']:<10} | {row['altitude']}")

        print("\n---------------------------------")
        
    except sqlite3.Error as e:
        print(f"Database error while reading contents: {e}")
    finally:
        if connection:
            connection.close()


def main(argv):
    ret_json = get_adsb_feed()
    l = ret_json['ac']
    d = hex_list_2_dict(ret_json['ac'])
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"--- {now} Upserting {len(l)} aircraft ---")
    process_aircraft_data(l)

    if '--silent' not in argv:
        print("\n--- Running cleanup task (1-hour threshold) ---")
    cleanup_old_aircraft(timeout_seconds=3600)
    
    if '--silent' not in argv:
        print("\n--- Verifying Database Contents (After Cleanup) ---")
        print_db_contents()


if __name__ == '__main__':
    main(sys.argv)