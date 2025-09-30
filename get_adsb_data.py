import requests
import os
import datetime
import sqlite3
import json
import sys
import time

# Configuration, global parameters
DB_FILE = "adsb.db"

def get_adsb_feed(URL: str) -> dict:
    headers = {
    }
    # Get ADS-B data for aircraft within 10NM of Winterthur
    # response = requests.get('https://adsbexchange.com/api/aircraft/lat/47.4999/lon/8.7262/dist/10/', headers = headers)\
    # JUST GET ALL ADSB DATA AT THIS POINT
    #URL_ADSB_ONE_MIL = 'https://api.adsb.one/v2/point/57.536472/18.528914/200?filterDbFlag=military'
    #URL_ADSB_ONE = 'https://api.adsb.one/v2/point/47.4999/8.7262/10'
    response = requests.get(URL, headers = headers)
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

def process_aircraft_data(aircraft_list, ac_table, pos_table):
    """
    Connects to the database and updates it with a list of aircraft data.
    """
    connection = get_db_connection()
    if not connection:
        print('Returning without doing anything')
        return

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

            cursor.execute(f"""
                INSERT INTO {ac_table} (icao24, flight, first_seen, last_seen, squawk)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(icao24) DO UPDATE SET
                    flight = excluded.flight,
                    squawk = excluded.squawk,
                    last_seen = excluded.last_seen;
            """, (icao, flight, current_timestamp, current_timestamp, squawk))
            
            cursor.execute(f"""
                INSERT INTO {pos_table} (icao24, timestamp, lat, lon, altitude, ground_speed, track)
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

def cleanup_old_aircraft(ac_table, pos_table, timeout_seconds=3600):
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

        cursor.execute(f"SELECT icao24 FROM {ac_table} WHERE last_seen < ?", (cleanup_timestamp,))
        stale_aircraft_tuples = cursor.fetchall()

        if not stale_aircraft_tuples:
            print(f"Cleanup: No aircraft found that haven't been seen for {timeout_seconds} seconds.")
            return

        stale_aircraft_ids = [item[0] for item in stale_aircraft_tuples]
        placeholders = ', '.join('?' for _ in stale_aircraft_ids)

        cursor.execute(f"DELETE FROM {pos_table} WHERE icao24 IN ({placeholders})", stale_aircraft_ids)
        positions_deleted = cursor.rowcount

        cursor.execute(f"DELETE FROM {ac_table} WHERE icao24 IN ({placeholders})", stale_aircraft_ids)
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

def get_last_location(icao: str) -> list:
    """ Gets the last known location data for an ICAO identifier. """
    locations = []
    connection = get_db_connection()
    if not connection:
        return

    try:
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()        
        cursor.execute(f"SELECT id, icao24, timestamp, lat, lon, altitude FROM positions WHERE icao24 = '{icao}' ORDER BY id")
        loc_rows = cursor.fetchall()
    except Exception as e:
        print(f'lookup for {icao}: {e}')
    else:
        for loc in loc_rows:
            loc_dict = {k: loc[k] for k in loc.keys()}
            locations.append(loc_dict.copy())
        # sort by timestamp
        locations.sort(key=lambda x: x['timestamp'], reverse=True)
        if locations: return locations[0]

def print_db_contents(ac_table):
    """Connects to the DB and prints the contents of all tables for verification."""
    connection = get_db_connection()
    if not connection:
        return
    
    try:
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        
        print("\n--- Current Database Contents ---")
        
        print("\n[ aircraft table ]")
        cursor.execute(f"SELECT icao24, flight, first_seen, last_seen, squawk FROM {ac_table}")
        rows = cursor.fetchall()
        if not rows:
            print("...empty...")
        else:
            print(f"{'ICAO24':<10} | {'Flight':<11} | {'Squawk': <10} | {'First Seen':<20} | {'Last Seen': <20} | {'Lat':<10} | {'Lon':<10} | {'Altitude'}")
            print("-" * 80)
            
            for row in rows:
                fs = datetime.datetime.fromtimestamp(row['first_seen']).strftime('%Y-%m-%d %H:%M:%S')
                ls = datetime.datetime.fromtimestamp(row['last_seen']).strftime('%Y-%m-%d %H:%M:%S')
                loc = get_last_location(row['icao24'])
                ac_info = f"{row['icao24']: <10} | {row['flight']: <11} | {row['squawk']: <10} | {fs: <12} | {ls: <20}"
                loc_info = f"| {loc['lat']: <10} | {loc['lon']: <10} | {loc['altitude']}" if loc else "Position not in db"
                print(ac_info + loc_info)

    except sqlite3.Error as e:
        print(f"Database error while reading contents: {e}")
    finally:
        if connection:
            connection.close()

def main(argv):
    PATH = os.path.dirname(os.path.abspath(__file__))
    print(PATH + '/config.json')
    with open(PATH + '/config.json', 'r', encoding='utf-8') as conf:
        config = json.load(conf)

    aircraft_data = {}
    for query in config['endpoints'].keys():
        # Add returned data under each key, sleep due to rate limiting
        aircraft_data[query] = get_adsb_feed(config['endpoints'][query]['url'])
        time.sleep(1)
        # Attach config data to use later
        aircraft_data[query].update(config['endpoints'][query])
    
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for query in list(aircraft_data.keys()):
        ac_list = aircraft_data[query]['ac']
        if len(aircraft_data[query]['tables']) >= 2:
            ac_table = aircraft_data[query]['tables'][0]
            pos_table = aircraft_data[query]['tables'][1]
        else:
            raise ValueError('Both aircraft table and positions table must be defined in config!')
        # TODO: Add argument to process, cleanup and print so correct table is treated
        # TODO: Then implement a lat/lon filtering function to filter for baltic
        print(f"--- {now} Upserting {len(ac_list)} aircraft ---")
        process_aircraft_data(ac_list, ac_table, pos_table)

        if '--silent' not in argv:
            print("\n--- Running cleanup task (1-hour threshold) ---")
        cleanup_old_aircraft(ac_table, pos_table, timeout_seconds=3600)
        
        if '--silent' not in argv:
            print("\n--- Verifying Database Contents (After Cleanup) ---")
            print_db_contents(ac_table)


if __name__ == '__main__':
    main(sys.argv)