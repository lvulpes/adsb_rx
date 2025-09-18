import requests
import os
import time
import sqlite3

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

def process_aircraft_data(aircraft_list):
    """
    Connects to the database and updates it with a list of aircraft data.
    
    This function handles both adding new aircraft and updating existing ones,
    as well as logging their current position.

    Args:
        aircraft_list: A list of dictionaries, where each dictionary represents
                       one aircraft's data from the JSON feed.
    """
    # Check if the database exists. If not, guide the user to run the create script.
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file '{DB_FILE}' not found.")
        print("Please run the 'create_database.py' script first.")
        return

    connection = None
    try:
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Get the current time as a Unix timestamp.
        # All entries in this batch will share the same timestamp.
        current_timestamp = int(time.time())

        for aircraft in aircraft_list:
            # --- Step 1: Handle the 'aircraft' table (UPSERT) ---
            # This is an "UPSERT" operation: UPDATE if the aircraft exists, INSERT if it doesn't.
            # We use `ON CONFLICT(icao24) DO UPDATE` which is highly efficient in SQLite.
            # - If icao24 is new, it INSERTS a new row.
            # - If icao24 exists, it UPDATES the specified fields.
            
            # Use .get() to safely access dictionary keys that might be missing
            icao = aircraft.get('icao24')
            flight = aircraft.get('flight', 'N/A').strip() # Provide default and strip whitespace

            # Skip entry if it's missing the essential ICAO24 identifier
            if not icao:
                continue

            cursor.execute("""
                INSERT INTO aircraft (icao24, flight, first_seen, last_seen)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(icao24) DO UPDATE SET
                    flight = excluded.flight,
                    last_seen = excluded.last_seen;
            """, (icao, flight, current_timestamp, current_timestamp))
            
            # --- Step 2: Handle the 'positions' table (INSERT) ---
            # We always insert a new position record for every sighting.
            # This creates the historical track for the aircraft.
            cursor.execute("""
                INSERT INTO positions (icao24, timestamp, lat, lon, altitude, ground_speed, track)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            """, (
                icao,
                current_timestamp,
                aircraft.get('lat'),
                aircraft.get('lon'),
                aircraft.get('altitude'),
                aircraft.get('ground_speed'),
                aircraft.get('track')
            ))
        
        # Commit all the changes for this batch to the database.
        connection.commit()
        print(f"Successfully processed {len(aircraft_list)} aircraft records.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        if connection:
            connection.rollback() # Roll back any changes if an error occurs
    finally:
        if connection:
            connection.close()
            # print("Database connection closed.") # Optional: uncomment for more verbose output

def cleanup_old_aircraft(timeout_seconds=3600):
    """
    Removes aircraft and their position data if they haven't been seen
    for the specified duration.

    Args:
        timeout_seconds (int): The duration in seconds after which an aircraft
                               is considered stale and should be removed.
                               Defaults to 3600 (1 hour).
    """
    connection = None
    try:
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Calculate the cutoff timestamp. Any aircraft seen before this time will be removed.
        cleanup_timestamp = int(time.time()) - timeout_seconds

        # Step 1: Find all stale aircraft ICAO codes.
        # This is safer than deleting directly, as we need the list for two tables.
        cursor.execute("SELECT icao24 FROM aircraft WHERE last_seen < ?", (cleanup_timestamp,))
        
        # .fetchall() returns a list of tuples, e.g., [('4b1803',), ('a8b4c2',)]
        stale_aircraft_tuples = cursor.fetchall()

        if not stale_aircraft_tuples:
            print(f"Cleanup: No aircraft found that haven't been seen for {timeout_seconds} seconds.")
            return

        # Convert the list of tuples into a simple list of strings: ['4b1803', 'a8b4c2']
        stale_aircraft_ids = [item[0] for item in stale_aircraft_tuples]

        # Create a string of placeholders ('?, ?, ?') for the SQL query.
        # This is the secure way to handle a variable number of parameters.
        placeholders = ', '.join('?' for _ in stale_aircraft_ids)

        # Step 2: Delete from the 'positions' table first.
        # This is critical to avoid violating the foreign key constraint.
        cursor.execute(f"DELETE FROM positions WHERE icao24 IN ({placeholders})", stale_aircraft_ids)
        positions_deleted = cursor.rowcount

        # Step 3: Delete the stale aircraft from the 'aircraft' table.
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


def main():
    ret_json = get_adsb_feed()
    l = ret_json['ac']
    d = hex_list_2_dict(ret_json['ac'])

    print("--- First run: Inserting new aircraft ---")
    process_aircraft_data(l)
    print("\n--- Running cleanup task (1-hour threshold) ---")
    cleanup_old_aircraft(timeout_seconds=3600)


    # print("--- Second run: Updating existing aircraft and adding a new one ---")
    # process_aircraft_data(updated_sample_data)


if __name__ == '__main__':
    main()