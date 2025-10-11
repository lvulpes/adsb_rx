import sqlite3
import os

# --- Configuration ---
DB_FILE = "adsb.db"

def create_database():
    """
    Connects to the SQLite database and creates the necessary tables 
    if they don't already exist.
    """
    # Check if the database file already exists to avoid overwriting.
    db_existed = os.path.exists(DB_FILE)

    connection = None  # Initialize connection to None
    try:
        # The connect function will create the file if it doesn't exist.
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()
        print(f"Successfully connected to SQLite database: {DB_FILE}")

        # --- Create 'aircraft' table ---
        # This table stores unique information about each aircraft.
        # - icao24: The unique 24-bit hex identifier for an aircraft. This is our primary key.
        # - flight: The callsign or flight number (e.g., "SWR123"). Can change during a flight.
        # - first_seen: Timestamp of when we first logged this aircraft.
        # - last_seen: Timestamp of the most recent position report.
        # - squawk: Squawk code
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aircraft (
                icao24 TEXT PRIMARY KEY NOT NULL,
                flight TEXT,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                squawk INTEGER NOT NULL
            );
        """)
        print("Table 'aircraft' created or already exists.")

        # --- Create 'positions' table ---
        # This table stores a historical track of each aircraft's position.
        # - id: A unique ID for each row entry.
        # - icao24: A foreign key that links this position report to a specific aircraft.
        # - timestamp: The exact time of this position report.
        # - lat/lon: The geographic coordinates. REAL is used for floating-point numbers.
        # - altitude: The aircraft's altitude in feet.
        # - ground_speed: Speed over the ground in knots.
        # - track: The direction of travel in degrees (0-360).
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                icao24 TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                lat REAL,
                lon REAL,
                altitude INTEGER,
                ground_speed REAL,
                track REAL,
                FOREIGN KEY (icao24) REFERENCES aircraft (icao24)
            );
        """)
        print("Table 'positions' created or already exists.")
        
        # --- Create Indexes ---
        # Indexes make querying the database much faster, especially for common lookups.
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_icao24 ON positions (icao24);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_timestamp ON positions (timestamp);")
        print("Indexes created or already exist.")

        # --- Create Military Aircraft Tables ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aircraft_military (
                icao24 TEXT PRIMARY KEY NOT NULL,
                flight TEXT,
                source TEXT,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions_military (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                icao24 TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                lat REAL,
                lon REAL,
                altitude INTEGER,
                ground_speed REAL,
                track REAL,
                FOREIGN KEY (icao24) REFERENCES aircraft_military (icao24) ON DELETE CASCADE
            );
        """)
        # --- Create Indexes for Military Positions Table ---
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_military_icao24 ON positions_military (icao24);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_military_timestamp ON positions_military (timestamp);")
        print("Indexes created or already exist.")

        # Commit the changes to the database
        connection.commit()
        print("Database schema is up to date.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        # Ensure the connection is closed even if an error occurs.
        if connection:
            connection.close()
            print("Database connection closed.")


def add_column(table: str, new: str) -> None:
    """
    Applies schema changes to an existing database.
    This script is intended to be run once.
    """
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file '{DB_FILE}' not found. Cannot migrate.")
        return

    print(f"Connecting to '{DB_FILE}' to apply migrations...")
    connection = None
    try:
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # --- MIGRATION 1: Add 'source' column to 'aircraft' table ---
        print(f"\nAttempting to add '{new}' column to {table} table...")
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {new} TEXT")
            print(f" -> Success: Added '{new}' column to {table}.")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print(f" -> Info: '{new}' column already exists. Skipping.")
            else:
                # Re-raise the exception if it's an unexpected error
                raise e
        
        connection.commit()
        print("\nDatabase migration complete.")

    except sqlite3.Error as e:
        print(f"An error occurred during migration: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()

def rename_column(table: str, old: str, new: str) -> None:
    """
    Applies schema changes to an existing database.
    This script is intended to be run once.
    """
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file '{DB_FILE}' not found. Cannot migrate.")
        return

    print(f"Connecting to '{DB_FILE}' to apply migrations...")
    connection = None
    try:
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()
        # --- MIGRATION 2: Rename 'ground_speed' to 'gs' in 'positions' table ---
        # Note: This requires SQLite version 3.25.0+
        # Raspberry Pi OS Bullseye/Buster should have a recent enough version.
        print(f"\nAttempting to rename {old} column to {new}...")
        try:
            # We first check if the old column exists before trying to rename
            cursor.execute(f"SELECT {old} FROM {table} LIMIT 1")
            # If the above line doesn't fail, the column exists, so we rename it
            cursor.execute(f"ALTER TABLE {table} RENAME COLUMN {old} TO {new}")
            print(f" -> Success: Renamed {old} to {new}.")
        except sqlite3.OperationalError as e:
            if f"no such column: {old}" in str(e):
                print(f" -> Info: {old} column not found. Assuming it was already renamed to {new}. Skipping.")
            else:
                print(f" -> Warning: Could not rename column. Your SQLite version might be too old or another error occurred.")
                print(f"    Error details: {e}")
        
        connection.commit()
        print("\nDatabase migration complete.")

    except sqlite3.Error as e:
        print(f"An error occurred during migration: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    # Check SQLite version for user awareness
    print(f"Using SQLite version: {sqlite3.sqlite_version}")
    create_database()
    add_column('aircraft_military', 'squawk')
    # rename_column('positions_military', 'gs', 'ground_speed')
    print('Terminating...')
