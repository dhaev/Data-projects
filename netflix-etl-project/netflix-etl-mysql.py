import csv
import mysql.connector
from datetime import datetime
import re # For regular expressions to extract numbers from strings

# --- Configuration ---
MYSQL_SERVER_CONFIG = {
    'host': 'localhost',
    'user': '***',    # <--- CHANGE THIS
    'password': '***', # <--- CHANGE THIS
}
DATABASE_NAME = 'netflix_db'

CSV_FILE_PATH = 'netflix_shows_info.csv'

# --- SQL Statements for Table Creation ---
CREATE_DATABASE_SQL = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}"

CREATE_SHOWS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS shows (
    show_id VARCHAR(255) PRIMARY KEY,
    type VARCHAR(255),
    title VARCHAR(255),
    date_added_day VARCHAR(20),
    date_added_year INT,
    release_year INT,
    rating VARCHAR(50),
    duration INT,
    seasons INT,
    description TEXT
);
"""

CREATE_DIRECTORS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS directors (
    director_id INT AUTO_INCREMENT PRIMARY KEY,
    director_name VARCHAR(255) UNIQUE
);
"""

CREATE_CAST_MEMBERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cast_members (
    cast_member_id INT AUTO_INCREMENT PRIMARY KEY,
    cast_member_name VARCHAR(255) UNIQUE
);
"""

CREATE_COUNTRIES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS countries (
    country_id INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(255) UNIQUE
);
"""

CREATE_GENRES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS genres (
    genre_id INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(255) UNIQUE
);
"""

CREATE_SHOW_DIRECTORS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS show_directors (
    show_id VARCHAR(255),
    director_id INT,
    PRIMARY KEY (show_id, director_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (director_id) REFERENCES directors(director_id) ON DELETE CASCADE
);
"""

CREATE_SHOW_CAST_MEMBERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS show_cast_members (
    show_id VARCHAR(255),
    cast_member_id INT,
    PRIMARY KEY (show_id, cast_member_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (cast_member_id) REFERENCES cast_members(cast_member_id) ON DELETE CASCADE
);
"""

CREATE_SHOW_COUNTRIES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS show_countries (
    show_id VARCHAR(255),
    country_id INT,
    PRIMARY KEY (show_id, country_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (country_id) REFERENCES countries(country_id) ON DELETE CASCADE
);
"""

CREATE_SHOW_GENRES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS show_genres (
    show_id VARCHAR(255),
    genre_id INT,
    PRIMARY KEY (show_id, genre_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);
"""

def create_database_and_tables():
    """
    Connects to MySQL server, creates the database if it doesn't exist,
    and then creates all necessary tables.
    """
    print(f"Attempting to connect to MySQL server at {MYSQL_SERVER_CONFIG['host']}...")
    try:
        # Connect without specifying a database to create it
        conn = mysql.connector.connect(**MYSQL_SERVER_CONFIG)
        cursor = conn.cursor()

        print(f"Creating database '{DATABASE_NAME}' if it doesn't exist...")
        cursor.execute(CREATE_DATABASE_SQL)
        conn.commit()
        print(f"Database '{DATABASE_NAME}' ensured.")

        # Reconnect to the specific database
        conn.close()
        MYSQL_SERVER_CONFIG['database'] = DATABASE_NAME # Add database to config for subsequent connections
        conn = mysql.connector.connect(**MYSQL_SERVER_CONFIG)
        cursor = conn.cursor()

        print("Creating tables if they don't exist...")
        cursor.execute(CREATE_SHOWS_TABLE_SQL)
        cursor.execute(CREATE_DIRECTORS_TABLE_SQL)
        cursor.execute(CREATE_CAST_MEMBERS_TABLE_SQL)
        cursor.execute(CREATE_COUNTRIES_TABLE_SQL)
        cursor.execute(CREATE_GENRES_TABLE_SQL)
        cursor.execute(CREATE_SHOW_DIRECTORS_TABLE_SQL) # Updated
        cursor.execute(CREATE_SHOW_CAST_MEMBERS_TABLE_SQL) # Updated
        cursor.execute(CREATE_SHOW_COUNTRIES_TABLE_SQL) # Updated
        cursor.execute(CREATE_SHOW_GENRES_TABLE_SQL) # Updated
        conn.commit()
        print("All tables ensured.")

        cursor.close()
        return conn # Return the connection to the specific database

    except mysql.connector.Error as err:
        print(f"Error during database/table creation: {err}")
        return None

def get_or_create_id(cursor, table_name, name_column, name_value):
    """
    Checks if an entity (director, cast, country, genre) exists and returns its ID.
    If not, it creates a new entry and returns the new ID.
    """
    select_sql = f"SELECT {name_column}_id FROM {table_name} WHERE {name_column}_name = %s"
    insert_sql = f"INSERT INTO {table_name} ({name_column}_name) VALUES (%s)"

    cursor.execute(select_sql, (name_value,))
    result = cursor.fetchone()

    if result:
        return result[0]
    else:
        try:
            cursor.execute(insert_sql, (name_value,))
            return cursor.lastrowid
        except mysql.connector.Error as err:
            if err.errno == 1062: # Duplicate entry error code (due to UNIQUE constraint)
                # If a race condition caused a duplicate, re-select the ID
                cursor.execute(select_sql, (name_value,))
                result = cursor.fetchone()
                if result:
                    return result[0]
                else:
                    raise err # Should not happen if 1062 was the cause
            else:
                raise err

def etl_process(conn, csv_file):
    """Orchestrates the ETL process."""
    cursor = conn.cursor()

    # Clear linking tables to prevent duplicate entries on re-run for simplicity
    # In a production environment, you might have more sophisticated upsert logic
    print("Clearing linking tables for fresh import...")
    cursor.execute("DELETE FROM show_directors") # Updated
    cursor.execute("DELETE FROM show_cast_members") # Updated
    cursor.execute("DELETE FROM show_countries") # Updated
    cursor.execute("DELETE FROM show_genres") # Updated
    # Clear main shows table (optional, depends on desired behavior for existing data)
    cursor.execute("DELETE FROM shows") # Updated
    conn.commit() # Commit deletions before inserts

    print("Starting ETL process...")
    with open(csv_file, 'r', encoding='latin-1') as f:
        reader = csv.reader(f, delimiter=',') # Corrected: Delimiter is now comma

        for i, row in enumerate(reader):
            if not row: # Skip empty rows
                continue

            # Skip header row if it exists (assuming first row is header)
            if i == 0 and 'show_id' in row[0].lower(): # Simple check for header
                continue

            # Validate row length before accessing indices
            # The sample error data showed 12 columns.
            # We will map the columns based on the observed 12-column structure
            # and set 'seasons' to None since it appears to be missing as a separate column.
            expected_cols = 12
            if len(row) < expected_cols:
                print(f"Skipping row {i+1} due to insufficient columns ({len(row)} instead of {expected_cols}). Row data: {row}")
                conn.rollback()
                continue
            elif len(row) > expected_cols:
                # This might happen if there are unexpected extra commas not quoted.
                # For now, we'll truncate, but ideally, investigate source data.
                print(f"Warning: Row {i+1} has more columns than expected ({len(row)} instead of {expected_cols}). Truncating. Row data: {row}")
                row = row[:expected_cols]


            try:
                # Re-indexed based on observed 12-column CSV structure:
                # show_id,type,title,director,cast_member,country,date_added,release_year,rating,duration_or_seasons_value,listed_in,description
                show_id = row[0].strip()
                item_type = row[1].strip()
                title = row[2].strip()
                directors_str = row[3].strip()
                cast_members_str = row[4].strip()
                countries_str = row[5].strip()
                date_added_full = row[6].strip()
                release_year = int(row[7].strip())
                rating = row[8].strip()

                # Handle duration and seasons: The original CSV structure implies 'duration' for movies
                # and 'seasons' for TV shows are often in the same field.
                # We'll parse this field and set duration/seasons accordingly.
                duration_seasons_raw = row[9].strip()
                duration = None
                seasons = None

                if item_type == 'Movie':
                    # Extract numbers from string like "114 min"
                    match = re.search(r'(\d+)', duration_seasons_raw)
                    if match:
                        duration = int(match.group(1))
                elif item_type == 'TV Show':
                    # Extract numbers from string like "1 Season" or "2 Seasons"
                    match = re.search(r'(\d+)', duration_seasons_raw)
                    if match:
                        seasons = int(match.group(1))
                else:
                    print(f"Warning: Unknown type '{item_type}' for show_id {show_id}. Cannot determine duration/seasons.")


                listed_in_str = row[10].strip()
                description = row[11].strip()

                # Parse date_added: Corrected to handle 'YYYY-MM-DD' format
                date_added_day = None
                date_added_year = None
                if date_added_full:
                    try:
                        date_obj = datetime.strptime(date_added_full, '%Y-%m-%d')
                        date_added_day = date_obj.strftime('%d')
                        date_added_year = date_obj.year
                    except ValueError:
                        print(f"Warning: Could not parse date '{date_added_full}' for show_id {show_id} (expected %Y-%m-%d). Setting to NULL.")


                # Insert into shows table (Updated)
                insert_shows_sql = """
                INSERT INTO shows (show_id, type, title, date_added_day, date_added_year,
                                     release_year, rating, duration, seasons, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    type=VALUES(type), title=VALUES(title), date_added_day=VALUES(date_added_day),
                    date_added_year=VALUES(date_added_year), release_year=VALUES(release_year),
                    rating=VALUES(rating), duration=VALUES(duration), seasons=VALUES(seasons),
                    description=VALUES(description)
                """
                shows_data = (
                    show_id, item_type, title, date_added_day, date_added_year,
                    release_year, rating, duration, seasons, description
                )
                cursor.execute(insert_shows_sql, shows_data)

                # Process directors
                if directors_str:
                    for director_name in directors_str.split(','):
                        director_name = director_name.strip()
                        if director_name:
                            director_id = get_or_create_id(cursor, 'directors', 'director', director_name)
                            insert_show_director_sql = """
                            INSERT IGNORE INTO show_directors (show_id, director_id) VALUES (%s, %s)
                            """
                            cursor.execute(insert_show_director_sql, (show_id, director_id))

                # Process cast members
                if cast_members_str:
                    for cast_member_name in cast_members_str.split(','):
                        cast_member_name = cast_member_name.strip()
                        if cast_member_name:
                            cast_member_id = get_or_create_id(cursor, 'cast_members', 'cast_member', cast_member_name)
                            insert_show_cast_member_sql = """
                            INSERT IGNORE INTO show_cast_members (show_id, cast_member_id) VALUES (%s, %s)
                            """
                            cursor.execute(insert_show_cast_member_sql, (show_id, cast_member_id))

                # Process countries
                if countries_str:
                    for country_name in countries_str.split(','):
                        country_name = country_name.strip()
                        if country_name:
                            country_id = get_or_create_id(cursor, 'countries', 'country', country_name)
                            insert_show_country_sql = """
                            INSERT IGNORE INTO show_countries (show_id, country_id) VALUES (%s, %s)
                            """
                            cursor.execute(insert_show_country_sql, (show_id, country_id))

                # Process genres (listed_in)
                if listed_in_str:
                    for genre_name in listed_in_str.split(','):
                        genre_name = genre_name.strip()
                        if genre_name:
                            genre_id = get_or_create_id(cursor, 'genres', 'genre', genre_name)
                            insert_show_genre_sql = """
                            INSERT IGNORE INTO show_genres (show_id, genre_id) VALUES (%s, %s)
                            """
                            cursor.execute(insert_show_genre_sql, (show_id, genre_id))

            except (ValueError, IndexError, AttributeError) as e:
                print(f"Skipping row {i+1} due to data parsing error: {e}. Row data: {row}")
                conn.rollback() # Rollback current transaction if error occurs
                continue # Move to the next row
            except mysql.connector.Error as err:
                print(f"MySQL error processing row {i+1}: {err}. Row data: {row}")
                conn.rollback() # Rollback current transaction
                continue # Move to the next row

            if (i + 1) % 100 == 0: # Commit every 100 rows
                conn.commit()
                print(f"Processed and committed {i+1} rows.")

    conn.commit() # Commit any remaining transactions
    cursor.close()
    print("ETL process completed.")

if __name__ == "__main__":
    db_connection = create_database_and_tables()
    if db_connection:
        try:
            etl_process(db_connection, CSV_FILE_PATH)
        finally:
            db_connection.close()
            print("Database connection closed.")