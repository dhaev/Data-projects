Data Modeling, Normalization, and ETL Pipeline for Netflix Content
1. Project Overview
Problem Statement

The original Netflix dataset, a flat CSV file, suffered from a highly denormalized structure. Key information like directors, cast, countries, and genres were embedded as comma-separated strings within single columns. This led to inefficient analytical querying, requiring complex and costly string manipulation.
Project Goal

This project aimed to design and implement a normalized relational database schema (MySQL), build an automated ETL pipeline to load and transform the raw CSV data, and enable efficient query execution that was challenging with the original structure.
2. Data Source & Initial Challenges

The project uses netflix_shows_info.csv, containing Netflix movie and TV show data.

Initial Data Structure Challenges:

    Multi-valued attributes: Columns like director, cast, country, and listed_in contained multiple values in a single string, violating 1NF and complicating direct queries.

    Inconsistent data formats: Fields like date_added (YYYY-MM-DD vs. Month Day, Year) and duration ("X min" vs. "X Seasons") required specific parsing.

3. Data Modeling & Schema Design

A normalized relational database schema was designed in MySQL to improve data integrity, reduce redundancy, and facilitate efficient querying.

Normalization Strategy:
Achieved Third Normal Form (3NF) by:

    Creating dimension tables for unique entities (e.g., directors, cast_members, countries, genres).

    Establishing many-to-many relationships between the main shows table (fact table) and dimension tables using dedicated fact tables (e.g., show_directors, show_cast_members).

New Schema (SQL DDL):

-- Create the main content table, renamed to 'shows'
CREATE TABLE shows (
    show_id VARCHAR(255) PRIMARY KEY,
    type VARCHAR(50),
    title VARCHAR(255),
    date_added_day VARCHAR(10),
    date_added_year INT,
    release_year INT,
    rating VARCHAR(50),
    duration VARCHAR(50),
    seasons INT,
    description TEXT
);

-- Create the Directors dimension table
CREATE TABLE directors (
    director_id INT AUTO_INCREMENT PRIMARY KEY,
    director_name VARCHAR(255) UNIQUE NOT NULL
);

-- Create the Cast Members dimension table
CREATE TABLE cast_members (
    cast_member_id INT AUTO_INCREMENT PRIMARY KEY,
    cast_member_name VARCHAR(255) UNIQUE NOT NULL
);

-- Create the Countries dimension table
CREATE TABLE countries (
    country_id INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(255) UNIQUE NOT NULL
);

-- Create the Genres dimension table
CREATE TABLE genres (
    genre_id INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(255) UNIQUE NOT NULL
);

-- Fact table for Shows to Directors (Many-to-Many)
CREATE TABLE show_directors (
    show_id VARCHAR(255),
    director_id INT,
    PRIMARY KEY (show_id, director_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (director_id) REFERENCES directors(director_id) ON DELETE CASCADE
);

-- Fact table for Shows to Cast Members (Many-to-Many)
CREATE TABLE show_cast_members (
    show_id VARCHAR(255),
    cast_member_id INT,
    PRIMARY KEY (show_id, cast_member_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (cast_member_id) REFERENCES cast_members(cast_member_id) ON DELETE CASCADE
);

-- Fact table for Shows to Countries (Many-to-Many)
CREATE TABLE show_countries (
    show_id VARCHAR(255),
    country_id INT,
    PRIMARY KEY (show_id, country_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (country_id) REFERENCES countries(country_id) ON DELETE CASCADE
);

-- Fact table for Shows to Genres (Many-to-Many)
CREATE TABLE show_genres (
    show_id VARCHAR(255),
    genre_id INT,
    PRIMARY KEY (show_id, genre_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

Schema Visualization (ERD):

(Add a brief caption for the ERD, e.g., "Entity-Relationship Diagram of the Normalized Netflix Database Schema")
4. ETL (Extract, Transform, Load) Pipeline

A Python script (test.py) automates the ETL process, loading data from netflix_shows_info.csv into the normalized MySQL database.

Key Transformation Logic:

    Database & Table Creation: Connects to MySQL, creates the database and all normalized tables.

    Data Extraction & Cleaning: Reads CSV, handling latin-1 encoding.

    Multi-valued Field Processing: Splits comma-separated strings, strips whitespace, and uses get_or_create_id to populate dimension tables and insert into fact tables.

    Date & Duration Parsing: Parses date_added (multiple formats) and extracts duration or seasons based on content type.

    Error Handling & Transactions: Robust try-except blocks and commit/rollback ensure data consistency.

Technologies Used:

    Python: ETL scripting.

    mysql.connector: MySQL database interaction.

    csv, re: CSV parsing and regex.

Core ETL Script Logic (Conceptual Snippets):
(Note: These are conceptual snippets. In your actual README.md, embed well-commented, concise code blocks from your test.py that illustrate these specific points. Do not paste the entire script.)

# Example: Database and table creation logic (from create_database_and_tables function)
# ...

# Example: get_or_create_id helper function
def get_or_create_id(cursor, table_name, name_column, name_value):
    # ...

# Example: Main ETL loop (from etl_process function)
# ...
for i, row in enumerate(reader):
    # ... (data extraction and cleaning) ...
    # Insert into main 'shows' table
    # ...
    # Process directors (similar for cast, countries, genres)
    if directors_str:
        for director_name in directors_str.split(','):
            d_name = director_name.strip()
            if d_name:
                director_id = get_or_create_id(cursor, 'directors', 'director_name', d_name)
                # Insert into fact table
                # ...
# ...

5. Data Analysis & Querying

The normalized database schema simplifies complex analytical queries.
Query 1: Count of Movies Released Between 1991 and 2000

Question: How many movies were released between 1991 and 2000 (inclusive) on Netflix?

SELECT
    COUNT(type)
FROM
    shows
WHERE
    (release_year BETWEEN 1991 AND 2000) AND type = 'Movie';

(Insert screenshot of Query 1 results here)
Query 2: TV Shows with the Maximum Number of Seasons

Question: Show the title, associated countries, listed genres, and seasons for TV shows that ran the longest (i.e., have the most seasons).

SELECT
    s.title,
    GROUP_CONCAT(DISTINCT c.country_name SEPARATOR ', ') AS countries,
    GROUP_CONCAT(DISTINCT g.genre_name SEPARATOR ', ') AS listed_in,
    s.seasons
FROM
    shows s
LEFT JOIN
    show_countries sc ON s.show_id = sc.show_id
LEFT JOIN
    countries c ON sc.country_id = c.country_id
LEFT JOIN
    show_genres sg ON s.show_id = sg.show_id
LEFT JOIN
    genres g ON sg.genre_id = g.genre_id
WHERE
    s.type = 'TV Show'
    AND s.seasons IN (SELECT MAX(seasons) FROM shows WHERE type = 'TV Show')
GROUP BY
    s.show_id, s.title, s.seasons;

(Insert screenshot of Query 2 results here)
Query 3: Top 10 Cast Members by Number of Titles

Question: Show the name of each cast member and the number of titles (movies or TV shows) where they were a cast member, for the top 10 cast members based on the number of titles.

SELECT
    cm.cast_member_name AS cast_member,
    COUNT(scm.show_id) AS number_of_titles
FROM
    cast_members cm
JOIN
    show_cast_members scm ON cm.cast_member_id = scm.cast_member_id
GROUP BY
    cm.cast_member_name
ORDER BY
    number_of_titles DESC
LIMIT 10;

(Insert screenshot of Query 3 results here)
Query 4: Movies where Director is also a Cast Member

Question: Show the title of the movies and the name(s) of the director(s) who also appeared as cast member(s) in that same movie.

SELECT
    s.title,
    GROUP_CONCAT(DISTINCT d.director_name) AS director_who_is_also_cast
FROM
    shows s
JOIN
    show_directors sd ON s.show_id = sd.show_id
JOIN
    directors d ON sd.director_id = d.director_id
JOIN
    show_cast_members scm ON s.show_id = scm.show_id
JOIN
    cast_members cm ON scm.cast_member_id = cm.cast_member_id
WHERE
    s.type = 'Movie'
    AND d.director_name = cm.cast_member_name
GROUP BY
    s.title;

(Insert screenshot of Query 4 results here)
6. Key Learnings & Challenges

This project provided valuable experience in building a robust data pipeline and highlighted the importance of a well-designed database schema.

Key Learnings:

    Importance of Normalization: Understanding how normalization reduces data redundancy, improves data integrity, and simplifies complex queries.

    ETL Process Design: Gained experience in designing and implementing an automated ETL pipeline.

    Handling Real-World Data: Learned to address inconsistencies (e.g., date formats, mixed data types).

    Database Interaction with Python: Proficiently used mysql.connector for programmatic interaction.

Challenges Faced & Solutions:

    UnicodeDecodeError: Resolved by explicitly specifying encoding='latin-1' for the CSV.

    list index out of range: Handled by careful column re-indexing and robust row length checks.

    Parsing Mixed duration/seasons Field: Implemented conditional regex parsing based on content type.

    Database Creation & Connection Flow: Refined the connection process to correctly create the database before table DDL and data loading.

7. Technologies Used

    Database: MySQL

    Programming Language: Python

    Python Libraries: mysql.connector, csv, re, datetime

    Data Format: CSV

    Database Tools: MySQL Workbench (for schema visualization), (Optional: dbdiagram.io)
