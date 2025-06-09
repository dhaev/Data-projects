# Data Modeling, Normalization, and ETL Pipeline for Netflix Content

## 1. Project Overview

### Problem Statement
The original Netflix dataset, a flat CSV file, suffered from a highly denormalized structure. Key information—such as directors, cast, countries, and genres—were embedded as comma-separated strings within single columns, making querying and analysis complex and violating best database design practices.

### Project Goal
This project aims to design and implement a normalized relational database schema (MySQL), build an automated ETL pipeline to load and transform the raw CSV data, and enable efficient query execution for analysis.

## 2. Data Source & Initial Challenges

- **Dataset:** `netflix_shows_info.csv`, containing Netflix movie and TV show data.

### Initial Data Structure Challenges
- **Multi-valued attributes:** Columns like director, cast, country, and listed_in contained multiple values in a single string, violating 1NF and complicating direct queries.
- **Inconsistent data formats:** Fields like date_added (YYYY-MM-DD vs. Month Day, Year) and duration ("X min" vs. "X Seasons") required specific parsing.

## 3. Data Modeling & Schema Design

A normalized relational database schema was designed in MySQL to improve data integrity, reduce redundancy, and facilitate efficient querying.

### Normalization Strategy
Achieved Third Normal Form (3NF) by:
- Creating dimension tables for unique entities (e.g., directors, cast_members, countries, genres).
- Establishing many-to-many relationships between the main shows table (fact table) and dimension tables using dedicated fact tables (e.g., show_directors, show_cast_members).

**Schema Visualization (ERD):**  
![netflix_show_info](https://github.com/user-attachments/assets/4e0534c6-45de-4fec-b135-6511f7265ea6)

## 4. ETL (Extract, Transform, Load) Pipeline

A Python script (`netflix-etl-mysql.py`) automates the ETL process, loading data from `netflix_shows_info.csv` into the normalized MySQL database.

### Key Transformation Logic
- **Database & Table Creation:** Connects to MySQL, creates the database and all normalized tables.
- **Data Extraction & Cleaning:** Reads CSV, handling latin-1 encoding.
- **Multi-valued Field Processing:** Splits comma-separated strings, strips whitespace, and uses `get_or_create_id` to populate dimension tables and insert into fact tables.
- **Date & Duration Parsing:** Parses `date_added` (multiple formats) and extracts duration/seasons based on content type.
- **Error Handling & Transactions:** Robust try-except blocks and commit/rollback for data consistency.

## 5. Data Analysis & Querying

### Example Queries

#### Query 1: Count of Movies Released Between 1991 and 2000
```sql
SELECT
    COUNT(type)
FROM
    shows
WHERE
    (release_year BETWEEN 1991 AND 2000) AND type = 'Movie';
```
![netflix-show-q1](https://github.com/user-attachments/assets/015ce6ad-e2e6-4508-9b6b-207939a70849)

The normalized database schema simplifies complex analytical queries.
#### Query 2: TV Shows with the Maximum Number of Seasons
```
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
```
![netflix-show-q2](https://github.com/user-attachments/assets/e8aef435-1324-4403-8838-c4836080dd64)

#### Query 3: Top 10 Cast Members by Number of Titles
```
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
```
![netflix-show-q3](https://github.com/user-attachments/assets/0539c784-430c-4788-b90d-6d600af5d151)

#### Query 4: Movies where Director is also a Cast Member
```
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
```
![netflix-show-q4](https://github.com/user-attachments/assets/28e3f62a-c548-41d7-ace7-8a4c94777050)

## 6. Key Learnings & Challenges

This project provided valuable experience in building a robust data pipeline and highlighted the importance of a well-designed database schema.

### Key Learnings

- **Importance of Normalization:** Reduces data redundancy, improves data integrity, and simplifies complex queries.
- **ETL Process Design:** Gained experience in designing and implementing an automated ETL pipeline.
- **Handling Real-World Data:** Addressed inconsistencies (e.g., date formats, mixed data types).
- **Database Interaction with Python:** Developed proficiency with `mysql.connector`.

### Challenges & Solutions

- **UnicodeDecodeError:** Resolved by explicitly specifying `encoding='latin-1'` for the CSV.
- **list index out of range:** Handled by careful column re-indexing and robust row length checks.
- **Parsing Mixed duration/seasons Field:** Implemented conditional regex parsing based on content type.
- **Database Creation & Connection Flow:** Refined the connection process to correctly create the database before table DDL and data loading.

## 7. Technologies Used

- **Database:** MySQL
- **Programming Language:** Python
- **Python Libraries:** `mysql.connector`, `csv`, `re`, `datetime`
- **Data Format:** CSV
- **Database Tools:** MySQL Workbench (for schema visualization)
