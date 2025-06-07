# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2
# Connect to MySQL
try:
        # Establish a connection to the MySQL database
        mysql_connection = mysql.connector.connect(
           user="root",
           password="jiXGz3ryXu5eCFyAE9Zr1MQ2",
           host="172.21.34.90",
           database="sales"
        )
       

except mysql.connector.Error as error:
    print("Error while connecting to MySQL", error)# Connect to DB2 or PostgreSql

try:
   postgresql_connection = psycopg2.connect(
            user="postgres",
            password="MjU1NDUtdmljYWJv",
            host="127.0.0.1",
            port="5432",
            database="sales"
        )     

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)


# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.



def get_last_rowid():
    cursor = postgresql_connection.cursor()
    cursor.execute("SELECT MAX(rowid) FROM sales_data")
    last_row_id = cursor.fetchone()[0]
    cursor.close()
    return last_row_id
last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    cursor = mysql_connection.cursor()
    query = f"SELECT * FROM sales_data WHERE rowid > {rowid}"
    cursor.execute(query)
    records = cursor.fetchall()
    cursor.close()
    return records
new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.



def insert_records(records):
    if len(records) > 0:
        cursor = postgresql_connection.cursor()
        for record in records:
            query = """
            INSERT INTO sales_data (rowid, product_id, customer_id, quantity)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, record)
        postgresql_connection.commit()
        cursor.close()
    else:
        print("No records to insert.")

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
mysql_connection.close()
# disconnect from DB2 or PostgreSql data warehouse 
postgresql_connection.close()

# End of program