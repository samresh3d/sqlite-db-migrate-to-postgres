import sqlite3
import psycopg2

sqlite_conn = sqlite3.connect("data.db")
sqlite_cur = sqlite_conn.cursor()

# Local Database details
pg_conn = psycopg2.connect(
    host="localhost",
    database="database-name",
    user="postgres",
    password="####",
    port="5432"
)
pg_cur = pg_conn.cursor()

# Dictionary mapping SQLite data types to PostgreSQL data types
data_type_mapping = {
    'INTEGER': 'INTEGER',
    'BIGINT': 'BIGINT',
    'FLOAT': 'REAL',
    'REAL': 'REAL',
    'DOUBLE': 'DOUBLE PRECISION',
    'CHAR': 'CHAR',
    'VARCHAR': 'VARCHAR',
    'TEXT': 'TEXT',
    'BLOB': 'BYTEA',
    'BOOLEAN': 'SMALLINT',
    'DATE': 'DATE',
    'DATETIME': 'TEXT'
}

# Get a cursor object
cur = sqlite_conn.cursor()

# Execute a SELECT statement to get the table names
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")

# Iterate over the tables in the SQLite database and migrate each one to PostgreSQL
for table_info in cur.fetchall():
    table_name = table_info[0]

    # Get the column information for the table from SQLite
    sqlite_cur.execute(f"PRAGMA table_info({table_name})")
    sqlite_columns = sqlite_cur.fetchall()

    # Generate the CREATE TABLE statement for the table in PostgreSQL
    columns = []
    for column_info in sqlite_columns:
        column_name = column_info[1]
        data_type = column_info[2]
        pg_data_type = data_type_mapping.get(data_type.upper(), 'TEXT')
        columns.append(f'"{column_name}" {pg_data_type.lower()}')
    create_table_statement = f'DROP TABLE IF EXISTS "{table_name}"; CREATE TABLE "{table_name}" ({", ".join(columns)});'

    # Create the table in PostgreSQL
    print(table_name)
    pg_cur.execute(create_table_statement)

    # Get the data from SQLite and insert it into PostgreSQL
    sqlite_cur.execute(f"SELECT * FROM {table_name}")
    rows = sqlite_cur.fetchall()
    insert_statement = f'INSERT INTO "{table_name}" VALUES ({", ".join(["%s"] * len(sqlite_columns))})'
    pg_cur.executemany(insert_statement, rows)

    # Print log message for the migrated table
    print(f"Table '{table_name}' migrated successfully.")
    pg_conn.commit()


pg_cur.close()
pg_conn.close()
sqlite_cur.close()
sqlite_conn.close()
