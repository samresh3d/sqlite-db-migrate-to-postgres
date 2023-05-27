# SQLite to PostgreSQL Data Migration

This repository contains a Python script that performs a data migration from a SQLite database to a PostgreSQL database. The script connects to the SQLite database, retrieves the table information, creates equivalent tables in the PostgreSQL database, and migrates the data from SQLite to PostgreSQL.

## Prerequisites

To run the migration script, you need the following prerequisites:

- Python (version 3 or above) installed on your system.
- The `sqlite3` and `psycopg2` Python packages installed. You can install them using `pip`:

### pip install sqlite3 psycopg2


## Setup

1. Clone this repository to your local machine or download the ZIP file and extract its contents.

2. Ensure that you have the SQLite database file (`data.db`) available in the same directory as the script.

3. Update the database connection details in the script based on your environment. You can choose between the AWS DB details or the local database details, depending on where you want to migrate the data.

4. Open a terminal or command prompt and navigate to the directory containing the script.

5. Run the migration script using the following command:


## python migration_script.py


The script will connect to the SQLite database, retrieve the table information, create equivalent tables in the PostgreSQL database, and migrate the data from SQLite to PostgreSQL. The progress and status of each table migration will be printed to the console.

6. Once the migration is complete, the script will close the connections and print a success message.

## Customization

If you need to modify the data type mapping between SQLite and PostgreSQL, you can update the `data_type_mapping` dictionary in the script. Add or modify the mappings based on your specific requirements.

## Notes

- Ensure that you have the necessary privileges and access rights to both the SQLite and PostgreSQL databases.
- This script assumes that the PostgreSQL database is already set up and accessible.
- The script uses the `sqlite3` and `psycopg2` libraries, which are widely used and maintained. However, always exercise caution when handling database connections and migrating data.

## Disclaimer

- This code is provided as an example and may require modifications to fit your specific use case. Use it at your own risk.
- Always backup your databases before performing any data migration or manipulation operations.
- Verify the migrated data in the PostgreSQL database to ensure the integrity and accuracy of the migration.
