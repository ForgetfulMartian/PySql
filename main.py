import mysql.connector
from mysql.connector import Error
from collections import Counter
import csv
import json
from decimal import Decimal


def create_mysql_connection(username, password, host, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=username,
            password=password,
            database=database
        )
        if connection.is_connected():
            print(f"Connected to MySQL database '{database}'")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None


def get_table_and_column_names(connection):
    try:
        cursor = connection.cursor()

        # Get the list of tables in the database
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]

        if not tables:
            print("No tables found in the database.")
            return None, None

        # Choose a table
        print("Available tables:")
        for idx, table in enumerate(tables):
            print(f"{idx + 1}. {table}")

        table_idx = int(input("Enter the index of the table: ")) - 1

        if table_idx < 0 or table_idx >= len(tables):
            print("Invalid table index.")
            return None, None

        table_name = tables[table_idx]

        # Get the list of columns in the chosen table
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns = [column[0] for column in cursor.fetchall()]

        # Choose a column
        print("Available columns:")
        for idx, column in enumerate(columns):
            print(f"{idx + 1}. {column}")

        column_idx = int(input("Enter the index of the column: ")) - 1

        if column_idx < 0 or column_idx >= len(columns):
            print("Invalid column index.")
            return None, None

        column_name = columns[column_idx]

        return table_name, column_name

    except Error as e:
        print(f"Error getting table and column names: {e}")
        return None, None
    finally:
        if 'cursor' in locals():
            cursor.close()


def describe_table(connection, table_name):
    try:
        cursor = connection.cursor(dictionary=True)

        # Get column information from information_schema.COLUMNS
        describe_query = f"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        cursor.execute(describe_query)
        columns_info = cursor.fetchall()

        # Display column information
        print(f"\nColumn Information for '{table_name}':")
        print("===================================")
        for col_info in columns_info:
            print(f"{col_info['COLUMN_NAME']:<20} {col_info['DATA_TYPE']:<20}")

    except Error as e:
        print(f"Error describing table: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()


def fill_missing_values(connection):
    try:
        cursor = connection.cursor()
        table_name, column_name = get_table_and_column_names(connection)

        # Prompt user for fill method
        print("Choose fill method:")
        print("1. Mean")
        print("2. Median")
        print("3. Mode")
        fill_method_choice = input("Enter your choice: ")

        if fill_method_choice not in ["1", "2", "3"]:
            print("Invalid choice.")
            return

        # Calculate fill value based on the chosen method
        fill_value = calculate_fill_value(connection, table_name, column_name, fill_method_choice)

        # Update the table to fill missing values
        update_query = f"UPDATE {table_name} SET {column_name} = {fill_value} WHERE {column_name} IS NULL"
        cursor.execute(update_query)

        # Commit the changes
        connection.commit()

        print(f"Missing values filled in column '{column_name}' using method {fill_method_choice}.")

    except Error as e:
        print(f"Error filling missing values: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()


def calculate_fill_value(connection, table_name, column_name, fill_method):
    cursor = connection.cursor()

    # Get all values from the specified column
    cursor.execute(f"SELECT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL")
    values = [row[0] for row in cursor.fetchall()]

    # Calculate fill value based on the chosen method
    if fill_method == "1":
        fill_value = sum(values) / len(values) if values else None  # Mean
    elif fill_method == "2":
        values.sort()
        n = len(values)
        fill_value = (values[n // 2] + values[(n - 1) // 2]) / 2 if values else None  # Median
    elif fill_method == "3":
        fill_value = max(set(values), key=values.count) if values else None  # Mode
    else:
        print("Invalid fill method.")
        fill_value = None

    cursor.close()
    return fill_value


def head(connection, table_name, num_rows=5):
    try:
        cursor = connection.cursor(dictionary=True)
        query = f"SELECT * FROM {table_name} LIMIT {num_rows}"
        cursor.execute(query)
        rows = cursor.fetchall()

        print(f"Top {num_rows} rows of table '{table_name}':")
        for row in rows:
            print(row)

    except Error as e:
        print(f"Error fetching data: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()


def foot(connection, table_name, num_rows=5):
    try:
        cursor = connection.cursor(dictionary=True)
        query = f"SELECT * FROM {table_name} ORDER BY 1 DESC LIMIT {num_rows}"
        cursor.execute(query)
        rows = cursor.fetchall()

        print(f"Bottom {num_rows} rows of table '{table_name}':")
        for row in rows:
            print(row)

    except Error as e:
        print(f"Error fetching data: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()


def create_categorical_encoding(connection):
    try:
        cursor = connection.cursor()
        table_name, existing_column = get_table_and_column_names(connection)

        new_column = input("Enter new column name for encoding: ")

        # Build the SQL query to create a new column for categorical encoding
        query = f"ALTER TABLE {table_name} ADD COLUMN {new_column} INT"
        cursor.execute(query)

        # Get distinct values from the existing column
        distinct_values_query = f"SELECT DISTINCT {existing_column} FROM {table_name} WHERE {existing_column} IS NOT NULL"
        cursor.execute(distinct_values_query)
        distinct_values = [row[0] for row in cursor.fetchall()]

        # Create a mapping dictionary for encoding
        encoding_map = {value: index + 1 for index, value in enumerate(distinct_values)}

        # Update the new column with categorical encoding based on the existing column
        for value, encoding in encoding_map.items():
            update_query = f"UPDATE {table_name} SET {new_column} = {encoding} WHERE {existing_column} = '{value}'"
            cursor.execute(update_query)

        # Commit the changes
        connection.commit()

        print(f"New categorical encoding column '{new_column}' created in MySQL table '{table_name}'")
    except Error as e:
        print(f"Error creating categorical encoding column in MySQL: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()


def calculate_stats(data):
    count = len(data)
    total = sum(data)
    mean = total / count if count > 0 else None
    sorted_data = sorted(data)
    mid = count // 2
    median = (sorted_data[mid] + sorted_data[mid - 1]) / 2 if count % 2 == 0 else sorted_data[mid]
    mode_result = Counter(data).most_common(1)
    mode = mode_result[0][0] if mode_result else None
    variance = float(sum((x - mean) ** 2 for x in data) / count) if count > 0 else None
    std_dev = variance ** 0.5 if variance is not None else None
    min_value = min(data) if count > 0 else None
    max_value = max(data) if count > 0 else None
    range_value = max_value - min_value if count > 0 else None
    interquartile_range = (
            sorted_data[int(0.75 * count)] - sorted_data[int(0.25 * count)]
    ) if count > 0 else None
    skewness = (
            float(float((sum((x - mean) ** 3 for x in data)) / count)) /
            ((variance ** 1.5) if variance is not None else None)
    ) if count > 0 else None
    kurtosis = (
                       (float(sum((x - mean) ** 4 for x in data)) / count) /
                       ((variance ** 2) if variance is not None else None)
               ) - 3 if count > 0 else None

    return {
        'count': count,
        'sum': total,
        'mean': mean,
        'median': median,
        'mode': mode,
        'std_dev': std_dev,
        'variance': variance,
        'min_value': min_value,
        'max_value': max_value,
        'range': range_value,
        'interquartile_range': interquartile_range,
        'skewness': skewness,
        'kurtosis': kurtosis
    }


def show_column_stats(connection, table_name, column_name):
    try:
        cursor = connection.cursor()

        # Retrieve data from the column
        cursor.execute(f"SELECT {column_name} FROM {table_name}")
        data = [row[0] for row in cursor.fetchall()]

        if not data:
            print(f"No data found in column '{column_name}' of table '{table_name}'.")
            return

        # Calculate and display statistics
        stats = calculate_stats(data)

        print("\nColumn Statistics:")
        print("===================")
        print(f"Count: {stats['count']}")
        print(f"Sum: {stats['sum']}")
        print(f"Mean: {stats['mean']}")
        print(f"Median: {stats['median']}")
        print(f"Mode: {stats['mode']}")
        print(f"Standard Deviation: {stats['std_dev']}")
        print(f"Variance: {stats['variance']}")
        print(f"Min Value: {stats['min_value']}")
        print(f"Max Value: {stats['max_value']}")
        print(f"Range: {stats['range']}")
        print(f"Interquartile Range: {stats['interquartile_range']}")
        print(f"Skewness: {stats['skewness']}")
        print(f"Kurtosis: {stats['kurtosis']}")

    except Error as e:
        print(f"Error retrieving column statistics: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()


def calculate_column_correlation(connection, table_name):
    try:
        import pandas as pd
        # Fetch data into a Pandas DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, connection)

        # Filter out non-numerical columns
        numerical_columns = df.select_dtypes(include=['number']).columns

        # Calculate and print correlation between numerical columns
        correlation_table = df[numerical_columns].corr()

        print("Correlation Table:")
        print(correlation_table)

    except Error as e:
        print(f"Error calculating correlation table: {e}")


def get_column_types(data):
    column_types = {}

    for record in data:
        for key, value in record.items():
            if key not in column_types:
                # Determine data type for the column based on the first non-empty value
                if value is not None and not isinstance(value, (int, float)):
                    column_types[key] = 'VARCHAR(255)'
                elif isinstance(value, (int, float)):
                    column_types[key] = 'DOUBLE'
                else:
                    column_types[key] = 'VARCHAR(255)'

    return column_types


def get_column_type(data):
    column_types = {}

    for row in data:
        for col_idx, value in enumerate(row):
            col_name = f'column_{col_idx + 1}'  # Generate a placeholder column name
            if col_name not in column_types:
                # Determine data type for the column based on the first non-empty value
                if value is not None and not isinstance(value, (int, float, Decimal)):
                    column_types[col_name] = 'VARCHAR(255)'
                elif isinstance(value, (int, float)):
                    column_types[col_name] = 'DOUBLE'
                else:
                    column_types[col_name] = 'VARCHAR(255)'

    return column_types


def import_data(connection, table_name, file_path):
    try:
        cursor = connection.cursor()

        # Determine file type based on extension
        file_extension = file_path.split('.')[-1].lower()

        if file_extension == 'csv':
            with open(file_path, 'r', encoding='utf-8', errors='replace') as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)

                # Read a few rows to determine column types
                sample_data = [next(csv_reader) for _ in range(5)]

                # Create table if it doesn't exist
                column_types = get_column_type(sample_data)
                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(f'{col} {column_types[col]}' for col in column_types)})"
                cursor.execute(create_table_query)

                # Insert data into the table
                insert_query = f"INSERT INTO {table_name} ({', '.join(column_types)}) VALUES ({', '.join(['%s' for _ in column_types])})"
                for row in csv_reader:
                    cursor.execute(insert_query, row)

        elif file_extension == 'json':
            with open(file_path, 'r') as jsonfile:
                data = json.load(jsonfile)
                if not data:
                    print("JSON file is empty.")
                    return

                # Extract column names from the first record
                header = list(data[0].keys())

                # Create table if it doesn't exist
                column_types = get_column_type(data)
                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(f'{col} {column_types[col]}' for col in column_types)})"
                cursor.execute(create_table_query)

                # Insert data into the table
                insert_query = f"INSERT INTO {table_name} ({', '.join(column_types)}) VALUES ({', '.join(['%s' for _ in column_types])})"
                for row in data:
                    cursor.execute(insert_query, tuple(row.values()))

        else:
            print("Unsupported file format. Only CSV and JSON are supported.")

        # Commit the changes
        connection.commit()
        print(f"Data imported from {file_extension.upper()} '{file_path}' to table '{table_name}'")

    except Error as e:
        print(f"Error importing data: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()


def main():
    connection = None

    while True:
        print("\n===== Menu =====")
        print("1. Connect to MySQL")
        print("2. Create a new column with categorical encoding")
        print("3. Describe Column")
        print("4. Describe Table")
        print("5. fill NULL using central tendencies")
        print("6. Correlation table")
        print("7. Head")
        print("8. Foot")
        print("A. Import CSV/JSON")
        print("B. Export CSV/JSON")
        print("0. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            # Connect to MySQL
            if connection is not None:
                print("Already connected to MySQL.")
            else:
                username = input("Enter MySQL username: ")
                password = input("Enter MySQL password: ")
                host = input("Enter MySQL host: ")
                database = input("Enter MySQL database: ")
                connection = create_mysql_connection(username, password, host, database)
                if connection is not None:
                    print("Connected to MySQL.")

        elif choice == "2":
            # Create a new column with categorical encoding
            if connection is None:
                print("Please connect to MySQL first.")
            else:
                create_categorical_encoding(connection)

        elif choice == "3":
            if connection is None:
                print("Please connect to MySQL first.")
            else:
                table_name, column_name = get_table_and_column_names(connection)
                show_column_stats(connection, table_name, column_name)

        elif choice == "4":
            # Describe a table
            if connection is None:
                print("Please connect to MySQL first.")
            else:
                table_name = input("Enter table name: ")
                describe_table(connection, table_name)

        elif choice == "5":
            # fill values
            if connection is None:
                print("Please connect to MySQL first.")
            else:
                fill_missing_values(connection)

        elif choice == "6":

            if connection is None:
                print("Please connect to MySQL first.")
            else:
                cursor = connection.cursor()

                # Get the list of tables in the database
                cursor.execute("SHOW TABLES")
                tables = [table[0] for table in cursor.fetchall()]

                if not tables:
                    print("No tables found in the database.")
                    return None, None

                # Choose a table
                print("Available tables:")
                for idx, table in enumerate(tables):
                    print(f"{idx + 1}. {table}")

                table_idx = int(input("Enter the index of the table: ")) - 1

                if table_idx < 0 or table_idx >= len(tables):
                    print("Invalid table index.")
                    return None, None

                table_name = tables[table_idx]
                calculate_column_correlation(connection, table_name)

        elif choice == "7":
            # fill values
            if connection is None:
                print("Please connect to MySQL first.")
            else:
                cursor = connection.cursor()

                # Get the list of tables in the database
                cursor.execute("SHOW TABLES")
                tables = [table[0] for table in cursor.fetchall()]

                if not tables:
                    print("No tables found in the database.")
                    return None, None

                # Choose a table
                print("Available tables:")
                for idx, table in enumerate(tables):
                    print(f"{idx + 1}. {table}")

                table_idx = int(input("Enter the index of the table: ")) - 1

                if table_idx < 0 or table_idx >= len(tables):
                    print("Invalid table index.")
                    return None, None

                table_name = tables[table_idx]
                num_rows = int(input("No. of rows:"))
                head(connection, table_name, num_rows)

        elif choice == "8":
            # fill values
            if connection is None:
                print("Please connect to MySQL first.")
            else:
                cursor = connection.cursor()

                # Get the list of tables in the database
                cursor.execute("SHOW TABLES")
                tables = [table[0] for table in cursor.fetchall()]

                if not tables:
                    print("No tables found in the database.")
                    return None, None

                # Choose a table
                print("Available tables:")
                for idx, table in enumerate(tables):
                    print(f"{idx + 1}. {table}")

                table_idx = int(input("Enter the index of the table: ")) - 1

                if table_idx < 0 or table_idx >= len(tables):
                    print("Invalid table index.")
                    return None, None

                table_name = tables[table_idx]
                num_rows = int(input("No. of rows:"))
                foot(connection, table_name, num_rows)

        elif choice == "9":

            if connection is None:
                print("Please connect to MySQL first.")
            else:

                table = input("Enter table name:")
                location = input("Enter location of file to be imported:")
                import_data(connection, table, location)

        elif choice == "0":
            # Exit the loop
            print("Exiting...")
            break

        else:
            print("Invalid choice. Please enter a valid option.")

    # Close the MySQL connection before exiting
    if connection is not None:
        connection.close()
        print("MySQL connection closed.")


if __name__ == "__main__":
    main()
