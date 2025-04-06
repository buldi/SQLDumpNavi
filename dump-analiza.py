#!/usr/bin/env python3

import re
import os
import bz2
import gzip
import argparse
from tabulate import tabulate
from tqdm import tqdm
import io  # brakujący import

class MySQLConnection:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            import mysql.connector
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
        except ImportError:
            import pymysql
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )

    def execute(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()

    def close(self):
        if self.connection:
            self.connection.close()


class PostgreSQLConnection:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            import psycopg2
            self.connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                dbname=self.database
            )
        except ImportError:
            import pg8000
            self.connection = pg8000.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )

    def execute(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()

    def close(self):
        if self.connection:
            self.connection.close()


class SQLDumpAnalyzer:
    def __init__(self, dump_file):
        self.dump_file = dump_file
        self.tables = {}

    def analyze(self):
        file_opener = gzip.open if self.dump_file.endswith('.gz') else open

        with file_opener(self.dump_file, 'rt', encoding='utf-8') as file:
            total_lines = sum(1 for _ in file)
            file.seek(0)

            current_table = None

            for _ in tqdm(range(total_lines), desc="Analyzing file", unit="lines"):
                pos_before_line = file.tell()
                line = file.readline().rstrip()

                create_table_match = re.match(r'CREATE TABLE `?(?P<table_name>\w+)`?', line, re.IGNORECASE)
                if create_table_match:
                    current_table = create_table_match.group('table_name')
                    start_pos = pos_before_line
                    self.tables[current_table] = {
                        'columns': [],
                        'data': [],
                        'insert_count': 0,
                        'estimated_size': 0,
                        'create_table_start': start_pos,
                        'create_table_end': None,
                        'insert_positions': []
                    }
                    continue

                if current_table:
                    column_match = re.match(r'\s*`?(?P<column_name>\w+)`?', line)
                    if column_match:
                        self.tables[current_table]['columns'].append(column_match.group('column_name'))
                        continue

                if current_table and re.match(r'\);', line):
                    self.tables[current_table]['create_table_end'] = file.tell()
                    continue

                insert_match = re.match(r'INSERT INTO `?(?P<table_name>\w+)`?', line, re.IGNORECASE)
                if insert_match:
                    current_table = insert_match.group('table_name')
                    start_pos = pos_before_line
                    self.tables.setdefault(current_table, {
                        'columns': [],
                        'data': [],
                        'insert_count': 0,
                        'estimated_size': 0,
                        'create_table_start': None,
                        'create_table_end': None,
                        'insert_positions': []
                    })
                    self.tables[current_table]['insert_positions'].append((start_pos, None))
                    self.tables[current_table]['insert_count'] += 1
                    continue

                if current_table and self.tables[current_table]['insert_positions']:
                    self.tables[current_table]['insert_positions'][-1] = (
                        self.tables[current_table]['insert_positions'][-1][0],
                        file.tell()
                    )

    def get_tables(self):
        if not self.tables:
            return "No tables found in the dump file."

        table_names = sorted(self.tables.keys())
        output = ["\nAvailable Tables:"]
        for idx, name in enumerate(table_names, 1):
            output.append(f"{idx:3d}. {name}")
        output.append(f"\nTotal tables: {len(table_names)}")
        return "\n".join(output)

    def get_table_stats(self):
        stats = []
        for table_name, table_info in self.tables.items():
            stats.append([
                table_name,
                table_info['insert_count'],
                f"{table_info['estimated_size'] / 1024:.2f} KB"
            ])
        stats.sort(key=lambda x: x[1])
        return stats

    def create_table(self, table_name, db_type='mysql', username=None, password=None, database_name=None, host=None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found in dump file.")

        file_opener = gzip.open if self.dump_file.endswith('.gz') else open

        with file_opener(self.dump_file, 'rt', encoding='utf-8') as file:
            start_pos = self.tables[table_name]['create_table_start']
            end_pos = self.tables[table_name]['create_table_end']
            file.seek(start_pos)
            create_table_sql = file.read(end_pos - start_pos)

        try:
            if db_type == 'mysql':
                connection = MySQLConnection(host, username, password, database_name)
            elif db_type == 'postgres':
                connection = PostgreSQLConnection(host, username, password, database_name)
            else:
                raise ValueError("Unsupported database type.")

            connection.connect()
            connection.execute(create_table_sql)
            connection.close()
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")

    def import_data(self, table_name, db_type='mysql', username=None, password=None, database_name=None, host=None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found in dump file.")

        file_opener = gzip.open if self.dump_file.endswith('.gz') else open

        try:
            if db_type == 'mysql':
                connection = MySQLConnection(host, username, password, database_name)
            elif db_type == 'postgres':
                connection = PostgreSQLConnection(host, username, password, database_name)
            else:
                raise ValueError("Unsupported database type.")

            connection.connect()
            with file_opener(self.dump_file, 'rt', encoding='utf-8') as file:
                for start_pos, end_pos in tqdm(self.tables[table_name]['insert_positions'], desc=f"Importing data into {table_name}", unit="rows"):
                    file.seek(start_pos)
                    insert_statement = file.read(end_pos - start_pos)
                    connection.execute(insert_statement)
            connection.close()
            print(f"Data imported into table {table_name} successfully.")
        except Exception as e:
            print(f"Error importing data into table {table_name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Analyze SQL dump files and import data into MySQL or PostgreSQL.")
    parser.add_argument('dump_file', type=str, help="Path to the SQL dump file (supports .sql and .sql.gz)")
    parser.add_argument('--table', type=str, help="Name of the table to create and import data")
    parser.add_argument('--db-type', type=str, choices=['mysql', 'postgres'], default='mysql',
                        help="Type of database (default: mysql)")
    parser.add_argument('--stats', action='store_true', help="Show statistics about inserts and estimated data size")
    parser.add_argument('--username', type=str, help="Database username")
    parser.add_argument('--password', type=str, help="Database password")
    parser.add_argument('--database', type=str, help="Database name")
    parser.add_argument('--host', type=str, default='localhost', help="Database host (default: localhost)")

    args = parser.parse_args()

    analyzer = SQLDumpAnalyzer(args.dump_file)
    analyzer.analyze()

    # Jeśli podano tylko plik dump bez --table, pokaż statystyki i dostępne tabele
    if not args.table:
        analyzer.get_tables()
        print("\nTable Statistics:")
        stats = analyzer.get_table_stats()
        print(tabulate(stats, headers=["Table Name", "Insert Count", "Estimated Size"], tablefmt="pretty"))
        return  # Zakończ program po analizie

    # Weryfikuj czy wymagane dane do połączenia są dostępne
    required = [args.username, args.password, args.database]
    if not all(required):
        print("Error: --username, --password, and --database are required when using --table")
        return

    # Tworzenie tabeli i import
    analyzer.create_table(args.table, args.db_type, args.username, args.password, args.database, args.host)
    analyzer.import_data(args.table, args.db_type, args.username, args.password, args.database, args.host)

if __name__ == "__main__":
    main()
