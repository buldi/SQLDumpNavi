#!/usr/bin/env python3

import re
import os
import gzip
import argparse
from tabulate import tabulate
from tqdm import tqdm
import pymysql  # Alternatywna biblioteka dla MySQL
import pg8000  # Alternatywna biblioteka dla PostgreSQL

class SQLDumpAnalyzer:
    def __init__(self, dump_file):
        self.dump_file = dump_file
        self.tables = {}

    def analyze(self):
        # Sprawdzamy, czy plik jest skompresowany
        # file_opener = gzip.open if self.dump_file.endswith('.gz') else open
        # Otwieramy plik i zliczamy liczbę linii (dla paska postępu)
        with file_opener(self.dump_file, 'rt', encoding='utf-8') as file:
            total_lines = sum(1 for _ in file)  # Zliczanie linii w pliku

        # Ponownie otwieramy plik do analizy
        with file_opener(self.dump_file, 'rt', encoding='utf-8') as file:
            current_table = None
            for line in tqdm(file, total=total_lines, desc="Analyzing file", unit="lines"):
                if create_table_match := re.match(
                    r'CREATE TABLE `?(?P<table_name>\w+)`?', line, re.IGNORECASE
                ):
                    current_table. = create_table_match.group('table_name')
                    # Zapisujemy pozycję początkową definicji tabeli
                    start_pos = file.tell() - len(line.encode('utf-8'))
                    self.tables[current_table] = {
                        'columns': [],
                        'data': [],
                        'insert_count': 0,
                        'estimated_size': 0,
                        'create_table_start': start_pos,
                        'create_table_end': None,
                        'insert_positions': []  # Lista krotek (start, end) dla INSERT
                    }
                    continue

                # Szukamy kolumn w definicji tabeli
                if current_table and re.match(r'\s*`?(?P<column_name>\w+)`?', line):
                    column_match = re.match(r'\s*`?(?P<column_name>\w+)`?', line)
                    self.tables[current_table]['columns'].append(column_match.group('column_name'))

                # Szukamy końca definicji tabeli
                if current_table and re.match(r'\);', line):
                    # Zapisujemy pozycję końcową definicji tabeli
                    self.tables[current_table]['create_table_end'] = file.tell()

                # Szukamy danych do wstawienia
                insert_match = re.match(r'INSERT INTO `?(?P<table_name>\w+)`?', line, re.IGNORECASE)
                if insert_match:
                    current_table = insert_match.group('table_name')
                    # Zapisujemy pozycję początkową INSERT
                    start_pos = file.tell() - len(line.encode('utf-8'))
                    self.tables[current_table]['insert_positions'].append((start_pos, None))
                    self.tables[current_table]['insert_count'] += 1

                # Zapisujemy pozycję końcową INSERT
                if current_table and self.tables[current_table]['insert_positions']:
                    self.tables[current_table]['insert_positions'][-1] = (
                        self.tables[current_table]['insert_positions'][-1][0],
                        file.tell()
                    )

    def get_tables(self):
        return list(self.tables.keys())

    def get_table_stats(self):
        stats = []
        stats.extend(
            [
                table_name,
                table_info['insert_count'],
                f"{table_info['estimated_size'] / 1024:.2f} KB",  # Rozmiar w KB - szacowany
            ]
            for table_name, table_info in self.tables.items()
        )
        # Sortowanie statystyk według kolumny "Insert Count" (od najmniejszej do największej)
        stats.sort(key=lambda x: x[1])  # x[1] to kolumna "Insert Count"
        return stats

    def create_table(self, table_name, db_type='mysql', username=None, password=None, database_name=None, host=None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found in dump file.")

        # Otwieramy plik i odczytujemy oryginalną definicję tabeli
        file_opener = gzip.open if self.dump_file.endswith('.gz') else open
        with file_opener(self.dump_file, 'rt', encoding='utf-8') as file:
            start_pos = self.tables[table_name]['create_table_start']
            end_pos = self.tables[table_name]['create_table_end']
            file.seek(start_pos)
            create_table_sql = file.read(end_pos - start_pos)

        try:
            if db_type == 'mysql':
                # Połączenie z MySQL za pomocą pymysql
                connection = pymysql.connect(
                    host=host,
                    user=username,
                    password=password,
                    database=database_name
                )
            elif db_type == 'postgres':
                # Połączenie z PostgreSQL za pomocą pg8000
                connection = pg8000.connect(
                    host=host,
                    user=username,
                    password=password,
                    database=database_name
                )
            else:
                raise ValueError("Unsupported database type.")

            cursor = connection.cursor()
            cursor.execute(create_table_sql)
            connection.commit()
            cursor.close()
            connection.close()
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")

    def import_data(self, table_name, db_type='mysql', username=None, password=None, database_name=None, host=None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found in dump file.")

        # Otwieramy plik i odczytujemy oryginalne INSERTy
        file_opener = gzip.open if self.dump_file.endswith('.gz') else open
        try:
            if db_type == 'mysql':
                # Połączenie z MySQL za pomocą pymysql
                connection = pymysql.connect(
                    host=host,
                    user=username,
                    password=password,
                    database=database_name
                )
            elif db_type == 'postgres':
                # Połączenie z PostgreSQL za pomocą pg8000
                connection = pg8000.connect(
                    host=host,
                    user=username,
                    password=password,
                    database=database_name
                )
            else:
                raise ValueError("Unsupported database type.")

            cursor = connection.cursor()
            with file_opener(self.dump_file, 'rt', encoding='utf-8') as file:
                for start_pos, end_pos in tqdm(self.tables[table_name]['insert_positions'], desc=f"Importing data into {table_name}", unit="rows"):
                    file.seek(start_pos)
                    insert_statement = file.read(end_pos - start_pos)
                    cursor.execute(insert_statement)
            connection.commit()
            cursor.close()
            connection.close()
            print(f"Data imported into table {table_name} successfully.")
        except Exception as e:
            print(f"Error importing data into table {table_name}: {e}")

def main():
    # Konfiguracja parsera argumentów
    parser = argparse.ArgumentParser(description="Analyze SQL dump files and import data into MySQL or PostgreSQL.")
    parser.add_argument('dump_file', type=str, help="Path to the SQL dump file (supports .sql and .sql.gz)")
    parser.add_argument('--table', type=str, help="Name of the table to create and import data")
    parser.add_argument('--db-type', type=str, choices=['mysql', 'postgres'], default='mysql',
                        help="Type of database (mysql or postgres, default: mysql)")
    parser.add_argument('--stats', action='store_true', help="Show statistics about inserts and estimated data size")
    parser.add_argument('--username', type=str, required=True, help="Database username")
    parser.add_argument('--password', type=str, required=True, help="Database password")
    parser.add_argument('--database', type=str, required=True, help="Database name")
    parser.add_argument('--host', type=str, default='localhost', help="Database host (default: localhost)")

    args = parser.parse_args()

    # Inicjalizacja analizatora
    analyzer = SQLDumpAnalyzer(args.dump_file)
    analyzer.analyze()

    # Wyświetlenie dostępnych tabel
    print("Available tables:", analyzer.get_tables())

    # Jeśli włączono statystyki, wyświetl je
    if args.stats:
        stats = analyzer.get_table_stats()
        print("\nTable Statistics:")
        print(tabulate(stats, headers=["Table Name", "Insert Count", "Estimated Size"], tablefmt="pretty"))

    # Jeśli podano tabelę, tworzymy ją i importujemy dane
    if args.table:
        analyzer.create_table(args.table, args.db_type, args.username, args.password, args.database, args.host)
        analyzer.import_data(args.table, args.db_type, args.username, args.password, args.database, args.host)
    else:
        print("No table specified. Use --table to specify a table to create and import data.")

if __name__ == "__main__":
    main()