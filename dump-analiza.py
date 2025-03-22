#!/usr/bin/env python3

import re
import os
import subprocess
import gzip
import argparse
from tabulate import tabulate
from tqdm import tqdm

class SQLDumpAnalyzer:
    def __init__(self, dump_file):
        self.dump_file = dump_file
        self.tables = {}

    def analyze(self):
        # Sprawdzamy, czy plik jest skompresowany
        if self.dump_file.endswith('.gz'):
            file_opener = gzip.open
        else:
            file_opener = open

        # Otwieramy plik i zliczamy liczbę linii (dla paska postępu)
        with file_opener(self.dump_file, 'rt', encoding='utf-8') as file:
            total_lines = sum(1 for _ in file)  # Zliczanie linii w pliku

        # Ponownie otwieramy plik do analizy
        with file_opener(self.dump_file, 'rt', encoding='utf-8') as file:
            current_table = None
            for line in tqdm(file, total=total_lines, desc="Analyzing file", unit="lines"):
                # Szukamy definicji tabel
                create_table_match = re.match(r'CREATE TABLE `?(?P<table_name>\w+)`?', line, re.IGNORECASE)
                if create_table_match:
                    current_table = create_table_match.group('table_name')
                    self.tables[current_table] = {'columns': [], 'data': [], 'insert_count': 0, 'estimated_size': 0}
                    continue

                # Szukamy kolumn w definicji tabeli
                if current_table and re.match(r'\s*`?(?P<column_name>\w+)`?', line):
                    column_match = re.match(r'\s*`?(?P<column_name>\w+)`?', line)
                    self.tables[current_table]['columns'].append(column_match.group('column_name'))

                # Szukamy danych do wstawienia
                insert_match = re.match(r'INSERT INTO `?(?P<table_name>\w+)`?', line, re.IGNORECASE)
                if insert_match:
                    current_table = insert_match.group('table_name')
                    self.tables[current_table]['data'].append(line.strip())
                    self.tables[current_table]['insert_count'] += 1

        # Szacowanie rozmiaru danych
        for table_name, table_info in self.tables.items():
            if table_info['insert_count'] > 0:
                # Średni rozmiar wiersza (w bajtach) - można dostosować
                avg_row_size = 100  # Przykładowa wartość, można dostosować na podstawie danych
                table_info['estimated_size'] = table_info['insert_count'] * avg_row_size

    def get_tables(self):
        return list(self.tables.keys())

    def get_table_stats(self):
        stats = []
        for table_name, table_info in self.tables.items():
            stats.append([
                table_name,
                table_info['insert_count'],
                f"{table_info['estimated_size'] / 1024:.2f} KB"  # Rozmiar w KB
            ])
        # Sortowanie statystyk według kolumny "Insert Count" (od najmniejszej do największej)
        stats.sort(key=lambda x: x[1])  # x[1] to kolumna "Insert Count"
        return stats

    def create_table(self, table_name, db_type='mysql', username=None, password=None, database_name=None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found in dump file.")

        columns = self.tables[table_name]['columns']
        create_table_sql = f"CREATE TABLE {table_name} (\n"
        create_table_sql += ",\n".join([f"    {col} VARCHAR(255)" for col in columns])
        create_table_sql += "\n);"

        if db_type == 'mysql':
            command = ['mysql', '-u', username, f"-p{password}", '-D', database_name]
            subprocess.run(command, input=create_table_sql, text=True)
        elif db_type == 'postgres':
            os.environ['PGPASSWORD'] = password  # Ustawienie hasła dla PostgreSQL
            command = ['psql', '-U', username, '-d', database_name]
            subprocess.run(command, input=create_table_sql, text=True)
        else:
            raise ValueError("Unsupported database type.")

    def import_data(self, table_name, db_type='mysql', username=None, password=None, database_name=None):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found in dump file.")

        data = self.tables[table_name]['data']
        for insert_statement in tqdm(data, desc=f"Importing data into {table_name}", unit="rows"):
            if db_type == 'mysql':
                command = ['mysql', '-u', username, f"-p{password}", '-D', database_name]
                subprocess.run(command, input=insert_statement, text=True)
            elif db_type == 'postgres':
                os.environ['PGPASSWORD'] = password  # Ustawienie hasła dla PostgreSQL
                command = ['psql', '-U', username, '-d', database_name]
                subprocess.run(command, input=insert_statement, text=True)
            else:
                raise ValueError("Unsupported database type.")

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
        analyzer.create_table(args.table, args.db_type, args.username, args.password, args.database)
        analyzer.import_data(args.table, args.db_type, args.username, args.password, args.database)
    else:
        print("No table specified. Use --table to specify a table to create and import data.")

if __name__ == "__main__":
    main()