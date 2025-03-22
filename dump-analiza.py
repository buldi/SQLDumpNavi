#!/usr/bin/env python3

import re

# import os
import subprocess
import gzip
import argparse


class SQLDumpAnalyzer:
    def __init__(self, dump_file):
        self.dump_file = dump_file
        self.tables = {}

    def analyze(self):
        # Sprawdzamy, czy plik jest skompresowany
        if self.dump_file.endswith(".gz"):
            file_opener = gzip.open
        else:
            file_opener = open

        with file_opener(self.dump_file, "rt", encoding="utf-8") as file:
            current_table = None
            for line in file:
                # Szukamy definicji tabel
                create_table_match = re.match(
                    r"CREATE TABLE `?(?P<table_name>\w+)`?", line, re.IGNORECASE
                )
                if create_table_match:
                    current_table = create_table_match.group("table_name")
                    self.tables[current_table] = {"columns": [], "data": []}
                    continue

                # Szukamy kolumn w definicji tabeli
                if current_table and re.match(r"\s*`?(?P<column_name>\w+)`?", line):
                    column_match = re.match(r"\s*`?(?P<column_name>\w+)`?", line)
                    self.tables[current_table]["columns"].append(
                        column_match.group("column_name")
                    )

                # Szukamy danych do wstawienia
                insert_match = re.match(
                    r"INSERT INTO `?(?P<table_name>\w+)`?", line, re.IGNORECASE
                )
                if insert_match:
                    current_table = insert_match.group("table_name")
                    self.tables[current_table]["data"].append(line.strip())

    def get_tables(self):
        return list(self.tables.keys())

    def create_table(self, table_name, db_type="mysql"):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found in dump file.")

        columns = self.tables[table_name]["columns"]
        create_table_sql = f"CREATE TABLE {table_name} (\n"
        create_table_sql += ",\n".join([f"    {col} VARCHAR(255)" for col in columns])
        create_table_sql += "\n);"

        if db_type == "mysql":
            subprocess.run(
                ["mysql", "-u", "username", "-p", "database_name"],
                input=create_table_sql,
                text=True,
            )
        elif db_type == "postgres":
            subprocess.run(
                ["psql", "-U", "username", "-d", "database_name"],
                input=create_table_sql,
                text=True,
            )
        else:
            raise ValueError("Unsupported database type.")

    def import_data(self, table_name, db_type="mysql"):
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found in dump file.")

        data = self.tables[table_name]["data"]
        for insert_statement in data:
            if db_type == "mysql":
                subprocess.run(
                    ["mysql", "-u", "username", "-p", "database_name"],
                    input=insert_statement,
                    text=True,
                )
            elif db_type == "postgres":
                subprocess.run(
                    ["psql", "-U", "username", "-d", "database_name"],
                    input=insert_statement,
                    text=True,
                )
            else:
                raise ValueError("Unsupported database type.")


def main():
    # Konfiguracja parsera argumentów
    parser = argparse.ArgumentParser(
        description="Analyze SQL dump files and import data into MySQL or PostgreSQL."
    )
    parser.add_argument(
        "dump_file",
        type=str,
        help="Path to the SQL dump file (supports .sql and .sql.gz)",
    )
    parser.add_argument(
        "--table", type=str, help="Name of the table to create and import data"
    )
    parser.add_argument(
        "--db-type",
        type=str,
        choices=["mysql", "postgres"],
        default="mysql",
        help="Type of database (mysql or postgres, default: mysql)",
    )

    args = parser.parse_args()

    # Inicjalizacja analizatora
    analyzer = SQLDumpAnalyzer(args.dump_file)
    analyzer.analyze()

    # Wyświetlenie dostępnych tabel
    print("Available tables:", analyzer.get_tables())

    # Jeśli podano tabelę, tworzymy ją i importujemy dane
    if args.table:
        analyzer.create_table(args.table, args.db_type)
        analyzer.import_data(args.table, args.db_type)
    else:
        print(
            "No table specified. Use --table to specify a table to create and import data."
        )


if __name__ == "__main__":
    main()
