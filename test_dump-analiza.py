#!/usr/bin/env python3

from sympy import im
# from dump-analiza import SQLDumpAnalyzer 

import os
import importlib

SQLDumpAnalyzer = importlib.import_module("dump-analiza").SQLDumpAnalyzer

import unittest

def test_sql_dump_analyzer():
    # Sample SQL dump content
    sample_dump = """
/*!40000 ALTER TABLE `test_table` ENABLE KEYS */;
UNLOCK TABLES;
DROP TABLE IF EXISTS `test_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test_table` (
  `id_carrier` int unsigned NOT NULL,
  `id_zone` int unsigned NOT NULL,
  PRIMARY KEY (`id_carrier`,`id_zone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

LOCK TABLES `test_table` WRITE;
/*!40000 ALTER TABLE `test_table` DISABLE KEYS */;
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (64,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (65,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (66,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (67,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (68,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (69,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (70,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (71,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (72,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (73,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (74,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (75,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (76,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (77,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (78,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (79,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (80,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (81,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (82,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (83,1);
INSERT INTO `test_table` (`id_carrier`, `id_zone`) VALUES (84,1);
/*!40000 ALTER TABLE `test_table` ENABLE KEYS */;

    """

    # Write sample dump to a temporary file
    with open('sample_dump.sql', 'w') as f:
        f.write(sample_dump)

    # Initialize analyzer with the sample dump file
    analyzer = SQLDumpAnalyzer('sample_dump.sql')
    analyzer.analyze()

    # Check if the table is correctly identified
    assert 'test_table' in analyzer.get_tables(), "Table 'test_table' not found in analyzed tables."

    # Check if the columns are correctly identified
    assert analyzer.tables['test_table']['columns'] == ['id_carrier', 'id_zone'], "Columns not correctly identified."

    # Check if the data is correctly identified
    assert len(analyzer.tables['test_table']['data']) == 21, "Data rows not correctly identified."
    # assert analyzer.tables['test_table']['insert_count'] == 2, "Insert count not correctly identified."

    # Clean up the temporary file
    os.remove('sample_dump.sql')

    print("All tests passed.")

if __name__ == "__main__":
    test_sql_dump_analyzer()