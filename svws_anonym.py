#!/usr/bin/env python3
"""
SVWS Database Anonymization Tool

This tool anonymizes MariaDB databases from the SVWS-Server project,
allowing them to be shared or used for testing and training purposes.
"""

import sys
import getpass
import random
import string
from typing import Optional, List, Dict, Any
import pymysql
from pymysql import Connection
from faker import Faker


class SVWSAnonymizer:
    """Main class for anonymizing SVWS databases."""

    def __init__(self, host: str, port: int, user: str, password: str):
        """
        Initialize the anonymizer.

        Args:
            host: Database host
            port: Database port
            user: Database username
            password: Database password
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection: Optional[Connection] = None
        self.fake = Faker('de_DE')  # German locale for realistic German data
        Faker.seed(42)  # For reproducible results

    def connect(self) -> None:
        """Establish connection to the database server."""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            print(f"✓ Verbindung zum Datenbankserver hergestellt: {self.host}:{self.port}")
        except Exception as e:
            print(f"✗ Fehler beim Verbinden zur Datenbank: {e}")
            sys.exit(1)

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            print("✓ Datenbankverbindung geschlossen")

    def list_schemas(self) -> List[str]:
        """
        List all available schemas (databases) on the server.

        Returns:
            List of schema names
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SHOW DATABASES")
                schemas = [row['Database'] for row in cursor.fetchall()]
                # Filter out system databases
                system_dbs = ['information_schema', 'performance_schema', 'mysql', 'sys']
                return [s for s in schemas if s not in system_dbs]
        except Exception as e:
            print(f"✗ Fehler beim Abrufen der Schemas: {e}")
            return []

    def select_schema(self) -> str:
        """
        Prompt user to select a schema to anonymize.

        Returns:
            Selected schema name
        """
        schemas = self.list_schemas()
        
        if not schemas:
            print("✗ Keine Schemas gefunden!")
            sys.exit(1)

        print("\nVerfügbare Schemas:")
        for i, schema in enumerate(schemas, 1):
            print(f"  {i}. {schema}")

        while True:
            try:
                choice = input("\nWelches Schema soll anonymisiert werden? (Nummer oder Name): ").strip()
                
                # Try to parse as number
                if choice.isdigit():
                    index = int(choice) - 1
                    if 0 <= index < len(schemas):
                        return schemas[index]
                # Try as direct name
                elif choice in schemas:
                    return choice
                
                print("✗ Ungültige Auswahl. Bitte versuchen Sie es erneut.")
            except KeyboardInterrupt:
                print("\n\nAbbruch durch Benutzer.")
                sys.exit(0)

    def get_tables(self, schema: str) -> List[str]:
        """
        Get all tables in the specified schema.

        Args:
            schema: Schema name

        Returns:
            List of table names
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SHOW TABLES FROM `{schema}`")
                return [list(row.values())[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"✗ Fehler beim Abrufen der Tabellen: {e}")
            return []

    def anonymize_schema(self, schema: str) -> None:
        """
        Anonymize all personal data in the specified schema.

        Args:
            schema: Schema name to anonymize
        """
        print(f"\n=== Anonymisierung von Schema '{schema}' ===\n")

        # Switch to the schema
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"USE `{schema}`")
        except Exception as e:
            print(f"✗ Fehler beim Wechseln zum Schema: {e}")
            return

        tables = self.get_tables(schema)
        print(f"Gefundene Tabellen: {len(tables)}\n")

        # Define anonymization rules for common SVWS tables/columns
        anonymization_rules = self._get_anonymization_rules()

        for table in tables:
            print(f"Verarbeite Tabelle: {table}")
            self._anonymize_table(schema, table, anonymization_rules)

        self.connection.commit()
        print(f"\n✓ Anonymisierung von Schema '{schema}' abgeschlossen!")

    def _get_anonymization_rules(self) -> Dict[str, Dict[str, Any]]:
        """
        Define anonymization rules for common column patterns.

        Returns:
            Dictionary mapping column patterns to anonymization functions
        """
        return {
            # Names
            'nachname': lambda: self.fake.last_name(),
            'vorname': lambda: self.fake.first_name(),
            'name': lambda: self.fake.last_name(),
            'familienname': lambda: self.fake.last_name(),
            'rufname': lambda: self.fake.first_name(),
            
            # Contact information
            'email': lambda: self.fake.email(),
            'e_mail': lambda: self.fake.email(),
            'telefon': lambda: self.fake.phone_number(),
            'telefonnummer': lambda: self.fake.phone_number(),
            'handy': lambda: self.fake.phone_number(),
            'mobilnummer': lambda: self.fake.phone_number(),
            
            # Address information
            'strasse': lambda: self.fake.street_name(),
            'strassenname': lambda: self.fake.street_name(),
            'hausnummer': lambda: str(random.randint(1, 999)),
            'plz': lambda: self.fake.postcode(),
            'postleitzahl': lambda: self.fake.postcode(),
            'ort': lambda: self.fake.city(),
            'wohnort': lambda: self.fake.city(),
            'stadt': lambda: self.fake.city(),
            
            # Personal information
            'geburtsdatum': lambda: self.fake.date_of_birth(minimum_age=6, maximum_age=80).strftime('%Y-%m-%d'),
            'geburtsort': lambda: self.fake.city(),
            'geburtsname': lambda: self.fake.last_name(),
            
            # Other sensitive data
            'bemerkung': lambda: 'Anonymisiert',
            'kommentar': lambda: 'Anonymisiert',
            'notiz': lambda: 'Anonymisiert',
        }

    def _anonymize_table(self, schema: str, table: str, rules: Dict[str, Any]) -> None:
        """
        Anonymize a specific table based on rules.

        Args:
            schema: Schema name
            table: Table name
            rules: Anonymization rules
        """
        try:
            # Get column information
            with self.connection.cursor() as cursor:
                cursor.execute(f"DESCRIBE `{table}`")
                columns = cursor.fetchall()

            # Find columns that need anonymization
            columns_to_anonymize = []
            for col in columns:
                col_name = col['Field'].lower()
                for pattern, func in rules.items():
                    if pattern in col_name:
                        columns_to_anonymize.append((col['Field'], func))
                        break

            if not columns_to_anonymize:
                print(f"  → Keine zu anonymisierenden Spalten gefunden")
                return

            # Get primary key to update rows individually
            with self.connection.cursor() as cursor:
                cursor.execute(f"SHOW KEYS FROM `{table}` WHERE Key_name = 'PRIMARY'")
                pk_result = cursor.fetchall()
                if not pk_result:
                    print(f"  → Übersprungen (kein Primärschlüssel gefunden)")
                    return
                pk_column = pk_result[0]['Column_name']

            # Fetch all primary keys
            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT `{pk_column}` FROM `{table}`")
                rows = cursor.fetchall()

            if not rows:
                print(f"  → Keine Zeilen zum Anonymisieren")
                return

            # Update each row
            count = 0
            with self.connection.cursor() as cursor:
                for row in rows:
                    pk_value = row[pk_column]
                    updates = []
                    values = []
                    
                    for col_name, func in columns_to_anonymize:
                        updates.append(f"`{col_name}` = %s")
                        values.append(func())
                    
                    if updates:
                        values.append(pk_value)
                        sql = f"UPDATE `{table}` SET {', '.join(updates)} WHERE `{pk_column}` = %s"
                        cursor.execute(sql, values)
                        count += 1

            print(f"  → {count} Zeile(n) anonymisiert ({len(columns_to_anonymize)} Spalte(n))")

        except Exception as e:
            print(f"  → Fehler: {e}")


def main():
    """Main entry point for the application."""
    print("=" * 60)
    print("SVWS Datenbank Anonymisierungstool")
    print("=" * 60)
    print()

    # Get database connection details
    print("Datenbank-Verbindungsinformationen:")
    host = input("  Host [localhost]: ").strip() or "localhost"
    port_str = input("  Port [3306]: ").strip() or "3306"
    try:
        port = int(port_str)
    except ValueError:
        print("✗ Ungültiger Port. Verwende Standard-Port 3306.")
        port = 3306

    user = input("  Benutzername: ").strip()
    if not user:
        print("✗ Benutzername ist erforderlich!")
        sys.exit(1)

    password = getpass.getpass("  Passwort: ")

    # Create anonymizer instance
    anonymizer = SVWSAnonymizer(host, port, user, password)

    try:
        # Connect to database
        anonymizer.connect()

        # Select schema to anonymize
        schema = anonymizer.select_schema()

        # Confirm before proceeding
        print(f"\n⚠ WARNUNG: Das Schema '{schema}' wird anonymisiert!")
        print("  Alle personenbezogenen Daten werden durch Fake-Daten ersetzt.")
        print("  Dieser Vorgang kann NICHT rückgängig gemacht werden!")
        
        confirmation = input("\nFortfahren? (ja/nein): ").strip().lower()
        if confirmation not in ['ja', 'j', 'yes', 'y']:
            print("\nAbbruch durch Benutzer.")
            sys.exit(0)

        # Perform anonymization
        anonymizer.anonymize_schema(schema)

    finally:
        # Cleanup
        anonymizer.disconnect()

    print("\n" + "=" * 60)
    print("Anonymisierung abgeschlossen!")
    print("=" * 60)


if __name__ == "__main__":
    main()
