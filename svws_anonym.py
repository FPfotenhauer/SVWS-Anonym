#!/usr/bin/env python3
"""
SVWS-Anonym - Anonymization tool for SVWS databases

This tool anonymizes personal data in SVWS database exports by replacing
real names with randomly generated German names from the JSON-Namen repository.
"""

import argparse
import calendar
import csv
import json
import random
import sys
from datetime import date, datetime
from getpass import getpass
from pathlib import Path

try:
    import mysql.connector

    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False


class DatabaseConfig:
    """Load database configuration and prompt for credentials."""

    def __init__(self, config_path=None):
        base_dir = Path(__file__).parent
        config_file = Path(config_path) if config_path else base_dir / "config.json"

        cfg = {}
        if config_file.exists():
            try:
                with open(config_file, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, dict):
                        cfg = loaded.get("database", {})
            except Exception as e:  # pragma: no cover - configuration parse errors
                print(f"Warning: Could not read config file {config_file}: {e}", file=sys.stderr)

        self.host = cfg.get("host", "localhost")
        self.port = cfg.get("port", 3306)
        self.charset = cfg.get("charset", "utf8mb4")
        self.collation = cfg.get("collation", "utf8mb4_unicode_ci")

        self.database = input("Datenbankname: ").strip()
        self.user = input("Benutzername: ").strip()
        self.password = getpass("Passwort: ")

        if not self.database:
            raise ValueError("Database name is required")
        if not self.user:
            raise ValueError("Database user is required")

    def get_connection_params(self):
        """Return a dict suitable for mysql.connector.connect."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "charset": self.charset,
            "collation": self.collation,
        }

    def __str__(self):
        return (
            f"{self.user}@{self.host}:{self.port}/{self.database} "
            f"(charset={self.charset}, collation={self.collation})"
        )


class NameAnonymizer:
    """Handles name anonymization using German name lists."""

    def __init__(self, data_dir=None):
        """Initialize the anonymizer with name data."""
        if data_dir is None:
            data_dir = Path(__file__).parent
        else:
            data_dir = Path(data_dir)

        with open(data_dir / "nachnamen.json", "r", encoding="utf-8") as f:
            self.nachnamen = json.load(f)

        with open(data_dir / "vornamen_m.json", "r", encoding="utf-8") as f:
            self.vornamen_m = json.load(f)

        with open(data_dir / "vornamen_w.json", "r", encoding="utf-8") as f:
            self.vornamen_w = json.load(f)

        self.name_mapping = {}

    def anonymize_firstname(self, name, gender=None):
        """Anonymize a first name."""
        if not name or name in self.name_mapping:
            return self.name_mapping.get(name, "")

        if gender == "m":
            name_list = self.vornamen_m
        elif gender == "w":
            name_list = self.vornamen_w
        else:
            name_list = random.choice([self.vornamen_m, self.vornamen_w])

        new_name = random.choice(name_list)
        self.name_mapping[name] = new_name
        return new_name

    def anonymize_lastname(self, name):
        """Anonymize a last name."""
        if not name or name in self.name_mapping:
            return self.name_mapping.get(name, "")

        new_name = random.choice(self.nachnamen)
        self.name_mapping[name] = new_name
        return new_name

    def anonymize_fullname(self, firstname, lastname, gender=None):
        """Anonymize a full name and return a tuple."""
        return (
            self.anonymize_firstname(firstname, gender),
            self.anonymize_lastname(lastname),
        )

    def get_gender_from_geschlecht(self, geschlecht_value):
        """Convert SVWS Geschlecht value to gender code."""
        if geschlecht_value == 3:
            return "m"
        if geschlecht_value == 4:
            return "w"
        return None

    def anonymize_multiple_names(self, names_string, gender=None, include_name=None):
        """Anonymize a space- or comma-separated list of names."""
        if not names_string:
            return names_string

        import re

        names = re.split(r"[,\s]+", names_string.strip())
        names = [n for n in names if n]

        new_names = []
        for name in names:
            new_name = self.anonymize_firstname(name, gender)
            new_names.append(new_name)

        if include_name and include_name not in new_names:
            if new_names:
                new_names[0] = include_name
            else:
                new_names.append(include_name)

        return " ".join(new_names)


class DatabaseAnonymizer:
    """Handles database connection and anonymization operations."""

    def __init__(self, db_config, name_anonymizer):
        if not MYSQL_AVAILABLE:
            raise ImportError(
                "mysql-connector-python is required for database operations.\n"
                "Install it with: pip install mysql-connector-python"
            )

        self.db_config = db_config
        self.anonymizer = name_anonymizer
        self.connection = None

    def connect(self):
        """Establish database connection."""
        try:
            self.connection = mysql.connector.connect(
                **self.db_config.get_connection_params()
            )
            return True
        except mysql.connector.Error as e:
            print(f"Database connection error: {e}", file=sys.stderr)
            return False

    def disconnect(self):
        """Close database connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()

    def anonymize_k_lehrer(self, dry_run=False):
        """Anonymize the K_Lehrer table."""
        if not self.connection or not self.connection.is_connected():
            raise RuntimeError("Not connected to database")

        cursor = self.connection.cursor(dictionary=True)

        def normalize_for_email(text):
            import re

            if not text:
                return ""
            replacements = {
                "ä": "ae",
                "ö": "oe",
                "ü": "ue",
                "Ä": "ae",
                "Ö": "oe",
                "Ü": "ue",
                "ß": "ss",
            }
            for k, v in replacements.items():
                text = text.replace(k, v)
            text = re.sub(r"[^A-Za-z0-9]", "", text)
            return text.lower()

        def generate_kuerzel(base_lastname, existing):
            base = (base_lastname or "").upper()[:4] or "X"
            candidate = base
            counter = 1
            while candidate in existing:
                candidate = f"{base}{counter}"
                counter += 1
            existing.add(candidate)
            return candidate

        def generate_email(first, last, existing, domain):
            local_first = normalize_for_email(first) or "user"
            local_last = normalize_for_email(last) or "anon"
            base_local = f"{local_first}.{local_last}"
            candidate = f"{base_local}@{domain}"
            counter = 1
            while candidate in existing:
                candidate = f"{base_local}{counter}@{domain}"
                counter += 1
            existing.add(candidate)
            return candidate

        def load_street_index():
            if hasattr(self, "_street_index") and self._street_index:
                return self._street_index
            street_index = {}
            streets_path = Path(__file__).parent / "Strassen.csv"
            if not streets_path.exists():
                print(
                    f"Warning: Strassen.csv not found at {streets_path}; streets will not be set",
                    file=sys.stderr,
                )
                self._street_index = street_index
                return street_index
            with open(streets_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) < 2:
                        continue
                    ort = (row[0] or "").strip()
                    strasse = (row[1] or "").strip()
                    if not ort or not strasse:
                        continue
                    street_index.setdefault(ort.lower(), []).append(strasse)
            self._street_index = street_index
            return street_index

        try:
            street_index = load_street_index()
            all_streets = [s for streets in street_index.values() for s in streets]

            cursor.execute("SELECT * FROM K_Ort")
            ort_records = cursor.fetchall()
            if not ort_records:
                raise RuntimeError("No entries found in K_Ort to assign Ort_ID")
            sample_keys = list(ort_records[0].keys())
            name_candidates = ["Ort", "Name", "Bezeichnung", "Ortname", "Ort_Name", "OrtBezeichnung"]
            ort_name_key = next((k for k in name_candidates if k in sample_keys), None)
            if not ort_name_key:
                raise RuntimeError("Could not determine Ort name column in K_Ort (tried Ort/Name/Bezeichnung)")
            available_ort_ids = [r["ID"] for r in ort_records]
            ort_name_by_id = {r["ID"]: r[ort_name_key] for r in ort_records}

            cursor.execute(
                "SELECT ID, Vorname, Nachname, Geschlecht, Kuerzel, Email, EmailDienstlich, Tel, Handy, LIDKrz, Geburtsdatum FROM K_Lehrer"
            )
            records = cursor.fetchall()

            print(f"\nFound {len(records)} records in K_Lehrer table")

            if dry_run:
                print("\nDRY RUN - No changes will be made:\n")

            updated_count = 0
            existing_kuerzel = {r["Kuerzel"] for r in records if r.get("Kuerzel")}
            existing_email = {r["Email"] for r in records if r.get("Email")}
            existing_email_dienst = {r["EmailDienstlich"] for r in records if r.get("EmailDienstlich")}
            existing_lidkrz = {r["LIDKrz"] for r in records if r.get("LIDKrz")}

            for record in records:
                record_id = record["ID"]
                old_vorname = record["Vorname"]
                old_nachname = record["Nachname"]
                geschlecht = record["Geschlecht"]
                old_kuerzel = record.get("Kuerzel")
                old_email = record.get("Email")
                old_email_dienst = record.get("EmailDienstlich")
                old_tel = record.get("Tel")
                old_handy = record.get("Handy")
                old_lidkrz = record.get("LIDKrz")
                old_geburtsdatum = record.get("Geburtsdatum")

                gender = self.anonymizer.get_gender_from_geschlecht(geschlecht)

                new_vorname, new_nachname = self.anonymizer.anonymize_fullname(
                    old_vorname, old_nachname, gender
                )

                new_kuerzel = generate_kuerzel(new_nachname, existing_kuerzel)

                new_email = generate_email(new_vorname, new_nachname, existing_email, "private.l.example.com")
                new_email_dienst = generate_email(
                    new_vorname, new_nachname, existing_email_dienst, "dienst.l.example.com"
                )

                new_tel = f"01234-{random.randint(0, 999999):06d}"
                new_handy = f"01709-{random.randint(0, 999999):06d}"

                base_lid = (new_kuerzel or "").upper()
                lid_candidate = base_lid[:4] or "XXXX"
                if lid_candidate in existing_lidkrz:
                    prefix3 = base_lid[:3] or "XXX"
                    counter = 1
                    while True:
                        lid_candidate = f"{prefix3}{counter}"
                        if lid_candidate not in existing_lidkrz:
                            break
                        counter += 1
                existing_lidkrz.add(lid_candidate)

                new_ort_id = random.choice(available_ort_ids)
                new_ort_name = ort_name_by_id.get(new_ort_id)
                new_strasse = None
                if new_ort_name and street_index:
                    streets = street_index.get(str(new_ort_name).strip().lower())
                    if streets:
                        new_strasse = random.choice(streets)
                if not new_strasse and all_streets:
                    # Fallback: any street from file when Ort not found
                    new_strasse = random.choice(all_streets)

                def randomize_birth_day(value):
                    if not value:
                        return value
                    base_date = None
                    if isinstance(value, datetime):
                        base_date = value.date()
                    elif isinstance(value, date):
                        base_date = value
                    else:
                        try:
                            base_date = datetime.strptime(str(value), "%Y-%m-%d").date()
                        except Exception:
                            return value
                    _, days_in_month = calendar.monthrange(base_date.year, base_date.month)
                    new_day = random.randint(1, days_in_month)
                    return date(base_date.year, base_date.month, new_day)

                new_geburtsdatum = randomize_birth_day(old_geburtsdatum)
                new_hausnr = random.randint(1, 100)
                new_hausnr_zusatz = None

                if dry_run:
                    gender_str = {3: "männlich", 4: "weiblich", 5: "neutral", 6: "neutral"}.get(
                        geschlecht, "unbekannt"
                    )
                    print(
                        f"ID {record_id} ({gender_str}): {old_vorname} {old_nachname} -> {new_vorname} {new_nachname}; "
                        f"Kuerzel: {old_kuerzel} -> {new_kuerzel}; "
                        f"Email: {old_email} -> {new_email}; "
                        f"EmailDienstlich: {old_email_dienst} -> {new_email_dienst}; "
                        f"Tel: {old_tel} -> {new_tel}; "
                        f"Handy: {old_handy} -> {new_handy}; "
                        f"LIDKrz: {old_lidkrz} -> {lid_candidate}; "
                        f"Geburtsdatum: {old_geburtsdatum} -> {new_geburtsdatum}; "
                        f"Ort_ID -> {new_ort_id}; Ortsteil_ID -> NULL; Strassenname -> {new_strasse}; HausNr -> {new_hausnr}; HausNrZusatz -> NULL"
                    )
                else:
                    update_cursor = self.connection.cursor()
                    update_cursor.execute(
                        "UPDATE K_Lehrer SET Vorname = %s, Nachname = %s, Kuerzel = %s, Email = %s, EmailDienstlich = %s, "
                        "Tel = %s, Handy = %s, LIDKrz = %s, Geburtsdatum = %s, Ort_ID = %s, Ortsteil_ID = %s, Strassenname = %s, HausNr = %s, HausNrZusatz = %s WHERE ID = %s",
                        (
                            new_vorname,
                            new_nachname,
                            new_kuerzel,
                            new_email,
                            new_email_dienst,
                            new_tel,
                            new_handy,
                            lid_candidate,
                            new_geburtsdatum,
                            new_ort_id,
                            None,
                            new_strasse,
                            new_hausnr,
                            new_hausnr_zusatz,
                            record_id,
                        ),
                    )
                    update_cursor.close()

                updated_count += 1

            if not dry_run:
                self.connection.commit()
                print(f"\nSuccessfully anonymized {updated_count} records in K_Lehrer table")
            else:
                print(f"\nDry run complete. {updated_count} records would be updated")

            return updated_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def anonymize_schueler(self, dry_run=False):
        """Anonymize the Schueler table."""
        if not self.connection or not self.connection.is_connected():
            raise RuntimeError("Not connected to database")

        cursor = self.connection.cursor(dictionary=True)

        try:
            cursor.execute(
                "SELECT ID, Vorname, Name, Zusatz, Geburtsname, Geschlecht FROM Schueler"
            )
            records = cursor.fetchall()

            print(f"\nFound {len(records)} records in Schueler table")

            if dry_run:
                print("\nDRY RUN - No changes will be made:\n")

            updated_count = 0

            for record in records:
                record_id = record["ID"]
                old_vorname = record["Vorname"]
                old_name = record["Name"]
                old_zusatz = record["Zusatz"]
                old_geburtsname = record["Geburtsname"]
                geschlecht = record["Geschlecht"]

                gender = self.anonymizer.get_gender_from_geschlecht(geschlecht)

                new_vorname, new_name = self.anonymizer.anonymize_fullname(
                    old_vorname, old_name, gender
                )

                new_zusatz = old_zusatz
                if old_zusatz:
                    new_zusatz = self.anonymizer.anonymize_multiple_names(
                        old_zusatz, gender, include_name=new_vorname
                    )

                new_geburtsname = old_geburtsname
                if old_geburtsname:
                    new_geburtsname = self.anonymizer.anonymize_lastname(old_geburtsname)

                if dry_run:
                    gender_str = {3: "männlich", 4: "weiblich", 5: "neutral", 6: "neutral"}.get(
                        geschlecht, "unbekannt"
                    )
                    print(f"ID {record_id} ({gender_str}):")
                    print(f"  Vorname: {old_vorname} -> {new_vorname}")
                    print(f"  Name: {old_name} -> {new_name}")
                    print(f"  Zusatz: {old_zusatz} -> {new_zusatz}")
                    print(f"  Geburtsname: {old_geburtsname} -> {new_geburtsname}")
                else:
                    update_cursor = self.connection.cursor()
                    update_cursor.execute(
                        "UPDATE Schueler SET Vorname = %s, Name = %s, Zusatz = %s, Geburtsname = %s WHERE ID = %s",
                        (new_vorname, new_name, new_zusatz, new_geburtsname, record_id),
                    )
                    update_cursor.close()

                updated_count += 1

            if not dry_run:
                self.connection.commit()
                print(f"\nSuccessfully anonymized {updated_count} records in Schueler table")
            else:
                print(f"\nDry run complete. {updated_count} records would be updated")

            return updated_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()


def main():
    """Main entry point for the SVWS anonymization tool."""
    parser = argparse.ArgumentParser(
        description="SVWS-Anonym - Anonymization tool for SVWS databases"
    )
    parser.add_argument(
        "--data-dir",
        help="Directory containing JSON name files (default: script directory)",
        default=None,
    )
    parser.add_argument(
        "--config",
        help="Path to database configuration file (default: config.json)",
        default=None,
    )
    parser.add_argument(
        "--anonymize",
        action="store_true",
        help="Anonymize K_Lehrer and Schueler tables in the database",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without actually updating the database",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="SVWS-Anonym 0.1.0",
    )

    args = parser.parse_args()

    try:
        anonymizer = NameAnonymizer(args.data_dir)
        print("SVWS-Anonym initialized successfully")
        print(f"Loaded {len(anonymizer.nachnamen)} last names")
        print(f"Loaded {len(anonymizer.vornamen_m)} male first names")
        print(f"Loaded {len(anonymizer.vornamen_w)} female first names")

        if args.anonymize or args.dry_run:
            if not MYSQL_AVAILABLE:
                print("\nError: mysql-connector-python is not installed.", file=sys.stderr)
                print("Install it with: pip install mysql-connector-python", file=sys.stderr)
                return 1

            try:
                db_config = DatabaseConfig(args.config)
                print("\nDatabase configuration loaded successfully:")
                print(f"  {db_config}")
            except Exception as e:
                print(f"\nError loading database configuration: {e}", file=sys.stderr)
                return 1

            db_anonymizer = DatabaseAnonymizer(db_config, anonymizer)

            print("\nConnecting to database...")
            if not db_anonymizer.connect():
                return 1

            print("Connected successfully!")

            try:
                db_anonymizer.anonymize_k_lehrer(dry_run=args.dry_run)
                db_anonymizer.anonymize_schueler(dry_run=args.dry_run)
            finally:
                db_anonymizer.disconnect()
                print("\nDatabase connection closed")
        else:
            print("\nExample anonymizations:")
            print(
                f"  Max Mustermann (male) -> {' '.join(anonymizer.anonymize_fullname('Max', 'Mustermann', 'm'))}"
            )
            print(
                f"  Erika Beispiel (female) -> {' '.join(anonymizer.anonymize_fullname('Erika', 'Beispiel', 'w'))}"
            )
            print(
                f"  Alex Muster (neutral) -> {' '.join(anonymizer.anonymize_fullname('Alex', 'Muster', None))}"
            )
            print("\nUse --anonymize to process K_Lehrer and Schueler tables")
            print("Use --dry-run to see what would be changed without updating")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
