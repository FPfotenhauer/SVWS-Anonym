#!/usr/bin/env python3
"""
SVWS-Anonym - Anonymization tool for SVWS databases

This tool anonymizes personal data in SVWS database exports by replacing
real names with randomly generated German names from the JSON-Namen repository.
"""

import argparse
import base64
import calendar
import csv
import json
import random
import secrets
import sys
from datetime import date, datetime
from getpass import getpass
from pathlib import Path

try:
    import mysql.connector

    MYSQL_AVAILABLE = True
except ImportError as e:
    MYSQL_AVAILABLE = False
    print(f"Error: mysql-connector-python import failed: {e}", file=sys.stderr)
    print("Install it with: pip install mysql-connector-python", file=sys.stderr)
    sys.exit(1)

try:
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError as e:
    CRYPTOGRAPHY_AVAILABLE = False
    print(f"Error: cryptography library import failed: {e}", file=sys.stderr)
    print("Install it with: pip install cryptography", file=sys.stderr)
    sys.exit(1)


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

        # Use config values if they are not null, otherwise prompt
        config_database = cfg.get("database")
        config_username = cfg.get("username")
        config_password = cfg.get("password")

        if config_database is not None:
            self.database = config_database
        else:
            self.database = input("Datenbankname: ").strip()
        
        if config_username is not None:
            self.user = config_username
        else:
            self.user = input("Benutzername: ").strip()
        
        if config_password is not None:
            self.password = config_password
        else:
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

        # Maintain separate mappings to avoid cross-gender collisions
        # First names are keyed by (original_name, gender_code 'm'/'w'/None)
        # Last names are keyed by original_name only
        self.firstname_mapping = {}
        self.lastname_mapping = {}

    def anonymize_firstname(self, name, gender=None):
        """Anonymize a first name."""
        if not name:
            return ""

        key = (name, gender)
        if key in self.firstname_mapping:
            return self.firstname_mapping[key]

        if gender == "m":
            name_list = self.vornamen_m
        elif gender == "w":
            name_list = self.vornamen_w
        else:
            name_list = random.choice([self.vornamen_m, self.vornamen_w])

        new_name = random.choice(name_list)
        self.firstname_mapping[key] = new_name
        return new_name

    def anonymize_lastname(self, name):
        """Anonymize a last name."""
        if not name:
            return ""

        if name in self.lastname_mapping:
            return self.lastname_mapping[name]

        new_name = random.choice(self.nachnamen)
        self.lastname_mapping[name] = new_name
        return new_name

    def anonymize_fullname(self, firstname, lastname, gender=None):
        """Anonymize a full name and return a tuple."""
        return (
            self.anonymize_firstname(firstname, gender),
            self.anonymize_lastname(lastname),
        )

    def get_gender_from_geschlecht(self, geschlecht_value):
        """Convert SVWS Geschlecht value to gender code."""
        # Handle both string and integer values
        val = str(geschlecht_value) if geschlecht_value is not None else None
        if val == "3":
            return "m"
        if val == "4":
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
                "SELECT ID, Vorname, Nachname, Geschlecht, Kuerzel, Email, EmailDienstlich, Tel, Handy, LIDKrz, Geburtsdatum, SerNr, PANr, LBVNr, Titel FROM K_Lehrer"
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

            update_cursor = self.connection.cursor() if not dry_run else None

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
                old_sernr = record.get("SerNr")
                old_panr = record.get("PANr")
                old_lbvnr = record.get("LBVNr")
                old_geburtsdatum = record.get("Geburtsdatum")
                old_titel = record.get("Titel")

                gender = self.anonymizer.get_gender_from_geschlecht(geschlecht)

                new_titel = None

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
                # LIDKrz is VARCHAR(4). Ensure candidate is always length <= 4.
                lid_candidate = base_lid[:4] or "XXXX"
                if lid_candidate in existing_lidkrz:
                    prefix3 = base_lid[:3] or "XXX"
                    chosen = None
                    # Try 0-9 for the 4th char
                    for d in range(10):
                        cand = f"{prefix3}{d}"
                        if cand not in existing_lidkrz:
                            chosen = cand
                            break
                    if not chosen:
                        # Fallback: random 4-char alphanumeric
                        import string
                        alphabet = string.ascii_uppercase + string.digits
                        for _ in range(50):
                            cand = "".join(random.choice(alphabet) for _ in range(4))
                            if cand not in existing_lidkrz:
                                chosen = cand
                                break
                        if not chosen:
                            chosen = prefix3 + "0"  # last resort
                    lid_candidate = chosen
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
                new_sernr = f"{random.randint(0, 9999):04d}X"
                new_panr = f"PA{random.randint(0, 9999999):07d}"
                new_lbvnr = f"LB{random.randint(0, 9999999):07d}"

                # Generate IdentNr1 from birthdate (ddmmyy) + gender
                new_ident_nr1 = None
                if new_geburtsdatum and geschlecht:
                    birth_str = new_geburtsdatum.strftime("%d%m%y")
                    new_ident_nr1 = f"{birth_str}{geschlecht}"

                if dry_run:
                    gender_str = {3: "männlich", 4: "weiblich", 5: "neutral", 6: "neutral"}.get(
                        geschlecht, "unbekannt"
                    )
                    print(
                        f"ID {record_id} ({gender_str}): {old_vorname} {old_nachname} -> {new_vorname} {new_nachname}; "
                        f"Kuerzel: {old_kuerzel} -> {new_kuerzel}; "
                        f"SerNr: {old_sernr} -> {new_sernr}; PANr: {old_panr} -> {new_panr}; LBVNr: {old_lbvnr} -> {new_lbvnr}; "
                        f"Email: {old_email} -> {new_email}; "
                        f"EmailDienstlich: {old_email_dienst} -> {new_email_dienst}; "
                        f"Tel: {old_tel} -> {new_tel}; "
                        f"Handy: {old_handy} -> {new_handy}; "
                        f"LIDKrz: {old_lidkrz} -> {lid_candidate}; "
                        f"Geburtsdatum: {old_geburtsdatum} -> {new_geburtsdatum}; "
                        f"Ort_ID -> {new_ort_id}; Ortsteil_ID -> NULL; Strassenname -> {new_strasse}; HausNr -> {new_hausnr}; HausNrZusatz -> NULL"
                    )
                else:
                    update_cursor.execute(
                        "UPDATE K_Lehrer SET Vorname = %s, Nachname = %s, Kuerzel = %s, SerNr = %s, PANr = %s, LBVNr = %s, Email = %s, EmailDienstlich = %s, "
                        "Tel = %s, Handy = %s, LIDKrz = %s, Geburtsdatum = %s, IdentNr1 = %s, Ort_ID = %s, Ortsteil_ID = %s, Strassenname = %s, HausNr = %s, HausNrZusatz = %s, Titel = %s WHERE ID = %s",
                        (
                            new_vorname,
                            new_nachname,
                            new_kuerzel,
                            new_sernr,
                            new_panr,
                            new_lbvnr,
                            new_email,
                            new_email_dienst,
                            new_tel,
                            new_handy,
                            lid_candidate,
                            new_geburtsdatum,
                            new_ident_nr1,
                            new_ort_id,
                            None,
                            new_strasse,
                            new_hausnr,
                            new_hausnr_zusatz,
                            new_titel,
                            record_id,
                        ),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
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
                "SELECT ID, Vorname, Name, Zusatz, Geburtsname, Geschlecht, Email, SchulEmail, Geburtsdatum, Ausweisnummer, Geburtsort, Telefon, Fax FROM Schueler"
            )
            records = cursor.fetchall()

            print(f"\nFound {len(records)} records in Schueler table")

            if dry_run:
                print("\nDRY RUN - No changes will be made:\n")

            import re

            def normalize_for_email(text):
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

            updated_count = 0
            existing_email = {r["Email"] for r in records if r.get("Email")}
            existing_schul_email = {r["SchulEmail"] for r in records if r.get("SchulEmail")}
            existing_ausweis = {r["Ausweisnummer"] for r in records if r.get("Ausweisnummer")}

            update_cursor = self.connection.cursor() if not dry_run else None

            for record in records:
                record_id = record["ID"]
                old_vorname = record["Vorname"]
                old_name = record["Name"]
                old_zusatz = record["Zusatz"]
                old_geburtsname = record["Geburtsname"]
                geschlecht = record["Geschlecht"]
                old_email = record.get("Email")
                old_schul_email = record.get("SchulEmail")
                old_geburtsdatum = record.get("Geburtsdatum")
                old_ausweis = record.get("Ausweisnummer")
                old_geburtsort = record.get("Geburtsort")
                old_telefon = record.get("Telefon")
                old_fax = record.get("Fax")

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

                new_email = generate_email(new_vorname, new_name, existing_email, "privat.s.example.com")
                new_schul_email = generate_email(new_vorname, new_name, existing_schul_email, "schule.s.example.com")

                def generate_ausweis(existing):
                    candidate = str(random.randint(0, 9_999_999_999)).zfill(10)
                    while candidate in existing:
                        candidate = str(random.randint(0, 9_999_999_999)).zfill(10)
                    existing.add(candidate)
                    return candidate

                new_ausweis = generate_ausweis(existing_ausweis)

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

                new_ort_id = random.choice(available_ort_ids)
                new_ort_name = ort_name_by_id.get(new_ort_id)
                new_strasse = None
                if new_ort_name and street_index:
                    streets = street_index.get(str(new_ort_name).strip().lower())
                    if streets:
                        new_strasse = random.choice(streets)
                if not new_strasse and all_streets:
                    new_strasse = random.choice(all_streets)

                new_hausnr = random.randint(1, 100)
                new_hausnr_zusatz = None

                new_ortsteil_id = None
                
                # Set Geburtsort to "Testort" when not NULL
                new_geburtsort = "Testort" if old_geburtsort is not None else None
                
                # Anonymize Telefon and Fax fields
                new_telefon = f"012345-{random.randint(100000, 999999)}" if old_telefon is not None else None
                new_fax = f"012345-{random.randint(100000, 999999)}" if old_fax is not None else None

                if dry_run:
                    gender_str = {3: "männlich", 4: "weiblich", 5: "neutral", 6: "neutral"}.get(
                        geschlecht, "unbekannt"
                    )
                    print(f"ID {record_id} ({gender_str}):")
                    print(f"  Vorname: {old_vorname} -> {new_vorname}")
                    print(f"  Name: {old_name} -> {new_name}")
                    print(f"  Zusatz: {old_zusatz} -> {new_zusatz}")
                    print(f"  Geburtsname: {old_geburtsname} -> {new_geburtsname}")
                    print(f"  Geburtsdatum: {old_geburtsdatum} -> {new_geburtsdatum}")
                    print(f"  Email: {old_email} -> {new_email}")
                    print(f"  SchulEmail: {old_schul_email} -> {new_schul_email}")
                    print(f"  Ausweisnummer: {old_ausweis} -> {new_ausweis}")
                    print(
                        f"  Ort_ID -> {new_ort_id}; Ortsteil_ID -> {new_ortsteil_id}; "
                        f"Strassenname -> {new_strasse}; HausNr -> {new_hausnr}; HausNrZusatz -> {new_hausnr_zusatz}"
                    )
                    print(f"  Geburtsort: {old_geburtsort} -> {new_geburtsort}")
                    print(f"  Telefon: {old_telefon} -> {new_telefon}")
                    print(f"  Fax: {old_fax} -> {new_fax}")
                else:
                    update_cursor.execute(
                        "UPDATE Schueler SET Vorname = %s, Name = %s, Zusatz = %s, Geburtsname = %s, Geburtsdatum = %s, Ausweisnummer = %s, Email = %s, SchulEmail = %s, "
                        "Ort_ID = %s, Ortsteil_ID = %s, Strassenname = %s, HausNr = %s, HausNrZusatz = %s, Geburtsort = %s, Telefon = %s, Fax = %s WHERE ID = %s",
                        (
                            new_vorname,
                            new_name,
                            new_zusatz,
                            new_geburtsname,
                            new_geburtsdatum,
                            new_ausweis,
                            new_email,
                            new_schul_email,
                            new_ort_id,
                            new_ortsteil_id,
                            new_strasse,
                            new_hausnr,
                            new_hausnr_zusatz,
                            new_geburtsort,
                            new_telefon,
                            new_fax,
                            record_id,
                        ),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
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

    def anonymize_eigene_schule(self, dry_run=False):
        """Anonymize EigeneSchule table with fake school data."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'EigeneSchule'")
            if not cursor.fetchone():
                print("\nSkipping EigeneSchule update: table 'EigeneSchule' not found")
                return 0

            cursor.execute("SELECT * FROM EigeneSchule")
            records = cursor.fetchall()
            
            if not records:
                print("\nNo records found in EigeneSchule table")
                return 0

            print(f"\nFound {len(records)} records in EigeneSchule table")
            
            if dry_run:
                print("\nDRY RUN - EigeneSchule changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                
                # Set specific values as requested
                new_schulnr = "123456"
                new_schultraegernr = None
                new_bezeichnung1 = "Städtische Schule"
                new_bezeichnung2 = "am Stadtgarten"
                new_bezeichnung3 = "Ganztagsschule des Landes NRW"
                new_strassenname = "Hauptstrasse"
                new_hausnr = "56"
                new_hausnrzusatz = None
                new_plz = "42107"
                new_ort = "Wuppertal"
                new_telefon = "0202-5551234"
                new_fax = "0202-5556667"
                new_email = "schule@schule.example.com"
                new_webadresse = "https://schule123456.schule.de"
                
                if dry_run:
                    print(f"  ID {record_id}: Would update SchulNr=123456, SchultraegerNr=NULL, Bezeichnung1-3, Strassenname, HausNr, HausNrZusatz, PLZ, Ort, Telefon, Fax, Email, WebAdresse")
                else:
                    update_cursor.execute(
                        "UPDATE EigeneSchule SET SchulNr = %s, SchultraegerNr = %s, Bezeichnung1 = %s, Bezeichnung2 = %s, Bezeichnung3 = %s, Strassenname = %s, HausNr = %s, HausNrZusatz = %s, PLZ = %s, Ort = %s, Telefon = %s, Fax = %s, Email = %s, WebAdresse = %s WHERE ID = %s",
                        (new_schulnr, new_schultraegernr, new_bezeichnung1, new_bezeichnung2, new_bezeichnung3, new_strassenname, new_hausnr, new_hausnrzusatz, new_plz, new_ort, new_telefon, new_fax, new_email, new_webadresse, record_id)
                    )
                
                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully anonymized {updated_count} records in EigeneSchule table")
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

    def anonymize_eigene_schule_email(self, dry_run=False):
        """Anonymize EigeneSchule_Email table with specific values."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'EigeneSchule_Email'")
            if not cursor.fetchone():
                print("\nSkipping EigeneSchule_Email update: table 'EigeneSchule_Email' not found")
                return 0

            cursor.execute("SELECT * FROM EigeneSchule_Email")
            records = cursor.fetchall()
            
            if not records:
                print("\nNo records found in EigeneSchule_Email table")
                return 0

            print(f"\nFound {len(records)} records in EigeneSchule_Email table")
            
            if dry_run:
                print("\nDRY RUN - EigeneSchule_Email changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                
                # Set specific values as requested
                new_domain = None
                new_smtpserver = ""
                new_smtpport = 25
                new_smtpstarttls = 1
                new_smtpusetls = 0
                new_smtptrusttlshost = None
                
                if dry_run:
                    print(f"  ID {record_id}: Would set Domain=NULL, SMTPServer=NULL, SMTPPort=25, SMTPStartTLS=1, SMTPUseTLS=0, SMTPTrustTLSHost=NULL")
                else:
                    update_cursor.execute(
                        "UPDATE EigeneSchule_Email SET Domain = %s, SMTPServer = %s, SMTPPort = %s, SMTPStartTLS = %s, SMTPUseTLS = %s, SMTPTrustTLSHost = %s WHERE ID = %s",
                        (new_domain, new_smtpserver, new_smtpport, new_smtpstarttls, new_smtpusetls, new_smtptrusttlshost, record_id)
                    )
                
                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully anonymized {updated_count} records in EigeneSchule_Email table")
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

    def anonymize_eigene_schule_teilstandorte(self, dry_run=False):
        """Reset EigeneSchule_Teilstandorte to a single anonymized entry."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Ensure table exists
            cursor.execute("SHOW TABLES LIKE 'EigeneSchule_Teilstandorte'")
            if not cursor.fetchone():
                print("\nSkipping EigeneSchule_Teilstandorte update: table not found")
                return 0

            cursor.execute("SELECT COUNT(*) AS cnt FROM EigeneSchule_Teilstandorte")
            row = cursor.fetchone()
            total = row["cnt"] if row and "cnt" in row else 0

            adrmerkmal = "A"
            plz = "42103"
            ort = "Wuppertal"
            strassenname = "Hauptstrasse"
            hausnr = "56"
            hausnrzusatz = None
            bemerkung = "Hauptstandort"
            kuerzel = "WtalA"

            if dry_run:
                print("\nDRY RUN - EigeneSchule_Teilstandorte update:")
                print(
                    "  Existing rows: "
                    f"{total} -> will delete all; insert AdrMerkmal={adrmerkmal}, "
                    f"PLZ={plz}, Ort={ort}, Strassenname={strassenname}, HausNr={hausnr}, "
                    f"HausNrZusatz={hausnrzusatz}, Bemerkung={bemerkung}, Kuerzel={kuerzel}"
                )
                return total
            else:
                update_cursor = self.connection.cursor()
                update_cursor.execute("DELETE FROM EigeneSchule_Teilstandorte")
                update_cursor.execute(
                    "INSERT INTO EigeneSchule_Teilstandorte (AdrMerkmal, PLZ, Ort, Strassenname, HausNr, HausNrZusatz, Bemerkung, Kuerzel) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (adrmerkmal, plz, ort, strassenname, hausnr, hausnrzusatz, bemerkung, kuerzel),
                )
                update_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully reset EigeneSchule_Teilstandorte (deleted {total} rows, inserted 1 row)"
                )
                return total

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def anonymize_eigene_schule_abteilungen(self, dry_run=False):
        """Anonymize EigeneSchule_Abteilungen table - set Email and clear Durchwahl and Raum."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'EigeneSchule_Abteilungen'")
            if not cursor.fetchone():
                print("\nSkipping EigeneSchule_Abteilungen update: table not found")
                return 0

            cursor.execute("SELECT * FROM EigeneSchule_Abteilungen")
            records = cursor.fetchall()
            
            if not records:
                print("\nNo records found in EigeneSchule_Abteilungen table")
                return 0

            print(f"\nFound {len(records)} records in EigeneSchule_Abteilungen table")
            
            if dry_run:
                print("\nDRY RUN - EigeneSchule_Abteilungen changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                
                # Set specific values
                new_email = "abteilung@schule.example.com"
                new_durchwahl = None
                new_raum = None
                
                if dry_run:
                    print(f"  ID {record_id}: Would set Email='{new_email}', Durchwahl=NULL, Raum=NULL")
                else:
                    update_cursor.execute(
                        "UPDATE EigeneSchule_Abteilungen SET Email = %s, Durchwahl = %s, Raum = %s WHERE ID = %s",
                        (new_email, new_durchwahl, new_raum, record_id)
                    )
                
                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully anonymized {updated_count} records in EigeneSchule_Abteilungen table")
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

    def anonymize_credentials_lernplattformen(self, dry_run=False):
        """Update CredentialsLernplattformen usernames based on K_Lehrer names via LehrerLernplattform."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Check if required tables exist
            cursor.execute("SHOW TABLES LIKE 'CredentialsLernplattformen'")
            if not cursor.fetchone():
                print("\nSkipping CredentialsLernplattformen update: table not found")
                return 0
                
            cursor.execute("SHOW TABLES LIKE 'LehrerLernplattform'")
            if not cursor.fetchone():
                print("\nSkipping CredentialsLernplattformen update: LehrerLernplattform table not found")
                return 0

            # Get the mapping via LehrerLernplattform
            cursor.execute("""
                SELECT 
                    c.ID as credential_id,
                    c.Benutzername as old_username,
                    l.Vorname,
                    l.Nachname
                FROM CredentialsLernplattformen c
                JOIN LehrerLernplattform ll ON c.ID = ll.CredentialID
                JOIN K_Lehrer l ON ll.LehrerID = l.ID
            """)
            records = cursor.fetchall()
            
            if not records:
                print("\nNo records found to update in CredentialsLernplattformen table")
                return 0

            print(f"\nFound {len(records)} records in CredentialsLernplattformen table")
            
            if dry_run:
                print("\nDRY RUN - CredentialsLernplattformen changes:")

            updated_count = 0
            # Pre-load all existing usernames to avoid unique constraint violations
            cursor.execute("SELECT Benutzername FROM CredentialsLernplattformen")
            existing_usernames = {row['Benutzername'] for row in cursor.fetchall()}

            update_cursor = self.connection.cursor() if not dry_run else None

            for record in records:
                credential_id = record.get("credential_id")
                old_username = record.get("old_username")
                vorname = record.get("Vorname")
                nachname = record.get("Nachname")
                
                # Create new username as Vorname.Nachname
                base_username = f"{vorname}.{nachname}"
                new_username = base_username
                counter = 1
                # Handle duplicates by adding a numeric suffix
                while new_username in existing_usernames:
                    new_username = f"{base_username}{counter}"
                    counter += 1
                # Update tracking set: remove old and add new
                if old_username in existing_usernames:
                    existing_usernames.remove(old_username)
                existing_usernames.add(new_username)
                
                # Generate random 8-digit password
                new_initialkennwort = ''.join([str(random.randint(0, 9)) for _ in range(8)])
                
                if dry_run:
                    print(f"  Credential ID {credential_id}: {old_username} -> {new_username}, Initialkennwort -> {new_initialkennwort}, PashwordHash/RSA/AES -> NULL")
                else:
                    update_cursor.execute(
                        "UPDATE CredentialsLernplattformen SET Benutzername = %s, Initialkennwort = %s, PashwordHash = %s, RSAPublicKey = %s, RSAPrivateKey = %s, AES = %s WHERE ID = %s",
                        (new_username, new_initialkennwort, None, None, None, None, credential_id)
                    )
                
                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated {updated_count} records in CredentialsLernplattformen table")
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

    def anonymize_credentials_lernplattformen_schueler(self, dry_run=False):
        """Update CredentialsLernplattformen usernames for students based on Schueler names via SchuelerLernplattform."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Check if required tables exist
            cursor.execute("SHOW TABLES LIKE 'CredentialsLernplattformen'")
            if not cursor.fetchone():
                print("\nSkipping student CredentialsLernplattformen update: table not found")
                return 0
                
            cursor.execute("SHOW TABLES LIKE 'SchuelerLernplattform'")
            if not cursor.fetchone():
                print("\nSkipping student CredentialsLernplattformen update: SchuelerLernplattform table not found")
                return 0

            # Get the mapping via SchuelerLernplattform
            cursor.execute("""
                SELECT 
                    c.ID as credential_id,
                    c.Benutzername as old_username,
                    s.Vorname,
                    s.Name
                FROM CredentialsLernplattformen c
                JOIN SchuelerLernplattform sl ON c.ID = sl.CredentialID
                JOIN Schueler s ON sl.SchuelerID = s.ID
            """)
            records = cursor.fetchall()
            
            if not records:
                print("\nNo student records found to update in CredentialsLernplattformen table")
                return 0

            print(f"\nFound {len(records)} student records in CredentialsLernplattformen table")
            
            if dry_run:
                print("\nDRY RUN - Student CredentialsLernplattformen changes:")

            updated_count = 0
            
            # Pre-load all existing usernames from the database to avoid duplicates
            cursor.execute("SELECT Benutzername FROM CredentialsLernplattformen")
            existing_usernames = {row['Benutzername'] for row in cursor.fetchall()}
            
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                credential_id = record.get("credential_id")
                old_username = record.get("old_username")
                vorname = record.get("Vorname")
                name = record.get("Name")
                
                # Create new username as Vorname.Name
                base_username = f"{vorname}.{name}"
                new_username = base_username
                counter = 1
                
                # Handle duplicates by adding a counter
                while new_username in existing_usernames:
                    new_username = f"{base_username}{counter}"
                    counter += 1
                
                # Remove old username and add new one to track duplicates
                if old_username in existing_usernames:
                    existing_usernames.remove(old_username)
                existing_usernames.add(new_username)
                
                # Generate random 8-digit password
                new_initialkennwort = ''.join([str(random.randint(0, 9)) for _ in range(8)])
                
                if dry_run:
                    print(f"  Credential ID {credential_id}: {old_username} -> {new_username}, Initialkennwort -> {new_initialkennwort}, PashwordHash/RSA/AES -> NULL")
                else:
                    update_cursor.execute(
                        "UPDATE CredentialsLernplattformen SET Benutzername = %s, Initialkennwort = %s, PashwordHash = %s, RSAPublicKey = %s, RSAPrivateKey = %s, AES = %s WHERE ID = %s",
                        (new_username, new_initialkennwort, None, None, None, None, credential_id)
                    )
                
                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated {updated_count} student records in CredentialsLernplattformen table")
            else:
                print(f"\nDry run complete. {updated_count} student records would be updated")

            return updated_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def anonymize_lernplattformen(self, dry_run=False):
        """Anonymize Lernplattformen table - set Bezeichnung to 'Lernplattform' + ID and clear Konfiguration."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'Lernplattformen'")
            if not cursor.fetchone():
                print("\nSkipping Lernplattformen update: table not found")
                return 0

            cursor.execute("SELECT ID, Bezeichnung, Konfiguration FROM Lernplattformen")
            records = cursor.fetchall()
            
            if not records:
                print("\nNo records found in Lernplattformen table")
                return 0

            print(f"\nFound {len(records)} records in Lernplattformen table")
            
            if dry_run:
                print("\nDRY RUN - Lernplattformen changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_bezeichnung = record.get("Bezeichnung")
                old_konfiguration = record.get("Konfiguration")
                
                # Set Bezeichnung to 'Lernplattform' + ID
                new_bezeichnung = f"Lernplattform{record_id}"
                # Set Konfiguration to NULL if not NULL
                new_konfiguration = None if old_konfiguration is not None else old_konfiguration
                
                if dry_run:
                    print(f"  ID {record_id}: Bezeichnung: '{old_bezeichnung}' -> '{new_bezeichnung}', Konfiguration: {'NULL' if new_konfiguration is None else 'unchanged'}")
                else:
                    update_cursor.execute(
                        "UPDATE Lernplattformen SET Bezeichnung = %s, Konfiguration = %s WHERE ID = %s",
                        (new_bezeichnung, new_konfiguration, record_id)
                    )
                
                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully anonymized {updated_count} records in Lernplattformen table")
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

    def update_schueler_erzadr_names(self, dry_run=False):
        """Update SchuelerErzAdr.Name1/Name2 with anonymized Schueler.Name when set."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check required tables
            cursor.execute("SHOW TABLES LIKE 'SchuelerErzAdr'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerErzAdr update: table not found")
                return 0

            cursor.execute("SHOW TABLES LIKE 'Schueler'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerErzAdr update: Schueler table not found")
                return 0

            cursor.execute(
                """
                SELECT se.ID, se.Name1, se.Name2, se.Schueler_ID, s.Name AS schueler_name
                FROM SchuelerErzAdr se
                JOIN Schueler s ON se.Schueler_ID = s.ID
                WHERE se.Name1 IS NOT NULL OR se.Name2 IS NOT NULL
                """
            )
            records = cursor.fetchall()

            if not records:
                print("\nNo SchuelerErzAdr records with Name1/Name2 present")
                return 0

            print(f"\nFound {len(records)} records in SchuelerErzAdr table with Name1/Name2 set")

            if dry_run:
                print("\nDRY RUN - SchuelerErzAdr changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_name1 = record.get("Name1")
                old_name2 = record.get("Name2")
                schueler_name = record.get("schueler_name")

                new_name1 = schueler_name if old_name1 is not None else None
                new_name2 = schueler_name if old_name2 is not None else None

                if dry_run:
                    print(
                        f"  ID {record_id}: Name1 {old_name1} -> {new_name1}, "
                        f"Name2 {old_name2} -> {new_name2}"
                    )
                else:
                    update_cursor.execute(
                        "UPDATE SchuelerErzAdr SET Name1 = %s, Name2 = %s WHERE ID = %s",
                        (new_name1, new_name2, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated {updated_count} records in SchuelerErzAdr table")
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

    def update_schueler_erzadr_vornamen(self, dry_run=False):
        """Update SchuelerErzAdr.Vorname1/Vorname2 based on salutation or student firstname."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            cursor.execute("SHOW TABLES LIKE 'SchuelerErzAdr'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerErzAdr Vornamen update: table not found")
                return 0

            cursor.execute("SHOW TABLES LIKE 'Schueler'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerErzAdr Vornamen update: Schueler table not found")
                return 0

            cursor.execute(
                """
                SELECT se.ID, se.Vorname1, se.Anrede1, se.Vorname2, se.Anrede2, se.ErzieherArt_ID,
                       s.Vorname AS schueler_vorname
                FROM SchuelerErzAdr se
                JOIN Schueler s ON se.Schueler_ID = s.ID
                WHERE se.Vorname1 IS NOT NULL OR se.Vorname2 IS NOT NULL
                """
            )
            records = cursor.fetchall()

            if not records:
                print("\nNo SchuelerErzAdr records with Vorname1/Vorname2 present")
                return 0

            print(f"\nFound {len(records)} records in SchuelerErzAdr table with Vorname1/Vorname2 set")

            if dry_run:
                print("\nDRY RUN - SchuelerErzAdr Vornamen changes:")

            def pick_name(old_firstname, salutation, erzieherart_id, student_firstname):
                if old_firstname is None:
                    return None
                if erzieherart_id in (3, 4):
                    return student_firstname
                sal = (salutation or "").strip().lower()
                if sal == "herr":
                    return self.anonymizer.anonymize_firstname(old_firstname, gender="m")
                if sal == "frau":
                    return self.anonymizer.anonymize_firstname(old_firstname, gender="w")
                return self.anonymizer.anonymize_firstname(old_firstname, gender=None)

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_vn1 = record.get("Vorname1")
                old_vn2 = record.get("Vorname2")
                sal1 = record.get("Anrede1")
                sal2 = record.get("Anrede2")
                erzieherart_id = record.get("ErzieherArt_ID")
                sch_vn = record.get("schueler_vorname")

                new_vn1 = pick_name(old_vn1, sal1, erzieherart_id, sch_vn)
                new_vn2 = pick_name(old_vn2, sal2, erzieherart_id, sch_vn)

                if dry_run:
                    print(
                        f"  ID {record_id}: Vorname1 {old_vn1} -> {new_vn1}, "
                        f"Vorname2 {old_vn2} -> {new_vn2}"
                    )
                else:
                    update_cursor.execute(
                        "UPDATE SchuelerErzAdr SET Vorname1 = %s, Vorname2 = %s WHERE ID = %s",
                        (new_vn1, new_vn2, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated {updated_count} records in SchuelerErzAdr table (Vornamen)")
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

    def update_schueler_erzadr_address(self, dry_run=False):
        """Align SchuelerErzAdr address fields with student Ort and sanitize street/house fields."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            cursor.execute("SHOW TABLES LIKE 'SchuelerErzAdr'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerErzAdr address update: table not found")
                return 0

            cursor.execute("SHOW TABLES LIKE 'Schueler'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerErzAdr address update: Schueler table not found")
                return 0

            cursor.execute(
                """
                SELECT se.ID, se.ErzOrt_ID, se.ErzOrtsteil_ID, se.ErzStrassenname, se.ErzHausNr,
                       s.Ort_ID AS schueler_ort_id
                FROM SchuelerErzAdr se
                JOIN Schueler s ON se.Schueler_ID = s.ID
                """
            )
            records = cursor.fetchall()

            if not records:
                print("\nNo SchuelerErzAdr records found for address update")
                return 0

            print(f"\nFound {len(records)} records in SchuelerErzAdr table for address update")

            if dry_run:
                print("\nDRY RUN - SchuelerErzAdr address changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_ort = record.get("ErzOrt_ID")
                old_ortsteil = record.get("ErzOrtsteil_ID")
                old_strasse = record.get("ErzStrassenname")
                old_hausnr = record.get("ErzHausNr")
                sch_ort = record.get("schueler_ort_id")

                new_ort = sch_ort
                new_ortsteil = None
                new_strasse = "Teststrasse" if old_strasse is not None else None
                new_hausnr = str(random.randint(1, 100)) if old_hausnr is not None else None

                if dry_run:
                    print(
                        f"  ID {record_id}: ErzOrt_ID {old_ort} -> {new_ort}, "
                        f"ErzOrtsteil_ID {old_ortsteil} -> {new_ortsteil}, "
                        f"ErzStrassenname {old_strasse} -> {new_strasse}, "
                        f"ErzHausNr {old_hausnr} -> {new_hausnr}"
                    )
                else:
                    update_cursor.execute(
                        """
                        UPDATE SchuelerErzAdr
                        SET ErzOrt_ID = %s,
                            ErzOrtsteil_ID = %s,
                            ErzStrassenname = %s,
                            ErzHausNr = %s
                        WHERE ID = %s
                        """,
                        (new_ort, new_ortsteil, new_strasse, new_hausnr, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated {updated_count} records in SchuelerErzAdr table (address)")
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

    def update_schueler_erzadr_email(self, dry_run=False):
        """Update SchuelerErzAdr.ErzEmail based on Name1."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            cursor.execute("SHOW TABLES LIKE 'SchuelerErzAdr'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerErzAdr email update: table not found")
                return 0

            cursor.execute(
                """
                SELECT ID, Name1, ErzEmail
                FROM SchuelerErzAdr
                WHERE ErzEmail IS NOT NULL OR Name1 IS NOT NULL
                """
            )
            records = cursor.fetchall()

            if not records:
                print("\nNo SchuelerErzAdr records found for email update")
                return 0

            print(f"\nFound {len(records)} records in SchuelerErzAdr table for email update")

            if dry_run:
                print("\nDRY RUN - SchuelerErzAdr email changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                name1 = record.get("Name1")

                if name1:
                    new_email = f"{name1}@e.example.com"
                else:
                    new_email = None

                old_email = record.get("ErzEmail")

                if dry_run:
                    print(f"  ID {record_id}: ErzEmail {old_email} -> {new_email}")
                else:
                    update_cursor.execute(
                        "UPDATE SchuelerErzAdr SET ErzEmail = %s WHERE ID = %s",
                        (new_email, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated {updated_count} records in SchuelerErzAdr table (ErzEmail)")
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

    def clear_schueler_erzadr_misc(self, dry_run=False):
        """Set ErzEmail2, Erz1StaatKrz, Erz2StaatKrz, ErzAdrZusatz to NULL."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            cursor.execute("SHOW TABLES LIKE 'SchuelerErzAdr'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerErzAdr misc clear: table not found")
                return 0

            cursor.execute(
                "SELECT ID, ErzEmail2, Erz1StaatKrz, Erz2StaatKrz, ErzAdrZusatz FROM SchuelerErzAdr"
            )
            records = cursor.fetchall()

            if not records:
                print("\nNo SchuelerErzAdr records found for misc clear")
                return 0

            if dry_run:
                print("\nDRY RUN - SchuelerErzAdr misc clear:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_email2 = record.get("ErzEmail2")
                old_staat1 = record.get("Erz1StaatKrz")
                old_staat2 = record.get("Erz2StaatKrz")
                old_adr_zusatz = record.get("ErzAdrZusatz")

                new_email2 = None
                new_staat1 = None
                new_staat2 = None
                new_adr_zusatz = None

                if dry_run:
                    print(
                        f"  ID {record_id}: ErzEmail2 {old_email2} -> {new_email2}, "
                        f"Erz1StaatKrz {old_staat1} -> {new_staat1}, Erz2StaatKrz {old_staat2} -> {new_staat2}, "
                        f"ErzAdrZusatz {old_adr_zusatz} -> {new_adr_zusatz}"
                    )
                else:
                    update_cursor.execute(
                        """
                        UPDATE SchuelerErzAdr
                        SET ErzEmail2 = %s,
                            Erz1StaatKrz = %s,
                            Erz2StaatKrz = %s,
                            ErzAdrZusatz = %s
                        WHERE ID = %s
                        """,
                        (new_email2, new_staat1, new_staat2, new_adr_zusatz, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully cleared misc fields for {updated_count} records in SchuelerErzAdr")
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

    def clear_schueler_erzadr_bemerkungen(self, dry_run=False):
        """Set SchuelerErzAdr.Bemerkungen to NULL for all rows."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            cursor.execute("SHOW TABLES LIKE 'SchuelerErzAdr'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerErzAdr Bemerkungen clear: table not found")
                return 0

            cursor.execute("SELECT ID, Bemerkungen FROM SchuelerErzAdr")
            records = cursor.fetchall()

            if not records:
                print("\nNo SchuelerErzAdr records found for Bemerkungen clear")
                return 0

            if dry_run:
                print("\nDRY RUN - SchuelerErzAdr Bemerkungen clear:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_bem = record.get("Bemerkungen")

                new_bem = None

                if dry_run:
                    print(f"  ID {record_id}: Bemerkungen present -> set to NULL")
                else:
                    update_cursor.execute(
                        "UPDATE SchuelerErzAdr SET Bemerkungen = %s WHERE ID = %s",
                        (new_bem, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully cleared Bemerkungen for {updated_count} records in SchuelerErzAdr")
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

    def delete_schueler_vermerke(self, dry_run=False):
        """Delete all entries from SchuelerVermerke table."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerVermerke'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerVermerke deletion: table not found")
                return 0

            # Count existing records
            cursor.execute("SELECT COUNT(*) as count FROM SchuelerVermerke")
            count_result = cursor.fetchone()
            record_count = count_result.get("count", 0) if count_result else 0
            
            if record_count == 0:
                print("\nNo records found in SchuelerVermerke table")
                return 0

            print(f"\nFound {record_count} records in SchuelerVermerke table")
            
            if dry_run:
                print("\nDRY RUN - SchuelerVermerke would be completely cleared")
            else:
                delete_cursor = self.connection.cursor()
                delete_cursor.execute("DELETE FROM SchuelerVermerke")
                delete_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully deleted all {record_count} records from SchuelerVermerke table")
            
            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def anonymize_k_allg_adresse(self, dry_run=False):
        """Anonymize K_AllgAdresse table by setting AllgAdrName1 to two random last names and clearing other fields."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'K_AllgAdresse'")
            if not cursor.fetchone():
                print("\nSkipping K_AllgAdresse anonymization: table not found")
                return 0

            # Get all existing Ort IDs
            cursor.execute("SELECT ID FROM K_Ort")
            ort_ids = [row['ID'] for row in cursor.fetchall()]
            if not ort_ids:
                print("\nWarning: No Ort IDs found in K_Ort table")
                return 0

            # Load streets from Strassen.csv
            street_index = {}
            streets_path = Path(__file__).parent / "Strassen.csv"
            if streets_path.exists():
                with open(streets_path, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    next(reader, None)  # Skip header
                    for row in reader:
                        if len(row) >= 2:
                            ort = (row[0] or "").strip()
                            strasse = (row[1] or "").strip()
                            if ort and strasse:
                                street_index.setdefault(ort.lower(), []).append(strasse)
            
            all_streets = [s for streets in street_index.values() for s in streets]
            if not all_streets:
                print("\nWarning: No streets loaded from Strassen.csv")
                all_streets = ["Teststraße"]  # Fallback

            cursor.execute("SELECT ID, AllgAdrName1, AllgAdrName2, AllgAdrHausNrZusatz, AllgOrtsteil_ID, AllgAdrStrassenname, AllgAdrHausNr, AllgAdrOrt_ID, AllgAdrTelefon1, AllgAdrTelefon2, AllgAdrFax, AllgAdrEmail, AllgAdrBemerkungen, AllgAdrZusatz1, AllgAdrZusatz2 FROM K_AllgAdresse")
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in K_AllgAdresse table")
                return 0

            print(f"\nFound {len(records)} records in K_AllgAdresse table")

            if dry_run:
                print("\nDRY RUN - K_AllgAdresse changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_name1 = record.get("AllgAdrName1")
                old_name2 = record.get("AllgAdrName2")
                old_hausnr_zusatz = record.get("AllgAdrHausNrZusatz")
                old_ortsteil_id = record.get("AllgOrtsteil_ID")
                old_strassenname = record.get("AllgAdrStrassenname")
                old_hausnr = record.get("AllgAdrHausNr")
                old_ort_id = record.get("AllgAdrOrt_ID")
                old_telefon1 = record.get("AllgAdrTelefon1")
                old_telefon2 = record.get("AllgAdrTelefon2")
                old_fax = record.get("AllgAdrFax")
                old_email = record.get("AllgAdrEmail")
                old_bemerkungen = record.get("AllgAdrBemerkungen")
                old_zusatz1 = record.get("AllgAdrZusatz1")
                old_zusatz2 = record.get("AllgAdrZusatz2")

                # Generate two different random last names and combine with " und "
                # Use unique seed based on record_id to ensure different names each time
                name1 = self.anonymizer.anonymize_lastname(f"seed1_{record_id}")
                name2 = self.anonymizer.anonymize_lastname(f"seed2_{record_id}")
                # Ensure names are different
                while name1 == name2:
                    name2 = self.anonymizer.anonymize_lastname(f"seed2_{record_id}_retry")
                new_name1 = f"{name1} und {name2}"

                # Generate random street name and house number
                new_strassenname = random.choice(all_streets)
                new_hausnr = str(random.randint(1, 100))
                
                # Select random Ort_ID from K_Ort
                new_ort_id = random.choice(ort_ids)
                
                # Generate random phone number: "01234-" + 6 random digits
                new_telefon1 = f"01234-{random.randint(100000, 999999)}"
                
                # Generate email from AllgAdrName1 without blanks
                new_email = f"{new_name1.replace(' ', '')}@betrieb.example.com"

                if dry_run:
                    print(f"  ID {record_id}: AllgAdrName1 {old_name1} -> {new_name1}, "
                          f"AllgAdrName2 {old_name2} -> NULL, "
                          f"AllgAdrHausNrZusatz {old_hausnr_zusatz} -> NULL, "
                          f"AllgOrtsteil_ID {old_ortsteil_id} -> NULL, "
                          f"AllgAdrStrassenname {old_strassenname} -> {new_strassenname}, "
                          f"AllgAdrHausNr {old_hausnr} -> {new_hausnr}, "
                          f"AllgAdrOrt_ID {old_ort_id} -> {new_ort_id}, "
                          f"AllgAdrTelefon1 {old_telefon1} -> {new_telefon1}, "
                          f"AllgAdrTelefon2 {old_telefon2} -> NULL, "
                          f"AllgAdrFax {old_fax} -> NULL, "
                          f"AllgAdrEmail {old_email} -> {new_email}, "
                          f"AllgAdrBemerkungen {old_bemerkungen} -> NULL, "
                          f"AllgAdrZusatz1 {old_zusatz1} -> NULL, "
                          f"AllgAdrZusatz2 {old_zusatz2} -> NULL")
                else:
                    update_cursor.execute(
                        "UPDATE K_AllgAdresse SET AllgAdrName1 = %s, AllgAdrName2 = NULL, "
                        "AllgAdrHausNrZusatz = NULL, AllgOrtsteil_ID = NULL, "
                        "AllgAdrStrassenname = %s, AllgAdrHausNr = %s, AllgAdrOrt_ID = %s, "
                        "AllgAdrTelefon1 = %s, AllgAdrTelefon2 = NULL, AllgAdrFax = NULL, "
                        "AllgAdrEmail = %s, AllgAdrBemerkungen = NULL, AllgAdrZusatz1 = NULL, "
                        "AllgAdrZusatz2 = NULL WHERE ID = %s",
                        (new_name1, new_strassenname, new_hausnr, new_ort_id, new_telefon1, new_email, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully anonymized {updated_count} records in K_AllgAdresse table")
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

    def anonymize_lehrer_abschnittsdaten(self, dry_run=False):
        """Update LehrerAbschnittsdaten.StammschulNr to 123456."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'LehrerAbschnittsdaten'")
            if not cursor.fetchone():
                print("\nSkipping LehrerAbschnittsdaten update: table not found")
                return 0

            # Get all records
            cursor.execute("SELECT ID, StammschulNr FROM LehrerAbschnittsdaten")
            records = cursor.fetchall()
            
            if not records:
                print("\nNo records found in LehrerAbschnittsdaten table")
                return 0

            print(f"\nFound {len(records)} records in LehrerAbschnittsdaten table")
            
            if dry_run:
                print("\nDRY RUN - LehrerAbschnittsdaten changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_stammschulnr = record.get("StammschulNr")
                new_stammschulnr = "123456"
                
                if dry_run:
                    print(f"  ID {record_id}: StammschulNr {old_stammschulnr} -> {new_stammschulnr}")
                else:
                    update_cursor.execute(
                        "UPDATE LehrerAbschnittsdaten SET StammschulNr = %s WHERE ID = %s",
                        (new_stammschulnr, record_id)
                    )
                
                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated {updated_count} records in LehrerAbschnittsdaten table")
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

    def anonymize_eigene_schule_logo(self, dry_run=False):
        """Replace logo in EigeneSchule_Logo table with provided base64 data."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Ensure table exists
            cursor.execute("SHOW TABLES LIKE 'EigeneSchule_Logo'")
            if not cursor.fetchone():
                print("\nSkipping EigeneSchule_Logo update: table 'EigeneSchule_Logo' not found")
                return 0

            # Get the EigeneSchule ID
            cursor.execute("SELECT ID FROM EigeneSchule LIMIT 1")
            result = cursor.fetchone()
            eigene_schule_id = result["ID"] if result else 1

            # Read logo from PNG file and convert to base64
            import base64
            logo_path = Path(__file__).parent / "Wappenzeichen_NRW_color.png"
            if logo_path.exists():
                with open(logo_path, 'rb') as f:
                    logo_base64 = base64.b64encode(f.read()).decode('utf-8')
            else:
                print(f"Warning: Logo file not found at {logo_path}", file=sys.stderr)
                logo_base64 = ""
            
            # Count rows
            cursor.execute("SELECT COUNT(*) AS cnt FROM EigeneSchule_Logo")
            row = cursor.fetchone()
            total = row["cnt"] if row and "cnt" in row else 0

            if dry_run:
                print("\nDRY RUN - EigeneSchule_Logo update:")
                print(f"  Existing rows: {total} -> will delete all")
                print(f"  Will insert EigeneSchule_ID={eigene_schule_id} with LogoBase64 length {len(logo_base64)}")
                return total
            else:
                update_cursor = self.connection.cursor()
                update_cursor.execute("DELETE FROM EigeneSchule_Logo")
                update_cursor.execute(
                    "INSERT INTO EigeneSchule_Logo (EigeneSchule_ID, LogoBase64) VALUES (%s, %s)",
                    (eigene_schule_id, logo_base64),
                )
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully reset EigeneSchule_Logo (deleted {total} rows, inserted 1 row)")
                return total

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def anonymize_benutzergruppen(self, dry_run=False):
        """Update Benutzergruppen.Bezeichnung with 'Bezeichnung '+ID, excluding protected values."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'Benutzergruppen'")
            if not cursor.fetchone():
                print("\nSkipping Benutzergruppen: table not found")
                return 0

            # Protected values that should not be changed
            protected_values = ["Administrator", "Schulleitung", "Lehrer", "Sekretariat"]
            placeholders = ",".join(["%s"] * len(protected_values))

            # Get records where Bezeichnung IS NOT NULL and not in protected list
            query = f"SELECT ID, Bezeichnung FROM Benutzergruppen WHERE Bezeichnung IS NOT NULL AND Bezeichnung NOT IN ({placeholders})"
            cursor.execute(query, protected_values)
            records = cursor.fetchall()

            if not records:
                print("\nNo Benutzergruppen records found with non-NULL Bezeichnung (excluding protected values)")
                return 0

            print(f"\nFound {len(records)} records in Benutzergruppen table with non-NULL Bezeichnung (excluding protected values)")

            if dry_run:
                print("\nDRY RUN - Benutzergruppen Bezeichnung update:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None

            for record in records:
                record_id = record.get("ID")
                old_bezeichnung = record.get("Bezeichnung")
                new_bezeichnung = f"Bezeichnung {record_id}"

                if dry_run:
                    print(f"  ID {record_id}: Bezeichnung '{old_bezeichnung}' -> '{new_bezeichnung}'")
                else:
                    update_cursor.execute(
                        "UPDATE Benutzergruppen SET Bezeichnung = %s WHERE ID = %s",
                        (new_bezeichnung, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated Bezeichnung for {updated_count} records in Benutzergruppen table")
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

    def anonymize_k_datenschutz(self, dry_run=False):
        """Update K_Datenschutz.Bezeichnung with 'Bezeichnung '+ID, excluding 'Verwendung Foto' and NULL values."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'K_Datenschutz'")
            if not cursor.fetchone():
                print("\nSkipping K_Datenschutz: table not found")
                return 0

            # Get records where Bezeichnung IS NOT NULL and not 'Verwendung Foto'
            query = "SELECT ID, Bezeichnung FROM K_Datenschutz WHERE Bezeichnung IS NOT NULL AND Bezeichnung != %s"
            cursor.execute(query, ("Verwendung Foto",))
            records = cursor.fetchall()

            if not records:
                print("\nNo K_Datenschutz records found with non-NULL Bezeichnung (excluding 'Verwendung Foto')")
                return 0

            print(f"\nFound {len(records)} records in K_Datenschutz table with non-NULL Bezeichnung (excluding 'Verwendung Foto')")

            if dry_run:
                print("\nDRY RUN - K_Datenschutz Bezeichnung update:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None

            for record in records:
                record_id = record.get("ID")
                old_bezeichnung = record.get("Bezeichnung")
                new_bezeichnung = f"Bezeichnung {record_id}"

                if dry_run:
                    print(f"  ID {record_id}: Bezeichnung '{old_bezeichnung}' -> '{new_bezeichnung}'")
                else:
                    update_cursor.execute(
                        "UPDATE K_Datenschutz SET Bezeichnung = %s WHERE ID = %s",
                        (new_bezeichnung, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated Bezeichnung for {updated_count} records in K_Datenschutz table")
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

    def anonymize_k_erzieherart(self, dry_run=False):
        """Update K_ErzieherArt.Bezeichnung with 'Erzieherart '+ID, excluding protected values."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'K_ErzieherArt'")
            if not cursor.fetchone():
                print("\nSkipping K_ErzieherArt: table not found")
                return 0

            # Protected values that should not be changed
            protected_values = ["Vater", "Mutter", "Schüler ist volljährig", "Schülerin ist volljährig", "Eltern", "Sonstige"]
            placeholders = ",".join(["%s"] * len(protected_values))

            # Get records where Bezeichnung IS NOT NULL and not in protected list
            query = f"SELECT ID, Bezeichnung FROM K_ErzieherArt WHERE Bezeichnung IS NOT NULL AND Bezeichnung NOT IN ({placeholders})"
            cursor.execute(query, protected_values)
            records = cursor.fetchall()

            if not records:
                print("\nNo K_ErzieherArt records found with non-NULL Bezeichnung (excluding protected values)")
                return 0

            print(f"\nFound {len(records)} records in K_ErzieherArt table with non-NULL Bezeichnung (excluding protected values)")

            if dry_run:
                print("\nDRY RUN - K_ErzieherArt Bezeichnung update:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None

            for record in records:
                record_id = record.get("ID")
                old_bezeichnung = record.get("Bezeichnung")
                new_bezeichnung = f"Erzieherart {record_id}"

                if dry_run:
                    print(f"  ID {record_id}: Bezeichnung '{old_bezeichnung}' -> '{new_bezeichnung}'")
                else:
                    update_cursor.execute(
                        "UPDATE K_ErzieherArt SET Bezeichnung = %s WHERE ID = %s",
                        (new_bezeichnung, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated Bezeichnung for {updated_count} records in K_ErzieherArt table")
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

    def anonymize_k_entlassgrund(self, dry_run=False):
        """Update K_EntlassGrund.Bezeichnung with 'Entlassgrund '+ID, excluding protected values."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'K_EntlassGrund'")
            if not cursor.fetchone():
                print("\nSkipping K_EntlassGrund: table not found")
                return 0

            # Protected values that should not be changed
            protected_values = ["Schulpflicht endet", "Normaler Abschluss", "Ohne Angabe", "Wechsel zu anderer Schule"]
            placeholders = ",".join(["%s"] * len(protected_values))

            # Get records where Bezeichnung IS NOT NULL and not in protected list
            query = f"SELECT ID, Bezeichnung FROM K_EntlassGrund WHERE Bezeichnung IS NOT NULL AND Bezeichnung NOT IN ({placeholders})"
            cursor.execute(query, protected_values)
            records = cursor.fetchall()

            if not records:
                print("\nNo K_EntlassGrund records found with non-NULL Bezeichnung (excluding protected values)")
                return 0

            print(f"\nFound {len(records)} records in K_EntlassGrund table with non-NULL Bezeichnung (excluding protected values)")

            if dry_run:
                print("\nDRY RUN - K_EntlassGrund Bezeichnung update:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None

            for record in records:
                record_id = record.get("ID")
                old_bezeichnung = record.get("Bezeichnung")
                new_bezeichnung = f"Entlassgrund {record_id}"

                if dry_run:
                    print(f"  ID {record_id}: Bezeichnung '{old_bezeichnung}' -> '{new_bezeichnung}'")
                else:
                    update_cursor.execute(
                        "UPDATE K_EntlassGrund SET Bezeichnung = %s WHERE ID = %s",
                        (new_bezeichnung, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated Bezeichnung for {updated_count} records in K_EntlassGrund table")
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

    def anonymize_allg_adr_ansprechpartner(self, dry_run=False):
        """Anonymize AllgAdrAnsprechpartner table with random names, emails, and phone numbers."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'AllgAdrAnsprechpartner'")
            if not cursor.fetchone():
                print("\nSkipping AllgAdrAnsprechpartner anonymization: table not found")
                return 0

            cursor.execute("SELECT ID, Name, Vorname, Email, Titel, Telefon FROM AllgAdrAnsprechpartner")
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in AllgAdrAnsprechpartner table")
                return 0

            print(f"\nFound {len(records)} records in AllgAdrAnsprechpartner table")

            if dry_run:
                print("\nDRY RUN - AllgAdrAnsprechpartner changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_name = record.get("Name")
                old_vorname = record.get("Vorname")
                old_email = record.get("Email")
                old_titel = record.get("Titel")
                old_telefon = record.get("Telefon")

                # Generate random first name and last name
                # Use record_id based seeds to ensure different names for each record
                new_vorname = self.anonymizer.anonymize_firstname(f"seed_vorname_{record_id}")
                new_name = self.anonymizer.anonymize_lastname(f"seed_name_{record_id}")

                # Generate email from new Name without spaces and special characters
                email_name = new_name.replace(" ", "").replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
                new_email = f"{email_name}@betrieb.example.com"

                # Generate phone number: "01234-" + 6 random digits
                new_telefon = f"01234-{random.randint(100000, 999999)}"

                if dry_run:
                    print(f"  ID {record_id}: Name {old_name} -> {new_name}, "
                          f"Vorname {old_vorname} -> {new_vorname}, "
                          f"Email {old_email} -> {new_email}, "
                          f"Titel {old_titel} -> NULL, "
                          f"Telefon {old_telefon} -> {new_telefon}")
                else:
                    update_cursor.execute(
                        "UPDATE AllgAdrAnsprechpartner SET Name = %s, Vorname = %s, "
                        "Email = %s, Titel = NULL, Telefon = %s WHERE ID = %s",
                        (new_name, new_vorname, new_email, new_telefon, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully anonymized {updated_count} records in AllgAdrAnsprechpartner table")
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

    def anonymize_schueler_telefone(self, dry_run=False):
        """Anonymize SchuelerTelefone table by setting Telefonnummer to '012345-' + 6 random digits and Bemerkung to NULL."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerTelefone'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerTelefone: table not found")
                return 0

            # Fetch all records
            cursor.execute("SELECT ID, Telefonnummer, Bemerkung FROM SchuelerTelefone")
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in SchuelerTelefone table")
                return 0

            print(f"\nFound {len(records)} records in SchuelerTelefone table")

            if dry_run:
                print("\nDRY RUN - SchuelerTelefone changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_telefon = record.get("Telefonnummer")
                old_bemerkung = record.get("Bemerkung")

                # Generate new phone number: "012345-" + 6 random digits
                new_telefon = f"012345-{random.randint(100000, 999999)}"
                new_bemerkung = None

                if dry_run:
                    print(f"  ID {record_id}: Telefonnummer {old_telefon} -> {new_telefon}, "
                          f"Bemerkung {old_bemerkung} -> NULL")
                else:
                    update_cursor.execute(
                        "UPDATE SchuelerTelefone SET Telefonnummer = %s, Bemerkung = %s WHERE ID = %s",
                        (new_telefon, new_bemerkung, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully anonymized {updated_count} records in SchuelerTelefone table")
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

    def clear_schueler_leistungsdaten(self, dry_run=False):
        """Clear Lernentw field in SchuelerLeistungsdaten table."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerLeistungsdaten'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerLeistungsdaten: table not found")
                return 0

            # Fetch all records
            cursor.execute("SELECT ID, Lernentw FROM SchuelerLeistungsdaten")
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in SchuelerLeistungsdaten table")
                return 0

            print(f"\nFound {len(records)} records in SchuelerLeistungsdaten table")

            if dry_run:
                print("\nDRY RUN - SchuelerLeistungsdaten field clearing:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")

                if dry_run:
                    print(f"  ID {record_id}: Lernentw -> NULL")
                else:
                    update_cursor.execute(
                        "UPDATE SchuelerLeistungsdaten SET Lernentw = NULL WHERE ID = %s",
                        (record_id,),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully cleared Lernentw for {updated_count} records in SchuelerLeistungsdaten table")
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

    def clear_schueler_ld_psfachbem(self, dry_run=False):
        """Clear specific fields in SchuelerLD_PSFachBem table: ASV, LELS, AUE, ESF, BemerkungFSP, BemerkungVersetzung."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerLD_PSFachBem'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerLD_PSFachBem: table not found")
                return 0

            # Fetch all records
            cursor.execute("SELECT ID, ASV, LELS, AUE, ESF, BemerkungFSP, BemerkungVersetzung FROM SchuelerLD_PSFachBem")
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in SchuelerLD_PSFachBem table")
                return 0

            print(f"\nFound {len(records)} records in SchuelerLD_PSFachBem table")

            if dry_run:
                print("\nDRY RUN - SchuelerLD_PSFachBem field clearing:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")

                if dry_run:
                    print(f"  ID {record_id}: ASV, LELS, AUE, ESF, BemerkungFSP, BemerkungVersetzung -> NULL")
                else:
                    update_cursor.execute(
                        "UPDATE SchuelerLD_PSFachBem SET ASV = NULL, LELS = NULL, AUE = NULL, ESF = NULL, "
                        "BemerkungFSP = NULL, BemerkungVersetzung = NULL WHERE ID = %s",
                        (record_id,),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully cleared fields for {updated_count} records in SchuelerLD_PSFachBem table")
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

    def clear_schueler_transport_fields(self, dry_run=False):
        """Set Schueler.Idext, Schueler.Fahrschueler_ID, Schueler.Haltestelle_ID to NULL for all rows."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'Schueler'")
            if not cursor.fetchone():
                print("\nSkipping Schueler transport fields clear: table not found")
                return 0

            # Fetch IDs to report progress
            cursor.execute("SELECT ID, Idext, Fahrschueler_ID, Haltestelle_ID FROM Schueler")
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in Schueler table for transport fields clear")
                return 0

            print(f"\nFound {len(records)} records in Schueler table for transport fields clear")

            if dry_run:
                print("\nDRY RUN - Schueler transport fields changes:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_idext = record.get("Idext")
                old_fahr = record.get("Fahrschueler_ID")
                old_halt = record.get("Haltestelle_ID")

                new_idext = None
                new_fahr = None
                new_halt = None

                if dry_run:
                    print(
                        f"  ID {record_id}: Idext {old_idext} -> NULL, Fahrschueler_ID {old_fahr} -> NULL, Haltestelle_ID {old_halt} -> NULL"
                    )
                else:
                    update_cursor.execute(
                        "UPDATE Schueler SET Idext = %s, Fahrschueler_ID = %s, Haltestelle_ID = %s WHERE ID = %s",
                        (new_idext, new_fahr, new_halt, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully cleared transport fields for {updated_count} records in Schueler table"
                )
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

    def set_schueler_modifiziert_von_admin(self, dry_run=False):
        """Set Schueler.ModifiziertVon to 'Admin' for all rows."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Ensure table and column exist
            cursor.execute("SHOW TABLES LIKE 'Schueler'")
            if not cursor.fetchone():
                print("\nSkipping Schueler ModifiziertVon update: table not found")
                return 0

            cursor.execute("SHOW COLUMNS FROM Schueler LIKE 'ModifiziertVon'")
            if not cursor.fetchone():
                print("\nSkipping Schueler ModifiziertVon update: column not found")
                return 0

            cursor.execute("SELECT ID, ModifiziertVon FROM Schueler")
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in Schueler table for ModifiziertVon update")
                return 0

            print(f"\nFound {len(records)} records in Schueler table for ModifiziertVon update")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_val = record.get("ModifiziertVon")
                new_val = "Admin"

                if dry_run:
                    # Report intended change
                    print(f"  ID {record_id}: ModifiziertVon {old_val} -> {new_val}")
                else:
                    update_cursor.execute(
                        "UPDATE Schueler SET ModifiziertVon = %s WHERE ID = %s",
                        (new_val, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully set ModifiziertVon='Admin' for {updated_count} records in Schueler table")
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

    def clear_schueler_dokumentenverzeichnis(self, dry_run=False):
        """Set Schueler.Dokumentenverzeichnis to NULL for all rows."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Ensure table and column exist
            cursor.execute("SHOW TABLES LIKE 'Schueler'")
            if not cursor.fetchone():
                print("\nSkipping Schueler Dokumentenverzeichnis clear: table not found")
                return 0

            cursor.execute("SHOW COLUMNS FROM Schueler LIKE 'Dokumentenverzeichnis'")
            if not cursor.fetchone():
                print("\nSkipping Schueler Dokumentenverzeichnis clear: column not found")
                return 0

            cursor.execute("SELECT ID, Dokumentenverzeichnis FROM Schueler")
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in Schueler table for Dokumentenverzeichnis clear")
                return 0

            print(f"\nFound {len(records)} records in Schueler table for Dokumentenverzeichnis clear")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_val = record.get("Dokumentenverzeichnis")
                new_val = None

                if dry_run:
                    print(f"  ID {record_id}: Dokumentenverzeichnis {old_val} -> NULL")
                else:
                    update_cursor.execute(
                        "UPDATE Schueler SET Dokumentenverzeichnis = %s WHERE ID = %s",
                        (new_val, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully cleared Dokumentenverzeichnis for {updated_count} records in Schueler table"
                )
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

    def clear_schueler_gsdaten(self, dry_run=False):
        """Set SchuelerGSDaten.Anrede_Klassenlehrer, Nachname_Klassenlehrer, GS_Klasse, and Bemerkungen to NULL for all rows."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerGSDaten'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerGSDaten clear: table not found")
                return 0

            # Count records
            cursor.execute("SELECT COUNT(*) as count FROM SchuelerGSDaten")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo records found in SchuelerGSDaten table for clearing")
                return 0

            print(f"\nFound {record_count} records in SchuelerGSDaten table for field clearing")

            if dry_run:
                print("\nDRY RUN - SchuelerGSDaten field clearing:")
                print(f"  Would set Anrede_Klassenlehrer, Nachname_Klassenlehrer, GS_Klasse, Bemerkungen to NULL for {record_count} records")
            else:
                update_cursor = self.connection.cursor()
                update_cursor.execute(
                    "UPDATE SchuelerGSDaten SET Anrede_Klassenlehrer = NULL, Nachname_Klassenlehrer = NULL, GS_Klasse = NULL, Bemerkungen = NULL"
                )
                update_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully cleared fields for {record_count} records in SchuelerGSDaten table"
                )

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def clear_schueler_kaoa_daten(self, dry_run=False):
        """Set SchuelerKAoADaten.Bemerkung to NULL for all rows."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerKAoADaten'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerKAoADaten clear: table not found")
                return 0

            # Count records
            cursor.execute("SELECT COUNT(*) as count FROM SchuelerKAoADaten")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo records found in SchuelerKAoADaten table for clearing")
                return 0

            print(f"\nFound {record_count} records in SchuelerKAoADaten table for field clearing")

            if dry_run:
                print("\nDRY RUN - SchuelerKAoADaten field clearing:")
                print(f"  Would set Bemerkung to NULL for {record_count} records")
            else:
                update_cursor = self.connection.cursor()
                update_cursor.execute(
                    "UPDATE SchuelerKAoADaten SET Bemerkung = NULL"
                )
                update_cursor.close()
                self.connection.commit()
                print(
                    f"\\nSuccessfully cleared Bemerkung for {record_count} records in SchuelerKAoADaten table"
                )

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def clear_schueler_lernabschnittsdaten(self, dry_run=False):
        """Set SchuelerLernabschnittsdaten.ZeugnisBem, PruefAlgoErgebnis, and PrognoseLog to NULL for all rows."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerLernabschnittsdaten'")
            if not cursor.fetchone():
                print("\\nSkipping SchuelerLernabschnittsdaten clear: table not found")
                return 0

            # Count records
            cursor.execute("SELECT COUNT(*) as count FROM SchuelerLernabschnittsdaten")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo records found in SchuelerLernabschnittsdaten table for clearing")
                return 0

            print(f"\nFound {record_count} records in SchuelerLernabschnittsdaten table for field clearing")

            if dry_run:
                print("\nDRY RUN - SchuelerLernabschnittsdaten field clearing:")
                print(f"  Would set ZeugnisBem, PruefAlgoErgebnis, PrognoseLog to NULL for {record_count} records")
            else:
                update_cursor = self.connection.cursor()
                update_cursor.execute(
                    "UPDATE SchuelerLernabschnittsdaten SET ZeugnisBem = NULL, PruefAlgoErgebnis = NULL, PrognoseLog = NULL"
                )
                update_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully cleared fields for {record_count} records in SchuelerLernabschnittsdaten table"
                )

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def update_schueler_allgadr_ausbilder(self, dry_run=False):
        """Replace Schueler_AllgAdr.Ausbilder with random last names from nachnamen.json."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'Schueler_AllgAdr'")
            if not cursor.fetchone():
                print("\nSkipping Schueler_AllgAdr: table not found")
                return 0

            # Count records where Ausbilder IS NOT NULL
            cursor.execute("SELECT ID, Ausbilder FROM Schueler_AllgAdr WHERE Ausbilder IS NOT NULL")
            records = cursor.fetchall()

            if not records:
                print("\nNo Schueler_AllgAdr records found with non-NULL Ausbilder")
                return 0

            print(f"\nFound {len(records)} records in Schueler_AllgAdr table with non-NULL Ausbilder")

            if dry_run:
                print("\nDRY RUN - Schueler_AllgAdr Ausbilder update:")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None
            
            for record in records:
                record_id = record.get("ID")
                old_ausbilder = record.get("Ausbilder")
                new_ausbilder = random.choice(self.anonymizer.nachnamen)

                if dry_run:
                    print(f"  ID {record_id}: Ausbilder '{old_ausbilder}' -> '{new_ausbilder}'")
                else:
                    update_cursor.execute(
                        "UPDATE Schueler_AllgAdr SET Ausbilder = %s WHERE ID = %s",
                        (new_ausbilder, record_id),
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated Ausbilder for {updated_count} records in Schueler_AllgAdr table")
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

    def update_schueler_bk_abschluss_thema(self, dry_run=False):
        """Replace SchuelerBKAbschluss.ThemaAbschlussarbeit with 'Thema der Arbeit'."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerBKAbschluss'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerBKAbschluss: table not found")
                return 0

            # Count records where ThemaAbschlussarbeit IS NOT NULL
            cursor.execute("SELECT COUNT(*) as count FROM SchuelerBKAbschluss WHERE ThemaAbschlussarbeit IS NOT NULL")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo SchuelerBKAbschluss records found with non-NULL ThemaAbschlussarbeit")
                return 0

            print(f"\nFound {record_count} records in SchuelerBKAbschluss table with non-NULL ThemaAbschlussarbeit")

            if dry_run:
                print("\nDRY RUN - SchuelerBKAbschluss ThemaAbschlussarbeit update:")
                print(f"  Would set ThemaAbschlussarbeit to 'Thema der Arbeit' for {record_count} records")
            else:
                update_cursor = self.connection.cursor()
                update_cursor.execute(
                    "UPDATE SchuelerBKAbschluss SET ThemaAbschlussarbeit = %s WHERE ThemaAbschlussarbeit IS NOT NULL",
                    ("Thema der Arbeit",),
                )
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated ThemaAbschlussarbeit for {record_count} records in SchuelerBKAbschluss table")

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def update_schueler_einzelleistungen_bemerkungen(self, dry_run=False):
        """Replace SchuelerEinzelleistungen.Bemerkung with 'Bemerkung'."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerEinzelleistungen'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerEinzelleistungen: table not found")
                return 0

            # Check if Bemerkung column exists
            cursor.execute("SHOW COLUMNS FROM SchuelerEinzelleistungen LIKE 'Bemerkung'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerEinzelleistungen: column Bemerkung not found")
                return 0

            # Count records where Bemerkung IS NOT NULL
            cursor.execute("SELECT COUNT(*) as count FROM SchuelerEinzelleistungen WHERE Bemerkung IS NOT NULL")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo SchuelerEinzelleistungen records found with non-NULL Bemerkung")
                return 0

            print(f"\nFound {record_count} records in SchuelerEinzelleistungen table with non-NULL Bemerkung")

            if dry_run:
                print("\nDRY RUN - SchuelerEinzelleistungen Bemerkung update:")
                print(f"  Would set Bemerkung to 'Bemerkung' for {record_count} records")
            else:
                update_cursor = self.connection.cursor()
                update_cursor.execute(
                    "UPDATE SchuelerEinzelleistungen SET Bemerkung = %s WHERE Bemerkung IS NOT NULL",
                    ("Bemerkung",),
                )
                update_cursor.close()
                self.connection.commit()
                print(f"\nSuccessfully updated Bemerkung for {record_count} records in SchuelerEinzelleistungen table")

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def update_schueler_liste_erzeuger(self, dry_run=False):
        """Set SchuelerListe.Erzeuger to 1 where not NULL."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerListe'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerListe: table not found")
                return 0

            # Count records where Erzeuger IS NOT NULL
            cursor.execute("SELECT COUNT(*) as count FROM SchuelerListe WHERE Erzeuger IS NOT NULL")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo records found in SchuelerListe table with non-NULL Erzeuger")
                return 0

            print(f"\nFound {record_count} records in SchuelerListe table with non-NULL Erzeuger")

            if dry_run:
                print("\nDRY RUN - SchuelerListe Erzeuger update:")
                print(f"  Would set Erzeuger to 1 for {record_count} records")
            else:
                update_cursor = self.connection.cursor()
                update_cursor.execute(
                    "UPDATE SchuelerListe SET Erzeuger = 1 WHERE Erzeuger IS NOT NULL"
                )
                update_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully updated Erzeuger to 1 for {record_count} records in SchuelerListe table"
                )

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def delete_personengruppen_personen(self, dry_run=False):
        """Delete all entries from Personengruppen_Personen table."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'Personengruppen_Personen'")
            if not cursor.fetchone():
                print("\nSkipping Personengruppen_Personen deletion: table not found")
                return 0

            # Count existing records
            cursor.execute("SELECT COUNT(*) as count FROM Personengruppen_Personen")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo records found in Personengruppen_Personen table")
                return 0

            print(f"\nFound {record_count} records in Personengruppen_Personen table")

            if dry_run:
                print("\nDRY RUN - Personengruppen_Personen would be completely cleared")
            else:
                delete_cursor = self.connection.cursor()
                delete_cursor.execute("DELETE FROM Personengruppen_Personen")
                delete_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully deleted all {record_count} records from Personengruppen_Personen table"
                )

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def delete_eigene_schule_texte(self, dry_run=False):
        """Delete all entries from EigeneSchule_Texte table."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'EigeneSchule_Texte'")
            if not cursor.fetchone():
                print("\nSkipping EigeneSchule_Texte deletion: table not found")
                return 0

            # Count existing records
            cursor.execute("SELECT COUNT(*) as count FROM EigeneSchule_Texte")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo records found in EigeneSchule_Texte table")
                return 0

            print(f"\nFound {record_count} records in EigeneSchule_Texte table")

            if dry_run:
                print("\nDRY RUN - EigeneSchule_Texte would be completely cleared")
            else:
                delete_cursor = self.connection.cursor()
                delete_cursor.execute("DELETE FROM EigeneSchule_Texte")
                delete_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully deleted all {record_count} records from EigeneSchule_Texte table"
                )

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def anonymize_k_telefonart(self, dry_run=False):
        """Anonymize K_TelefonArt table by replacing Bezeichnung with 'Telefonart ' + ID.
        
        Protected values that will NOT be changed:
        - Eltern
        - Mutter
        - Vater
        - Notfallnummer
        - Festnetz
        - Handynummer
        - Mobilnummer
        - Großeltern
        """
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'K_TelefonArt'")
            if not cursor.fetchone():
                print("\nSkipping K_TelefonArt anonymization: table not found")
                return 0

            # Protected values that should not be changed
            protected_values = [
                'Eltern', 'Mutter', 'Vater', 'Notfallnummer',
                'Festnetz', 'Handynummer', 'Mobilnummer', 'Großeltern'
            ]

            # Fetch all records
            cursor.execute("SELECT ID, Bezeichnung FROM K_TelefonArt")
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in K_TelefonArt table")
                return 0

            print(f"\nFound {len(records)} records in K_TelefonArt table")

            # Filter records to update (exclude protected values)
            records_to_update = [
                rec for rec in records 
                if rec.get("Bezeichnung") not in protected_values
            ]

            if not records_to_update:
                print("No records to update (all values are protected)")
                return 0

            print(f"  {len(records_to_update)} records will be updated (excluding protected values)")

            if dry_run:
                print("\nDRY RUN - K_TelefonArt anonymization:")
                print(f"  (showing first 5 of {len(records_to_update)} records)")
                for i, record in enumerate(records_to_update[:5]):
                    record_id = record.get("ID")
                    old_bezeichnung = record.get("Bezeichnung")
                    new_bezeichnung = f"Telefonart {record_id}"
                    print(f"  ID {record_id}: {old_bezeichnung} -> {new_bezeichnung}")
            else:
                updated_count = 0
                update_cursor = self.connection.cursor()

                for record in records_to_update:
                    record_id = record.get("ID")
                    new_bezeichnung = f"Telefonart {record_id}"
                    
                    update_cursor.execute(
                        "UPDATE K_TelefonArt SET Bezeichnung = %s WHERE ID = %s",
                        (new_bezeichnung, record_id)
                    )
                    updated_count += 1

                update_cursor.close()
                self.connection.commit()
                print(f"Successfully anonymized {updated_count} records in K_TelefonArt table")

            return len(records_to_update)

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def anonymize_k_kindergarten(self, dry_run=False):
        """Anonymize K_Kindergarten table with new designations, random locations, and street names."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'K_Kindergarten'")
            if not cursor.fetchone():
                print("\nSkipping K_Kindergarten anonymization: table not found")
                return 0

            # First, try to get column names to determine the actual structure
            cursor.execute("DESCRIBE K_Kindergarten")
            describe_results = cursor.fetchall()
            columns = [col['Field'] for col in describe_results]
            
            # Check required columns exist
            required_cols = ['ID', 'Bezeichnung', 'PLZ', 'Ort', 'Strassenname']
            missing_cols = [col for col in required_cols if col not in columns]
            if missing_cols:
                print(f"\nSkipping K_Kindergarten anonymization: Missing required columns: {', '.join(missing_cols)}")
                return 0

            # Build the SELECT query with actual columns
            select_cols = "ID, Bezeichnung, PLZ, Ort, Strassenname"
            optional_cols = []
            if 'HausNrZusatz' in columns:
                optional_cols.append('HausNrZusatz')
            if 'Tel' in columns:
                optional_cols.append('Tel')
            if 'Email' in columns:
                optional_cols.append('Email')
            if 'Bemerkung' in columns:
                optional_cols.append('Bemerkung')
            
            if optional_cols:
                select_cols += ", " + ", ".join(optional_cols)

            # Fetch all K_Kindergarten records
            cursor.execute(f"SELECT {select_cols} FROM K_Kindergarten")
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in K_Kindergarten table")
                return 0

            print(f"\nFound {len(records)} records in K_Kindergarten table")

            # Load all K_Ort records for random location selection (get both PLZ and Bezeichnung)
            cursor.execute("SELECT PLZ, Bezeichnung FROM K_Ort")
            ort_records = cursor.fetchall()
            
            if not ort_records:
                print("Warning: No records found in K_Ort table for location assignment")
                return 0

            # Load all street names from Strassen.csv
            csv_path = Path(__file__).parent / "Strassen.csv"
            if not csv_path.exists():
                print(f"Warning: Strassen.csv not found at {csv_path}")
                return 0

            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                strassen_records = list(reader)

            if not strassen_records:
                print("Warning: No records found in Strassen.csv")
                return 0

            strassen_list = [rec.get("Strasse") for rec in strassen_records if rec.get("Strasse")]

            if dry_run:
                print("DRY RUN - K_Kindergarten anonymization:")
                print(f"  (showing first 5 of {len(records)} records)")

            updated_count = 0
            update_cursor = self.connection.cursor() if not dry_run else None

            for record in records:
                record_id = record.get("ID")
                
                # Set Bezeichnung to "Kindergarten " + ID
                new_bezeichnung = f"Kindergarten {record_id}"
                
                # Get random K_Ort record (contains both PLZ and Bezeichnung values)
                random_ort = random.choice(ort_records)
                new_plz = random_ort.get("PLZ")
                new_ort = random_ort.get("Bezeichnung")
                
                # Get random street name
                new_strassenname = random.choice(strassen_list) if strassen_list else None
                
                if dry_run and updated_count < 5:
                    print(f"  ID {record_id}: Bezeichnung -> {new_bezeichnung}, PLZ -> {new_plz}, Ort -> {new_ort}, Strassenname -> {new_strassenname}")
                elif not dry_run:
                    # Build UPDATE statement with actual columns
                    set_clauses = [
                        "Bezeichnung = %s",
                        "PLZ = %s",
                        "Ort = %s",
                        "Strassenname = %s"
                    ]
                    params = [new_bezeichnung, new_plz, new_ort, new_strassenname]
                    
                    for col in ['HausNrZusatz', 'Tel', 'Email', 'Bemerkung']:
                        if col in columns:
                            set_clauses.append(f"{col} = NULL")
                    
                    set_clause_str = ", ".join(set_clauses)
                    update_cursor.execute(
                        f"UPDATE K_Kindergarten SET {set_clause_str} WHERE ID = %s",
                        params + [record_id]
                    )

                updated_count += 1

            if not dry_run:
                update_cursor.close()
                self.connection.commit()
                print(f"Successfully anonymized {updated_count} records in K_Kindergarten table")
            else:
                print(f"Dry run complete. {updated_count} records would be updated")

            return updated_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        except Exception as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Error in K_Kindergarten anonymization: {type(e).__name__}: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def anonymize_personengruppen(self, dry_run=False):
        """Anonymize Personengruppen table.
        
        Sets:
        - Gruppenname to "Gruppe " + ID
        - Zusatzinfo to "Info"
        - SammelEmail to "gruppeID@gruppe.example.com"
        """
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'Personengruppen'")
            if not cursor.fetchone():
                print("\nSkipping Personengruppen anonymization: table not found")
                return 0

            # Check column structure
            cursor.execute("DESCRIBE Personengruppen")
            describe_results = cursor.fetchall()
            columns = [col['Field'] for col in describe_results]
            
            # Check required columns exist
            if 'ID' not in columns:
                print("\nSkipping Personengruppen anonymization: Missing required column ID")
                return 0

            # Build SELECT query with available columns
            select_cols = ['ID']
            optional_cols = ['Gruppenname', 'Zusatzinfo', 'SammelEmail']
            available_optional = [col for col in optional_cols if col in columns]
            
            if not available_optional:
                print("\nSkipping Personengruppen anonymization: No updatable columns found")
                return 0
            
            select_cols.extend(available_optional)
            select_query = "SELECT " + ", ".join(select_cols) + " FROM Personengruppen"

            # Fetch all records
            cursor.execute(select_query)
            records = cursor.fetchall()

            if not records:
                print("\nNo records found in Personengruppen table")
                return 0

            print(f"\nFound {len(records)} records in Personengruppen table")

            if dry_run:
                print("DRY RUN - Personengruppen anonymization:")
                print(f"  (showing first 5 of {len(records)} records)")
                for i, record in enumerate(records[:5]):
                    record_id = record.get("ID")
                    updates = []
                    if 'Gruppenname' in available_optional:
                        updates.append(f"Gruppenname -> Gruppe {record_id}")
                    if 'Zusatzinfo' in available_optional:
                        updates.append(f"Zusatzinfo -> Info")
                    if 'SammelEmail' in available_optional:
                        updates.append(f"SammelEmail -> gruppe{record_id}@gruppe.example.com")
                    print(f"  ID {record_id}: {', '.join(updates)}")
            else:
                updated_count = 0
                update_cursor = self.connection.cursor()

                for record in records:
                    record_id = record.get("ID")
                    
                    # Build UPDATE statement with available columns
                    set_clauses = []
                    params = []
                    
                    if 'Gruppenname' in available_optional:
                        set_clauses.append("Gruppenname = %s")
                        params.append(f"Gruppe {record_id}")
                    
                    if 'Zusatzinfo' in available_optional:
                        set_clauses.append("Zusatzinfo = %s")
                        params.append("Info")
                    
                    if 'SammelEmail' in available_optional:
                        set_clauses.append("SammelEmail = %s")
                        params.append(f"gruppe{record_id}@gruppe.example.com")
                    
                    if set_clauses:
                        set_clause_str = ", ".join(set_clauses)
                        params.append(record_id)
                        update_cursor.execute(
                            f"UPDATE Personengruppen SET {set_clause_str} WHERE ID = %s",
                            params
                        )
                        updated_count += 1

                update_cursor.close()
                self.connection.commit()
                print(f"Successfully anonymized {updated_count} records in Personengruppen table")

            return len(records) if dry_run else updated_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        except Exception as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Error in Personengruppen anonymization: {type(e).__name__}: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def reset_schule_credentials(self, dry_run=False):
        """Reset SchuleCredentials table with new RSA keypair and AES key.
        
        Deletes all entries, retrieves SchulNr from EigeneSchule, generates:
        - RSA 2048-bit keypair (public and private keys in PEM format)
        - AES 256-bit key (base64 encoded)
        """
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        if not CRYPTOGRAPHY_AVAILABLE:
            print("\nWarning: cryptography library not available. Skipping SchuleCredentials reset.")
            print("Install it with: pip install cryptography", file=sys.stderr)
            return 0

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuleCredentials'")
            if not cursor.fetchone():
                print("\nSkipping SchuleCredentials reset: table not found")
                return 0

            # Get SchulNr from EigeneSchule
            cursor.execute("SELECT SchulNr FROM EigeneSchule LIMIT 1")
            result = cursor.fetchone()
            if not result:
                print("\nWarning: No SchulNr found in EigeneSchule table")
                return 0
            
            schulnr = result.get("SchulNr")
            print(f"\nResetting SchuleCredentials with SchulNr: {schulnr}")

            # Count existing records
            cursor.execute("SELECT COUNT(*) as count FROM SchuleCredentials")
            count_result = cursor.fetchone()
            record_count = count_result.get("count", 0) if count_result else 0

            if dry_run:
                print("\nDRY RUN - SchuleCredentials reset:")
                if record_count > 0:
                    print(f"  Would delete {record_count} existing records")
                print(f"  Would generate new RSA 2048-bit keypair")
                print(f"  Would generate new AES 256-bit key")
                print(f"  Would insert new record with Schulnummer={schulnr}")
                return record_count

            # Generate RSA 2048-bit keypair
            print("  Generating RSA 2048-bit keypair...")
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )
            public_key = private_key.public_key()

            # Serialize private key to PEM format
            private_pem_full = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            ).decode('utf-8')

            # Serialize public key to PEM format
            public_pem_full = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            ).decode('utf-8')
            
            # Remove PEM headers/footers, keeping only the base64 content
            private_pem = '\n'.join([line for line in private_pem_full.split('\n') 
                                     if not line.startswith('-----')])
            public_pem = '\n'.join([line for line in public_pem_full.split('\n') 
                                    if not line.startswith('-----')])

            # Generate AES 256-bit key (32 bytes)
            print("  Generating AES 256-bit key...")
            aes_key = secrets.token_bytes(32)
            aes_key_base64 = base64.b64encode(aes_key).decode('utf-8')

            # Delete existing records
            delete_cursor = self.connection.cursor()
            if record_count > 0:
                delete_cursor.execute("DELETE FROM SchuleCredentials")
                print(f"  Deleted {record_count} existing records")

            # Insert new record with generated keys
            delete_cursor.execute(
                "INSERT INTO SchuleCredentials (Schulnummer, RSAPublicKey, RSAPrivateKey, AES) VALUES (%s, %s, %s, %s)",
                (schulnr, public_pem, private_pem, aes_key_base64)
            )
            delete_cursor.close()
            self.connection.commit()
            
            print(f"  Successfully inserted new credentials")
            print(f"  RSA Public Key length: {len(public_pem)} bytes")
            print(f"  RSA Private Key length: {len(private_pem)} bytes")
            print(f"  AES Key (base64): {len(aes_key_base64)} characters")
            print(f"\nSuccessfully reset SchuleCredentials table")

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def delete_and_reload_k_schule(self, dry_run=False):
        """Delete all K_Schule entries and reload from K_Schule.csv file.
        
        Loads CSV with headers, parses data, deletes all existing records,
        and inserts new entries into K_Schule table.
        """
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'K_Schule'")
            if not cursor.fetchone():
                print("\nSkipping K_Schule reload: table not found")
                return 0

            # Load CSV file
            csv_path = Path(__file__).parent / "K_Schule.csv"
            if not csv_path.exists():
                print(f"\nWarning: K_Schule.csv not found at {csv_path}")
                return 0

            # Count existing records before deletion
            cursor.execute("SELECT COUNT(*) as count FROM K_Schule")
            result = cursor.fetchone()
            old_record_count = result.get("count", 0) if result else 0

            print(f"\nFound {old_record_count} existing records in K_Schule")

            if dry_run:
                # Count records that would be inserted
                with open(csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    new_records = list(reader)
                print(f"DRY RUN - K_Schule reload:")
                print(f"  Would delete {old_record_count} existing records")
                print(f"  Would insert {len(new_records)} records from K_Schule.csv")
                return old_record_count

            # Delete existing records
            delete_cursor = self.connection.cursor()
            if old_record_count > 0:
                delete_cursor.execute("DELETE FROM K_Schule")
                print(f"  Deleted {old_record_count} existing records")

            # Load and insert records from CSV
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                records = list(reader)

            if not records:
                print("\nNo records found in K_Schule.csv")
                delete_cursor.close()
                return old_record_count

            # Get column names from CSV header (first record keys)
            columns = list(records[0].keys())
            
            # Build INSERT statement
            placeholders = ", ".join(["%s"] * len(columns))
            columns_str = ", ".join(columns)
            insert_query = f"INSERT INTO K_Schule ({columns_str}) VALUES ({placeholders})"

            # Insert records
            inserted_count = 0
            for record in records:
                values = tuple(record.get(col) for col in columns)
                # Handle empty strings as NULL for some fields
                values = tuple(None if v == "" else v for v in values)
                delete_cursor.execute(insert_query, values)
                inserted_count += 1

            delete_cursor.close()
            self.connection.commit()

            print(f"  Inserted {inserted_count} records from K_Schule.csv")
            print(f"\nSuccessfully reloaded K_Schule table")

            return old_record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        except Exception as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Error reading K_Schule.csv: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def delete_schueler_fotos(self, dry_run=False):
        """Delete all entries from SchuelerFotos table."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerFotos'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerFotos deletion: table not found")
                return 0

            # Count existing records
            cursor.execute("SELECT COUNT(*) as count FROM SchuelerFotos")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo records found in SchuelerFotos table")
                return 0

            print(f"\nFound {record_count} records in SchuelerFotos table")

            if dry_run:
                print("\nDRY RUN - SchuelerFotos would be completely cleared")
            else:
                delete_cursor = self.connection.cursor()
                delete_cursor.execute("DELETE FROM SchuelerFotos")
                delete_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully deleted all {record_count} records from SchuelerFotos table"
                )

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def delete_schueler_foerderempfehlungen(self, dry_run=False):
        """Delete all entries from SchuelerFoerderempfehlungen table."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'SchuelerFoerderempfehlungen'")
            if not cursor.fetchone():
                print("\nSkipping SchuelerFoerderempfehlungen deletion: table not found")
                return 0

            # Count existing records
            cursor.execute("SELECT COUNT(*) as count FROM SchuelerFoerderempfehlungen")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo records found in SchuelerFoerderempfehlungen table")
                return 0

            print(f"\nFound {record_count} records in SchuelerFoerderempfehlungen table")

            if dry_run:
                print("\nDRY RUN - SchuelerFoerderempfehlungen would be completely cleared")
            else:
                delete_cursor = self.connection.cursor()
                delete_cursor.execute("DELETE FROM SchuelerFoerderempfehlungen")
                delete_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully deleted all {record_count} records from SchuelerFoerderempfehlungen table"
                )

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def update_schueler_lsschulnummer(self, dry_run=False):
        """Update Schueler.LSSchulnummer for two ranges:
        
        Range 1 (100000-199999): Replace with random SchulNr from K_Schule
        where K_Schule.SchulformKrz matches Schueler.SchulformSIM.
        
        Range 2 (200000-299999): Replace with random SchulNr from K_Schule
        that is also in the range 200000-299999.
        """
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if tables exist
            cursor.execute("SHOW TABLES LIKE 'Schueler'")
            if not cursor.fetchone():
                print("\nSkipping Schueler LSSchulnummer update: Schueler table not found")
                return 0

            cursor.execute("SHOW TABLES LIKE 'K_Schule'")
            if not cursor.fetchone():
                print("\nSkipping Schueler LSSchulnummer update: K_Schule table not found")
                return 0

            # Load all K_Schule records grouped by SchulformKrz
            cursor.execute("SELECT SchulNr, SchulformKrz FROM K_Schule")
            k_schule_records = cursor.fetchall()
            
            if not k_schule_records:
                print("\nNo records found in K_Schule table")
                return 0

            # Build a mapping of SchulformKrz -> list of SchulNr
            schulform_to_schulnr = {}
            # Also collect SchulNr values in range 200000-299999
            schulnr_range_2 = []
            
            for record in k_schule_records:
                schulform_krz = record.get("SchulformKrz")
                schulnr = record.get("SchulNr")
                if schulnr:
                    # Check if in range 200000-299999
                    try:
                        schulnr_int = int(schulnr)
                        if 200000 <= schulnr_int <= 299999:
                            schulnr_range_2.append(schulnr)
                    except (ValueError, TypeError):
                        pass
                
                if schulform_krz and schulnr:
                    if schulform_krz not in schulform_to_schulnr:
                        schulform_to_schulnr[schulform_krz] = []
                    schulform_to_schulnr[schulform_krz].append(schulnr)

            total_updated = 0
            total_skipped = 0

            # === RANGE 1: 100000-199999 ===
            cursor.execute(
                "SELECT ID, LSSchulNr, LSSchulformSIM FROM Schueler WHERE LSSchulNr >= 100000 AND LSSchulNr <= 199999"
            )
            range1_records = cursor.fetchall()

            if range1_records:
                print(f"\nFound {len(range1_records)} Schueler records with LSSchulNr in range 100000-199999")

                if dry_run:
                    print("DRY RUN - Schueler LSSchulNr range 1 (100000-199999) update based on SchulformKrz:")

                updated_count = 0
                skipped_count = 0
                update_cursor = self.connection.cursor() if not dry_run else None

                for record in range1_records:
                    record_id = record.get("ID")
                    old_lsschulnr = record.get("LSSchulNr")
                    schulform_sim = record.get("LSSchulformSIM")

                    # Find matching SchulNr from K_Schule with same SchulformKrz
                    if schulform_sim not in schulform_to_schulnr:
                        if dry_run:
                            print(f"  ID {record_id}: No K_Schule records found for SchulformSIM={schulform_sim}, skipping")
                        skipped_count += 1
                        continue

                    available_schulnrs = schulform_to_schulnr[schulform_sim]
                    if not available_schulnrs:
                        if dry_run:
                            print(f"  ID {record_id}: No SchulNr available for SchulformSIM={schulform_sim}, skipping")
                        skipped_count += 1
                        continue

                    new_lsschulnr = random.choice(available_schulnrs)

                    if dry_run:
                        print(f"  ID {record_id}: LSSchulNr {old_lsschulnr} -> {new_lsschulnr} (LSSchulformSIM={schulform_sim})")
                    else:
                        update_cursor.execute(
                            "UPDATE Schueler SET LSSchulNr = %s WHERE ID = %s",
                            (new_lsschulnr, record_id),
                        )

                    updated_count += 1

                if not dry_run:
                    update_cursor.close()
                    self.connection.commit()
                    print(f"Successfully updated {updated_count} records in Schueler LSSchulNr (range 1)")
                    if skipped_count > 0:
                        print(f"Skipped {skipped_count} records due to no matching SchulformKrz")
                else:
                    print(f"Dry run: {updated_count} records would be updated, {skipped_count} skipped")

                total_updated += updated_count
                total_skipped += skipped_count
            else:
                print("\nNo Schueler records found with LSSchulNr in range 100000-199999")

            # === RANGE 2: 200000-299999 ===
            cursor.execute(
                "SELECT ID, LSSchulNr FROM Schueler WHERE LSSchulNr >= 200000 AND LSSchulNr <= 299999"
            )
            range2_records = cursor.fetchall()

            if range2_records:
                print(f"\nFound {len(range2_records)} Schueler records with LSSchulNr in range 200000-299999")

                if not schulnr_range_2:
                    print("Warning: No K_Schule records found with SchulNr in range 200000-299999, skipping range 2 update")
                else:
                    if dry_run:
                        print("DRY RUN - Schueler LSSchulNr range 2 (200000-299999) update with matching range values:")

                    updated_count = 0
                    update_cursor = self.connection.cursor() if not dry_run else None

                    for record in range2_records:
                        record_id = record.get("ID")
                        old_lsschulnr = record.get("LSSchulNr")

                        new_lsschulnr = random.choice(schulnr_range_2)

                        if dry_run:
                            print(f"  ID {record_id}: LSSchulNr {old_lsschulnr} -> {new_lsschulnr}")
                        else:
                            update_cursor.execute(
                                "UPDATE Schueler SET LSSchulNr = %s WHERE ID = %s",
                                (new_lsschulnr, record_id),
                            )

                        updated_count += 1

                    if not dry_run:
                        update_cursor.close()
                        self.connection.commit()
                        print(f"Successfully updated {updated_count} records in Schueler LSSchulNr (range 2)")
                    else:
                        print(f"Dry run: {updated_count} records would be updated")

                    total_updated += updated_count
            else:
                print("\nNo Schueler records found with LSSchulNr in range 200000-299999")

            # === UPDATE SchulwechselNr ===
            cursor.execute("SELECT COUNT(*) as count FROM Schueler WHERE SchulwechselNr IS NOT NULL")
            result = cursor.fetchone()
            schulwechsel_count = result.get("count", 0) if result else 0

            if schulwechsel_count > 0:
                print(f"\nFound {schulwechsel_count} Schueler records with SchulwechselNr set")

                # Get all SchulNr from K_Schule for random selection
                cursor.execute("SELECT SchulNr FROM K_Schule")
                schulnr_records = cursor.fetchall()
                schulnr_list = [rec.get("SchulNr") for rec in schulnr_records if rec.get("SchulNr")]

                if not schulnr_list:
                    print("Warning: No SchulNr values found in K_Schule table for SchulwechselNr update")
                else:
                    if dry_run:
                        print(f"DRY RUN - Schueler SchulwechselNr update:")
                        print(f"  Would replace {schulwechsel_count} SchulwechselNr values with random SchulNr from K_Schule")
                    else:
                        cursor.execute("SELECT ID, SchulwechselNr FROM Schueler WHERE SchulwechselNr IS NOT NULL")
                        schulwechsel_records = cursor.fetchall()

                        updated_count = 0
                        update_cursor = self.connection.cursor()

                        for record in schulwechsel_records:
                            record_id = record.get("ID")
                            old_schulwechselnr = record.get("SchulwechselNr")
                            new_schulwechselnr = random.choice(schulnr_list)

                            update_cursor.execute(
                                "UPDATE Schueler SET SchulwechselNr = %s WHERE ID = %s",
                                (new_schulwechselnr, record_id),
                            )
                            updated_count += 1

                        update_cursor.close()
                        self.connection.commit()
                        print(f"Successfully updated {updated_count} records in Schueler SchulwechselNr")
            else:
                print("\nNo Schueler records found with SchulwechselNr set")

            # === DELETE SchuelerAbgaenge ===
            cursor.execute("SHOW TABLES LIKE 'SchuelerAbgaenge'")
            if cursor.fetchone():
                cursor.execute("SELECT COUNT(*) as count FROM SchuelerAbgaenge")
                result = cursor.fetchone()
                abgaenge_count = result.get("count", 0) if result else 0

                if abgaenge_count > 0:
                    print(f"\nFound {abgaenge_count} records in SchuelerAbgaenge table")

                    if dry_run:
                        print("DRY RUN - SchuelerAbgaenge would be completely cleared")
                    else:
                        delete_cursor = self.connection.cursor()
                        delete_cursor.execute("DELETE FROM SchuelerAbgaenge")
                        delete_cursor.close()
                        self.connection.commit()
                        print(f"Successfully deleted all {abgaenge_count} records from SchuelerAbgaenge table")
                else:
                    print("\nNo records found in SchuelerAbgaenge table")
            else:
                print("\nSchuelerAbgaenge table not found, skipping deletion")

            # === CLEAR LSBemerkung ===
            cursor.execute("SHOW TABLES LIKE 'Schueler'")
            if cursor.fetchone():
                cursor.execute("SELECT COUNT(*) as count FROM Schueler WHERE LSBemerkung IS NOT NULL")
                result = cursor.fetchone()
                lsbemerkung_count = result.get("count", 0) if result else 0

                if lsbemerkung_count > 0:
                    print(f"\nFound {lsbemerkung_count} records in Schueler with non-NULL LSBemerkung")

                    if dry_run:
                        print("DRY RUN - Schueler LSBemerkung would be cleared for all records with values")
                    else:
                        update_cursor = self.connection.cursor()
                        update_cursor.execute("UPDATE Schueler SET LSBemerkung = NULL")
                        update_cursor.close()
                        self.connection.commit()
                        print(f"Successfully cleared LSBemerkung for {lsbemerkung_count} records in Schueler table")
                else:
                    print("\nNo records found in Schueler with LSBemerkung set")
            else:
                print("\nSchueler table not found, skipping LSBemerkung clear")

            return total_updated

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def delete_lehrer_fotos(self, dry_run=False):
        """Delete all entries from LehrerFotos table."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        try:
            cursor = self.connection.cursor(dictionary=True)

            # Check if table exists
            cursor.execute("SHOW TABLES LIKE 'LehrerFotos'")
            if not cursor.fetchone():
                print("\nSkipping LehrerFotos deletion: table not found")
                return 0

            # Count existing records
            cursor.execute("SELECT COUNT(*) as count FROM LehrerFotos")
            result = cursor.fetchone()
            record_count = result.get("count", 0) if result else 0

            if record_count == 0:
                print("\nNo records found in LehrerFotos table")
                return 0

            print(f"\nFound {record_count} records in LehrerFotos table")

            if dry_run:
                print("\nDRY RUN - LehrerFotos would be completely cleared")
            else:
                delete_cursor = self.connection.cursor()
                delete_cursor.execute("DELETE FROM LehrerFotos")
                delete_cursor.close()
                self.connection.commit()
                print(
                    f"\nSuccessfully deleted all {record_count} records from LehrerFotos table"
                )

            return record_count

        except mysql.connector.Error as e:
            if not dry_run:
                self.connection.rollback()
            print(f"Database error: {e}", file=sys.stderr)
            raise
        finally:
            cursor.close()

    def delete_general_admin_tables(self, dry_run=False):
        """Delete all entries from general/admin-related tables.

        Tables targeted:
        - Schild_Verwaltung
        - Client_Konfiguration_Global
        - Client_Konfiguration_Benutzer
        - Wiedervorlage
        - ZuordnungReportvorlagen
        - BenutzerEmail
        - ImpExp_EigeneImporte
        - ImpExp_EigeneImporte_Felder
        - ImpExp_EigeneImporte_Tabellen
        - SchuleOAuthSecrets
        - Logins
        - TextExportVorlagen
        - Benutzer (recreates admin entry)
        - BenutzerAllgemein (recreates admin entry)
        - Credentials (recreates admin entry)
        """
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        targets = [
            "Schild_Verwaltung",
            "Client_Konfiguration_Global",
            "Client_Konfiguration_Benutzer",            
            "Wiedervorlage",
            "ZuordnungReportvorlagen",
            "BenutzerEmail",
            "ImpExp_EigeneImporte",
            "ImpExp_EigeneImporte_Felder",
            "ImpExp_EigeneImporte_Tabellen",
            "SchuleOAuthSecrets",
            "Logins",
            "TextExportVorlagen",
        ]

        # Special tables that need recreation after deletion
        special_tables = {
            "Credentials": "INSERT INTO Credentials (ID, Benutzername) VALUES (1, 'Admin')",
            "BenutzerAllgemein": "INSERT INTO BenutzerAllgemein (ID, Anzeigename, CredentialID) VALUES (1, 'Administrator', 1)",
            "Benutzer": "INSERT INTO Benutzer (ID, Typ, Allgemein_ID, Lehrer_ID, Schueler_ID, Erzieher_ID, IstAdmin) VALUES (1, 0, 1, NULL, NULL, NULL, 1)",
        }

        total_deleted = 0
        try:
            cursor = self.connection.cursor(dictionary=True)

            print("\nGeneral admin tables cleanup:")
            
            # Process regular tables first
            for table in targets:
                # Check existence
                cursor.execute(f"SHOW TABLES LIKE '{table}'")
                if not cursor.fetchone():
                    print(f"  Skipping {table}: table not found")
                    continue

                # Count records
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                result = cursor.fetchone()
                record_count = result.get("count", 0) if result else 0

                if record_count == 0:
                    print(f"  {table}: no records to delete")
                    continue

                if dry_run:
                    print(f"  {table}: would delete {record_count} records")
                else:
                    delete_cursor = self.connection.cursor()
                    delete_cursor.execute(f"DELETE FROM {table}")
                    delete_cursor.close()
                    print(f"  {table}: deleted {record_count} records")
                    total_deleted += record_count

            # Process special tables with recreation (order matters: Credentials -> BenutzerAllgemein -> Benutzer)
            for table in ["Credentials", "BenutzerAllgemein", "Benutzer"]:
                cursor.execute(f"SHOW TABLES LIKE '{table}'")
                if not cursor.fetchone():
                    print(f"  Skipping {table}: table not found")
                    continue

                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                result = cursor.fetchone()
                record_count = result.get("count", 0) if result else 0

                if dry_run:
                    if record_count > 0:
                        print(f"  {table}: would delete {record_count} records and recreate admin entry")
                    else:
                        print(f"  {table}: would recreate admin entry (no existing records)")
                else:
                    delete_cursor = self.connection.cursor()
                    if record_count > 0:
                        delete_cursor.execute(f"DELETE FROM {table}")
                        print(f"  {table}: deleted {record_count} records")
                        total_deleted += record_count
                    # Recreate admin entry
                    delete_cursor.execute(special_tables[table])
                    delete_cursor.close()
                    print(f"  {table}: recreated admin entry")

            if not dry_run and total_deleted > 0:
                self.connection.commit()
                print(f"\nSuccessfully deleted {total_deleted} records across general admin tables")
            elif dry_run:
                print("\nDry run complete for general admin tables cleanup")

            return total_deleted

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
                # EigeneSchule operations
                db_anonymizer.anonymize_eigene_schule(dry_run=args.dry_run)
                db_anonymizer.anonymize_eigene_schule_email(dry_run=args.dry_run)
                db_anonymizer.anonymize_eigene_schule_teilstandorte(dry_run=args.dry_run)
                db_anonymizer.anonymize_eigene_schule_abteilungen(dry_run=args.dry_run)
                db_anonymizer.anonymize_eigene_schule_logo(dry_run=args.dry_run)
                db_anonymizer.delete_eigene_schule_texte(dry_run=args.dry_run)
                db_anonymizer.anonymize_benutzergruppen(dry_run=args.dry_run)
                db_anonymizer.anonymize_k_telefonart(dry_run=args.dry_run)
                db_anonymizer.anonymize_k_kindergarten(dry_run=args.dry_run)
                db_anonymizer.anonymize_k_datenschutz(dry_run=args.dry_run)
                db_anonymizer.anonymize_k_erzieherart(dry_run=args.dry_run)
                db_anonymizer.anonymize_k_entlassgrund(dry_run=args.dry_run)
                db_anonymizer.anonymize_personengruppen(dry_run=args.dry_run)
                db_anonymizer.reset_schule_credentials(dry_run=args.dry_run)
                db_anonymizer.delete_and_reload_k_schule(dry_run=args.dry_run)
                
                # K_Lehrer (teacher) operations
                db_anonymizer.anonymize_k_lehrer(dry_run=args.dry_run)
                db_anonymizer.anonymize_credentials_lernplattformen(dry_run=args.dry_run)
                db_anonymizer.anonymize_lehrer_abschnittsdaten(dry_run=args.dry_run)
                db_anonymizer.delete_lehrer_fotos(dry_run=args.dry_run)
                
                # Lernplattformen operations
                db_anonymizer.anonymize_lernplattformen(dry_run=args.dry_run)
                
                # Schueler (student) operations
                db_anonymizer.anonymize_schueler(dry_run=args.dry_run)
                db_anonymizer.anonymize_credentials_lernplattformen_schueler(dry_run=args.dry_run)
                db_anonymizer.update_schueler_erzadr_names(dry_run=args.dry_run)
                db_anonymizer.update_schueler_erzadr_vornamen(dry_run=args.dry_run)
                db_anonymizer.update_schueler_erzadr_address(dry_run=args.dry_run)
                db_anonymizer.update_schueler_erzadr_email(dry_run=args.dry_run)
                db_anonymizer.clear_schueler_erzadr_misc(dry_run=args.dry_run)
                db_anonymizer.clear_schueler_erzadr_bemerkungen(dry_run=args.dry_run)
                db_anonymizer.delete_schueler_vermerke(dry_run=args.dry_run)
                db_anonymizer.anonymize_schueler_telefone(dry_run=args.dry_run)
                db_anonymizer.clear_schueler_ld_psfachbem(dry_run=args.dry_run)
                db_anonymizer.clear_schueler_leistungsdaten(dry_run=args.dry_run)
                db_anonymizer.clear_schueler_transport_fields(dry_run=args.dry_run)
                db_anonymizer.set_schueler_modifiziert_von_admin(dry_run=args.dry_run)
                db_anonymizer.clear_schueler_dokumentenverzeichnis(dry_run=args.dry_run)
                db_anonymizer.clear_schueler_gsdaten(dry_run=args.dry_run)
                db_anonymizer.clear_schueler_kaoa_daten(dry_run=args.dry_run)
                db_anonymizer.clear_schueler_lernabschnittsdaten(dry_run=args.dry_run)
                db_anonymizer.delete_personengruppen_personen(dry_run=args.dry_run)
                db_anonymizer.delete_schueler_fotos(dry_run=args.dry_run)
                db_anonymizer.delete_schueler_foerderempfehlungen(dry_run=args.dry_run)
                db_anonymizer.update_schueler_lsschulnummer(dry_run=args.dry_run)
                db_anonymizer.update_schueler_allgadr_ausbilder(dry_run=args.dry_run)
                db_anonymizer.update_schueler_bk_abschluss_thema(dry_run=args.dry_run)
                db_anonymizer.update_schueler_einzelleistungen_bemerkungen(dry_run=args.dry_run)
                
                # K_AllgAdresse operations
                db_anonymizer.anonymize_k_allg_adresse(dry_run=args.dry_run)
                
                # AllgAdrAnsprechpartner operations
                db_anonymizer.anonymize_allg_adr_ansprechpartner(dry_run=args.dry_run)

                # SchuelerListe operations
                db_anonymizer.update_schueler_liste_erzeuger(dry_run=args.dry_run)

                # General admin tables cleanup
                db_anonymizer.delete_general_admin_tables(dry_run=args.dry_run)
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
