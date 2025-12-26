# SVWS-Anonym

Anonymisierungstool für SVWS Datenbanken (Anonymization tool for SVWS databases)

## Überblick (Overview)

SVWS-Anonym ist ein Tool zur Anonymisierung personenbezogener Daten in SVWS-Datenbankexporten. Es ersetzt echte Namen durch zufällig generierte deutsche Namen aus dem [JSON-Namen Repository](https://github.com/FPfotenhauer/JSON-Namen).

*SVWS-Anonym is a tool for anonymizing personal data in SVWS database exports. It replaces real names with randomly generated German names from the [JSON-Namen repository](https://github.com/FPfotenhauer/JSON-Namen).*

## Features

- Anonymisierung von Vornamen (First name anonymization)
- Anonymisierung von Nachnamen (Last name anonymization)
- Geschlechtsspezifische Vornamen (Gender-specific first names)
- Konsistente Zuordnung (Consistent mapping across multiple occurrences)
- Verwendung authentischer deutscher Namen (Uses authentic German names)
- Randomisierung von Geburtsdaten (Tag bleibt im gleichen Monat/Jahr)
- Generierung von IdentNr1 aus Geburtsdatum und Geschlecht
- E-Mail- und Telefonnummer-Generierung
- Adressdaten-Integration aus CSV-Dateien
- Schulinformations-Anonymisierung mit spezifischen Werten
- Teilstandort-Anonymisierung (setzt einen Hauptstandort-Eintrag)
- SMTP-Konfigurations-Anonymisierung
- Logo-Ersetzung aus PNG-Datei mit Base64-Kodierung
- EigeneSchule_Texte-Löschung (vollständige Bereinigung)
- Lernplattform-Anmeldedaten-Anonymisierung (Lehrer und Schüler)
- Lehrerabschnittsdaten-Anonymisierung
- Schülervermerke-Löschung (vollständige Bereinigung)
- SchuelerErzAdr-Anonymisierung (Eltern-/Erzieherdaten)
- SchuelerTelefone-Anonymisierung (Schülertelefonummern)
- SchuelerLD_PSFachBem-Anonymisierung (Fachbereichsbemerkungen clearing)
- SchuelerLeistungsdaten-Anonymisierung (Lernentwicklung clearing)
- Schueler-Transportfelder-Bereinigung (setzt Idext/Fahrschueler_ID/Haltestelle_ID auf NULL)
- Schueler-Änderungsmarker (setzt ModifiziertVon auf "Admin")
- Personengruppen_Personen-Löschung (vollständige Bereinigung)
- K_AllgAdresse-Anonymisierung (allgemeine Adressen mit Namen, Adressen, Kontaktdaten)
- SchuelerFotos-Löschung (vollständige Bereinigung)
- LehrerFotos-Löschung (vollständige Bereinigung)

*Features include: name anonymization, gender-specific first names, consistent mapping, authentic German names, birthdate randomization, IdentNr1 generation, email/phone generation, CSV address integration, school information anonymization, SMTP configuration, logo replacement from PNG files, learning platform credentials for teachers and students, teacher section data anonymization, complete deletion of student notes, parent/guardian data anonymization, and general address anonymization with names, addresses, and contact information.*

## Voraussetzungen (Requirements)

- Python 3.6 oder höher
- MariaDB Server (für Datenbankverbindung)
- `mysql-connector-python` (für Datenbankoperationen): `pip install mysql-connector-python`

*Python 3.6 or higher required. MariaDB server (for database connection). mysql-connector-python for database operations: `pip install mysql-connector-python`*

## Installation

1. Repository klonen:
```bash
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
```

2. Python-Abhängigkeiten installieren:
```bash
pip install mysql-connector-python
```

3. Konfigurationsdatei erstellen:
```bash
cp config.example.json config.json
```

4. `config.json` bearbeiten und die Datenbankverbindungsparameter anpassen:
```json
{
  "database": {
    "host": "localhost",
    "port": 3306,
    "charset": "utf8mb4"
  }
}
```

**Hinweis:** Datenbankname, Benutzername und Passwort werden beim Programmstart abgefragt und nicht in der Konfigurationsdatei gespeichert.

*Note: Database name, username and password are prompted at program startup and not stored in the configuration file.*

4. Das Skript ausführbar machen (optional):
```bash
chmod +x svws_anonym.py
```

## Verwendung (Usage)

### Basis-Verwendung (Basic Usage)

```bash
python svws_anonym.py
```

Zeigt Beispiel-Anonymisierungen an, ohne die Datenbank zu ändern.

*Shows example anonymizations without modifying the database.*

### Datenbank anonymisieren (Anonymize Database)

```bash
python svws_anonym.py --anonymize
```

Verbindet sich mit der Datenbank und anonymisiert folgende Tabellen:

**EigeneSchule Tabelle:**
- `SchulNr` wird auf "123456" gesetzt
- `Bezeichnung1` wird auf "Städtische Schule" gesetzt
- `Bezeichnung2` wird auf "am Stadtgarten" gesetzt
- `Bezeichnung3` wird auf "Ganztagsschule des Landes NRW" gesetzt
- Adresse: Hauptstrasse 56, 42107 Wuppertal
- Kontaktdaten: Telefon, Fax, E-Mail, Webseite werden anonymisiert

**EigeneSchule_Email Tabelle:**
- SMTP-Konfiguration wird standardisiert (Port 25, StartTLS aktiviert, TLS deaktiviert)
- `Domain` und `SMTPServer` werden auf NULL/leer gesetzt

**EigeneSchule_Teilstandorte Tabelle:**
- Alle vorhandenen Einträge werden gelöscht und ein Eintrag wird gesetzt mit: `AdrMerkmal=A`, `PLZ=42103`, `Ort=Wuppertal`, `Strassenname=Hauptstrasse`, `HausNr=56`, `HausNrZusatz=NULL`, `Bemerkung=Hauptstandort`, `Kuerzel=WtalA`

**EigeneSchule_Logo Tabelle:**
- Logo wird durch ein standardisiertes Base64-kodiertes Bild ersetzt

**EigeneSchule_Texte Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**K_Lehrer Tabelle:**
- `Vorname` wird durch einen zufälligen Vornamen ersetzt (geschlechtsspezifisch basierend auf dem `Geschlecht` Feld)
- `Nachname` wird durch einen zufälligen Nachnamen ersetzt
- `Kuerzel` wird aus den ersten Buchstaben von Vorname und Nachname generiert
- `Email` und `EmailDienstlich` werden mit neuen Namen generiert (@schule.nrw.de)
- `Tel` und `Handy` werden mit zufälligen Telefonnummern ersetzt
- `Geburtsdatum` wird randomisiert (Tag wird zufällig geändert, Monat und Jahr bleiben erhalten)
- `IdentNr1` wird aus Geburtsdatum (TTMMJJ) und Geschlecht generiert (z.B. "1008703")
- `LIDKrz` wird als eindeutiges, maximal 4-stelliges Kürzel generiert (Duplikate werden vermieden)
- Adressdaten (`Ort_ID`, `Strassenname`, `HausNr`, `HausNrZusatz`) werden aus CSV-Daten zugewiesen

**CredentialsLernplattformen Tabelle (Lehrer):**
- `Benutzername` wird auf Format "Vorname.Nachname" gesetzt (basierend auf K_Lehrer Namen via LehrerLernplattform)
 - Duplikate werden mit numerischen Suffixen behandelt (Name, Name1, Name2, etc.)

**CredentialsLernplattformen Tabelle (Schüler):**
- `Benutzername` wird auf Format "Vorname.Name" gesetzt (basierend auf Schueler Namen via SchuelerLernplattform)
- Duplikate werden mit numerischen Suffixen behandelt (Name, Name1, Name2, etc.)

**LehrerAbschnittsdaten Tabelle:**
- `StammschulNr` wird auf "123456" gesetzt

**LehrerFotos Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**SchuelerVermerke Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**SchuelerErzAdr Tabelle:**
- `Name1` und `Name2` werden – sofern nicht NULL – auf den Wert aus `Schueler.Name` gesetzt
- `Vorname1` und `Vorname2` werden geschlechtsabhängig aus den Namenslisten gesetzt (`Herr` → männlich, `Frau` → weiblich); bei `ErzieherArt_ID` ∈ {3, 4} wird `Schueler.Vorname` übernommen
- Adressen: `ErzOrt_ID` ← `Schueler.Ort_ID`, `ErzOrtsteil_ID` ← NULL, `ErzStrassenname` ← "Teststrasse" (wenn vorher nicht NULL), `ErzHausNr` ← Zufallszahl 1–100 (wenn vorher nicht NULL)
- `ErzEmail` ← `Name1@e.example.com` (wenn `Name1` nicht NULL, sonst NULL)
- Bereinigung: `ErzEmail2`, `Erz1StaatKrz`, `Erz2StaatKrz`, `ErzAdrZusatz` ← NULL
- `Bemerkungen` ← NULL

**Schueler Tabelle:**
- `Vorname` wird durch einen zufälligen Vornamen ersetzt (geschlechtsspezifisch)
- `Name` wird durch einen zufälligen Nachnamen ersetzt
- `Geburtsdatum` wird randomisiert (Tag wird zufällig geändert, Monat und Jahr bleiben erhalten)
- `Geburtsort` wird auf "Testort" gesetzt (wenn nicht NULL, sonst NULL)
- Adressdaten (`Ort_ID`, `Strassenname`, `HausNr`) werden aus CSV-Daten zugewiesen
 - Transportfelder: `Idext` ← NULL, `Fahrschueler_ID` ← NULL, `Haltestelle_ID` ← NULL
 - Änderungsmarker: `ModifiziertVon` ← "Admin"

**K_AllgAdresse Tabelle:**
- `AllgAdrName1` wird auf zwei zufällige Nachnamen kombiniert wie "Name1 und Name2" gesetzt
- `AllgAdrName2`, `AllgAdrHausNrZusatz`, `AllgOrtsteil_ID` ← NULL
- `AllgAdrStrassenname` wird auf einen zufälligen Straßennamen aus CSV-Daten gesetzt
- `AllgAdrHausNr` wird auf eine Zufallszahl zwischen 1 und 100 gesetzt
- `AllgAdrOrt_ID` wird auf eine zufällig existierende Ort-ID aus K_Ort gesetzt
- `AllgAdrTelefon1` wird auf "01234-" + 6 zufällige Ziffern gesetzt (z.B. "01234-567890")
- `AllgAdrTelefon2`, `AllgAdrFax` ← NULL
- `AllgAdrEmail` wird auf `AllgAdrName1` ohne Leerzeichen + "@betrieb.example.com" gesetzt (z.B. "MülleundSchmidt@betrieb.example.com")
- `AllgAdrBemerkungen`, `AllgAdrZusatz1`, `AllgAdrZusatz2` ← NULL

**AllgAdrAnsprechpartner Tabelle:**
- `Name` wird durch einen zufälligen Nachnamen ersetzt
- `Vorname` wird durch einen zufälligen Vornamen ersetzt
- `Email` wird auf `Name@betrieb.example.com` gesetzt (z.B. "Mueller@betrieb.example.com")
- `Titel` ← NULL
- `Telefon` wird auf "01234-" + 6 zufällige Ziffern gesetzt (z.B. "01234-123456")

**SchuelerTelefone Tabelle:**
- `Telefonnummer` wird auf "012345-" + 6 zufällige Ziffern gesetzt (z.B. "012345-123456")
- `Bemerkung` ← NULL

**SchuelerLD_PSFachBem Tabelle:**
- `ASV` ← NULL
- `LELS` ← NULL
- `AUE` ← NULL
- `ESF` ← NULL
- `BemerkungFSP` ← NULL
- `BemerkungVersetzung` ← NULL

**SchuelerLeistungsdaten Tabelle:**
- `Lernentw` ← NULL

**SchuelerFotos Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**Personengruppen_Personen Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

```
- `Vorname` wird durch einen zufälligen Vornamen ersetzt (geschlechtsspezifisch)
- `Name` wird durch einen zufälligen Nachnamen ersetzt
- `Zusatz` (zusätzliche Vornamen) wird mit zufälligen Namen des gleichen Geschlechts ersetzt, wobei der neue `Vorname` enthalten sein muss
- `Geburtsname` wird durch einen zufälligen Nachnamen ersetzt (nur wenn nicht NULL)
- `Email` und weitere Kontaktdaten werden anonymisiert
- Adressdaten werden ähnlich wie bei K_Lehrer behandelt



**Geschlecht-Werte:** 3 = männlich, 4 = weiblich, 5/6 = neutral (zufälliges Geschlecht)

Das Programm fragt nach Datenbankname, Benutzername und Passwort für die Datenbankverbindung.

*Connects to the database and anonymizes the following tables: EigeneSchule (school information with standardized values), EigeneSchule_Email (SMTP configuration), EigeneSchule_Logo (base64 logo from PNG file), K_Lehrer (teachers with comprehensive field anonymization including names, emails, phones, birthdate randomization, IdentNr1 generation, addresses from CSV, and 4-character unique `LIDKrz`), CredentialsLernplattformen (username format for teachers and students with duplicate handling), LehrerAbschnittsdaten (StammschulNr), Schueler (students with similar comprehensive anonymization), SchuelerErzAdr (names, first names, address normalization, email and misc clears), and SchuelerVermerke (complete deletion). The program prompts for database name, username and password.*

### Dry-Run Modus (Dry-Run Mode)

```bash
python svws_anonym.py --dry-run
```

Zeigt an, welche Änderungen vorgenommen würden, ohne die Datenbank tatsächlich zu ändern.

*Shows what changes would be made without actually modifying the database.*

### Mit benutzerdefinierter Konfiguration (With custom configuration)

```bash
python svws_anonym.py --config /path/to/config.json --anonymize
```

### Programmatische Verwendung (Programmatic Usage)

```python
from svws_anonym import NameAnonymizer, DatabaseConfig

# Datenbankkonfiguration laden
db_config = DatabaseConfig()  # Lädt config.json aus dem aktuellen Verzeichnis
# oder mit benutzerdefiniertem Pfad:
# db_config = DatabaseConfig("/path/to/config.json")

# Verbindungsparameter abrufen
conn_params = db_config.get_connection_params()
print(f"Verbinde mit Datenbank: {db_config.database}")

# Anonymizer initialisieren
anonymizer = NameAnonymizer()

# Vornamen anonymisieren
new_firstname_m = anonymizer.anonymize_firstname("Max", gender='m')
new_firstname_w = anonymizer.anonymize_firstname("Erika", gender='w')

# Nachnamen anonymisieren
new_lastname = anonymizer.anonymize_lastname("Mustermann")

# Vollständige Namen anonymisieren
firstname, lastname = anonymizer.anonymize_fullname("Max", "Mustermann", gender='m')
```

## Konfigurationsdatei (Configuration File)

Die `config.json` enthält die Datenbankverbindungsparameter für den MariaDB-Server:

- `host`: Hostname oder IP-Adresse des Datenbankservers (Standard: localhost)
- `port`: Port des Datenbankservers (Standard: 3306)
- `charset`: Zeichensatz (Standard: utf8mb4)

**Datenbankname, Benutzername und Passwort** werden beim Programmstart interaktiv abgefragt und nicht in der Konfigurationsdatei gespeichert. Dies erhöht die Sicherheit, da keine Zugangsdaten im Klartext gespeichert werden.

**Wichtig:** Die `config.json` wird nicht ins Git-Repository eingecheckt. Verwenden Sie `config.example.json` als Vorlage.

*The `config.json` file contains database connection parameters for the MariaDB server. Database name, username and password are prompted interactively at program startup and not stored in the configuration file. This improves security by not storing credentials in plain text. Important: `config.json` is not checked into the git repository. Use `config.example.json` as a template.*

## Namenslisten (Name Lists)

Das Tool verwendet die folgenden JSON-Dateien aus dem [JSON-Namen Repository](https://github.com/FPfotenhauer/JSON-Namen):

- `nachnamen.json`: 2000 deutsche Nachnamen
- `vornamen_m.json`: 500 männliche Vornamen
- `vornamen_w.json`: 500 weibliche Vornamen

Zusätzlich wird eine CSV-Datei für Adressdaten verwendet:
- `strassen.csv`: Straßendaten mit Ort_ID, PLZ, Ort und Straßenname für realistische Adresszuordnung

*The tool uses the following JSON files from the JSON-Namen repository: nachnamen.json (2000 German last names), vornamen_m.json (500 male first names), vornamen_w.json (500 female first names). Additionally, a CSV file is used for address data: strassen.csv with location IDs, postal codes, cities and street names for realistic address assignment.*

## Lizenz (License)

Siehe [LICENSE](LICENSE) Datei für Details.

## Mitwirken (Contributing)

Beiträge sind willkommen! Bitte öffnen Sie ein Issue oder einen Pull Request.

*Contributions are welcome! Please open an issue or pull request.*

## Credits

Namenslisten von / Name lists from: [JSON-Namen Repository](https://github.com/FPfotenhauer/JSON-Namen)
