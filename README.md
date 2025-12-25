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

Verbindet sich mit der Datenbank und anonymisiert alle Namen in beiden Tabellen (`K_Lehrer` und `Schueler`):

**K_Lehrer Tabelle:**
- `Vorname` wird durch einen zufälligen Vornamen ersetzt (geschlechtsspezifisch basierend auf dem `Geschlecht` Feld)
- `Nachname` wird durch einen zufälligen Nachnamen ersetzt

**Schueler Tabelle:**
- `Vorname` wird durch einen zufälligen Vornamen ersetzt (geschlechtsspezifisch)
- `Name` wird durch einen zufälligen Nachnamen ersetzt
- `Zusatz` (zusätzliche Vornamen) wird mit zufälligen Namen des gleichen Geschlechts ersetzt, wobei der neue `Vorname` enthalten sein muss
- `Geburtsname` wird durch einen zufälligen Nachnamen ersetzt (nur wenn nicht NULL)

- Geschlecht-Werte: 3 = männlich, 4 = weiblich, 5/6 = neutral (zufälliges Geschlecht)

Das Programm fragt nach Datenbankname, Benutzername und Passwort für die Datenbankverbindung.

*Connects to the database and anonymizes all names in both tables (`K_Lehrer` and `Schueler`). The program prompts for database name, username and password.*

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

- `nachnamen.json`: 1000+ deutsche Nachnamen
- `vornamen_m.json`: 500+ männliche Vornamen
- `vornamen_w.json`: 500+ weibliche Vornamen

## Lizenz (License)

Siehe [LICENSE](LICENSE) Datei für Details.

## Mitwirken (Contributing)

Beiträge sind willkommen! Bitte öffnen Sie ein Issue oder einen Pull Request.

*Contributions are welcome! Please open an issue or pull request.*

## Credits

Namenslisten von / Name lists from: [JSON-Namen Repository](https://github.com/FPfotenhauer/JSON-Namen)
