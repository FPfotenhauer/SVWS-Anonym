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
- Keine zusätzlichen Dependencies erforderlich (nur Standard-Bibliothek)

*Python 3.6 or higher required. No additional dependencies needed (only standard library).*

## Installation

1. Repository klonen:
```bash
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
```

2. Das Skript ausführbar machen (optional):
```bash
chmod +x svws_anonym.py
```

## Verwendung (Usage)

### Basis-Verwendung (Basic Usage)

```bash
python svws_anonym.py
```

Dies lädt die Namenslisten und zeigt Beispiel-Anonymisierungen an.

*This loads the name lists and shows example anonymizations.*

### Programmatische Verwendung (Programmatic Usage)

```python
from svws_anonym import NameAnonymizer

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
