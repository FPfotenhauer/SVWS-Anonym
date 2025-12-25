# SVWS-Anonym
Anonymisierungstool für SVWS Datenbanken

Ein Python-Tool zur Anonymisierung von MariaDB-Datenbanken aus dem [SVWS-Server-Projekt](https://github.com/SVWS-NRW/SVWS-Server). Dieses Tool ermöglicht es, bestehende Datenbanken so zu anonymisieren, dass sie weitergegeben oder für Testdaten und Schulungsdatenbanken verwendet werden können.

## Features

- 🔒 Anonymisierung personenbezogener Daten in MariaDB-Datenbanken
- 🎯 Interaktive Schema-Auswahl
- 🇩🇪 Generierung realistischer deutscher Fake-Daten
- ⚠️ Sicherheitsabfrage vor der Anonymisierung
- 📊 Detaillierte Fortschrittsanzeige
- 🔧 Automatische Erkennung zu anonymisierender Spalten

## Voraussetzungen

- Python 3.7 oder höher
- Zugriff auf eine MariaDB-Datenbank mit entsprechenden Berechtigungen
- Die Datenbank sollte vom SVWS-Server-Projekt stammen

## Installation

1. Repository klonen:
```bash
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
```

2. Abhängigkeiten installieren:
```bash
pip install -r requirements.txt
```

## Verwendung

### Interaktiver Modus (empfohlen)

Starten Sie das Tool einfach:

```bash
python svws_anonym.py
```

Das Tool wird Sie nach folgenden Informationen fragen:
1. **Host**: Datenbankserver-Adresse (Standard: localhost)
2. **Port**: Datenbankserver-Port (Standard: 3306)
3. **Benutzername**: Datenbankbenutzer mit Zugriff auf die Schemas
4. **Passwort**: Passwort für den Datenbankbenutzer
5. **Schema-Auswahl**: Wählen Sie das zu anonymisierende Schema

### Beispiel

```
============================================================
SVWS Datenbank Anonymisierungstool
============================================================

Datenbank-Verbindungsinformationen:
  Host [localhost]: localhost
  Port [3306]: 3306
  Benutzername: svws_user
  Passwort: 
✓ Verbindung zum Datenbankserver hergestellt: localhost:3306

Verfügbare Schemas:
  1. schule_musterschule
  2. schule_testschule
  3. schule_beispiel

Welches Schema soll anonymisiert werden? (Nummer oder Name): 1

⚠ WARNUNG: Das Schema 'schule_musterschule' wird anonymisiert!
  Alle personenbezogenen Daten werden durch Fake-Daten ersetzt.
  Dieser Vorgang kann NICHT rückgängig gemacht werden!

Fortfahren? (ja/nein): ja

=== Anonymisierung von Schema 'schule_musterschule' ===

Gefundene Tabellen: 42

Verarbeite Tabelle: Schueler
  → 150 Zeile(n) anonymisiert (5 Spalte(n))
Verarbeite Tabelle: Lehrer
  → 25 Zeile(n) anonymisiert (6 Spalte(n))
...

✓ Anonymisierung von Schema 'schule_musterschule' abgeschlossen!
✓ Datenbankverbindung geschlossen

============================================================
Anonymisierung abgeschlossen!
============================================================
```

## Anonymisierte Datentypen

Das Tool anonymisiert automatisch folgende Arten von Daten:

### Personennamen
- Nachnamen, Vornamen, Familiennamen, Rufnamen, Geburtsnamen

### Kontaktinformationen
- E-Mail-Adressen
- Telefonnummern (Festnetz und Mobil)

### Adressdaten
- Straßennamen
- Hausnummern
- Postleitzahlen
- Orte/Städte

### Persönliche Informationen
- Geburtsdaten
- Geburtsorte

### Weitere Daten
- Bemerkungen, Kommentare, Notizen

## Wichtige Hinweise

⚠️ **WARNUNG**: 
- Dieser Prozess ist **NICHT rückgängig zu machen**!
- Erstellen Sie **immer** ein Backup vor der Anonymisierung!
- Testen Sie das Tool zunächst auf einer **Kopie** Ihrer Datenbank!

### Backup erstellen

Vor der Anonymisierung sollten Sie ein Backup erstellen:

```bash
mysqldump -u username -p schema_name > backup_$(date +%Y%m%d_%H%M%S).sql
```

### Backup wiederherstellen

Falls nötig, können Sie das Backup wiederherstellen:

```bash
mysql -u username -p schema_name < backup_20231225_150000.sql
```

## Konfiguration

Optional können Sie eine Konfigurationsdatei erstellen:

```bash
cp config.example.ini config.ini
```

Passen Sie die Werte in `config.ini` an Ihre Umgebung an.

## Technische Details

### Architektur

Das Tool basiert auf Python und verwendet:
- **PyMySQL**: MariaDB/MySQL-Datenbankverbindung
- **Faker**: Generierung realistischer Fake-Daten (deutsche Lokalisierung)

### Anonymisierungsstrategie

1. **Verbindung**: Verbindung zum Datenbankserver herstellen
2. **Schema-Auswahl**: Benutzer wählt das zu anonymisierende Schema
3. **Tabellen-Analyse**: Alle Tabellen im Schema werden analysiert
4. **Spalten-Erkennung**: Spalten mit personenbezogenen Daten werden erkannt
5. **Daten-Anonymisierung**: Zeile für Zeile werden die Daten ersetzt
6. **Commit**: Änderungen werden gespeichert

### Spalten-Erkennung

Das Tool erkennt zu anonymisierende Spalten anhand von Namensmustern:
- Spalten mit "nachname", "vorname", "name" → Personennamen
- Spalten mit "email", "e_mail" → E-Mail-Adressen
- Spalten mit "telefon", "handy" → Telefonnummern
- Spalten mit "strasse", "plz", "ort" → Adressdaten
- usw.

## Lizenz

Siehe [LICENSE](LICENSE) Datei für Details.

## Mitwirken

Beiträge sind willkommen! Bitte erstellen Sie einen Pull Request oder öffnen Sie ein Issue für Vorschläge oder Fehlerberichte.

## Support

Bei Fragen oder Problemen:
1. Prüfen Sie die Dokumentation
2. Durchsuchen Sie vorhandene Issues
3. Erstellen Sie ein neues Issue mit detaillierten Informationen

## Haftungsausschluss

Dieses Tool wird "wie besehen" bereitgestellt. Die Autoren übernehmen keine Haftung für Datenverlust oder andere Schäden, die durch die Verwendung dieses Tools entstehen. Verwenden Sie es auf eigene Verantwortung und erstellen Sie immer Backups!
