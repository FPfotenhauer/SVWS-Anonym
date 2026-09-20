---
layout: default
title: SVWS-Anonym
---

# SVWS-Anonym

Anwenderdokumentation für SVWS-Anonym. Das Tool anonymisiert personenbezogene Daten in einer SVWS-MariaDB-Datenbank und ersetzt sie durch konsistente Testdaten.

> **Wichtig:** Die Anonymisierung verändert beziehungsweise löscht Daten dauerhaft. Arbeiten Sie ausschließlich mit einer Kopie der Datenbank und erstellen Sie vor jedem Lauf ein geprüftes Backup.

## Dokumentation

- [Installation unter Windows](installation-windows.html)
- [Installation unter macOS](installation-macOS.html)
- [Installation unter Linux](installation-linux.html)
- [Konfiguration](konfiguration.html)
- [Bedienung](bedienung.html)
- [Anonymisierte Daten](anonymisierte-daten.html)
- [Fehlerbehebung](fehlerbehebung.html)

## Schnellstart

```text
1. Python und die Abhängigkeiten installieren.
2. config.example.json nach config.json kopieren.
3. Datenbankverbindung in config.json eintragen.
4. Mit --dry-run prüfen.
5. Backup erstellen und mit --anonymize ausführen.
```

## Voraussetzungen

- Python 3.6 oder höher
- Zugriff auf eine MariaDB-Datenbank mit einem Benutzer, der die betroffenen Tabellen lesen und ändern darf
- Die Dateien `nachnamen.json`, `vornamen_m.json`, `vornamen_w.json`, `Strassen.csv` und `K_Schule.csv` im Projektverzeichnis

Die Namenslisten stammen aus dem [JSON-Namen Repository](https://github.com/FPfotenhauer/JSON-Namen).

## GitHub Pages aktivieren

1. Öffnen Sie im GitHub-Repository **Settings > Pages**.
2. Wählen Sie unter **Build and deployment** die Quelle **Deploy from a branch**.
3. Wählen Sie den Branch, meist `main`, und als Ordner `/docs`.
4. Speichern Sie die Einstellung.

## Installation

### Windows

Installieren Sie Python 3.6 oder höher von [python.org](https://www.python.org/downloads/windows/) und aktivieren Sie dabei **Add Python to PATH**.

```powershell
python --version
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
python -m pip install --upgrade pip
python -m pip install mysql-connector-python cryptography
Copy-Item config.example.json config.json
python svws_anonym.py --help
```

Das Repository kann alternativ als ZIP-Datei heruntergeladen und entpackt werden.

### macOS

Installieren Sie Python über [python.org](https://www.python.org/downloads/macos/) oder Homebrew:

```bash
brew install python
python3 --version
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install mysql-connector-python cryptography
cp config.example.json config.json
python svws_anonym.py --help
```

### Linux

Für Debian und Ubuntu:

```bash
sudo apt update
sudo apt install python3 python3-venv python3-pip git
python3 --version
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install mysql-connector-python cryptography
cp config.example.json config.json
chmod +x svws_anonym.py
python svws_anonym.py --help
```

## Konfiguration

Kopieren Sie zunächst `config.example.json` nach `config.json`.

```json
{
	"database": {
		"host": "localhost",
		"port": 3306,
		"database": null,
		"username": null,
		"password": null,
		"charset": "utf8mb4",
		"collation": "utf8mb4_unicode_ci"
	}
}
```

- `host`: Hostname oder IP-Adresse des MariaDB-Servers
- `port`: MariaDB-Port, normalerweise `3306`
- `database`: Name der SVWS-Datenbank
- `username`: Datenbankbenutzer
- `password`: Passwort des Datenbankbenutzers
- `charset`: Zeichensatz, standardmäßig `utf8mb4`
- `collation`: Sortierung, standardmäßig `utf8mb4_unicode_ci`

Sind `database`, `username` oder `password` nicht gesetzt, fragt das Programm die fehlenden Werte beim Start ab. Für automatisierte Abläufe können sie direkt eingetragen werden. Schützen Sie `config.json` dann besonders sorgfältig und speichern Sie sie nicht in Git.

Mit `--config` kann eine andere Konfigurationsdatei verwendet werden:

```bash
python svws_anonym.py --config /pfad/zur/config.json --dry-run
```

## Bedienung

### Vorschau ausführen

```bash
python svws_anonym.py --dry-run
```

Der Dry-Run zeigt geplante Anonymisierungen an, schreibt aber keine Änderungen.

### Anonymisierung starten

Erstellen und prüfen Sie zuerst ein Backup. Starten Sie danach:

```bash
python svws_anonym.py --anonymize
```

Mit einer alternativen Konfiguration:

```bash
python svws_anonym.py --config /pfad/zur/config.json --anonymize
```

Das Programm arbeitet die unterstützten Tabellen nacheinander ab. Fehlende Tabellen werden in der Regel übersprungen und im Protokoll ausgegeben.

### Nach dem Lauf

1. Prüfen Sie die Ausgabe auf Fehlermeldungen.
2. Kontrollieren Sie die anonymisierten Daten in einer Testumgebung.
3. Testen Sie Anmeldung und wichtige SVWS-Funktionen.
4. Verwenden Sie die Datenbank erst danach für Entwicklung, Support oder Tests.

## Anonymisierte Daten

Das Programm anonymisiert unter anderem Vor- und Nachnamen, geschlechtsspezifische Vornamen, Geburtsdaten, E-Mail-Adressen, Telefon- und Faxnummern, Adressen, Eltern- und Erzieherdaten, Schuldaten, Katalogbezeichnungen, Lernplattformen und Zugangsdaten.

Je nach vorhandener Datenbank werden unter anderem Fotos, Schülervermerke, Förderempfehlungen, administrative Konfigurationen und sensible Freitextfelder gelöscht oder geleert.

Einige Standardbezeichnungen bleiben erhalten, darunter `Administrator`, `Schulleitung`, `Lehrer` und `Sekretariat`.

## Fehlerbehebung

### Python oder Abhängigkeiten fehlen

Prüfen Sie Python mit `python3 --version` beziehungsweise unter Windows mit `python --version`. Installieren Sie fehlende Pakete mit:

```bash
python -m pip install mysql-connector-python cryptography
```

### Verbindung zur Datenbank schlägt fehl

Prüfen Sie Hostname, Port, Datenbankname, Benutzername und Passwort in `config.json`. Kontrollieren Sie außerdem Netzwerkzugriff, Firewall, den laufenden MariaDB-Dienst und die Rechte des Datenbankbenutzers.

### Tabelle oder Spalte fehlt

Das Tool kann Tabellen überspringen, die in der verwendeten SVWS-Version nicht vorhanden sind. Bei strukturell inkompatiblen Tabellen verwenden Sie eine zur Tool-Version passende SVWS-Datenbank.