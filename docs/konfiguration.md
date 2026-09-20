---
layout: default
title: Konfiguration
---

# Konfiguration

## Konfigurationsdatei erstellen

Kopieren Sie die Vorlage:

```bash
cp config.example.json config.json
```

Unter Windows verwenden Sie in PowerShell:

```powershell
Copy-Item config.example.json config.json
```

## Datenbankverbindung

Die Datei enthält die Verbindungsdaten für MariaDB:

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

Sind `database`, `username` oder `password` nicht gesetzt, fragt das Programm die fehlenden Werte beim Start ab. Für automatisierte Abläufe können die Werte direkt eingetragen werden. Schützen Sie die Datei dann besonders sorgfältig und speichern Sie sie nicht in Git.

## Alternative Konfigurationsdatei

Mit `--config` kann eine andere Datei verwendet werden:

```bash
python svws_anonym.py --config /pfad/zur/config.json --dry-run
```
