---
layout: default
title: Konfiguration
---

# Konfiguration

## Konfigurationsdatei erstellen

```bash
cp config.example.json config.json
```

Unter Windows verwenden Sie:

```powershell
Copy-Item config.example.json config.json
```

## Datenbankverbindung

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

Sind `database`, `username` oder `password` nicht gesetzt, fragt das Programm die fehlenden Werte beim Start ab. Mit `--config` kann eine alternative Datei verwendet werden:

```bash
python svws_anonym.py --config /pfad/zur/config.json --dry-run
```
