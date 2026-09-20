---
layout: default
title: Bedienung
---

# Bedienung

## Hilfe anzeigen

```bash
python svws_anonym.py --help
```

## Dry-Run ausführen

```bash
python svws_anonym.py --dry-run
```

Der Dry-Run zeigt geplante Anonymisierungen an, schreibt aber keine Änderungen.

## Anonymisierung starten

Erstellen und prüfen Sie zuerst ein Backup. Starten Sie danach:

```bash
python svws_anonym.py --anonymize
```

Mit einer alternativen Konfiguration:

```bash
python svws_anonym.py --config /pfad/zur/config.json --anonymize
```

Prüfen Sie anschließend die Ausgabe und testen Sie die anonymisierte Datenbank in einer Testumgebung.
