---
layout: default
title: Fehlerbehebung
permalink: /fehlerbehebung/
---

# Fehlerbehebung

## Python oder Abhängigkeiten fehlen

Prüfen Sie Python mit `python3 --version` beziehungsweise unter Windows mit `python --version`. Installieren Sie fehlende Pakete mit:

```bash
python -m pip install mysql-connector-python cryptography
```

## Verbindung zur Datenbank schlägt fehl

Prüfen Sie Hostname, Port, Datenbankname, Benutzername und Passwort in `config.json`. Kontrollieren Sie außerdem Netzwerkzugriff, Firewall, den laufenden MariaDB-Dienst und die Rechte des Datenbankbenutzers.

## Tabelle oder Spalte fehlt

Das Tool kann Tabellen überspringen, die in der verwendeten SVWS-Version nicht vorhanden sind. Bei strukturell inkompatiblen Tabellen verwenden Sie eine zur Tool-Version passende SVWS-Datenbank.

## Lauf nach einem Fehler wiederholen

Stellen Sie die Datenbank aus dem Backup wieder her. Beheben Sie die Ursache und führen Sie anschließend erneut den Dry-Run aus.
