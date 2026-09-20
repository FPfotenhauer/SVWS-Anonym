---
layout: default
title: Fehlerbehebung
---

# Fehlerbehebung

## Python wird nicht gefunden

Prüfen Sie die Installation:

```bash
python3 --version
```

Unter Windows:

```powershell
python --version
```

Falls der Befehl fehlt, installieren Sie Python erneut und aktivieren Sie unter Windows **Add Python to PATH**.

## Modul `mysql.connector` fehlt

Installieren Sie die Abhängigkeit in der aktiven Python-Umgebung:

```bash
python -m pip install mysql-connector-python
```

## Modul `cryptography` fehlt

```bash
python -m pip install cryptography
```

## Verbindung zur Datenbank schlägt fehl

Prüfen Sie:

- Hostname und Port in `config.json`
- Datenbankname, Benutzername und Passwort
- Netzwerkzugriff und Firewall
- ob MariaDB läuft und Verbindungen von Ihrem Rechner akzeptiert
- ob der Datenbankbenutzer die nötigen Rechte besitzt

## Tabelle oder Spalte nicht gefunden

Das Tool kann Tabellen überspringen, die in der verwendeten SVWS-Version nicht vorhanden sind. Prüfen Sie die Konsolenausgabe. Bei strukturell inkompatiblen Tabellen verwenden Sie eine zur Tool-Version passende SVWS-Datenbank.

## Lauf nach einem Fehler wiederholen

Stellen Sie die Datenbank aus dem Backup wieder her. Führen Sie anschließend zuerst erneut den Dry-Run aus. Starten Sie die echte Anonymisierung erst, wenn die Ursache behoben und die Vorschau plausibel ist.
