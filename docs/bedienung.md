---
layout: default
title: Bedienung
---

# Bedienung

## Hilfe anzeigen

```bash
python svws_anonym.py --help
```

Die Hilfe zeigt alle verfügbaren Kommandozeilenoptionen.

## Dry-Run ausführen

Starten Sie zunächst immer eine Vorschau:

```bash
python svws_anonym.py --dry-run
```

Der Dry-Run verbindet sich mit der Datenbank und zeigt geplante Anonymisierungen an, schreibt aber keine Änderungen. Prüfen Sie insbesondere die Verbindung, die Anzahl der gefundenen Datensätze und die erzeugten Beispielwerte.

Mit einer alternativen Konfiguration:

```bash
python svws_anonym.py --config /pfad/zur/config.json --dry-run
```

## Anonymisierung starten

Erstellen und prüfen Sie zuerst ein Backup. Starten Sie danach:

```bash
python svws_anonym.py --anonymize
```

Das Programm arbeitet die unterstützten Tabellen nacheinander ab. Fehlende Tabellen werden in der Regel übersprungen und im Protokoll ausgegeben.

Mit einer alternativen Konfiguration:

```bash
python svws_anonym.py --config /pfad/zur/config.json --anonymize
```

## Ablauf nach der Anonymisierung

1. Prüfen Sie die Ausgabe auf Fehlermeldungen.
2. Kontrollieren Sie die anonymisierten Daten in einer Testumgebung.
3. Testen Sie die Anmeldung und die für Ihre Umgebung wichtigen SVWS-Funktionen.
4. Verwenden Sie die Datenbank erst danach für Entwicklung, Support oder Tests.

## Abbrechen und Fehler

Bei einem Fehler beendet sich der betroffene Verarbeitungsschritt mit einer Fehlermeldung. Sichern Sie die Ausgabe, beheben Sie zunächst die Ursache und stellen Sie für einen erneuten Versuch die Datenbank aus dem Backup wieder her.
