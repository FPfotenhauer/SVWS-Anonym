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

GitHub Pages verwendet `docs/Index.md` als Startseite. Falls der Hoster eine kleingeschriebene Startdatei erwartet, kann zusätzlich eine Kopie als `docs/index.md` angelegt werden.
