---
layout: default
title: Anonymisierte Daten
---

# Anonymisierte Daten

Das Programm anonymisiert unter anderem folgende Bereiche:

## Personen und Kontaktdaten

- Vor- und Nachnamen von Schülerinnen, Schülern und Lehrkräften
- geschlechtsspezifische Vornamen
- Geburtsdaten durch Änderung des Tages innerhalb von Monat und Jahr
- E-Mail-Adressen, Telefon- und Faxnummern
- Adressen und Ortszuordnungen aus den mitgelieferten CSV-Daten
- Eltern-, Erzieher- und Ansprechpartnerdaten

## Schul- und Katalogdaten

- Schuldaten, Teilstandort und Abteilungen
- Benutzergruppen und verschiedene Katalogbezeichnungen
- Lernplattformen und Zugangsdaten
- Schul-Credentials mit neuen RSA- und AES-Schlüsseln

## Gelöschte oder geleerte Daten

Je nach vorhandener Datenbank werden unter anderem Fotos, Schülervermerke, Förderempfehlungen, administrative Konfigurationen und weitere sensible Freitextfelder gelöscht oder geleert.

## Geschützte Werte

Einige Standardbezeichnungen bleiben erhalten, damit die anonymisierte Datenbank funktionsfähig bleibt. Dazu gehören beispielsweise `Administrator`, `Schulleitung`, `Lehrer`, `Sekretariat` und bestimmte Katalogwerte.

Die genaue Auswahl hängt von der implementierten Version und den vorhandenen Tabellen und Spalten ab. Die Konsolenausgabe des Programms ist für die Kontrolle des konkreten Laufs maßgeblich.
