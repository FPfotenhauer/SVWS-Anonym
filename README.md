# SVWS-Anonym

Anonymisierungstool für SVWS Datenbanken (Anonymization tool for SVWS databases)

## Überblick (Overview)

SVWS-Anonym ist ein Tool zur Anonymisierung personenbezogener Daten in SVWS-Datenbankexporten. Es ersetzt echte Namen durch zufällig generierte deutsche Namen aus dem [JSON-Namen Repository](https://github.com/FPfotenhauer/JSON-Namen).

*SVWS-Anonym is a tool for anonymizing personal data in SVWS database exports. It replaces real names with randomly generated German names from the [JSON-Namen repository](https://github.com/FPfotenhauer/JSON-Namen).*

## Aktuelle Updates (Recent Updates)

**Version mit erweiterten Anonymisierungsfunktionen:**
- ✅ Benutzergruppen-Anonymisierung mit geschützten Werten (Administrator, Schulleitung, Lehrer, Sekretariat)
- ✅ Telefonabschnitt-Anonymisierung in Schueler-Tabelle (Telefon und Fax Felder)
- ✅ SchuelerEinzelleistungen-Bemerkung-Anonymisierung
- ✅ Schueler_AllgAdr Ausbilder-Anonymisierung
- ✅ SchuelerBKAbschluss ThemaAbschlussarbeit-Anonymisierung
- ✅ SchuelerListe Erzeuger-Aktualisierung (auf ID=1 für Admin-Benutzer)

*Extended anonymization functions including Benutzergruppen with protected values, phone number anonymization in Schueler table, SchuelerEinzelleistungen remarks anonymization, and SchuelerListe creator field update.*

## Features

- Anonymisierung von Vornamen (First name anonymization)
- Anonymisierung von Nachnamen (Last name anonymization)
- Geschlechtsspezifische Vornamen (Gender-specific first names)
- Konsistente Zuordnung (Consistent mapping across multiple occurrences)
- Verwendung authentischer deutscher Namen (Uses authentic German names)
- Randomisierung von Geburtsdaten (Tag bleibt im gleichen Monat/Jahr)
- Generierung von IdentNr1 aus Geburtsdatum und Geschlecht
- E-Mail- und Telefonnummer-Generierung
- Adressdaten-Integration aus CSV-Dateien
- Schulinformations-Anonymisierung mit spezifischen Werten
- Teilstandort-Anonymisierung (setzt einen Hauptstandort-Eintrag)
- Abteilungs-Anonymisierung (EigeneSchule_Abteilungen Email/Durchwahl/Raum)
- SMTP-Konfigurations-Anonymisierung
- Logo-Ersetzung aus PNG-Datei mit Base64-Kodierung
- EigeneSchule_Texte-Löschung (vollständige Bereinigung)
- Benutzergruppen-Anonymisierung (Bezeichnung zu "Bezeichnung " + ID, schützt Standard-Werte)
- K_TelefonArt-Anonymisierung (Bezeichnung zu "Telefonart " + ID, schützt Standard-Werte)
- K_Kindergarten-Anonymisierung (Bezeichnung, PLZ/Ort aus K_Ort, Straßennamen, Kontaktfelder)
- K_Datenschutz-Anonymisierung (Bezeichnung zu "Bezeichnung " + ID, schützt "Verwendung Foto")
- Personengruppen-Anonymisierung (Gruppenname, Zusatzinfo, SammelEmail)
- SchuleCredentials-Reset (generiert neue RSA 2048-bit Schlüsselpaare und AES 256-bit Schlüssel)
- Lernplattformen-Anonymisierung (Bezeichnung und Konfiguration)
- Lernplattform-Anmeldedaten-Anonymisierung (Lehrer und Schüler mit Initialkennwort und Sicherheitsfeld-Bereinigung)
- Lehrerabschnittsdaten-Anonymisierung
- Schülervermerke-Löschung (vollständige Bereinigung)
- SchuelerErzAdr-Anonymisierung (Eltern-/Erzieherdaten)
- SchuelerTelefone-Anonymisierung (Schülertelefonummern)
- SchuelerLD_PSFachBem-Anonymisierung (Fachbereichsbemerkungen clearing)
- SchuelerLeistungsdaten-Anonymisierung (Lernentwicklung clearing)
- Schueler-Transportfelder-Bereinigung (setzt Idext/Fahrschueler_ID/Haltestelle_ID auf NULL)
- Schueler-Änderungsmarker (setzt ModifiziertVon auf "Admin")
- SchuelerGSDaten-Bereinigung (setzt Anrede_Klassenlehrer, Nachname_Klassenlehrer, GS_Klasse auf NULL)
- Schueler LSSchulNr-Aktualisierung (Bereich 100000-199999 mit SchulformKrz-Matching)
- Schueler LSSchulNr-Aktualisierung (Bereich 200000-299999 mit Bereichsfilterung)
- Schueler SchulwechselNr-Aktualisierung (random K_Schule.SchulNr Ersetzung)
- Schueler LSBemerkung-Bereinigung (setzt auf NULL)
- SchuelerAbgaenge-Löschung (vollständige Bereinigung)
- Personengruppen_Personen-Löschung (vollständige Bereinigung)
- K_AllgAdresse-Anonymisierung (allgemeine Adressen mit Namen, Adressen, Kontaktdaten)
- Schueler_AllgAdr Ausbilder-Anonymisierung (Ausbildername mit zufälligen Nachnamen ersetzen)
- SchuelerBKAbschluss ThemaAbschlussarbeit-Anonymisierung (Thema mit standardisiertem Text ersetzen)
- SchuelerEinzelleistungen Bemerkung-Anonymisierung (Bemerkung mit standardisiertem Text ersetzen)
- SchuelerFotos-Löschung (vollständige Bereinigung)
- SchuelerFoerderempfehlungen-Löschung (vollständige Bereinigung)
- LehrerFotos-Löschung (vollständige Bereinigung)
- K_Lehrer-SerNr-Anonymisierung (setzt "SerNr" auf ddddX)
- K_Lehrer-PANr-Anonymisierung (setzt "PANr" auf PA + 7 Ziffern)
- K_Lehrer-LBVNr-Anonymisierung (setzt "LBVNr" auf LB + 7 Ziffern)
- K_Lehrer-Titel-Bereinigung (setzt "Titel" auf NULL)
- Allgemeine Verwaltungs-Bereinigung (löscht Einträge aus: Schild_Verwaltung, Client_Konfiguration_Global, Client_Konfiguration_Benutzer, Wiedervorlage, ZuordnungReportvorlagen, BenutzerEmail, ImpExp_EigeneImporte, ImpExp_EigeneImporte_Felder, ImpExp_EigeneImporte_Tabellen, SchuleOAuthSecrets, Logins, TextExportVorlagen; setzt Admin-Benutzer in Benutzer, BenutzerAllgemein, Credentials zurück)

*Features include: name anonymization, gender-specific first names, consistent mapping, authentic German names, birthdate randomization, IdentNr1 generation, email/phone generation, CSV address integration, school information anonymization, SMTP configuration, logo replacement from PNG files, K_TelefonArt anonymization with protected standard values, K_Kindergarten anonymization with designation formatting and random addresses, learning platform credentials for teachers and students, teacher section data anonymization, teacher `SerNr` anonymization (sets to ddddX), teacher `PANr` anonymization (sets to PA + 7 digits), teacher `LBVNr` anonymization (sets to LB + 7 digits), complete deletion of student notes, parent/guardian data anonymization, SchuelerGSDaten field clearing (Anrede_Klassenlehrer, Nachname_Klassenlehrer, GS_Klasse), SchuelerFoerderempfehlungen deletion, general address anonymization with names, addresses, and contact information, and general administrative tables cleanup (deletes entries from Schil_Verwaltung, Client_Konfiguration_Global, Client_Konfiguration_Benutzer, Wiedervorlage, ZuordnungenReportvorlagen, BenutzerEmail, ImpExp_EigeneImporte tables, SchuleOAuthSecrets, Logins, and TextExportVorlagen).*

## Voraussetzungen (Requirements)

- Python 3.6 oder höher
- MariaDB Server (für Datenbankverbindung)
- `mysql-connector-python` (für Datenbankoperationen): `pip install mysql-connector-python`
- `cryptography` (für RSA/AES Schlüsselgenerierung): `pip install cryptography`

*Python 3.6 or higher required. MariaDB server (for database connection). mysql-connector-python for database operations: `pip install mysql-connector-python`. cryptography for RSA/AES key generation: `pip install cryptography`*

## Installation

1. Repository klonen:
```bash
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
```
2. Python-Abhängigkeiten installieren:
```bash
pip install mysql-connector-python cryptography
``` install mysql-connector-python
```

3. Konfigurationsdatei erstellen:
```bash
cp config.example.json config.json
```

4. `config.json` bearbeiten und die Datenbankverbindungsparameter anpassen:
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

**Hinweis:** Wenn `database`, `username` und `password` auf `null` gesetzt sind (oder fehlen), werden diese Werte beim Programmstart interaktiv abgefragt. Sie können diese Werte auch direkt in der Konfigurationsdatei setzen für automatisierte Ausführung ohne Eingabeaufforderungen.

*Note: If `database`, `username` and `password` are set to `null` (or omitted), these values are prompted interactively at program startup. You can also set these values directly in the configuration file for automated execution without prompts.*

4. Das Skript ausführbar machen (optional):
```bash
chmod +x svws_anonym.py
```

## Verwendung (Usage)

### Anzeige aller möglichen Parameter (Show Parameters)

```bash
python svws_anonym.py --help
```

### Basis-Verwendung (Basic Usage)

```bash
python svws_anonym.py --dry-run
```

Zeigt Beispiel-Anonymisierungen an, ohne die Datenbank zu ändern.

*Shows example anonymizations without modifying the database.*

### Datenbank anonymisieren (Anonymize Database)

```bash
python svws_anonym.py --anonymize
```

Verbindet sich mit der Datenbank und anonymisiert folgende Tabellen:

**EigeneSchule Tabelle:**
- `SchulNr` wird auf "123456" gesetzt
- `Bezeichnung1` wird auf "Städtische Schule" gesetzt
- `Bezeichnung2` wird auf "am Stadtgarten" gesetzt
- `Bezeichnung3` wird auf "Ganztagsschule des Landes NRW" gesetzt
- Adresse: Hauptstrasse 56, 42107 Wuppertal
- Kontaktdaten: Telefon, Fax, E-Mail, Webseite werden anonymisiert

**EigeneSchule_Email Tabelle:**
- SMTP-Konfiguration wird standardisiert (Port 25, StartTLS aktiviert, TLS deaktiviert)
- `Domain` und `SMTPServer` werden auf NULL/leer gesetzt

**EigeneSchule_Teilstandorte Tabelle:**
- Alle vorhandenen Einträge werden gelöscht und ein Eintrag wird gesetzt mit: `AdrMerkmal=A`, `PLZ=42103`, `Ort=Wuppertal`, `Strassenname=Hauptstrasse`, `HausNr=56`, `HausNrZusatz=NULL`, `Bemerkung=Hauptstandort`, `Kuerzel=WtalA`

**EigeneSchule_Abteilungen Tabelle:**
- `Email` wird auf "abteilung@schule.example.com" gesetzt
- `Durchwahl` wird auf NULL gesetzt
- `Raum` wird auf NULL gesetzt

**EigeneSchule_Logo Tabelle:**
- Logo wird durch ein standardisiertes Base64-kodiertes Bild ersetzt

**EigeneSchule_Texte Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**Benutzergruppen Tabelle:**
- `Bezeichnung` wird auf "Bezeichnung " + ID gesetzt (z.B. "Bezeichnung 1", "Bezeichnung 5")
- Geschützte Werte werden NICHT geändert: Administrator, Schulleitung, Lehrer, Sekretariat

**K_TelefonArt Tabelle:**
- `Bezeichnung` wird auf "Telefonart " + ID gesetzt (z.B. "Telefonart 5", "Telefonart 22")
- Geschützte Werte werden NICHT geändert: Eltern, Mutter, Vater, Notfallnummer, Festnetz, Handynummer, Mobilnummer, Großeltern

**K_Kindergarten Tabelle:**
- `Bezeichnung` wird auf "Kindergarten " + ID gesetzt (z.B. "Kindergarten 1", "Kindergarten 2")
- `PLZ` wird durch eine zufällige PLZ aus K_Ort ersetzt
- `Ort` wird durch eine zufällige Ortsbezeichnung (K_Ort.Bezeichnung) ersetzt
- `Strassenname` wird durch einen zufälligen Straßennamen aus Strassen.csv ersetzt
- Bereinigung: `HausNrZusatz`, `Tel`, `Email`, `Bemerkung` ← NULL

**K_Datenschutz Tabelle:**
- `Bezeichnung` wird auf "Bezeichnung " + ID gesetzt (z.B. "Bezeichnung 1", "Bezeichnung 3")
- Geschützte Werte werden NICHT geändert: "Verwendung Foto"

**Personengruppen Tabelle:**
- `Gruppenname` wird auf "Gruppe " + ID gesetzt (z.B. "Gruppe 1", "Gruppe 2")
- `Zusatzinfo` wird auf "Info" gesetzt
- `SammelEmail` wird auf "gruppe" + ID + "@gruppe.example.com" gesetzt (z.B. "gruppe1@gruppe.example.com")

**SchuleCredentials Tabelle:**
- Alle Einträge werden gelöscht
- Neuer Eintrag wird erstellt mit `Schulnummer` aus `EigeneSchule.SchulNr`
- `RSAPublicKey`: Neu generierter RSA 2048-bit öffentlicher Schlüssel (PEM-Format)
- `RSAPrivateKey`: Neu generierter RSA 2048-bit privater Schlüssel (PEM-Format)
- `AES`: Neu generierter AES 256-bit Schlüssel (Base64-kodiert)

**K_Lehrer Tabelle:**
- `Vorname` wird durch einen zufälligen Vornamen ersetzt (geschlechtsspezifisch basierend auf dem `Geschlecht` Feld)
- `Nachname` wird durch einen zufälligen Nachnamen ersetzt
- `Titel` wird auf NULL gesetzt
- `Kuerzel` wird aus den ersten Buchstaben von Vorname und Nachname generiert
- `Email` und `EmailDienstlich` werden mit neuen Namen generiert (@schule.nrw.de)
- `Tel` und `Handy` werden mit zufälligen Telefonnummern ersetzt
- `Geburtsdatum` wird randomisiert (Tag wird zufällig geändert, Monat und Jahr bleiben erhalten)
- `IdentNr1` wird aus Geburtsdatum (TTMMJJ) und Geschlecht generiert (z.B. "1008703")
- `SerNr` wird auf eine zufällige vierstellige Zahl gefolgt von 'X' gesetzt (z. B. 0123X)
- `PANr` wird auf "PA" gefolgt von einer zufälligen siebenstelligen Zahl gesetzt (z. B. PA0123456)
- `LBVNr` wird auf "LB" gefolgt von einer zufälligen siebenstelligen Zahl gesetzt (z. B. LB0123456)
- `LIDKrz` wird als eindeutiges, maximal 4-stelliges Kürzel generiert (Duplikate werden vermieden)
- Adressdaten (`Ort_ID`, `Strassenname`, `HausNr`, `HausNrZusatz`) werden aus CSV-Daten zugewiesen

**CredentialsLernplattformen Tabelle (Lehrer):**
- `Benutzername` wird auf Format "Vorname.Nachname" gesetzt (basierend auf K_Lehrer Namen via LehrerLernplattform)
- Duplikate werden mit numerischen Suffixen behandelt (Name, Name1, Name2, etc.)
- `Initialkennwort` wird auf eine zufällige 8-stellige Ziffernfolge gesetzt
- `PashwordHash` wird auf NULL gesetzt
- `RSAPublicKey` wird auf NULL gesetzt
- `RSAPrivateKey` wird auf NULL gesetzt
- `AES` wird auf NULL gesetzt

**CredentialsLernplattformen Tabelle (Schüler):**
- `Benutzername` wird auf Format "Vorname.Name" gesetzt (basierend auf Schueler Namen via SchuelerLernplattform)
- Duplikate werden mit numerischen Suffixen behandelt (Name, Name1, Name2, etc.)
- `Initialkennwort` wird auf eine zufällige 8-stellige Ziffernfolge gesetzt
- `PashwordHash` wird auf NULL gesetzt
- `RSAPublicKey` wird auf NULL gesetzt
- `RSAPrivateKey` wird auf NULL gesetzt
- `AES` wird auf NULL gesetzt

**LehrerAbschnittsdaten Tabelle:**
- `StammschulNr` wird auf "123456" gesetzt

**LehrerFotos Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**Lernplattformen Tabelle:**
- `Bezeichnung` wird auf "Lernplattform" + ID gesetzt (z.B. "Lernplattform1")
- `Konfiguration` wird auf NULL gesetzt (wenn nicht bereits NULL)

**SchuelerVermerke Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**SchuelerErzAdr Tabelle:**
- `Name1` und `Name2` werden – sofern nicht NULL – auf den Wert aus `Schueler.Name` gesetzt
- `Vorname1` und `Vorname2` werden geschlechtsabhängig aus den Namenslisten gesetzt (`Herr` → männlich, `Frau` → weiblich); bei `ErzieherArt_ID` ∈ {3, 4} wird `Schueler.Vorname` übernommen
- Adressen: `ErzOrt_ID` ← `Schueler.Ort_ID`, `ErzOrtsteil_ID` ← NULL, `ErzStrassenname` ← "Teststrasse" (wenn vorher nicht NULL), `ErzHausNr` ← Zufallszahl 1–100 (wenn vorher nicht NULL)
- `ErzEmail` ← `Name1@e.example.com` (wenn `Name1` nicht NULL, sonst NULL)
- Bereinigung: `ErzEmail2`, `Erz1StaatKrz`, `Erz2StaatKrz`, `ErzAdrZusatz` ← NULL
- `Bemerkungen` ← NULL

**Schueler Tabelle:**
- `Vorname` wird durch einen zufälligen Vornamen ersetzt (geschlechtsspezifisch)
- `Name` wird durch einen zufälligen Nachnamen ersetzt
- `Geburtsdatum` wird randomisiert (Tag wird zufällig geändert, Monat und Jahr bleiben erhalten)
- `Geburtsort` wird auf "Testort" gesetzt (wenn nicht NULL, sonst NULL)
- `Telefon` wird auf "012345-" + 6 zufällige Ziffern gesetzt (wenn nicht NULL, z.B. "012345-123456")
- `Fax` wird auf "012345-" + 6 zufällige Ziffern gesetzt (wenn nicht NULL, z.B. "012345-654321")
- Adressdaten (`Ort_ID`, `Strassenname`, `HausNr`) werden aus CSV-Daten zugewiesen
 - Transportfelder: `Idext` ← NULL, `Fahrschueler_ID` ← NULL, `Haltestelle_ID` ← NULL
 - Änderungsmarker: `ModifiziertVon` ← "Admin"

**K_AllgAdresse Tabelle:**
- `AllgAdrName1` wird auf zwei zufällige Nachnamen kombiniert wie "Name1 und Name2" gesetzt
- `AllgAdrName2`, `AllgAdrHausNrZusatz`, `AllgOrtsteil_ID` ← NULL
- `AllgAdrStrassenname` wird auf einen zufälligen Straßennamen aus CSV-Daten gesetzt
- `AllgAdrHausNr` wird auf eine Zufallszahl zwischen 1 und 100 gesetzt
- `AllgAdrOrt_ID` wird auf eine zufällig existierende Ort-ID aus K_Ort gesetzt
- `AllgAdrTelefon1` wird auf "01234-" + 6 zufällige Ziffern gesetzt (z.B. "01234-567890")
- `AllgAdrTelefon2`, `AllgAdrFax` ← NULL
- `AllgAdrEmail` wird auf `AllgAdrName1` ohne Leerzeichen + "@betrieb.example.com" gesetzt (z.B. "MuellerundSchmidt@betrieb.example.com")
- `AllgAdrBemerkungen`, `AllgAdrZusatz1`, `AllgAdrZusatz2` ← NULL

**AllgAdrAnsprechpartner Tabelle:**
- `Name` wird durch einen zufälligen Nachnamen ersetzt
- `Vorname` wird durch einen zufälligen Vornamen ersetzt
- `Email` wird auf `Name@betrieb.example.com` gesetzt (z.B. "Mueller@betrieb.example.com")
- `Titel` ← NULL
- `Telefon` wird auf "01234-" + 6 zufällige Ziffern gesetzt (z.B. "01234-123456")

**SchuelerTelefone Tabelle:**
- `Telefonnummer` wird auf "012345-" + 6 zufällige Ziffern gesetzt (z.B. "012345-123456")
- `Bemerkung` ← NULL

**SchuelerLD_PSFachBem Tabelle:**
- `ASV` ← NULL
- `LELS` ← NULL
- `AUE` ← NULL
- `ESF` ← NULL
- `BemerkungFSP` ← NULL
- `BemerkungVersetzung` ← NULL

**SchuelerLeistungsdaten Tabelle:**
- `Lernentw` ← NULL

**SchuelerFotos Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**SchuelerFoerderempfehlungen Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**Schueler LSSchulNr Tabelle (Bereich 100000-199999):**
- `LSSchulNr` wird durch eine zufällige SchulNr aus K_Schule ersetzt, die den gleichen `SchulformKrz` hat wie `LSSchulformSIM`
- Schüler ohne passendes SchulformKrz werden übersprungen

**Schueler LSSchulNr Tabelle (Bereich 200000-299999):**
- `LSSchulNr` wird durch eine zufällige SchulNr aus K_Schule ersetzt, die auch im Bereich 200000-299999 liegt

**Schueler SchulwechselNr Tabelle:**
- `SchulwechselNr` wird durch eine zufällige SchulNr aus K_Schule ersetzt (für alle nicht-NULL Einträge)

**Schueler LSBemerkung Tabelle:**
- `LSBemerkung` ← NULL (für alle Einträge mit non-NULL Werten)

**Schueler_AllgAdr Tabelle:**
- `Ausbilder` wird durch einen zufälligen Nachnamen aus `nachnamen.json` ersetzt (nur für Einträge mit non-NULL Werten)

**SchuelerBKAbschluss Tabelle:**
- `ThemaAbschlussarbeit` wird auf "Thema der Arbeit" gesetzt (nur für Einträge mit non-NULL Werten)

**SchuelerEinzelleistungen Tabelle:**
- `Bemerkung` wird auf "Bemerkung" gesetzt (nur für Einträge mit non-NULL Werten)

**SchuelerAbgaenge Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**SchuelerGSDaten Tabelle:**
- `Anrede_Klassenlehrer` ← NULL
- `Nachname_Klassenlehrer` ← NULL
- `GS_Klasse` ← NULL

**Personengruppen_Personen Tabelle:**
- Alle Einträge werden gelöscht (vollständige Bereinigung)

**SchuelerListe Tabelle:**
- `Erzeuger` wird auf ID=1 (Admin-Benutzer) gesetzt (nur für Einträge mit non-NULL Werten)

**Allgemeine Verwaltungs-Tabellen:**
- `Schil_Verwaltung`: Alle Einträge werden gelöscht
- `Client_Konfiguration_Global`: Alle Einträge werden gelöscht
- `Client_Konfiguration_Benutzer`: Alle Einträge werden gelöscht
- `Wiedervorlage`: Alle Einträge werden gelöscht
- `ZuordnungenReportvorlagen`: Alle Einträge werden gelöscht
- `BenutzerEmail`: Alle Einträge werden gelöscht
- `ImpExp_EigeneImporte`: Alle Einträge werden gelöscht
- `ImpExp_EigeneImporte_Felder`: Alle Einträge werden gelöscht
- `ImpExp_EigeneImporte_Tabellen`: Alle Einträge werden gelöscht
- `SchuleOAuthSecrets`: Alle Einträge werden gelöscht
- `Logins`: Alle Einträge werden gelöscht
- `TextExportVorlagen`: Alle Einträge werden gelöscht
- `Benutzer`: Alle Einträge werden gelöscht, Admin-Benutzer wird neu angelegt (ID=1, Typ=0, Allgemein_ID=1, IstAdmin=1)
- `BenutzerAllgemein`: Alle Einträge werden gelöscht, Administrator wird neu angelegt (ID=1, Anzeigename=Administrator, CredentialID=1)
- `Credentials`: Alle Einträge werden gelöscht, Admin-Credential wird neu angelegt (ID=1, Benutzername=Admin)



### Dry-Run Modus (Dry-Run Mode)

```bash
python svws_anonym.py --dry-run
```

Zeigt an, welche Änderungen vorgenommen würden, ohne die Datenbank tatsächlich zu ändern.

*Shows what changes would be made without actually modifying the database.*

### Mit benutzerdefinierter Konfiguration (With custom configuration)

```bash
python svws_anonym.py --config /path/to/config.json --anonymize
```

## Konfigurationsdatei (Configuration File)

Die `config.json` enthält die Datenbankverbindungsparameter für den MariaDB-Server:

- `host`: Hostname oder IP-Adresse des Datenbankservers (Standard: localhost)
- `port`: Port des Datenbankservers (Standard: 3306)
- `database`: Datenbankname (optional, wird bei NULL abgefragt)
- `username`: Benutzername (optional, wird bei NULL abgefragt)
- `password`: Passwort (optional, wird bei NULL abgefragt)
- `charset`: Zeichensatz (Standard: utf8mb4)
- `collation`: Kollation (Standard: utf8mb4_unicode_ci)

**Flexible Authentifizierung:** Wenn `database`, `username` und `password` auf `null` gesetzt sind (oder fehlen), werden diese Werte beim Programmstart interaktiv abgefragt. Dies erhöht die Sicherheit, da keine Zugangsdaten im Klartext gespeichert werden müssen. Alternativ können diese Werte auch direkt in der Konfigurationsdatei gesetzt werden für automatisierte Ausführung ohne Eingabeaufforderungen.

**Wichtig:** Die `config.json` wird nicht ins Git-Repository eingecheckt. Verwenden Sie `config.example.json` als Vorlage.

*The `config.json` file contains database connection parameters for the MariaDB server. Flexible authentication: If `database`, `username` and `password` are set to `null` (or omitted), these values are prompted interactively at program startup. This improves security by not requiring credentials to be stored in plain text. Alternatively, these values can also be set directly in the configuration file for automated execution without prompts. Important: `config.json` is not checked into the git repository. Use `config.example.json` as a template.*

## Namenslisten (Name Lists)

Das Tool verwendet die folgenden JSON-Dateien aus dem [JSON-Namen Repository](https://github.com/FPfotenhauer/JSON-Namen):

- `nachnamen.json`: 2000 deutsche Nachnamen
- `vornamen_m.json`: 500 männliche Vornamen
- `vornamen_w.json`: 500 weibliche Vornamen

Zusätzlich wird eine CSV-Datei für Adressdaten verwendet:
- `strassen.csv`: Straßendaten mit Ort_ID, PLZ, Ort und Straßenname für realistische Adresszuordnung

*The tool uses the following JSON files from the JSON-Namen repository: nachnamen.json (2000 German last names), vornamen_m.json (500 male first names), vornamen_w.json (500 female first names). Additionally, a CSV file is used for address data: strassen.csv with location IDs, postal codes, cities and street names for realistic address assignment.*

## Lizenz (License)

Siehe [LICENSE](LICENSE) Datei für Details.

## Mitwirken (Contributing)

Beiträge sind willkommen! Bitte öffnen Sie ein Issue oder einen Pull Request.

*Contributions are welcome! Please open an issue or pull request.*

## Credits

Namenslisten von / Name lists from: [JSON-Namen Repository](https://github.com/FPfotenhauer/JSON-Namen)
