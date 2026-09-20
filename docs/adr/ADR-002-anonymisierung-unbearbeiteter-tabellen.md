# ADR-002: Anonymisierung bisher unberuehrter Tabellen

- Status: Vorgeschlagen
- Datum: 2026-09-20
- Geltungsbereich: SVWS-Datenbank und `svws_anonym.py`
- Vorgaenger: ADR-001 kann nachtraeglich ergaenzt werden

## Kontext

Der aktuelle Anonymisierungslauf referenziert 53 von 274 Tabellen. 221 Tabellen werden vom Skript derzeit nicht gelesen, geprueft, veraendert oder geloescht.

Der Abgleich wurde mit `SHOW TABLES` gegen das neu angelegte Schema `anonym` durchgefuehrt. Die Liste ist daher eine Momentaufnahme dieses Schemas. Andere SVWS-Versionen koennen weitere oder abweichende Tabellen enthalten.

Besonders relevant sind die bisher nicht beruecksichtigten Bereiche:

- `Nationalitaeten_Keys` und alle Staatsangehoerigkeitsfelder in Personen- und Schuelertabellen
- Stundenplan
- Unterrichtsverteilung (`UV_*`)
- Kursplanung und Kursteilnahmen
- Klassen, Faecher und Lehrer-Unterrichtszuordnungen
- weitere Schueler-, Lehrer- und Verwaltungsdaten

## Entscheidung

Die Tabellen werden in fachlichen Gruppen untersucht und erst nach einer Feldanalyse in den Anonymisierungslauf aufgenommen. Fuer jede Tabelle wird dokumentiert:

1. ob sie personenbezogene oder organisatorische Daten enthaelt,
2. welche Felder anonymisiert, geloescht oder unveraendert bleiben,
3. welche Fremdschluessel und fachlichen Beziehungen erhalten bleiben muessen,
4. ob die Verarbeitung nur bei vorhandener Tabelle und vorhandenen Spalten erfolgen darf,
5. wie der Dry-Run und die Rueckgabe aus einem Backup geprueft werden.

Es werden keine pauschalen `UPDATE`- oder `DELETE`-Anweisungen fuer unbekannte Tabellen eingefuehrt. Jede neue Tabellenfamilie bekommt eine eigene Methode, gezielte Spaltenauswahl und Tests.

## Vollstaendige Liste der derzeit unberuehrten Tabellen

### Administration, Konfiguration und Synchronisation

```text
BenutzerEmail
BenutzergruppenKompetenzen
BenutzergruppenMitglieder
BenutzerKompetenzen
Client_Konfiguration_Benutzer
Client_Konfiguration_Global
DavRessourceCollections
DavRessourceCollectionsACL
DavRessources
DavSyncTokenLehrer
DavSyncTokenSchueler
ImpExp_EigeneImporte
ImpExp_EigeneImporte_Felder
ImpExp_EigeneImporte_Tabellen
Logins
Notenmodul_Credentials
Notenmodul_Konfiguration_Client
Notenmodul_Konfiguration_Server
Notenmodul_Verbindungen
Schild_Verwaltung
Schema_AutoInkremente
Schema_Core_Type_Versionen
Schema_Status
SchuleOAuthSecrets
TextExportVorlagen
TimestampsNotenmodulCredentials
V_Benutzer
V_BenutzerDetails
V_Benutzerkompetenzen
Wiedervorlage
ZuordnungReportvorlagen
SchildFilter
Schulleitung
Schulbewerbung_Importe
TimestampsSchuelerAnkreuzkompetenzen
TimestampsSchuelerLeistungsdaten
TimestampsSchuelerLernabschnittsdaten
TimestampsSchuelerTeilleistungen
TimestampsSchuelerZP10
TimestampsSchuelerZuweisungen
```

### Schueler- und Lehrerbereiche

```text
ErzieherDatenschutz
ErzieherLernplattform
Gost_Schueler
Gost_Schueler_Fachwahlen
Klassen
KlassenLehrer
LehrerAnrechnung
LehrerDatenschutz
LehrerEntlastung
LehrerFunktionen
LehrerLeitungsfunktion_Keys
LehrerMehrleistung
LehrerPersonaldatenLehramt
LehrerPersonaldatenLehramtFachrichtung
LehrerPersonaldatenLehramtLehrbefaehigung
LehrerUnterrichtsfaecher
Kompetenzen
Kompetenzgruppen
SchuelerAbiFaecher
SchuelerAbitur
SchuelerAnkreuzfloskeln
SchuelerBKFaecher
SchuelerDatenschutz
SchuelerFehlstunden
SchuelerFHR
SchuelerFHRFaecher
SchuelerListe_Inhalt
SchuelerMerkmale
SchuelerReportvorlagen
SchuelerSprachenfolge
SchuelerSprachpruefungen
SchuelerStatus_Keys
SchuelerZP10
SchuelerZuweisungen
```

### Kataloge, Herkunft und allgemeine Fachdaten

```text
AllgemeineMerkmaleKatalog_Keys
Ankreuzkompetenz_Jahrgang
Berufskolleg_Anlagen
Berufskolleg_Berufsebenen1
Berufskolleg_Berufsebenen2
Berufskolleg_Berufsebenen3
Berufskolleg_Fachklassen_Keys
EinschulungsartKatalog_Keys
Fach_Gliederungen
Fachgruppen
FachKatalog
FachKatalog_Keys
FachKatalog_Schulformen
Herkunft
Herkunft_Keys
Herkunft_Schulformen
Herkunftsart
Herkunftsart_Keys
Herkunftsart_Schulformen
Jahrgaenge_Keys
K_Adressart
K_BeschaeftigungsArt
K_EinschulungsArt
K_Einzelleistungen
K_ErzieherFunktion
K_Foerderschwerpunkt
K_Ortsteil
K_Religion
K_Schwerpunkt
K_Textdateien
K_Zertifikate
KAoA_Anschlussoption_Keys
KAoA_Berufsfeld_Keys
KAoA_Kategorie_Keys
KAoA_Merkmal_Keys
KAoA_SBO_Ebene4_Keys
KAoA_Zusatzmerkmal_Keys
KlassenartenKatalog_Keys
KursartenKatalog_Keys
Nationalitaeten_Keys
NichtMoeglAbiFachKombi
OrganisationsformenKatalog_Keys
PersonalTypen
Religionen_Keys
Schulformen
Schuljahresabschnitte
Stundentafel
Stundentafel_Faecher
EigeneSchule_Abt_Kl
EigeneSchule_Fachklassen
EigeneSchule_FachTeilleistungen
EigeneSchule_Faecher
EigeneSchule_Jahrgaenge
EigeneSchule_KAoADaten
EigeneSchule_Kursart
EigeneSchule_Merkmale
EigeneSchule_Schulformen
EigeneSchule_Zertifikate
```

### Kurse, Blockungen und Kursplanung

```text
Gost_Blockung
Gost_Blockung_Kurse
Gost_Blockung_Kurslehrer
Gost_Blockung_Regeln
Gost_Blockung_Regelparameter
Gost_Blockung_Schienen
Gost_Blockung_Zwischenergebnisse
Gost_Blockung_Zwischenergebnisse_Kurs_Schienen
Gost_Blockung_Zwischenergebnisse_Kurs_Schueler
Gost_Jahrgang_Beratungslehrer
Gost_Jahrgang_Fachkombinationen
Gost_Jahrgang_Fachwahlen
Gost_Jahrgang_Faecher
Gost_Jahrgangsdaten
Kurs_Schueler
KursFortschreibungsarten
KursLehrer
Kurse
Noten
Katalog_Aufsichtsbereich
Katalog_Floskeln_Gruppen
Katalog_Floskeln_Jahrgaenge
Katalog_Pausenzeiten
Katalog_Raeume
Katalog_Zeitraster
```

### Klausuren und Oberstufe

```text
Gost_Klausuren_Kalenderinformationen
Gost_Klausuren_Kursklausuren
Gost_Klausuren_NtaZeiten
Gost_Klausuren_Raeume
Gost_Klausuren_Raumstunden
Gost_Klausuren_Raumstunden_Aufsichten
Gost_Klausuren_Schuelerklausuren
Gost_Klausuren_Schuelerklausuren_Termine
Gost_Klausuren_SchuelerklausurenTermine_Raumstunden
Gost_Klausuren_Termine
Gost_Klausuren_Vorgaben
```

### Stundenplan

```text
Stundenplan
Stundenplan_Aufsichtsbereiche
Stundenplan_Kalenderwochen_Zuordnung
Stundenplan_Pausenaufsichten
Stundenplan_PausenaufsichtenBereich
Stundenplan_Pausenzeit
Stundenplan_Pausenzeit_Klassenzuordnung
Stundenplan_Raeume
Stundenplan_Schienen
Stundenplan_Unterricht
Stundenplan_UnterrichtKlasse
Stundenplan_UnterrichtLehrer
Stundenplan_UnterrichtRaum
Stundenplan_UnterrichtSchiene
Stundenplan_Zeitraster
```

### Unterrichtsverteilung und Stundenplanergebnisse

```text
UV_Faecher
UV_Klassen
UV_Klassen_Lehrer
UV_Kurse
UV_Lehrer
UV_LehrerAnrechnungsstunden
UV_LehrerPflichtstundensoll
UV_LehrerUnterrichtsfaecher
UV_Lerngruppen
UV_Lerngruppen_Lehrer
UV_Lerngruppen_Schienen
UV_Planungsabschnitte
UV_Planungsabschnitte_Lehrer
UV_Planungsabschnitte_Schueler
UV_Planungsabschnitte_Zeitraster
UV_PlanungsabschnitteZeitraster_Constraint_Jahrgaenge
UV_Raeume
UV_Raumgruppen
UV_Schienen
UV_Schienen_Constraint_Jahrgaenge
UV_Schuelergruppen
UV_Schuelergruppen_Constraint_Jahrgaenge
UV_Schuelergruppen_Constraint_Schuelergruppen
UV_Schuelergruppen_Schueler
UV_StundenplanErgebnisse
UV_StundenplanErgebnisse_Unterricht_Lehrer
UV_StundenplanErgebnisse_Unterricht_Raeume
UV_StundenplanErgebnisse_Unterricht_Zeitraster
UV_StundenplanKonfigurationen
UV_StundenplanRegeln
UV_StundenplanRegelparameter
UV_Stundentafeln
UV_Stundentafeln_Faecher
UV_Unterrichte
UV_Unterrichte_Lerngruppenlehrer
UV_Unterrichte_Raeume
UV_Zeitraster
UV_ZeitrasterEintraege
```

## Schrittweiser Plan

### Schritt 1: Schema und Risiko erfassen

- Schema-Version und Tabellenliste bei jedem Lauf protokollieren.
- Fuer jede unberuehrte Tabelle `SHOW COLUMNS` und die Fremdschluesselbeziehungen erfassen.
- Felder nach Kategorien markieren: Name, Adresse, Kontakt, Freitext, Kennung, Fremdschluessel, Zeit-/Planungsdaten und technische Daten.
- Tabellen mit Fotos, Schluesseln, Tokens oder Zugangsdaten als hohe Prioritaet markieren.

### Schritt 2: Staatsangehoerigkeit und personenbezogene Schluessel

- `Nationalitaeten_Keys` auf fachliche Schluessel und Bezeichnungen pruefen.
- Alle Tabellen nach `Nationalitaet`, `Nationalitaeten`, `Staat`, `StaatKrz`, `Staat_ID` und aehnlichen Feldern durchsuchen.
- Entscheiden, ob Personenfelder auf einen neutralen vorhandenen Schluessel zeigen oder auf `NULL` gesetzt werden.
- Die neue ID-Struktur in einem isolierten Patch fuer `Schueler` und zugehoerige Stammdaten umsetzen.
- Konsistenzpruefung einfuehren: Jeder verbleibende Fremdschluessel muss in der Katalogtabelle existieren.

### Schritt 3: Direkte Personen- und Kontaktdaten

- Noch nicht abgedeckte Schueler-, Lehrer-, Erzieher- und Ansprechpartnerdaten bearbeiten.
- Namen, Adressen, Telefon, E-Mail, Freitext, Geburts- und Identifikationsdaten anonymisieren.
- Beziehungen und IDs nicht zufaellig ersetzen, wenn sie als Fremdschluessel benoetigt werden.
- Freitextfelder auf personenbezogene Inhalte pruefen und entweder standardisieren oder leeren.

### Schritt 4: Kursplanung und Unterrichtsverteilung

- `Kurse`, `Kurs_Schueler`, `KursLehrer` und die `UV_*`-Tabellen analysieren.
- Personenbezug ueber Schueler-, Lehrer-, Klassen-, Lerngruppen- und Raum-IDs identifizieren.
- Namen und Bezeichnungen von Kursen, Lerngruppen, Raeumen und Schienen standardisieren.
- Schueler- und Lehrerzuordnungen konsistent halten, ohne die IDs unkontrolliert zu aendern.
- Planungsabschnitte, Constraint-Texte und Bemerkungen auf Freitext und personenbezogene Inhalte pruefen.

### Schritt 5: Stundenplan

- `Stundenplan_*` nach Lehrer-, Schueler-, Klassen-, Kurs-, Raum- und Freitextfeldern untersuchen.
- Raum-, Schienen-, Zeitraster- und Aufsichtsbezeichnungen anonymisieren, wenn sie personenbezogene oder schulinterne Informationen enthalten.
- Verknuepfungen zu Unterricht, Klassen und Lehrkraeften erhalten.
- Kalender- und Zeitdaten nur veraendern, wenn sie einen Personenbezug oder vertrauliche Schulinformationen enthalten.

### Schritt 6: Oberstufe, Klausuren und Leistungsdaten

- `Gost_*`, `Noten`, Klausur-, Fachwahl- und Abiturtabellen analysieren.
- Themen, Bemerkungen, Raumangaben und Aufsichtsinformationen anonymisieren oder leeren.
- Noten und fachliche Leistungswerte nur bearbeiten, wenn sie personenbezogene Zusatzinformationen enthalten oder die Anforderungen dies ausdruecklich verlangen.
- Schueler- und Lehrerbeziehungen konsistent halten.

### Schritt 7: Kataloge und Verwaltungsdaten

- Katalogbezeichnungen, Herkunft, Religion, Foerderschwerpunkte und Einschulungsdaten auf Personenbezug pruefen.
- Geschuetzte Standardwerte dokumentieren.
- Konfiguration, Logins, Tokens, OAuth-Secrets und Importdaten loeschen oder neutralisieren, sofern sie fuer die Testdatenbank nicht benoetigt werden.
- Technische Schema-, Versions- und Autoinkrementtabellen nicht veraendern, ausser ein konkreter Lauf erfordert es.

### Schritt 8: Implementierung pro Tabellenfamilie

Jede neue Tabellenfamilie wird in dieser Reihenfolge umgesetzt:

1. Tabellen- und Spaltenexistenz pruefen.
2. Nur benoetigte Spalten selektieren.
3. Dry-Run-Ausgabe mit Anzahl und Beispielen ergaenzen.
4. Anonymisierung in einer Transaktion ausfuehren.
5. Fremdschluessel und Pflichtfelder validieren.
6. Einen Test fuer vorhandene, fehlende und optionale Spalten ergaenzen.
7. README und Anwenderdokumentation aktualisieren.

### Schritt 9: Test und Abnahme

- Testdatenbank aus einem Backup oder einer Kopie erzeugen.
- Dry-Run und echten Lauf getrennt pruefen.
- Vorher-/Nachher-Zaehlen pro Tabelle dokumentieren.
- Nach dem Lauf nach Originalnamen, E-Mail-Domains, Telefonnummern, Freitext und Schluesseln suchen.
- SVWS-Funktionen fuer Schueler, Lehrer, Kurse, Unterrichtsverteilung und Stundenplan testen.
- Erst nach erfolgreicher Abnahme weitere Tabellenfamilien freischalten.

## Offene Punkte

- Welche SVWS-Version und welcher Schema-Stand sind verbindlich?
- Sollen Leistungswerte und Noten erhalten bleiben oder ebenfalls neutralisiert werden?
- Sollen Stundenplan- und Unterrichtszeiten nur personenbezogen bereinigt oder vollstaendig zufaellig erzeugt werden?
- Welche Standardwerte muessen fuer SVWS-Funktionen erhalten bleiben?
- Soll ADR-001 nachtraeglich die bereits umgesetzten Anonymisierungsbereiche dokumentieren?
