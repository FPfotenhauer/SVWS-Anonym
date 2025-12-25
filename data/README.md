# Name Lists for Anonymization

This directory contains JSON files with German names used for anonymizing SVWS database entries.

## Files

### nachnamen.json
- **Description**: List of German last names (Nachnamen)
- **Count**: 2000 entries
- **Format**: JSON array of strings
- **Example**: `["Müller", "Schmidt", "Schneider", ...]`

### vornamen_m.json
- **Description**: List of German male first names (männliche Vornamen)
- **Count**: 500 entries
- **Format**: JSON array of strings
- **Example**: `["Ben", "Luca", "Paul", ...]`

### vornamen_w.json
- **Description**: List of German female first names (weibliche Vornamen)
- **Count**: 500 entries
- **Format**: JSON array of strings
- **Example**: `["Mia", "Emma", "Hannah", ...]`

## Source

These name lists are fetched from the [JSON-Namen repository](https://github.com/FPfotenhauer/JSON-Namen).

## Usage

These files are used by the SVWS anonymization tool to replace real names in database entries with randomly selected German names while maintaining data consistency.
