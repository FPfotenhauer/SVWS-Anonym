# Installation unter Linux

## 1. Python installieren

Installieren Sie Python 3.6 oder höher mit dem Paketmanager Ihrer Distribution. Für Debian und Ubuntu:

```bash
sudo apt update
sudo apt install python3 python3-venv python3-pip git
```

Prüfen Sie die Installation:

```bash
python3 --version
```

## 2. Projekt herunterladen

```bash
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
```

## 3. Virtuelle Umgebung und Abhängigkeiten

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install mysql-connector-python cryptography
```

## 4. Konfiguration anlegen

```bash
cp config.example.json config.json
```

Bearbeiten Sie anschließend `config.json`. Details stehen in [Konfiguration](konfiguration.md).

Optional kann das Skript ausführbar gemacht werden:

```bash
chmod +x svws_anonym.py
```

## 5. Installation prüfen

```bash
python svws_anonym.py --help
python svws_anonym.py --dry-run
```
