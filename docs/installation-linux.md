---
layout: default
title: Installation unter Linux
permalink: /installation-linux/
---

# Installation unter Linux

## 1. Python installieren

Für Debian und Ubuntu:

```bash
sudo apt update
sudo apt install python3 python3-venv python3-pip git
python3 --version
```

## 2. Projekt und Abhängigkeiten

```bash
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install mysql-connector-python cryptography
cp config.example.json config.json
chmod +x svws_anonym.py
```

Bearbeiten Sie anschließend `config.json`. Details stehen in [Konfiguration](konfiguration.html).

## 3. Installation prüfen

```bash
python svws_anonym.py --help
python svws_anonym.py --dry-run
```
