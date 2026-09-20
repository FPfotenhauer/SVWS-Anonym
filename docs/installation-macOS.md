---
layout: default
title: Installation unter macOS
permalink: /installation-macOS/
---

# Installation unter macOS

## 1. Python installieren

Installieren Sie Python 3.6 oder höher von [python.org](https://www.python.org/downloads/macos/) oder über Homebrew:

```bash
brew install python
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
```

Bearbeiten Sie anschließend `config.json`. Details stehen in [Konfiguration](konfiguration.html).

## 3. Installation prüfen

```bash
python svws_anonym.py --help
python svws_anonym.py --dry-run
```
