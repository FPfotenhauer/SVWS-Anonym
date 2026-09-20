---
layout: default
title: Installation unter Windows
permalink: /installation-windows/
---

# Installation unter Windows

## 1. Python installieren

Installieren Sie Python 3.6 oder höher von [python.org](https://www.python.org/downloads/windows/). Aktivieren Sie **Add Python to PATH**.

```powershell
python --version
```

## 2. Projekt und Abhängigkeiten

```powershell
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
python -m pip install --upgrade pip
python -m pip install mysql-connector-python cryptography
Copy-Item config.example.json config.json
```

Bearbeiten Sie anschließend `config.json`. Details stehen in [Konfiguration](konfiguration.html).

## 3. Installation prüfen

```powershell
python svws_anonym.py --help
python svws_anonym.py --dry-run
```
