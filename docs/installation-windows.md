---
layout: default
title: Installation unter Windows
---

# Installation unter Windows

## 1. Python installieren

Installieren Sie Python 3.6 oder höher von [python.org](https://www.python.org/downloads/windows/). Aktivieren Sie im Installationsprogramm die Option **Add Python to PATH**.

Prüfen Sie anschließend in PowerShell:

```powershell
python --version
```

## 2. Projekt herunterladen

```powershell
git clone https://github.com/FPfotenhauer/SVWS-Anonym.git
cd SVWS-Anonym
```

Alternativ kann das Repository als ZIP-Datei heruntergeladen und entpackt werden.

## 3. Abhängigkeiten installieren

```powershell
python -m pip install --upgrade pip
python -m pip install mysql-connector-python cryptography
```

## 4. Konfiguration anlegen

```powershell
Copy-Item config.example.json config.json
```

Bearbeiten Sie anschließend `config.json`. Details stehen in [Konfiguration](konfiguration.html).

## 5. Installation prüfen

```powershell
python svws_anonym.py --help
python svws_anonym.py --dry-run
```
