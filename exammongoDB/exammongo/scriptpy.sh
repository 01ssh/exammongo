#!/bin/bash

# Démarrer les conteneurs Docker avec Docker Compose
docker-compose up -d


# Exécuter le script Python pour se connecter à MongoDB
python3 python.py
