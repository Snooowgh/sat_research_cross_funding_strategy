#!/bin/bash
export PATH=/Applications/Docker.app/Contents/Resources/bin:$PATH
docker compose up -d
docker compose restart arbitrage_monitor
docker compose logs -f --tail=10 arbitrage_monitor
