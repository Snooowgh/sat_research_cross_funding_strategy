#!/bin/bash
export PATH=/Applications/Docker.app/Contents/Resources/bin:$PATH
docker compose up -d
docker compose restart arbitrage_process
docker compose logs -f --tail=10 arbitrage_process
