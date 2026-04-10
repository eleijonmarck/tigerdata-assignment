.PHONY: up down dev build

up:
	docker compose up -d

down: 
	docker compose down

dev:
	air