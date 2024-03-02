PROJECT_NAME := txrx-redis

MAKEFLAGS += --always-make

GIT_NAME := $(shell git config --get user.name)
GIT_EMAIL := $(shell git config --get user.email)

export GIT_NAME
export GIT_EMAIL

UID := $(shell id -u)
GID := $(shell id -g)

export UID
export GID

build: 
	docker compose build

install:
	docker compose up -d
	docker compose exec node npm install

clear: 
	docker compose down --remove-orphans
	docker compose rm -f

clear_all:
	docker compose down --volumes --remove-orphans --rmi local
	docker compose rm -f
	docker system prune --force

logs:
	docker compose logs -f -t

shell:
	docker compose exec -it node bash

test: jest syntax

jest:
	docker compose exec node npm run test

syntax:
	docker compose exec node npm run lint

lint:
	docker compose exec node npm run lint -- --fix

coverage: jest
	open coverage/lcov-report/index.html

rebuild: clear build install

publish:
	docker compose exec node npx tsc
	docker compose exec node npm publish

.SUFFIXES: