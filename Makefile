.PHONY: colima
colima:
	colima stop && colima start

.PHONY: up
up: 
	docker-compose up --build 

.PHONY: down
down: 
	docker-compose down

.PHONY: watch
watch: 
	docker-compose watch

.PHONY: clean
clean: 
	rm -fr .venv || true
	rm -fr __pycache__ || true