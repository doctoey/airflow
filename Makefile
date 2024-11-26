init:
	docker compose up airflow-init

up:
	docker compose up

down:
	docker compose down

reset:
	docker compose down --volumes --rmi all