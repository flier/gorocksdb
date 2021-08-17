.PHONY: test-docker

docker-clean:
	@docker compose down -v --remove-orphans

docker-test:
	@docker compose build test && docker compose run --rm test go test -race=1 -v ./...
