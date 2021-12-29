default: rabbitmq test remove-rabbitmq

rabbitmq:
	docker run --restart always -d --hostname eh-rabbitmq --name eh-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

remove-rabbitmq:
	docker rm -f eh-rabbitmq

test:
	go test -v -race -short ./...