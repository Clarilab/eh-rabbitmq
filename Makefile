all: vet lint vuln test_integration

vet:
	go vet ./...

lint:
	golangci-lint run ./...

vuln:
	govulncheck ./...

test_integration:
	./run_integration_tests.sh

test_benchmark:
	go test -benchmem -bench=. -failfast -race -coverprofile=coverage.out