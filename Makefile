# Default values for environment variables
ENV_FILE := .env
GO_POOL := ./cmd/pool
GO_DASHBOARD_API := ./cmd/dashboard-api

BIN_DIR := bin
POOL_BIN := $(BIN_DIR)/pool
DASHBOARD_API_BIN := $(BIN_DIR)/dashboard-api

.PHONY: run run-pool run-dashboard-api build build-pool build-dashboard-api clean

all: build-pool

run: run-pool

build: build-pool build-dashboard-api

build-pool:
	@mkdir -p $(BIN_DIR)
	@go build -o $(POOL_BIN) $(GO_POOL)

build-dashboard-api:
	@mkdir -p $(BIN_DIR)
	@go build -o $(DASHBOARD_API_BIN) $(GO_DASHBOARD_API)

run-pool: build-pool
	@if [ -f $(ENV_FILE) ]; then \
		export $$(grep -v '^#' $(ENV_FILE) | xargs); \
	fi; \
	$(POOL_BIN)

run-dashboard-api: build-dashboard-api
	@if [ -f $(ENV_FILE) ]; then \
		export $$(grep -v '^#' $(ENV_FILE) | xargs); \
	fi; \
	$(DASHBOARD_API_BIN)

clean:
	@go clean
	@rm -rf $(BIN_DIR)
