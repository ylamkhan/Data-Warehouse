# Variables
COMPOSE=docker-compose
COMPOSE_FILE=docker-compose.yml

# Default target
.PHONY: all
all: up

# Start the services
.PHONY: up
up:
	@echo "Starting containers..."
	$(COMPOSE) -f $(COMPOSE_FILE) up -d --build

# Stop the services
.PHONY: down
down:
	@echo "Stopping containers..."
	$(COMPOSE) -f $(COMPOSE_FILE) down

# Clean everything: containers, volumes, images, networks
.PHONY: clean
clean:
	@echo "Removing containers, networks, images, and volumes..."
	$(COMPOSE) -f $(COMPOSE_FILE) down --volumes --rmi all --remove-orphans
	@echo "Pruning dangling docker resources..."
	docker system prune -f
	docker volume prune -f
	docker network prune -f
	docker image prune -f

# Show status
.PHONY: ps
ps:
	$(COMPOSE) -f $(COMPOSE_FILE) ps

# Logs
.PHONY: logs
logs:
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f

# Rebuild without cache
.PHONY: rebuild
rebuild:
	$(COMPOSE) -f $(COMPOSE_FILE) build --no-cache
