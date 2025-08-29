# user-platform

A production-grade Go microservice project for user registration, login, auditing, and event streaming. It includes gRPC services, Kafka for audit events, PostgreSQL with sqlc for data access, Redis caching, SSE for real-time notifications, Vault integration for secrets, Docker and Kubernetes manifests, and comprehensive unit and integration tests.

## Features

- **User Service**: Provides `Register`, `Login`, `GetProfile`, `RefreshSession`, and `Logout` via gRPC. Validates inputs with user-friendly error messages. Passwords hashed with Argon2id. Sessions stored in Postgres and cached in Redis. Emits audit events to Kafka on significant actions.

- **Logger Service**: Consumes audit events from Kafka, persists to Postgres (simplified here), and exposes an optional gRPC endpoint for direct audit writes.

- **Gateway**: Uses gRPC-Gateway to expose HTTP REST endpoints translating to the User Service. Also exposes `/events` via SSE for real-time notifications of user events.

- **Infrastructure**: Postgres, Redis, Kafka, Zookeeper, and Vault are provided via `docker-compose`. Kubernetes manifests demonstrate deploying the services with Vault Agent Injector.

- **Observability**: Structured JSON logging with request IDs, Prometheus metrics, basic tracing hooks.

- **Validation and Error Handling**: Custom error codes mapped to gRPC statuses and user-friendly messages. Input validation using `go-playground/validator`.

- **Security**: JWT authentication with access and refresh tokens. Keys loaded from Vault. Secrets never stored in code; `.env.example` documents non-secret variables.

## Getting Started

### Prerequisites

- Go 1.22 or later
- Docker and Docker Compose

### Running Locally with Docker Compose

1. Clone the repository:
   ```bash
   git clone https://github.com/example/user-platform.git
   cd user-platform
   ```

## Build and start the services:

## Build and Start the Services
```bash
    make docker
    docker-compose up -d
```
This will start:

- Postgres  
- Redis  
- Kafka  
- Vault  
- User Service  
- Logger Service  
- Gateway  

---

## Register and Login via gRPC or REST

- **gRPC**: Use grpcurl to call methods on `localhost:50051`
```bash
      grpcurl -plaintext localhost:50051 list
      grpcurl -plaintext -d '{"email":"a@b.com","username":"alice","password":"secretpw"}' \
        localhost:50051 user.v1.UserService/Register
```
- **REST**: Example register request
```bash
      curl -X POST http://localhost:8080/v1/register \
        -H "Content-Type: application/json" \
        -d '{ "email": "a@b.com", "username": "alice", "password": "secretpw" }'
```
---

## View SSE Events

    curl http://localhost:8080/events

---

## Running Tests

Run all unit and integration tests:

    make test

---

## Kubernetes Deployment

The `build/k8s` directory contains manifests to deploy the platform on Kubernetes with Vault integration.  
Apply in order:
```bash
    kubectl apply -f build/k8s/namespace.yaml
    kubectl apply -f build/k8s/postgres.yaml
    kubectl apply -f build/k8s/redis.yaml
    kubectl apply -f build/k8s/kafka-zookeeper.yaml
    kubectl apply -f build/k8s/user-service.yaml
    kubectl apply -f build/k8s/logger-service.yaml
    kubectl apply -f build/k8s/gateway.yaml
```
> You may need to configure **Vault** and its injector to supply secrets to the services.  
> See HashiCorp Vault documentation for details.

---

## Generating Protobuf and SQLC Code

This project uses **buf** and **sqlc** for code generation.
```bash
    make proto   # generates gRPC code from proto files
    make sqlc    # generates type-safe DB code from SQL queries
```
Generated files are excluded from version control.  
See `api/` and `internal/db/` for source definitions.

---

## Contributing

This project is intended as a learning example.  
Feel free to open issues or submit pull requests to improve the implementation, fix bugs, or add features.

---

## License

**MIT License**

---

> Each file above is part of the `user-platform` project. Save them accordingly to build and run the complete service.
