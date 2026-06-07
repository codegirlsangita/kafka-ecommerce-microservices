# Kafka E-Commerce Microservices

Event-driven microservices demo built with Node.js, Kafka, and MongoDB.

This repository is structured like a real backend platform with separate services, explicit event contracts, Dockerized infrastructure, and CI validation.

## Tech Stack

- Node.js + Express
- Apache Kafka (via KafkaJS)
- MongoDB + Mongoose
- Docker Compose
- GitHub Actions (CI)

## Architecture

Services communicate asynchronously through Kafka topics:

- `user-service` (port `3001`): creates users and publishes `user.created`
- `order-service` (port `3002`): creates orders, consumes `user.created`, publishes `order.created`
- `payment-service` (port `3003`): consumes `order.created`, publishes `payment.completed`
- `notification-service` (port `3004`): consumes `user.created` and `payment.completed`

Kafka topics:

- `user.created`
- `order.created`
- `payment.completed`

## Repository Standards

- Environment-based config (`.env.example`)
- Health endpoints for all services (`GET /health`)
- CI pipeline for syntax validation on Node 18/20 (`.github/workflows/ci.yml`)
- Contribution and security docs (`CONTRIBUTING.md`, `SECURITY.md`)
- MIT license

## Quick Start

### 1. Install dependencies

```bash
npm run install:all
```

### 2. Configure environment

```bash
cp .env.example .env
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

### 3. Start infrastructure

```bash
npm run infra:up
```

### 4. Create topics

```bash
npm run topics:create
```

### 5. Start services (4 terminals)

```bash
npm run user-service
npm run order-service
npm run payment-service
npm run notification-service
```

### 6. Run end-to-end workflow

```bash
npm run workflow:test
```

## API Endpoints

### User Service (`:3001`)

- `POST /users`
- `GET /users`
- `GET /users/:id`
- `GET /health`

### Order Service (`:3002`)

- `POST /orders`
- `GET /orders`
- `GET /orders/:id`
- `GET /health`

### Payment Service (`:3003`)

- `GET /payments`
- `GET /payments/:orderId`
- `GET /health`

### Notification Service (`:3004`)

- `GET /notifications`
- `GET /notifications/user/:userId`
- `GET /notifications/stats`
- `GET /health`

## Event Flow

1. `POST /users` -> `user-service` saves user and publishes `user.created`
2. `POST /orders` -> `order-service` saves order and publishes `order.created`
3. `payment-service` consumes `order.created`, processes payment, publishes `payment.completed`
4. `notification-service` consumes events and stores notifications

## Useful Commands

```bash
npm run infra:logs   # follow Docker logs
npm run infra:down   # stop infrastructure
npm run check:syntax # validate JS syntax across repo
```

## Project Structure

```text
.
|-- .github/
|   |-- workflows/ci.yml
|   `-- pull_request_template.md
|-- scripts/
|   |-- create-topics.js
|   `-- test-workflow.js
|-- services/
|   |-- user-service/
|   |-- order-service/
|   |-- payment-service/
|   `-- notification-service/
|-- docker-compose.yml
|-- CONTRIBUTING.md
|-- SECURITY.md
`-- README.md
```
