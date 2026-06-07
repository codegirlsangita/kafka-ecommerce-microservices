# Quick Start

## 1. Install dependencies

```bash
npm run install:all
```

## 2. Create `.env`

```bash
cp .env.example .env
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

## 3. Start infrastructure (Kafka, Zookeeper, MongoDB, Kafka UI)

```bash
npm run infra:up
```

## 4. Create Kafka topics

```bash
npm run topics:create
```

## 5. Start services in separate terminals

```bash
npm run user-service
npm run order-service
npm run payment-service
npm run notification-service
```

## 6. Run workflow test

```bash
npm run workflow:test
```

## 7. Check health endpoints

- http://localhost:3001/health
- http://localhost:3002/health
- http://localhost:3003/health
- http://localhost:3004/health

## 8. Monitor Kafka

- Kafka UI: http://localhost:8080

## Stop infrastructure

```bash
npm run infra:down
```
