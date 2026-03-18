# Hardware Store — Event-Driven Order Processing System

A production-pattern, event-driven microservices system for processing hardware store orders. Built with **Apache Kafka**, **Spring Boot 3**, and the **Saga choreography pattern** to coordinate distributed transactions across independent services.

---

## Architecture Overview

```
Customer places order (REST API)
           │
           ▼
    [order.placed] ──────────────────────────────────────┐
           │                                             │
           ▼                                             ▼
   PaymentService                              NotificationService
   (charge card)                               (always fires ≤ 5 min)
           │
    [payment.result]
           │
     SUCCESS ──► InventoryService (reserve stock)
     FAILURE ──► CompensationService (notify + fail order)
                        │
                 [inventory.result]
                        │
                  RESERVED ──► ShippingService (book carrier)
                  FAILED   ──► CompensationService (refund + fail)
                                       │
                                [shipping.result]
                                       │
                                BOOKED ──► OrderService (CONFIRMED ✅)
                                FAILED ──► CompensationService (refund + release + fail)
```

### Saga Guarantee
An order is only **CONFIRMED** when all three succeed:
- ✅ Payment charged
- ✅ Inventory reserved
- ✅ Shipping booked

Any failure triggers **automatic compensation** (refund → release inventory → notify customer).

---

## Tech Stack

| Technology | Purpose |
|---|---|
| Spring Boot 3.3 | Application framework |
| Apache Kafka | Event streaming backbone |
| Spring Kafka | Kafka producer/consumer integration |
| Java 21 Virtual Threads | Efficient I/O-bound consumer handling |
| H2 (dev) / PostgreSQL (prod) | Persistence |
| Spring Data JPA | Repository layer |
| Docker Compose | Local infrastructure |

---

## Kafka Topics

| Topic | Producer | Consumers |
|---|---|---|
| `order.placed` | OrderService | PaymentService, NotificationService |
| `payment.result` | PaymentService | InventoryService, NotificationService, CompensationService |
| `inventory.result` | InventoryService | ShippingService, NotificationService, CompensationService |
| `shipping.result` | ShippingService | OrderService, NotificationService, CompensationService |
| `order.confirmed` | OrderService | NotificationService |
| `order.failed` | CompensationService | NotificationService |
| `order.compensation` | CompensationService | PaymentService, InventoryService |
| `*.DLT` | Spring (auto) | DLT Monitor |

All topics use **3 partitions**, keyed by `orderId` — guaranteeing event ordering per order.

---

## Key Patterns

### Saga Choreography
No central orchestrator. Each service reacts to events and publishes results. Failure at any stage triggers a `CompensationEvent` that cascades backwards.

### Idempotent Consumers
Every consumer checks a `processed_events` table before processing. Safe under Kafka's at-least-once delivery — no double charges, no double reservations.

### Notification SLA
Notification service subscribes to every topic independently. A `@Scheduled` job sweeps every 30 seconds to enforce the 5-minute SLA — customer always notified whether order succeeded or failed.

### Dead Letter Topics
Failed messages (after 3 retries with exponential backoff: 1s → 2s → 4s) route to `*.DLT` topics for investigation and replay.

---

## Project Structure

```
src/main/java/com/example/hardware/
├── domain/
│   ├── events/       ← Kafka event records (immutable, past-tense)
│   ├── entities/     ← JPA entities per service
│   └── enums/        ← OrderStatus, PaymentStatus, etc.
├── config/
│   ├── KafkaConfig.java            ← Topics, error handler, DLT
│   └── KafkaTopicsProperties.java  ← Type-safe @ConfigurationProperties
├── order/            ← REST API, order lifecycle
├── payment/          ← Payment processing + storage
├── inventory/        ← Stock reservation + release
├── shipping/         ← Carrier booking + scheduling
├── notification/     ← Customer comms + SLA enforcer
└── compensation/     ← Saga rollback coordinator
```

---

## Quick Start

### Prerequisites
- Java 21
- Docker + Docker Compose
- Maven 3.9+

### 1. Start Kafka

```bash
cd docker
docker compose up -d

# Wait for healthy status
docker compose ps

# Kafka UI available at:
open http://localhost:8080
```

### 2. Start the Application

```bash
mvn spring-boot:run
```

### 3. Place an Order

```bash
curl -X POST http://localhost:8082/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "customerEmail": "john@example.com",
    "customerPhone": "+1-555-0100",
    "items": [
      {
        "productId": "PROD-DRILL-001",
        "productName": "DeWalt 20V Cordless Drill",
        "productCategory": "POWER_TOOLS",
        "quantity": 1,
        "unitPrice": 149.99
      },
      {
        "productId": "PROD-PIPE-010",
        "productName": "3/4 inch Copper Pipe (10ft)",
        "productCategory": "PLUMBING",
        "quantity": 3,
        "unitPrice": 24.99
      }
    ],
    "shippingAddress": "456 Oak Street, Chicago IL 60601"
  }'
```

### 4. Watch the Saga Execute

```bash
# Follow application logs
mvn spring-boot:run 2>&1 | grep -E "(ORDER|PAYMENT|INVENTORY|SHIPPING|NOTIFICATION|COMPENSATION)"

# Or watch in Kafka UI
open http://localhost:8080
# → Topics → order.placed → Messages
# → Consumer Groups → payment-service → Lag
```

---

## Observing Failures and Compensation

The system simulates realistic failures for demo purposes:

```bash
# Place 10 orders — some will fail at random stages
curl -X POST "http://localhost:8082/api/orders/batch?count=10"

# Then in Kafka UI:
# → Topics → order.compensation  (triggered compensations)
# → Topics → order.failed        (failed orders)
# → Topics → order.placed.DLT    (messages that exhausted retries)
# → Consumer Groups → payment-service → see per-partition lag
```

---

## Database (H2 Console)

```
URL:      http://localhost:8082/h2-console
JDBC URL: jdbc:h2:mem:hardware_store
Username: sa
Password: (empty)
```

Tables: `orders`, `order_items`, `payments`, `inventory_items`,
`inventory_reservations`, `shipments`, `notifications`, `processed_events`

---

## Running Tests

```bash
# Unit + integration tests with embedded Kafka (no Docker needed)
mvn test
```

---

## What This Demonstrates

- **Event-Driven Architecture** — services communicate only via Kafka events, never direct HTTP calls
- **Saga Pattern (Choreography)** — distributed transactions without a central coordinator
- **Compensating Transactions** — automatic rollback on partial failures
- **Idempotent Consumers** — safe message redelivery under at-least-once semantics
- **Dead Letter Topics** — failed message handling and observability
- **Notification SLA** — scheduler-enforced 5-minute customer notification guarantee
- **Virtual Threads** — Java 21 efficient concurrency for I/O-bound Kafka consumers
- **Type-safe Configuration** — `@ConfigurationProperties` over scattered `@Value`

---

## License

MIT