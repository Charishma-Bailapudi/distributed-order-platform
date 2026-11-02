# 02 - Architecture & Service Boundaries Specification

## 1. Bounded Context Isolation

- **`order-service`** connects to schema `orders_schema`. Stores order status and initiates Sagas.
- **`payment-service`** connects to schema `payments_schema`. Stores customer wallet balances, payment records, and ledger history.
- **`inventory-service`** connects to schema `inventory_schema`. Stores product catalog, stock levels, and reservation status.

No database cross-reads, joins, or shared database credentials are permitted.

---

## 2. Event Choreography & Kafka Topology

- **Topic Partitions:** 4 partitions per topic.
- **Message Key:** `orderId` (UUID string).

### Topic Routing Matrix

| Topic Name            | Producer            | Consumer                             | Purpose                                                       |
| --------------------- | ------------------- | ------------------------------------ | ------------------------------------------------------------- |
| `order-events`        | `order-service`     | `payment-service`                    | Publishes `OrderCreated`                                      |
| `payment-events`      | `payment-service`   | `inventory-service`, `order-service` | Publishes `PaymentCompleted` or `PaymentFailed`               |
| `inventory-events`    | `inventory-service` | `order-service`, `payment-service`   | Publishes `InventoryReserved` or `InventoryReservationFailed` |
| `compensation-events` | `inventory-service` | `payment-service`                    | Publishes `PaymentRefundRequested`                            |
| `saga-dlq`            | Any service         | Operator inspection                  | Captures poison messages and compensation failures            |

---

## 3. Finite State Machines

### Order Lifecycle (`order-service`)

- `PENDING`: Created upon incoming client request.
- `CONFIRMED`: Transitioned upon receiving `InventoryReserved`.
- `CANCELLED`: Transitioned upon receiving `PaymentFailed` or after receiving confirmation that compensation completed.
- `ORDER_REQUIRES_MANUAL_REVIEW`: Transitioned if a compensating refund fails or routes to DLQ.

### Payment Lifecycle (`payment-service`)

- `AUTHORIZING` -> `CHARGED` (balance deducted)
- `AUTHORIZING` -> `FAILED` (insufficient balance or simulated decline)
- `CHARGED` -> `REFUNDING` -> `REFUNDED` (compensation succeeded)
- `CHARGED` -> `REFUNDING` -> `REFUND_FAILED` (compensation failed, sent to DLQ)

### Inventory Lifecycle (`inventory-service`)

- `RESERVING` -> `RESERVED` (stock decremented)
- `RESERVING` -> `FAILED` (out of stock or simulated rejection)
