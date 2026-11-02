# 03 - Event Contract Specifications

## 1. Standard Event Envelope Pattern

Every event emitted across Kafka in this system wraps its domain data inside a standard envelope. This decouples event routing and tracing metadata from the business payload.

### Envelope Structure
```json
{
  "eventId": "UUID string (unique per emitted message instance)",
  "eventType": "String (e.g., OrderCreated, PaymentCompleted)",
  "aggregateId": "UUID string (always orderId)",
  "timestamp": "ISO-8601 UTC timestamp",
  "version": 1,
  "payload": { ... }
}
```

### Kafka Headers & Transport Rules
- **Kafka Message Key:** `aggregateId` (`orderId`). Used consistently to route an order's lifecycle through partition hashing.
- **Header `X-Correlation-Id`:** Propagated across all services for distributed trace continuity.
- **Header `X-Idempotency-Key`:** Set to `orderId` to enable exact-once deduplication checks on consumers.

---

## 2. Topic Event Schemas & Routing Matrix

### 2.1 Topic: `order-events`

#### Event: `OrderCreated`
- **Producer:** `order-service`
- **Consumer:** `payment-service`
- **Purpose:** Initiates the payment authorization step upon order placement.

```json
{
  "eventId": "a1b2c3d4-0001-4000-8000-000000000001",
  "eventType": "OrderCreated",
  "aggregateId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "timestamp": "2026-11-02T14:00:00Z",
  "version": 1,
  "payload": {
    "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "customerId": "cust-101",
    "productId": "prod-001",
    "quantity": 2,
    "totalAmount": 6000.00
  }
}
```

---

### 2.2 Topic: `payment-events`

#### Event: `PaymentCompleted`
- **Producer:** `payment-service`
- **Consumers:**
  - `inventory-service` (advances the Saga to stock reservation)
  - `order-service` (records payment state for audit)
- **Purpose:** Signals successful wallet balance deduction and triggers stock reservation.

```json
{
  "eventId": "a1b2c3d4-0002-4000-8000-000000000002",
  "eventType": "PaymentCompleted",
  "aggregateId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "timestamp": "2026-11-02T14:00:01Z",
  "version": 1,
  "payload": {
    "paymentId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
    "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "customerId": "cust-101",
    "amount": 6000.00,
    "remainingWalletBalance": 4000.00
  }
}
```

#### Event: `PaymentFailed`
- **Producer:** `payment-service`
- **Consumer:** `order-service`
- **Purpose:** Signals payment failure (insufficient balance or simulated decline); terminates the Saga and marks the order as `CANCELLED`.

```json
{
  "eventId": "a1b2c3d4-0003-4000-8000-000000000003",
  "eventType": "PaymentFailed",
  "aggregateId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "timestamp": "2026-11-02T14:00:01Z",
  "version": 1,
  "payload": {
    "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "customerId": "cust-102",
    "amount": 6000.00,
    "reason": "INSUFFICIENT_FUNDS"
  }
}
```
*Valid reasons:* `INSUFFICIENT_FUNDS`, `GATEWAY_DECLINED`, `ACCOUNT_SUSPENDED`.

---

### 2.3 Topic: `inventory-events`

#### Event: `InventoryReserved`
- **Producer:** `inventory-service`
- **Consumer:** `order-service`
- **Purpose:** Signals that stock has been successfully allocated; marks the order as `CONFIRMED` and completes the happy path.

```json
{
  "eventId": "a1b2c3d4-0004-4000-8000-000000000004",
  "eventType": "InventoryReserved",
  "aggregateId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "timestamp": "2026-11-02T14:00:02Z",
  "version": 1,
  "payload": {
    "reservationId": "d3b07384-d113-4672-8869-79f9446d3ea0",
    "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "productId": "prod-001",
    "quantityReserved": 2
  }
}
```

#### Event: `InventoryReservationFailed`
- **Producer:** `inventory-service`
- **Consumer:** `order-service`
- **Purpose:** Signals that stock reservation failed (e.g., out of stock or simulated rejection); notifies `order-service` that compensation has been initiated.

```json
{
  "eventId": "a1b2c3d4-0005-4000-8000-000000000005",
  "eventType": "InventoryReservationFailed",
  "aggregateId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "timestamp": "2026-11-02T14:00:02Z",
  "version": 1,
  "payload": {
    "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "productId": "prod-002",
    "requestedQuantity": 2,
    "reason": "OUT_OF_STOCK"
  }
}
```
*Valid reasons:* `OUT_OF_STOCK`, `PRODUCT_DISCONTINUED`, `ALLOCATION_TIMEOUT`.

---

### 2.4 Topic: `compensation-events`

#### Event: `PaymentRefundRequested`
- **Producer:** `inventory-service`
- **Consumer:** `payment-service`
- **Purpose:** Triggers the compensating transaction in `payment-service` to refund the deducted amount back to the customer's wallet balance.

```json
{
  "eventId": "a1b2c3d4-0006-4000-8000-000000000006",
  "eventType": "PaymentRefundRequested",
  "aggregateId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "timestamp": "2026-11-02T14:00:02Z",
  "version": 1,
  "payload": {
    "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "reason": "INVENTORY_ALLOCATION_FAILED"
  }
}
```

#### Event: `PaymentRefunded`
- **Producer:** `payment-service`
- **Consumer:** `order-service`
- **Purpose:** Confirms the refund has completed successfully; transitions the order state from `PENDING` to `CANCELLED`.

```json
{
  "eventId": "a1b2c3d4-0007-4000-8000-000000000007",
  "eventType": "PaymentRefunded",
  "aggregateId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "timestamp": "2026-11-02T14:00:03Z",
  "version": 1,
  "payload": {
    "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "refundedAmount": 6000.00
  }
}
```

#### Event: `PaymentRefundFailed`
- **Producer:** `payment-service`
- **Consumer:** `order-service`
- **Purpose:** Signals that the refund attempt failed (the dual-failure scenario); causes `order-service` to transition the order to `ORDER_REQUIRES_MANUAL_REVIEW`.

```json
{
  "eventId": "a1b2c3d4-0008-4000-8000-000000000008",
  "eventType": "PaymentRefundFailed",
  "aggregateId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "timestamp": "2026-11-02T14:00:04Z",
  "version": 1,
  "payload": {
    "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "amount": 6000.00,
    "failureReason": "LEDGER_LOCK_TIMEOUT"
  }
}
```

---

### 2.5 Topic: `saga-dlq`

#### Dead Letter Queue Envelope
- **Producer:** Any service (`order-service`, `payment-service`, `inventory-service`)
- **Consumer:** Operator Inspection / DLQ Dashboard
- **Purpose:** Holds poison pills, malformed JSON events, or compensating transactions that exhausted their retry limits.

```json
{
  "failedEventId": "a1b2c3d4-0006-4000-8000-000000000006",
  "sourceTopic": "compensation-events",
  "consumerGroup": "payment-service-group",
  "failureTime": "2026-11-02T14:00:05Z",
  "exceptionClass": "org.springframework.dao.CannotAcquireLockException",
  "exceptionMessage": "Lock wait timeout exceeded; try restarting transaction",
  "retryCount": 3,
  "rawPayload": "{\"eventId\":\"a1b2c3d4-0006-4000-8000-000000000006\",\"eventType\":\"PaymentRefundRequested\",\"aggregateId\":\"f47ac10b-58cc-4372-a567-0e02b2c3d479\",\"payload\":{\"orderId\":\"f47ac10b-58cc-4372-a567-0e02b2c3d479\"}}"
}
```

---

## 3. Idempotency & Deduplication Specification

### 3.1 Deduplication Table Contract
Every service maintains an internal deduplication table in its own private schema:

```sql
CREATE TABLE <service_schema>.processed_events (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_events_aggregate ON <service_schema>.processed_events(aggregate_id);
```

### 3.2 Processing Algorithm
1. Consumer pulls an event record from a Kafka partition.
2. Inside the local database transaction:
   - Check if `event_id` exists in `<service_schema>.processed_events`.
   - **If found:** Commit the Kafka offset immediately and drop the message (idempotent skip).
   - **If not found:**
     1. Insert the `event_id` into `processed_events`.
     2. Execute the local domain mutation (e.g., wallet balance deduction, stock decrement).
     3. Commit the database transaction.
3. Commit the Kafka consumer offset.
4. If an unhandled exception or crash occurs before the database commits, the entire transaction rolls back, preventing partial updates, and Kafka will redeliver the event.