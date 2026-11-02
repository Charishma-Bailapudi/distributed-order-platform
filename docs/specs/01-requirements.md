# 01 - Product & Domain Requirements Specification

## 1. High-Level Idea
A Distributed Order Processing Platform built with Java 17, Spring Boot, and Apache Kafka. The system simulates an e-commerce order flowing through independent services without a shared database, coordinating via Kafka using the Saga pattern (Choreography). 

The platform is designed to learn distributed systems concepts hands-on:
- Concurrency and thread safety
- Kafka internals (partitions, offsets, consumer lag)
- Caching strategies and failure modes
- System resilience and compensating transactions
- Load testing with measurable before/after metrics

---

## 2. Actors
- **Customer:** An entity with an ID, name, and wallet balance (simulated currency). Can place multiple orders concurrently.
- **System Services (Autonomous):**
  - `order-service`
  - `payment-service`
  - `inventory-service`
- **Operator:** Conceptual role inspecting order health and dead-letter queues.

---

## 3. Core Domain Entities & Ownership
- **Customer & Wallet:** Owned exclusively by `payment-service`. Holds `walletBalance`.
- **Product & Stock:** Owned exclusively by `inventory-service`. Holds `availableStock`.
- **Order:** Owned exclusively by `order-service`. Contains customer reference, product reference, quantity, total amount, and lifecycle state.
- **Payment:** Owned by `payment-service`. Records charges and refunds tied to an order.
- **Inventory Reservation:** Owned by `inventory-service`. Records stock allocations tied to an order.

---

## 4. Key Rules & Constraints
1. **Single-Line Items:** Baseline orders are strictly single-product-line items. Multi-line atomicity is deferred to an advanced phase.
2. **Immutability:** Once created, an order cannot be modified—only transitioned through lifecycle states.
3. **Partition Key:** Kafka messages are keyed by `orderId`. This allows concurrent orders for the same customer to land on different partitions and process simultaneously, deliberately exposing concurrency races on wallet balances.
4. **No Cross-Service Direct Calls:** Services communicate strictly via Kafka events. No direct REST calls between services.
5. **Fallible Compensations:** If an inventory reservation fails, a refund is requested. If the refund itself fails, the order transitions to `ORDER_REQUIRES_MANUAL_REVIEW` and routes to a Dead Letter Queue (DLQ).