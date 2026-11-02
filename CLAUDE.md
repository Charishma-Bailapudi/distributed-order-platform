# Agent Guidelines & Project Operating System: Distributed Order Processing Platform

## Core Philosophy: Specification-Driven Development (SDD)

1. **Never generate application code before specifications are finalized and approved.**
2. **Pedagogical focus:** This project is built to deeply learn distributed systems, Kafka internals, caching, concurrency, and load testing. Every failure mode must be observable before being fixed.
3. **No assumed numbers:** Performance claims (throughput, p50/p95/p99) must come from executed k6 scripts logged in `LEARNINGS.md`.

## System Architecture Summary

- **Architecture:** 3 independent Spring Boot microservices (`order-service`, `payment-service`, `inventory-service`).
- **Communication:** Purely asynchronous via Apache Kafka; no direct REST calls between services.
- **Saga Pattern:** Choreography-based with semantic locking (intermediate states: `PENDING`, `AUTHORIZING`, `RESERVING`).
- **Partition Key Strategy:** Keyed by `orderId` (UUID) across all topics (4 partitions) to deliberately allow concurrent customer orders to race on wallet balance.
- **Database Isolation:** PostgreSQL schemas (`orders_schema`, `payments_schema`, `inventory_schema`). No cross-schema queries or foreign keys.
- **Testing:** Testcontainers for integration tests (PostgreSQL + Kafka); k6 for performance and concurrency benchmarks.

## Repository Layout

- `docs/specs/`: Requirements, architecture, event contracts, and test plans.
- `load-tests/`: Versioned k6 load, stress, and race-condition scripts.
- `LEARNINGS.md`: Living benchmark and experiment observation log.
- `order-service/`, `payment-service/`, `inventory-service/`: Microservice roots.

## Git Branching Conventions

- Main branch: `main` (clean, verified milestone code).
- Feature branches: `feat/<phase-or-feature-name>` (e.g., `feat/specs-step-1-and-2`).
- Experiment branches: `exp/<experiment-name>` (e.g., `exp/hashmap-concurrency-race`).
