# Mega-Traffic Order API — 마스터 스펙 문서 (v2)

> 대형 카페(메가커피 스타일) 주문/결제 백엔드. 동시 주문 상황에서 **재고·포인트 정합성**을 지키고, 비동기 분산 흐름에서 **보상 트랜잭션(Saga)** 과 **멱등성**을 구현하는 것이 목표.

---

## 1. 프로젝트 개요

### 1.1 목표

| 구분 | 내용 |
|---|---|
| 핵심 문제 | 동시 주문이 몰릴 때 재고 초과 판매, 포인트 중복 차감, 메시지 중복/유실로 인한 데이터 불일치 |
| 해결 방향 | 단계별 로컬 트랜잭션 + 이벤트 기반 Saga, 보상 트랜잭션, Transactional Outbox, 멱등성 컨슈머 |
| 기간 | 2주 |

### 1.2 성공 기준 (검증 가능한 지표)

"초당 N건 처리" 같은 처리량 목표 대신, 로컬 환경에서도 검증 가능한 **정합성 지표**를 기준으로 삼는다.

- 동시 사용자 X명이 재고 N개 상품을 주문할 때 **초과 판매 0건**, **재고 음수 0건**
- 결제 실패 시 **차감된 재고가 100% 복구**
- 같은 메시지가 중복 전달돼도 **재고/포인트 변경은 1회만** 반영
- 같은 주문 요청이 재전송돼도 **주문은 1건만** 생성
- 처리량(TPS)과 응답 시간은 부하 테스트에서 **측정값과 테스트 조건을 함께** 기록 (목표치로 선언하지 않음)

### 1.3 아키텍처: 모듈러 모놀리스

- 초기부터 물리적 MSA로 나누면 네트워크·인프라 관리 비용이 커지므로, **하나의 애플리케이션 안에서 도메인 패키지를 엄격히 분리**한다.
- 도메인 간 호출은 **직접 메서드 호출이 아닌 이벤트(RabbitMQ)** 로만 한다. 향후 서버 분리 시 메시징 경계를 그대로 유지할 수 있다.
- 단일 MySQL을 사용하지만, **각 Saga 단계는 독립된 로컬 트랜잭션으로 커밋**한다. 그래야 앞 단계를 되돌리는 보상 트랜잭션이 실제로 의미를 가진다.

---

## 2. 기술 스택

| 분류 | 기술 |
|---|---|
| Language / Framework | Java 17, Spring Boot 3.x, Spring Data JPA |
| Database | MySQL 8 (Docker Compose) |
| Cache | Redis (상품 정보 캐시) — 동시성 비교 실험 시 Redisson 분산 락 |
| Message Queue | RabbitMQ (Saga 이벤트, 재시도, DLQ) |
| Load Test | k6 |
| 실행 환경 | 로컬 Docker Compose (MySQL, Redis, RabbitMQ) |

**범위 제외 (2주 이후 확장)**: Nginx/ALB, AWS 배포, Prometheus/Grafana, nGrinder/JMeter

---

## 3. REST API 명세

| 기능 | Method | Endpoint | 설명 |
|---|---|---|---|
| 테스트 데이터 | POST | `/api/v1/users/{userId}/points` | 포인트 충전 |
| 테스트 데이터 | POST | `/api/v1/products` | 상품 및 초기 재고 등록 |
| 포인트 조회 | GET | `/api/v1/users/{userId}/points` | 테스트 결과 검증용 잔액 조회 |
| 상품 조회 | GET | `/api/v1/products/{productId}` | 상품 정보 조회 (이름/가격은 캐시, **재고는 DB 조회**) |
| 주문 접수 | POST | `/api/v1/orders` | 주문 생성 후 `PENDING` 저장, 이후 결제는 비동기 처리 |
| 주문 상태 | GET | `/api/v1/orders/{orderId}/status` | 비동기 처리 결과 폴링 |

> 기존 `POST /api/v1/payments`는 제거. 결제는 클라이언트가 호출하지 않고 **이벤트로 진행**된다.

### 3.1 주문 접수 상세

**Request**

```
POST /api/v1/orders
Idempotency-Key: 7f3c2a1e-...   (클라이언트가 요청마다 생성하는 UUID)
```

```json
{
  "userId": 1,
  "items": [
    { "productId": 7, "quantity": 2 },
    { "productId": 9, "quantity": 1 }
  ]
}
```

**Response `202 Accepted`**

```json
{
  "orderId": 123,
  "status": "PENDING"
}
```

- 같은 `Idempotency-Key`로 재요청이 오면 새 주문을 만들지 않고 **기존 주문을 그대로 반환**한다.
- 주문 금액은 클라이언트 값이 아닌 **서버에서 상품 가격으로 계산**한다.

### 3.2 주문 상태 응답

```json
{
  "orderId": 123,
  "status": "CANCELED",
  "cancelReason": "INSUFFICIENT_POINT"
}
```

---

## 4. 데이터베이스 설계 (DBML)

```dbml
Table users {
  id bigint [pk, increment]
  name varchar(50) [not null]
  point_balance decimal(12,2) [not null, default: 0]
  created_at datetime [not null]
}

Table products {
  id bigint [pk, increment]
  name varchar(100) [not null]
  price decimal(12,2) [not null]
  stock_quantity int [not null]
  created_at datetime [not null]
}

Table orders {
  id bigint [pk, increment]
  user_id bigint [not null]
  idempotency_key varchar(64) [not null, unique]
  total_price decimal(12,2) [not null]
  status varchar(20) [not null]         // PENDING, STOCK_RESERVED, COMPLETED, CANCELED
  cancel_reason varchar(50)             // OUT_OF_STOCK, INSUFFICIENT_POINT
  created_at datetime [not null]
  updated_at datetime [not null]
}

Table order_items {
  id bigint [pk, increment]
  order_id bigint [not null]
  product_id bigint [not null]
  quantity int [not null]
  unit_price decimal(12,2) [not null]   // 주문 시점 가격 스냅샷
}

Table payments {
  id bigint [pk, increment]
  order_id bigint [not null, unique]
  amount decimal(12,2) [not null]
  status varchar(20) [not null]         // SUCCESS, FAILED
  failed_reason varchar(255)
  created_at datetime [not null]
}

Table outbox_events {
  id bigint [pk, increment]
  event_id varchar(36) [not null, unique]  // UUID, 컨슈머 멱등성 키로 사용
  aggregate_type varchar(30) [not null]    // ORDER, STOCK, PAYMENT
  aggregate_id bigint [not null]
  event_type varchar(50) [not null]
  payload json [not null]
  status varchar(20) [not null]            // READY, PUBLISHED
  created_at datetime [not null]
  published_at datetime
}

Table processed_events {
  id bigint [pk, increment]
  consumer varchar(50) [not null]          // 예: STOCK_RESERVE, STOCK_RESTORE
  event_id varchar(36) [not null]
  processed_at datetime [not null]

  indexes {
    (consumer, event_id) [unique]
  }
}

Ref: orders.user_id > users.id
Ref: order_items.order_id > orders.id
Ref: order_items.product_id > products.id
Ref: payments.order_id - orders.id
```

**설계 포인트**

- `orders.idempotency_key` unique: API 중복 요청 방어
- `processed_events (consumer, event_id)` unique: 컨슈머 중복 처리 방어. **같은 이벤트를 여러 컨슈머가 받을 수 있으므로** `event_id` 단독이 아니라 컨슈머 이름과 묶는다
- `order_items.unit_price`: 주문 이후 상품 가격이 바뀌어도 주문 금액이 흔들리지 않도록 스냅샷

---

## 5. Saga 흐름 (Choreography)

### 5.1 단계와 보상

| 단계 | 담당 도메인 | 로컬 트랜잭션 | 성공 이벤트 | 실패 이벤트 | 보상 |
|---|---|---|---|---|---|
| 1. 주문 생성 | order | 주문 `PENDING` 저장 | `OrderCreated` | - | - |
| 2. 재고 예약 | product | 주문 상품 전체 재고 차감 | `StockReserved` | `StockReserveFailed` | 없음 (한 트랜잭션에서 전부 롤백) |
| 3. 포인트 결제 | payment | 포인트 차감 + 결제 기록 | `PaymentCompleted` | `PaymentFailed` | **재고 복구** |
| 4. 주문 확정 | order | 주문 `COMPLETED` | - | - | - |

- 2단계는 이미 커밋된 상태에서 3단계가 실패할 수 있다. 이때 **재고 복구가 보상 트랜잭션**이 된다.
- 재고 복구 수량은 `order_items`를 기준으로 계산한다.

### 5.2 성공 흐름

```mermaid
sequenceDiagram
    autonumber
    actor Client as 고객
    participant Order as order
    participant MQ as RabbitMQ
    participant Stock as product(재고)
    participant Pay as payment

    Client->>Order: POST /orders (Idempotency-Key)
    Order->>Order: [TX] 주문 PENDING + Outbox(OrderCreated)
    Order-->>Client: 202 Accepted
    Order->>MQ: Outbox Relay가 OrderCreated 발행

    MQ->>Stock: OrderCreated
    Stock->>Stock: [TX] processed_events + 재고 차감 + Outbox(StockReserved)
    Stock->>MQ: StockReserved

    MQ->>Pay: StockReserved
    Pay->>Pay: [TX] processed_events + 포인트 차감 + 결제 SUCCESS + Outbox(PaymentCompleted)
    Pay->>MQ: PaymentCompleted

    MQ->>Order: PaymentCompleted
    Order->>Order: [TX] processed_events + 주문 COMPLETED
```

### 5.3 실패 흐름 (포인트 부족 → 보상)

```mermaid
sequenceDiagram
    autonumber
    participant Order as order
    participant MQ as RabbitMQ
    participant Stock as product(재고)
    participant Pay as payment

    MQ->>Stock: OrderCreated
    Stock->>Stock: [TX] 재고 차감 (커밋 완료)
    Stock->>MQ: StockReserved

    MQ->>Pay: StockReserved
    Pay->>Pay: [TX] 포인트 부족 → 결제 FAILED + Outbox(PaymentFailed)
    Pay->>MQ: PaymentFailed

    par 보상 트랜잭션
        MQ->>Stock: PaymentFailed
        Stock->>Stock: [TX] processed_events + 재고 복구
    and 주문 취소
        MQ->>Order: PaymentFailed
        Order->>Order: [TX] processed_events + 주문 CANCELED (INSUFFICIENT_POINT)
    end
```

재고 부족(`StockReserveFailed`)인 경우에는 되돌릴 커밋이 없으므로 주문만 `CANCELED (OUT_OF_STOCK)` 처리한다.

---

## 6. 데이터 정합성 설계

### 6.1 이벤트 유실 방지 — Transactional Outbox

- 문제: DB 커밋 후 MQ 발행 전에 서버가 죽으면 이벤트가 사라진다.
- 해결: 비즈니스 데이터와 `outbox_events`를 **같은 트랜잭션**에 저장하고, 별도 Relay(`@Scheduled` 폴링)가 `READY` 이벤트를 발행한 뒤 `PUBLISHED`로 바꾼다.
- 결과: 발행이 실패하거나 Relay가 두 번 발행할 수 있다 → **중복은 6.2에서 방어**한다.

### 6.2 중복 처리 방지 — 멱등성 컨슈머

- 문제: RabbitMQ는 at-least-once 전달이라 같은 메시지가 두 번 올 수 있다.
- 해결: 컨슈머는 한 트랜잭션 안에서
  1. `processed_events`에 `(consumer, event_id)` INSERT
  2. 비즈니스 로직 수행
- 같은 이벤트가 다시 오면 unique 제약 위반 → 롤백 후 건너뛴다.
- **Redis(SET NX)가 아닌 MySQL에 기록하는 이유**: Redis 키 저장과 MySQL 재고 변경은 한 트랜잭션으로 묶이지 않는다. 처리 도중 서버가 죽으면 Redis 키만 남아 재전송된 메시지가 무시되고, 재고는 영영 반영되지 않는다. 같은 DB에 두면 둘이 함께 커밋되거나 함께 롤백된다.

### 6.3 메시지 처리 실패 — ack / 재시도 / DLQ

- 컨슈머는 **처리 완료 후 ack** 한다. 처리 중 서버가 죽으면 ack가 가지 않아 RabbitMQ가 재전송한다.
- 일시적 오류는 재시도 N회 후 **DLQ(Dead Letter Queue)** 로 이동해 수동 확인한다.
- 잔액 부족·재고 부족은 재시도 대상이 아닌 **비즈니스 실패**로 보고 실패 이벤트를 발행한다.

### 6.4 API 중복 요청 방지

- `Idempotency-Key` 헤더를 `orders.idempotency_key`(unique)에 저장한다.
- 중복 INSERT 시 기존 주문을 조회해 같은 응답을 돌려준다.

### 6.5 재고·포인트 동시성 제어

컨슈머가 여러 스레드로 동작하면 같은 상품 재고를 동시에 차감할 수 있다. 아래 방식들을 **같은 조건에서 k6로 비교**한 뒤 최종 방식을 결정하고 근거를 기록한다.

| 방식 | 개요 |
|---|---|
| 원자적 조건부 UPDATE (기본안) | `UPDATE ... SET stock = stock - ? WHERE id = ? AND stock >= ?`, 영향받은 행 수로 성공 판단 |
| 비관적 락 | `SELECT ... FOR UPDATE` 후 차감 |
| 낙관적 락 | `@Version` + 충돌 시 재시도 |
| Redisson 분산 락 | 상품 ID 단위 락 획득 후 차감 |

- 비교 지표: 초과 판매 건수, 처리 시간, 실패율, 락 대기/충돌 횟수
- 포인트 차감도 같은 방식(조건부 UPDATE: 잔액 ≥ 금액)으로 음수 잔액을 막는다.

### 6.6 캐시 정책

- 캐시 대상: 상품 이름·가격 (Cache-Aside, TTL 적용)
- **재고는 캐시하지 않는다.** 결제마다 바뀌는 값이라 캐시하면 정합성이 깨진다.

---

## 7. 시스템 아키텍처

```mermaid
graph TB
    subgraph ClientLayer["클라이언트"]
        App["사용자 앱 / 키오스크"]
        K6["k6 부하 테스트"]
    end

    subgraph AppLayer["Spring Boot (모듈러 모놀리스)"]
        OrderMod["order"]
        StockMod["product (재고)"]
        PayMod["payment (포인트)"]
        Relay["Outbox Relay"]
    end

    subgraph DataLayer["데이터 계층"]
        MySQL[("MySQL<br/>비즈니스 데이터 / outbox / processed_events")]
        Redis[("Redis<br/>상품 정보 캐시")]
    end

    subgraph MQLayer["메시징"]
        RabbitMQ["RabbitMQ<br/>Saga 이벤트 / DLQ"]
    end

    App --> OrderMod
    K6 --> OrderMod
    OrderMod --> MySQL
    StockMod --> MySQL
    PayMod --> MySQL
    OrderMod --> Redis
    Relay -->|READY 이벤트 조회| MySQL
    Relay -->|발행| RabbitMQ
    RabbitMQ -->|구독| OrderMod
    RabbitMQ -->|구독| StockMod
    RabbitMQ -->|구독| PayMod
```

---

## 8. 패키지 구조

```
com.mega.order
 ┣ MegaApplication.java
 ┣ global
 ┃  ┣ config          // Redis, RabbitMQ(Exchange/Queue/DLQ), JPA
 ┃  ┣ exception       // 전역 예외 처리
 ┃  ┣ outbox          // OutboxEvent 엔티티, 저장 헬퍼, Relay 스케줄러
 ┃  ┗ idempotency     // ProcessedEvent 엔티티, 중복 체크 헬퍼
 ┣ user               // 회원, 포인트 조회/충전
 ┣ product            // 상품, 재고 예약/복구
 ┃  ┗ messaging       // OrderCreated, PaymentFailed 컨슈머
 ┣ order              // 주문 접수, 상태 조회, 확정/취소
 ┃  ┗ messaging       // PaymentCompleted, PaymentFailed, StockReserveFailed 컨슈머
 ┗ payment            // 포인트 차감, 결제 기록
    ┗ messaging       // StockReserved 컨슈머
```

- 각 도메인은 `controller / service / repository / entity`를 가진다.
- **도메인 간 서비스 직접 호출 금지.** 다른 도메인에 영향을 주는 작업은 이벤트로만 전달한다.

---

## 9. 2주 일정

| 주차 | 일정 | 산출물 |
|---|---|---|
| 1주차 | Day 1–2 | Docker Compose, 엔티티/ERD, 테스트 데이터 API |
| | Day 3–4 | 주문 접수 + Idempotency-Key, 재고 차감 동시성 방식 4종 구현 |
| | Day 5–7 | k6 동시성 비교 테스트, 방식 결정 및 근거 기록 |
| 2주차 | Day 8–9 | RabbitMQ 연결, Outbox + Relay, Saga 성공 흐름 |
| | Day 10–11 | 실패 흐름 + 보상 트랜잭션, 멱등성 컨슈머, 재시도/DLQ |
| | Day 12–13 | 정합성 검증 테스트 (중복 메시지 주입, 서버 강제 종료, 포인트 부족 시나리오) |
| | Day 14 | README, 트러블슈팅 정리, 회고 |

### 확장 과제 (시간이 남으면)

- `PENDING` 상태로 오래 멈춘 주문 타임아웃 처리
- 처리 완료된 `outbox_events`, `processed_events` 정리 배치
- Prometheus/Grafana 모니터링, AWS 배포