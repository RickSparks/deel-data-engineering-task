## Data Engineering Take-Home Task

### Welcome

Welcome to Deel's Data Engineering Take-Home task, as mentioned in the Task specification document, this is the pre-built stack that will help you on your solution development. This repository contains a pre-configured database containing the database represented by the following DER:


![Database Diagram](./diagrams/database-diagram.png)


### Database Configuration

Once you have [Docker](https://www.docker.com/products/docker-desktop/) and [docker-compose](https://docs.docker.com/compose/install/) configured in your computer, with your Docker engine running, you must execute the following command provision the source database:


> docker-compose up


:warning:**Important**: Before running this command make sure you're in the root folder of the project.

Once you have the Database up and running feel free to connect to this using any tool you want, for this you can use the following credentials:

- **Username**: `finance_db_user`
- **Password**: `1234`
- **Database**: `finance_db`

### Debezium CDC

The stack includes a Debezium CDC pipeline that streams database changes to Kafka in real-time. Kafka is available at `localhost:9092`.

#### Topics

| Kafka Topic | Source Table |
|---|---|
| `finance_db.operations.customers` | `operations.customers` |
| `finance_db.operations.products` | `operations.products` |
| `finance_db.operations.orders` | `operations.orders` |
| `finance_db.operations.order_items` | `operations.order_items` |

#### Kafka Connection Example

```properties
bootstrap.servers=localhost:9092
```

Extra informations and tips about the task execution can be found in the task description document shared by our recruiting team.

For any questions, feel free to reach us out through data-platform@deel.com


---

# Ricardo's Solution on the Take-Home Task

## Table of Contents

- [Getting Started](#getting-started)
- [Solution Overview](#solution-overview)
- [Architecture Overview](#architecture-overview)
- [Data Model](#data-model)
- [API](#api)
- [Decisions & Considerations](#decisions--considerations)
- [Limitations & Future Improvements](#limitations--future-improvements)


---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/RickSparks/deel-data-engineering-task.git
cd deel-data-engineering-task
```

### 2. Start the full stack

```bash
docker compose up --build -d
```

### 3. Register the Debezium connector

The Debezium connector must be registered once after the stack starts. This tells Debezium which tables to watch and which Kafka topics to publish changes to:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "finance-db-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "transactions-db",
      "database.port": "5432",
      "database.user": "finance_db_user",
      "database.password": "1234",
      "database.dbname": "finance_db",
      "database.server.name": "finance_db",
      "table.include.list": "operations.customers,operations.products,operations.orders,operations.order_items",
      "plugin.name": "pgoutput",
      "topic.prefix": "finance_db",
      "slot.name": "debezium_slot",
      "publication.name": "debezium_publication",
      "topic.creation.enable": "true",
      "topic.creation.default.replication.factor": "1",
      "topic.creation.default.partitions": "1"
    }
  }'
```

Verify it is running:

```bash
curl http://localhost:8083/connectors/finance-db-connector/status
```

You should see `"state":"RUNNING"` in the response. 

### 4. Verify the stack is healthy

```bash
docker compose ps
```

All services should show `Up`. If that's so, the Solution I implemented is working on your side.
The `analytics-db` and `kafka` services include health checks.

### 5. Test the API

```bash
curl http://localhost:8000/health
```

Interactive API documentation is available at: **http://localhost:8000/docs**



---

## Solution Overview

This problem required a streaming solution, given the streaming mode of data arrival. Initially, I thought I would have to implement the CDC part and the producer, but it was already there, in the code repository I forked.

Given that, I focused on the downstream part of the problem: implementing a kafka consumer that would rout the topics to the appropriate database tables. For that, I created the utils library to store the create statements for the raw schema and its tables. Also, I included the upsert logic for each table in that library.

After the analytics layer was ready, I built four api endpoints to query the business queries available on the exercise description. I had to turn the order status into a parameter, since otherwise nothing would be returned, for the tests I did, I only saw order_status as COMPLETED, not others.

Then, I dockerised everything as requested.

I used AI to help me with this exercise solution: most of the docker implementation and the debug of issues setting up the docker compose was assisted by Claude. It also helped me troubleshoot 


---

## Architecture Overview

Bellow is a diagram of the architecture implemented for this solution, including part of the initial design by the Deel team:


![Architecture Diagram](./diagrams/architecture-diagram.png)


--- 



## Data Model

### Three-Layer Architecture

Data flows through three schemas in the analytical database, following the Medallion Architecture:

- raw schema: is the landing zone, written by the kafka consumer. There's one table per source table to which the _op and _ingested at metadata columns where added, to track the type of CDC operations Debezium sends and also the time of ingestion
- staging:
- analytics: the final, analytics-ready gold layer. I'll describe it shortly in more detail, but it implements.

The API is built on top of this last layer, to query it in four standardized (but parameterised) ways.


### Slowly Changing Dimensions Type 2

All three dimensions (customers, products, orders) are implemented as SCD Type 2. This means every change to a row in the source database creates a new version in the analytical layer.

This was a requirement: "the platform must support querying historical information alongside current order state".


### Star Schema

The star schema on the gold layer has 4 dimension tables and one fact table:
- dim_date
- dim_customers
- dim_products
- dim_orders
- fact_order_items

Here's more details on each table:

#### `analytics.fact_order_items`
One row per order line item.

COlumns:
- order_item_id
- order_id
- product_id
- customer_id
- order_date_key
- delivery_date_key
- quantity
- unity_price
- total_amount
- order_status
- delivery_date
- source_created_at
- source_updated_at
- _ingested_at

The fact table is an incremental dbt model — on each dbt run it only processes records ingested since the last run, using `_ingested_at`.

#### `analytics.dim_orders` (SCD Type 2)
Tracks the full history of every order status change.

#### `analytics.dim_customers` (SCD Type 2)
Tracks customer attribute changes over time.

#### `analytics.dim_products` (SCD Type 2)
Tracks product changes over time, including price changes.

#### `analytics.dim_date`
Calendar table covering 2020–2030

### Diagram
![Star Schema Diagram](./diagrams/star-schema-diagram.png)


---

## API Reference

Base URL: `http://localhost:8000`

Interactive documentation: `http://localhost:8000/docs`


### `GET /health`
Liveness check.

**Response:**
```json
{"status": "ok"}
```


### `GET /analytics/orders`
Number of orders grouped by delivery date and status.

**Query Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| status | string | `open` | Filter by order status |

**Example:**
```bash
curl "http://localhost:8000/analytics/orders?status=COMPLETED"
```

**Response:**
```json
{
  "status_filter": "COMPLETED",
  "total_records": 1,
  "data": [
    {
      "delivery_date": "2026-06-06",
      "status": "COMPLETED",
      "total_orders": 6
    }
  ]
}
```

---

### `GET /analytics/orders/top`
Top N delivery dates with the most orders of a given status.

**Query Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| limit | integer | `3` | Number of results to return |
| status | string | `open` | Filter by order status |

**Example:**
```bash
curl "http://localhost:8000/analytics/orders/top?limit=3&status=COMPLETED"
```

---

### `GET /analytics/orders/product`
Total quantity of items per product, filtered by order status.

**Query Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| status | string | `pending` | Filter by order status |

**Example:**
```bash
curl "http://localhost:8000/analytics/orders/product?status=COMPLETED"
```

---

### `GET /analytics/orders/customers`
Top N customers ranked by number of orders of a given status.

**Query Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| status | string | `open` | Filter by order status |
| limit | integer | `3` | Number of results to return |

**Example:**
```bash
curl "http://localhost:8000/analytics/orders/customers?status=COMPLETED&limit=3"
```

---
## Decisions & Considerations

### Using dbt-core for Transformations

- dbt was chosen deliberately to align with Deel's own data stack. That's the sole reason, I picked it. It helped me with the SCD Type 2 implementation and it also made the medallion model architecture implementation simpler, but dbt is fundamentally a batch transformation tool. It is not designed to be triggered after every Kafka message — it works best when run on a schedule.
- dbt is triggered by the Python consumer in my solution after every N messages. This works, but it is NOT IDEAL and ADDS LATENCY. But it was a known issue before implementation (see above)
- I also followed this route with dbt, because I didn't think I was committing any big mistake, since the documentation wiht the problem statement mentioned "a streaming fashion is preferred but not required"

### Order Status Values

- the source database generates orders with a `COMPLETED` status only. The task specification refers to `open` and `pending` statuses in its business questions. To handle it correctly, all four API endpoints accept a `status` query parameter. 

---

### On Debezium Data Encoding

I had some issues witht Debezium data encoding which were solved by:
- having the kagka consumer converting epochs to Python `date` objects before inserting into the raw schema.
- having the kafka consumer decoding decimal columns to integer before inserting.
- fixing a typo on the order_items column: it was named `quanity` instead of `quantity`. The kafka consumer handles this  by remapping the key before inserting. It's a data cleaning operation.


---

## Known Limitations & Future Improvements

- Debezium connector registration is manual
- dbt is not the go to for streaming (apache Flink or Spark Streaming would reduce end-to-end latency)
- dbt is triggered by message count, not by schedule.
- No monitoring or alerting was implemented
- Each topic currently has one partition, which limits throughput