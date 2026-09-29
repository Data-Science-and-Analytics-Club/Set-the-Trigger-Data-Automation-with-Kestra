# Set the Trigger: Data Automation with Kestra
A hands-on workshop by Data Science and Analytics Club; building a real e-commerce data pipeline from scratch, then automating it with [Kestra](https://kestra.io/).


---

## Table of contents

- [Why Automation? The Manual Pipeline Problem](#why-automation-the-manual-pipeline-problem)
- [Kestra vs. n8n](#kestra-vs-n8n--why-orchestration-not-just-automation)
- [Project overview](#project-overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Run Kestra locally](#run-kestra-locally)
- [Part 1: Build the enriched CSV pipeline](#part-1-build-the-enriched-csv-pipeline)
- [Part 2: Load SQLite and add reliability](#part-2-load-sqlite-and-add-reliability)
- [Current complete flow](#current-complete-flow)
- [Expected results](#expected-results)
- [Kestra concepts demonstrated](#kestra-concepts-demonstrated)
- [Troubleshooting](#troubleshooting)
- [Try It Yourself: Data Quality Challenge](#try-it-yourself-data-quality-challenge)
- [Author](#author)

---

## Why Automation? The Manual Pipeline Problem

Imagine a pipeline that pulls order data, fetches live product info from an API, cleans and joins them, then loads the result into a database. Doing this once, manually, is easy. Now imagine running it every day, with no visibility into failures and no way to retry just the step that broke.

**This is the gap orchestration tools solve** — scheduling, retries, monitoring, and reliability, without babysitting every run yourself.

---

## Kestra vs. n8n — Why Orchestration, Not Just Automation

**n8n** is a visual, node-based automation tool — great for connecting apps (Slack, Sheets, CRMs) and prototyping automations quickly, with a huge integration library.

**Where it falls short for data engineering:** it isn't built for high-throughput data pipelines or complex infrastructure orchestration. Visual canvases get unwieldy at scale, and there's no clean "infrastructure as code" story for versioning and reviewing complex logic.

**Kestra** is code-first and YAML-based — workflows are files you can version in Git and deploy through CI/CD, like any other infrastructure code. It's language-agnostic (Python, R, Shell, anything) and built specifically for orchestrating data pipelines and technical jobs, with retries, failure handling, and observability designed for production reliability.

> **In short:** n8n connects apps. Kestra orchestrates pipelines at production scale.

---

## Project overview

We act as data engineers for an e-commerce company with two data sources.

**Orders CSV:**
```
order_id,product_id,quantity
```

**Product REST API** — from the [DummyJSON Products API](https://dummyjson.com/products):
```
id,title,category,price
```

The pipeline joins `orders.product_id` with `products.id` and produces:
```
order_id,product_id,quantity,title,category,price,revenue
```

---

## Architecture

```
Orders CSV ───────────────┐
                          ▼
                    create_orders
                          │
DummyJSON API ──► fetch_product_data
                          │
                   inspect_product_data
                          │
                          ▼
                    clean_orders
              clean, validate, join,
              enrich, calculate revenue
                          │
                          ▼
                   clean_orders.csv
                          │
                          ▼
                  load_to_database
                          │
                          ▼
                    ecommerce.db
                   orders table
                          │
                          ▼
                    sql_analytics
                          │
                          ▼
                   Business insights
                          │
                          ▼
                   processing_complete

Failures: retries → pipeline_failed → alert log
```

---

## Prerequisites

- Windows
- Docker Desktop, configured to use the WSL 2 backend
- Internet access for the DummyJSON API

A Docker account is not required for this local setup.

---

## Run Kestra locally

1. Install and start [Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/).
2. Wait until the Docker Engine is running.
3. Open PowerShell and verify Docker:
   ```
   docker --version
   ```
4. Start Kestra:
   ```
   docker run --pull=always --rm -it -p 8080:8080 `
     --user=root `
     --name kestra `
     -v kestra_data:/app/storage `
     -v kestra_db:/app/data `
     -v /var/run/docker.sock:/var/run/docker.sock `
     -v /tmp:/tmp `
     kestra/kestra:latest-slim server local
   ```
5. Open <http://localhost:8080> and create the local administrator account.

---

# Part 1: Build the enriched CSV pipeline

Follow these steps in order to reproduce the learning journey.

### 1. Create a first flow with log tasks

```yaml
id: ecommerce_pipeline
namespace: dsa.dataengineering

tasks:
  - id: pipeline_started
    type: io.kestra.plugin.core.log.Log
    message: "E-commerce data pipeline started"

  - id: orders_received
    type: io.kestra.plugin.core.log.Log
    message: "500 new orders received for processing"

  - id: processing_complete
    type: io.kestra.plugin.core.log.Log
    message: "Order processing completed successfully"
```

Save and **Execute**. Inspect the Gantt view and logs — this introduces flow, task, execution, and logs.

### 2. Create the orders CSV

```yaml
- id: create_orders
  type: io.kestra.plugin.core.storage.Write
  content: |
    order_id,product_id,product,quantity,price
    1001,1,Laptop Stand,2,25.00
    1002,2,Wireless Mouse,1,15.00
    1002,2,Wireless Mouse,1,15.00
    1003,3,USB-C Hub,3,30.00
    1004,4,Webcam,1,45.00
    1005,5,Keyboard,-2,40.00
  extension: .csv
```

Intentionally contains a duplicate order (`1002`) and an invalid one (negative quantity, `1005`).

### 3. Clean orders with Python and Pandas

```yaml
- id: clean_orders
  type: io.kestra.plugin.scripts.python.Script
  beforeCommands:
    - pip install pandas
  script: |
    import pandas as pd

    df = pd.read_csv("{{ outputs.create_orders.uri }}")
    df = df.drop_duplicates(subset=["order_id"])
    df = df[df["quantity"] > 0]
    df["revenue"] = df["quantity"] * df["price"]

    df.to_csv("clean_orders.csv", index=False)
  outputFiles:
    - "clean_orders.csv"
```

### 4. Add analytics for the initial CSV schema

```yaml
- id: analyze_orders
  type: io.kestra.plugin.scripts.python.Script
  beforeCommands:
    - pip install pandas
  script: |
    import pandas as pd

    df = pd.read_csv(
        "{{ outputs.clean_orders.outputFiles['clean_orders.csv'] }}"
    )
    total_revenue = df["revenue"].sum()
    total_orders = len(df)
    average_order_value = total_revenue / total_orders
    top_product = df.loc[df["revenue"].idxmax(), "product"]

    print("Total Orders:", total_orders)
    print("Total Revenue:", total_revenue)
    print("Average Order Value:", average_order_value)
    print("Highest Revenue Product:", top_product)
```

### 5. Fetch product data from the REST API

```yaml
- id: fetch_product_data
  type: io.kestra.plugin.core.http.Request
  uri: https://dummyjson.com/products
  method: GET
```

### 6. Inspect the API response with `jq`

```yaml
- id: inspect_product_data
  type: io.kestra.plugin.core.log.Log
  message: |
    Product API successfully fetched.
    First product: {{ outputs.fetch_product_data.body | jq('.products[0].title') | first }}
    Category: {{ outputs.fetch_product_data.body | jq('.products[0].category') | first }}
    Price: ${{ outputs.fetch_product_data.body | jq('.products[0].price') | first }}
```

### 7. Redesign the order source for enrichment

So far we've been cheating — the CSV had `product` and `price` hardcoded in it. Real order systems don't work that way: an order only ever records *what* was bought and *how many*, never the product's name or current price (that lives in the product catalog, which can change independently). Drop those two columns to make the CSV realistic again:

```yaml
- id: create_orders
  type: io.kestra.plugin.core.storage.Write
  content: |
    order_id,product_id,quantity
    1001,1,2
    1002,2,1
    1002,2,1
    1003,3,3
    1004,4,1
    1005,5,-2
  extension: .csv
```

The orders CSV now knows only order ID, product ID, and quantity — product names, categories, and prices come from the API instead. The join key is `orders.product_id ↔ products.id`.

### 8. Join and enrich the two sources

```yaml
- id: clean_orders
  type: io.kestra.plugin.scripts.python.Script
  beforeCommands:
    - pip install pandas
  script: |
    import json
    import pandas as pd

    orders = pd.read_csv("{{ outputs.create_orders.uri }}")
    products_json = json.loads(r'''{{ outputs.fetch_product_data.body }}''')
    products = pd.DataFrame(products_json["products"])

    orders = orders.drop_duplicates(subset=["order_id"])
    orders = orders[orders["quantity"] > 0]

    products = products[["id", "title", "category", "price"]]
    products = products.rename(columns={"id": "product_id"})

    enriched = orders.merge(products, on="product_id", how="left")
    enriched["revenue"] = enriched["quantity"] * enriched["price"]

    enriched.to_csv("clean_orders.csv", index=False)
  outputFiles:
    - "clean_orders.csv"
```

### 9. Update analytics for the enriched schema

The product-name field is now `title`, not `product` — update `analyze_orders` accordingly.

### Part 1 troubleshooting lessons

**`Function or Macro [json] does not exist`** — `{{ json(outputs...) }}` isn't available. Use:
```
{{ outputs.fetch_product_data.body | jq('.products[0].title') | first }}
```

**`KeyError: 'product'`** — after enrichment the schema changed from `product` to `title`. Any downstream code referencing the old column name breaks — a live, real example of what happens when a schema changes and downstream tasks aren't updated.

---

# Part 2: Load SQLite and add reliability

### 1. Test SQLite first (throwaway task, remove after confirming it works)

```yaml
- id: test_database
  type: io.kestra.plugin.scripts.python.Script
  script: |
    import sqlite3
    connection = sqlite3.connect("ecommerce.db")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER, message TEXT)")
    cursor.execute("INSERT INTO test_table VALUES (1, 'Database connection successful')")
    connection.commit()
    print(cursor.execute("SELECT * FROM test_table").fetchall())
    connection.close()
```

### 2. Load the enriched CSV into SQLite

```yaml
- id: load_to_database
  type: io.kestra.plugin.scripts.python.Script
  beforeCommands:
    - pip install pandas
  script: |
    import pandas as pd
    import sqlite3

    df = pd.read_csv("{{ outputs.clean_orders.outputFiles['clean_orders.csv'] }}")
    connection = sqlite3.connect("ecommerce.db")
    df.to_sql("orders", connection, if_exists="replace", index=False)
    print(pd.read_sql("SELECT * FROM orders", connection))
    connection.close()
  outputFiles:
    - "ecommerce.db"
```

### 3. Run SQL analytics

```yaml
- id: sql_analytics
  type: io.kestra.plugin.scripts.python.Script
  script: |
    import sqlite3

    connection = sqlite3.connect("{{ outputs.load_to_database.outputFiles['ecommerce.db'] }}")
    cursor = connection.cursor()

    cursor.execute("SELECT SUM(revenue) FROM orders")
    print("Total Revenue:", cursor.fetchone()[0])

    cursor.execute("""
        SELECT category, SUM(revenue) AS total_revenue
        FROM orders GROUP BY category ORDER BY total_revenue DESC
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.execute("""
        SELECT title, SUM(revenue) AS total_revenue
        FROM orders GROUP BY title ORDER BY total_revenue DESC LIMIT 1
    """)
    print("Top Product:", cursor.fetchone())
    connection.close()
```

### 4. Schedule the flow

```yaml
triggers:
  - id: every_two_minutes
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "*/2 * * * *"
```

### 5. Add retries

```yaml
- id: fetch_product_data
  type: io.kestra.plugin.core.http.Request
  uri: https://dummyjson.com/products
  method: GET
  retry:
    type: constant
    interval: PT5S
    maxAttempts: 3
```

### 6. Add failure handling

```yaml
errors:
  - id: pipeline_failed
    type: io.kestra.plugin.core.log.Log
    message: |
      ALERT: E-commerce pipeline failed.
      Execution ID: {{ execution.id }}
      Please check the Kestra execution logs.
```

Failure lifecycle: `Task fails → retry → retry → retries exhausted → pipeline_failed`

---

## Current complete flow

```yaml
id: ecommerce_pipeline
namespace: dsa.dataengineering

tasks:
  - id: pipeline_started
    type: io.kestra.plugin.core.log.Log
    message: "E-commerce data pipeline started"

  - id: create_orders
    type: io.kestra.plugin.core.storage.Write
    content: |
      order_id,product_id,quantity
      1001,1,2
      1002,2,1
      1002,2,1
      1003,3,3
      1004,4,1
      1005,5,-2
    extension: .csv

  - id: fetch_product_data
    type: io.kestra.plugin.core.http.Request
    uri: https://dummyjson.com/products
    method: GET
    retry:
      type: constant
      interval: PT5S
      maxAttempts: 3

  - id: inspect_product_data
    type: io.kestra.plugin.core.log.Log
    message: |
      Product API successfully fetched.
      First product: {{ outputs.fetch_product_data.body | jq('.products[0].title') | first }}
      Category: {{ outputs.fetch_product_data.body | jq('.products[0].category') | first }}
      Price: ${{ outputs.fetch_product_data.body | jq('.products[0].price') | first }}

  - id: clean_orders
    type: io.kestra.plugin.scripts.python.Script
    beforeCommands:
      - pip install pandas
    script: |
      import json
      import pandas as pd

      orders = pd.read_csv("{{ outputs.create_orders.uri }}")
      products_json = json.loads(r'''{{ outputs.fetch_product_data.body }}''')
      products = pd.DataFrame(products_json["products"])

      orders = orders.drop_duplicates(subset=["order_id"])
      orders = orders[orders["quantity"] > 0]

      products = products[["id", "title", "category", "price"]]
      products = products.rename(columns={"id": "product_id"})

      enriched = orders.merge(products, on="product_id", how="left")
      enriched["revenue"] = enriched["quantity"] * enriched["price"]
      enriched.to_csv("clean_orders.csv", index=False)

      print("ENRICHED ORDERS")
      print(enriched)
    outputFiles:
      - "clean_orders.csv"

  - id: load_to_database
    type: io.kestra.plugin.scripts.python.Script
    beforeCommands:
      - pip install pandas
    script: |
      import pandas as pd
      import sqlite3

      df = pd.read_csv("{{ outputs.clean_orders.outputFiles['clean_orders.csv'] }}")
      connection = sqlite3.connect("ecommerce.db")
      df.to_sql("orders", connection, if_exists="replace", index=False)
      print(pd.read_sql("SELECT * FROM orders", connection))
      connection.close()
    outputFiles:
      - "ecommerce.db"

  - id: sql_analytics
    type: io.kestra.plugin.scripts.python.Script
    script: |
      import sqlite3

      connection = sqlite3.connect("{{ outputs.load_to_database.outputFiles['ecommerce.db'] }}")
      cursor = connection.cursor()

      cursor.execute("SELECT SUM(revenue) FROM orders")
      print("Total Revenue:", cursor.fetchone()[0])

      cursor.execute("""
          SELECT category, SUM(revenue) AS total_revenue
          FROM orders GROUP BY category ORDER BY total_revenue DESC
      """)
      for row in cursor.fetchall():
          print(row)

      cursor.execute("""
          SELECT title, SUM(revenue) AS total_revenue
          FROM orders GROUP BY title ORDER BY total_revenue DESC LIMIT 1
      """)
      print("Top Product:", cursor.fetchone())
      connection.close()

  - id: processing_complete
    type: io.kestra.plugin.core.log.Log
    message: "E-commerce data pipeline completed successfully."

errors:
  - id: pipeline_failed
    type: io.kestra.plugin.core.log.Log
    message: |
      ALERT: E-commerce pipeline failed.
      Execution ID: {{ execution.id }}
      Please check the Kestra execution logs.

triggers:
  - id: every_two_minutes
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "*/2 * * * *"
```

---

## Expected results

After cleaning: duplicate order `1002` is reduced to one row, invalid order `1005` is removed, four valid orders remain. Product details are joined from the API, revenue is calculated using quantity and API price, and the enriched records land in SQLite as the `orders` table. SQL analytics prints total revenue, revenue by category, and the top product. *(Exact revenue values may vary since product prices come from a live external API.)*

---

## Kestra concepts demonstrated

- **Flow** — the complete workflow
- **Task** — one operation in the workflow
- **Namespace** — logical grouping for flows (`dsa.dataengineering`)
- **Trigger** — what starts a flow automatically (the `Schedule` trigger here)
- **Execution** — one run of the flow
- **Outputs** — artifacts passed between tasks
- **Orchestration** — task order, dependencies, schedules, and failure behavior
- **Observability** — logs, execution status, outputs, the Gantt view
- **ETL** — extract from CSV/API, transform with Python/Pandas, load into SQLite

Key output references used throughout:
```
{{ outputs.create_orders.uri }}
{{ outputs.clean_orders.outputFiles['clean_orders.csv'] }}
{{ outputs.load_to_database.outputFiles['ecommerce.db'] }}
```

---

## Troubleshooting

| Issue | Fix |
|---|---|
| `Function or Macro [json] does not exist` | Use `jq` filters in expressions instead of `json()` |
| `KeyError: 'product'` | Schema changed to `title` after enrichment — update downstream code |
| `Unrecognized field "maxAttempt"` | Use the plural `maxAttempts` |
| SQLite file unavailable downstream | Declare it under `outputFiles`, then reference via `{{ outputs.load_to_database.outputFiles['ecommerce.db'] }}` |
| Docker not running | Start Docker Desktop before running the setup command |
| Port 8080 already in use | Map to a different port, e.g. `-p 8081:8080` |

---

## Try It Yourself: Data Quality Challenge

**Pipeline technically succeeded ≠ data is necessarily correct.**

Copy your working flow and change the `create_orders` content to this messier file:

```
order_id,product_id,quantity
4001,1,2
4002,2,1
4003,3,4
4004,4,1
4004,4,1
4005,5,0
4006,6,3
4007,,2
4008,8,2
4009,9,-3
4010,10,1
4011,11,2
4012,9999,1
4013,13,1
4014,14,3
4015,15,1
4015,15,1
4016,16,2
4017,,1
4018,18,4
4019,8888,2
4020,20,1
```

Update `clean_orders` so it rejects orders that have:
```
duplicate order_id            → keep the first, reject repeats
quantity <= 0                 → invalid
product_id missing            → invalid
product not found in catalog  → invalid (price is empty after the join)
```

Write **two files**: `clean_orders.csv` (valid orders) and `rejected_orders.csv` (bad orders, with a `reject_reason` column), and print a short report of total, clean, and rejected rows. The rest of the pipeline must still run.

**Hints**
```python
df["col"].isnull()                 # True where a value is missing
df.duplicated(subset=["order_id"]) # True for repeated copies of an order_id
df.loc[condition, "reject_reason"] = "some text"
```
Remember to list every file you create under `outputFiles`.


---

