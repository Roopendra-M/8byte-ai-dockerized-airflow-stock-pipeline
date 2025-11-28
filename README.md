# Dockerized Stock Market Data Pipeline (Airflow + PostgreSQL)

##  Overview

This project implements a fully dockerized data pipeline using **Apache Airflow** to automate the fetching, processing, and storage of stock market data into **PostgreSQL**.

The pipeline:
- Retrieves daily stock price data from **Alpha Vantage API**
- Parses the JSON response
- Inserts or updates (UPSERT) records into PostgreSQL
- Runs automatically on a schedule

It is designed for **automation, resilience, and scalability**.

---

##  Architecture

```
┌────────────────┐      Fetch JSON      ┌───────────────┐
│ Airflow DAG    │ ───────────────────▶ │ Alpha Vantage │
└───────┬────────┘                     └───────────────┘
        │
 Schedule│Run ETL
        ▼
┌───────────────────┐   Upsert Data    ┌───────────────────┐
│ ETL Python Script │ ───────────────▶ │ PostgreSQL (DB)   │
└───────────────────┘                  └───────────────────┘
```

---

## Requirements

- Docker Desktop or Docker Engine installed
- Docker Compose v2 or higher

This project works on Windows, Linux, and macOS.

---

##  Environment Variables

Create a `.env` file in the project root:

```
STOCK_API_KEY=YOUR_ALPHA_VANTAGE_API_KEY
STOCK_API_SYMBOL=IBM
STOCK_API_BASE_URL=https://www.alphavantage.co/query
```

Get a free API key from:  
https://www.alphavantage.co/support/#api-key

---

##  How to Run the Pipeline

### 1️⃣ Clone the repository

```
git clone <your repo link>
cd <project folder>
```

### 2️⃣ Start the services

```
docker compose up --build -d
```

### 3️⃣ Access Apache Airflow

Open browser:

```
http://localhost:8080
```

Login with:

| Username | Password |
|----------|----------|
| admin | admin |

(Admin user is automatically created.)

---

##  Running the Pipeline

Once Airflow UI is open:

1. Locate the DAG named: `stock_market_etl`
2. Turn the toggle **ON**
3. Click **Trigger DAG**

Or trigger via terminal:

```
docker exec -it airflow_stock_pipeline airflow dags trigger stock_market_etl
```

---

##  Verify Data in PostgreSQL

Run:

```
docker exec -it stocks_postgres psql -U stocks_user -d airflow_meta -c "SELECT COUNT(*) FROM stock_prices;"
```

Or preview sample records:

```
docker exec -it stocks_postgres psql -U stocks_user -d airflow_meta -c "SELECT * FROM stock_prices LIMIT 10;"
```

---

##  Testing the ETL Task Manually (Optional)

```
docker exec -it airflow_stock_pipeline airflow tasks test stock_market_etl fetch_and_store_stock_data 2025-11-28
```

---

##  Error Handling

This pipeline includes:
- Retry logic
- HTTP request exception handling
- JSON validation
- ON CONFLICT UPSERT logic to prevent duplicates

---

##  Shutdown

To stop containers:

```
docker compose down
```

To remove database volume (reset everything):

```
docker compose down -v
```

---

##  Project Structure

```
.
├── docker-compose.yml
├── dags/
│   ├── stock_pipeline_dag.py        # Airflow DAG
│   └── stock_etl.py                 # ETL logic
├── sql/
│   └── init.sql                      # Database schema
├── .env                              # Environment variables
└── README.md
```

---

##  Deliverables Checklist

| Item | Status |
|------|--------|
| Docker Compose for full stack | ✔ |
| Airflow DAG | ✔ |
| API ETL Python script | ✔ |
| Environment variable security | ✔ |
| Error handling | ✔ |
| Automated scheduling | ✔ |

---

##  Future Enhancements (Optional)

- Add PGAdmin for database UI management
- Add Grafana dashboards
- Support multiple stock symbols
- Convert database engine to TimescaleDB for time-series optimization

---

##  Conclusion

This project demonstrates a production-style data engineering pipeline that is:
- Fully automated
- Fault-tolerant
- Secure through environment variables
- Designed for scalability

---

