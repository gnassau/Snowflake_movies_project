# 🎬 Movies Data Engineering Pipeline

End-to-end **Data Engineering pipeline** built to ingest, process, validate and serve movie data using a **Medallion Architecture (Bronze → Silver → Gold)**.

The project demonstrates modern data engineering practices including:

* Data Lake storage
* Incremental and full data processing
* Data Quality checks
* Pipeline orchestration
* Monitoring and alerting

---
## 🛠 Tech Stack


| Componente | Ferramenta | Função |
|:--|:--|:--|
| 🪣 **Data Lake** | **AWS S3** | Armazenamento escalável de dados em object storage |
| 🔄 **Ingestão de Dados** | **Python 3.12.3** | Scripts para ingestão de dados a partir de APIs externas |
| 🔀 **Orquestração** | **Apache Airflow 2.8** | Gerenciamento de DAGs, scheduling e execução de pipelines |
| 🔎 **Query Engine** | **AWS Athena** | Execução de consultas SQL serverless diretamente no Data Lake |
| 📊 **Visualização** | **Power BI** | Criação de dashboards analíticos e KPIs de negócio |
| 🐳 **Infraestrutura** | **Docker Compose** | Containerização e orquestração local dos serviços |

---

**Languages**

* Python
* SQL

**Data Engineering**

* Docker + Apache Airflow
* AWS S3
* AWS Athena


**Data Processing**

* Incremental pipelines
* Partitioned datasets
* Medallion architecture

**Data Quality**

* Null check
* Uniqueness values check
* Range check
* Invalid values check
* Count rows check
* Freshness check

---

# 📌 Architecture

The pipeline follows the **Medallion Architecture**, a common pattern used in modern data platforms.

```
Raw Data → Bronze Layer → Silver Layer → Gold Layer → Analytics
```

Pipeline orchestration is handled by **Apache Airflow**, and data is stored in **AWS S3** and queried using **Athena**.

```
External API / Data Source
          │
          ▼
       Bronze (Raw Data - S3)
          │
          ▼
       Silver (Clean Data)
          │
          ▼
       Gold (Analytics Tables)
          │
          ▼
      Amazon Athena
```


---

# 📂 Project Structure

```
movies-data-pipeline/

├── dags/
│   └── movies_pipeline.py
│
├── src/
│   ├── bronze/
│   │   └── ingestion.py
│   │
│   ├── silver/
│   │   └── transform_movies.py
│   │
│   ├── gold/
│   │   └── analytics_tables.py
│   │
│   └── data_quality/
│       ├── dq_queries.py
│       └── dq_checks.py
│
├── tests/
│
├── requirements.txt
└── README.md
```

---

# 🥉 Bronze Layer

The **Bronze layer** stores raw ingested data exactly as received from the source.

Characteristics:

* Raw and immutable data
* Stored in **AWS S3**
* Partitioned by ingestion timestamp
* Serves as the historical source of truth

Example fields:

* movie_id
* title
* release_date
* ingestion_timestamp

---

# 🥈 Silver Layer

The **Silver layer** applies transformations and cleaning.

Processes include:

* Schema normalization
* Data type standardization
* Deduplication
* Filtering invalid records

This layer produces **clean and structured datasets** ready for analytics.

---

# 🥇 Gold Layer

The **Gold layer** contains aggregated and analytics-ready tables.

Examples:

* Movie performance metrics
* Release trends
* Analytical views for BI tools

These datasets are optimized for **query performance in Athena**.

---

# 🔁 Incremental Processing

The pipeline processes data **incrementally** to avoid full refreshes.

Strategy used:

* Identify latest `ingestion_timestamp`
* Process only new records
* Append results to Silver and Gold layers

Benefits:

* Faster pipelines
* Lower compute cost
* Scalable processing

---

# ✅ Data Quality Checks

Automated Data Quality checks are executed after pipeline runs.

Examples of validations:

* Row count validation
* Missing data checks
* Unexpected volume spikes
* Schema consistency

Example checks:

```
Rows in Silver should not decrease compared to previous batch
Rows processed should be within expected threshold
```

---

# 🚨 Monitoring and Alerts

The pipeline includes monitoring logic to detect failures or anomalies.

Alert scenarios:

* Pipeline execution failure
* Unexpected row volume
* Data Quality check failure

Notifications are sent via **email alerts**.

---

# ⚙️ Orchestration

The entire workflow is orchestrated using **Apache Airflow**.

The DAG controls:

1. Data ingestion
2. Bronze → Silver transformation
3. Silver → Gold aggregation
4. Data Quality checks
5. Alerting

Example pipeline flow:

```
Ingestion
   ↓
Bronze Storage
   ↓
Silver Transformation
   ↓
Gold Aggregation
   ↓
Data Quality Checks
   ↓
Alerts
```

---

# ▶️ Running the Pipeline

### 1 Install dependencies

```
pip install -r requirements.txt
```

### 2 Configure environment variables

```
AWS_ACCESS_KEY
AWS_SECRET_KEY
ATHENA_DATABASE
S3_BUCKET_NAME
```

### 3 Start Airflow

```
airflow standalone
```

### 4 Trigger DAG

Run the DAG from the **Airflow UI**.

---

# 📊 Example Analytics Query (Athena)

```
SELECT
    release_date,
    COUNT(*) AS movies_released
FROM gold_movies
GROUP BY release_date
ORDER BY release_date;
```

---

# 🚀 Future Improvements

Planned improvements for the project:

* Data lineage tracking
* Automated testing
* Data catalog integration
* Dashboard for pipeline monitoring
* CI/CD for pipeline deployment

---

# 👨‍💻 Author

**Gustavo Nassau**

Data Coordinator with experience in analytics, data platforms and data-driven decision making.

LinkedIn: *(add your link here)*
GitHub: *(add your link here)*
