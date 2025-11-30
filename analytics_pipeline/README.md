# 🚀 Automated E-commerce Analytics Pipeline (GA4 + dbt + BigQuery)

This project demonstrates a fully structured analytics engineering pipeline built to transform messy, nested **Google Analytics 4 (GA4)** event data into clean, business-ready metrics like **GMV** and **AOV**.

### Architecture
The following diagram illustrates the data flow from the BigQuery Public Dataset through the Staging and Mart layers.
![Data Pipeline Lineage Graph]({{ 'images/pipeline_flow.png' }})

### 📊 Data Modeling Layers

The pipeline is structured into Staging (Cleaning) and Marts (Business Logic).

#### Staging Layer (Source Ingestion & Cleaning)
* **stg_ga4__events:** Extracts core event fields (date, session ID, user ID).
* **stg_ga4__item_purchases:** Handles the complex **UNNEST** of the nested `items` array to extract individual product details, filtering for valid transactions (`transaction_id IS NOT NULL`).
* **stg_users:** Aggregates event data to provide user-level dimensions (first/last activity, total sessions).

#### Mart Layer (Final Metrics)
* **fct_ecommerce_orders:** **Aggregates** item-level data into order-level facts, calculating key business metrics:
    * **GMV (Gross Merchandise Value)**
    * **AOV (Average Order Value)**

### ⚙️ Getting Started (Local Setup)

This project requires a connection to a Google Cloud Platform account with BigQuery access.

1.  **Clone the repository:**
    ```bash
    git clone [Your-Repo-URL]
    ```
2.  **Install dependencies (Python & dbt):**
    ```bash
    pip install dbt-core dbt-bigquery
    ```
3.  **Configure Profile:** Set up your Service Account credentials in `~/.dbt/profiles.yml`.
4.  **Execute the Pipeline:**
    ```bash
    dbt run
    dbt test
    dbt docs generate
    ```
