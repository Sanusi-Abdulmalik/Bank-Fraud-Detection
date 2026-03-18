# 🏦 Bank Fraud Detection System

A production-style data engineering portfolio project simulating a bank's data infrastructure for fraud detection. The pipeline ingests CRM and ERP data, transforms it through a medallion architecture (Bronze → Silver → Gold), and exposes analytics via an interactive dashboard.

[![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-blue?logo=docker)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Architecture](#-architecture)
- [Tech Stack](#️-tech-stack)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [Environment Variables](#-environment-variables)
- [Dashboard Preview](#-dashboard-preview)
- [Key Insights](#-key-insights)
- [What I Learned](#-what-i-learned)
- [Future Improvements](#-future-improvements)
- [Contributing](#-contributing)
- [License](#-license)
- [Contact](#-contact)

---

## 🔍 Overview

This project models a bank's end-to-end data pipeline — from raw transaction ingestion to analytics-ready fraud signals. It is designed as a portfolio project to demonstrate real-world data engineering skills including ETL design, medallion architecture, feature engineering, and dashboard development.

**Dataset:** 500 customers · 750 accounts · 10,000 transactions · ~1% fraud rate

---

## 🎯 Features

- **Realistic Data Generation** — Synthetic CRM/ERP data with configurable fraud injection.
- **Medallion Architecture** — Bronze (raw), Silver (cleaned & validated), Gold (analytics-ready) layers.
- **Fraud Feature Engineering** — 40+ features including amount Z-score, time since last transaction, rolling velocity counts, and merchant risk scores.
- **Interactive Dashboard** — Streamlit app with KPI cards, hourly fraud trends, merchant risk analysis, and live alert feed.
- **Containerized Database** — PostgreSQL running in Docker; zero local installation required.

---

## 🏗️ Architecture

```
Raw Data (CSV)
     │
     ▼
┌─────────────┐
│   Bronze    │  Raw ingestion — no transformations, full audit trail
└─────────────┘
     │
     ▼
┌─────────────┐
│   Silver    │  Cleaned, validated, deduplicated, typed correctly
└─────────────┘
     │
     ▼
┌─────────────┐
│    Gold     │  Aggregated, feature-engineered, analytics-ready
└─────────────┘
     │
     ▼
┌─────────────┐
│  Dashboard  │  Streamlit — KPIs, charts, fraud alerts
└─────────────┘
```

All layers are stored as tables in PostgreSQL and versioned via consistent naming conventions.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.9+ |
| Data Processing | Pandas, SQLAlchemy |
| Database | PostgreSQL 15 |
| Containerization | Docker |
| Dashboard | Streamlit, Plotly |
| Version Control | Git & GitHub |

---

## 📁 Project Structure

```
Bank-Fraud-Detection/
├── data/                        # Generated CSV files (not committed)
├── src/                         # ETL pipeline scripts
│   ├── generate_data.py         # Synthetic data generator
│   ├── load_bronze.py           # Raw ingestion to Bronze layer
│   ├── transform_to_silver.py   # Cleaning & validation to Silver layer
│   └── transform_to_gold.py     # Feature engineering to Gold layer
├── sql/                         # Database schema definitions
│   └── schema.sql
├── dashboard/                   # Streamlit dashboard
│   ├── app.py
│   └── dashboard_preview.png
├── .env.example                 # Example environment variable file
├── requirements.txt             # Python dependencies
├── LICENSE
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed and running
- [Python 3.9+](https://www.python.org/downloads/) installed

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/Sanusi-Abdulmalik/Bank-Fraud-Detection.git
   cd Bank-Fraud-Detection
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your preferred values
   ```

3. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Start PostgreSQL with Docker**
   ```bash
   docker run --name bank-db \
     -e POSTGRES_USER=${DB_USER} \
     -e POSTGRES_PASSWORD=${DB_PASSWORD} \
     -e POSTGRES_DB=${DB_NAME} \
     -p 5432:5432 \
     -d postgres
   ```

5. **Generate synthetic data**
   ```bash
   python data/generate_data.py
   ```

6. **Run the ETL pipeline**
   ```bash
   python src/load_bronze.py
   python src/transform_to_silver.py
   python src/transform_to_gold.py
   ```

7. **Launch the dashboard**
   ```bash
   streamlit run dashboard/app.py
   ```
   Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## 🔐 Environment Variables

Create a `.env` file in the project root based on `.env.example`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=bank_fraud
DB_USER=postgres
DB_PASSWORD=your_password_here
```

> ⚠️ Never commit your `.env` file. It is already included in `.gitignore`.

---

## 📊 Dashboard Preview

![Dashboard Preview](dashboard/dashboard_preview.png)

---

## 📈 Key Insights

- **Fraud rate** — ~1% of transactions are flagged as fraudulent.
- **High-risk hours** — Fraud activity peaks between 2–5 AM.
- **Risky categories** — Gambling, cryptocurrency, and foreign merchants carry the highest fraud rates.
- **Customer segments** — "Wealth" and "Business" tiers show disproportionately higher fraud exposure.

---

## 🧠 What I Learned

- Designing a medallion (Bronze/Silver/Gold) architecture for data pipelines.
- Feature engineering for fraud detection: velocity counts, amount Z-scores, time deltas.
- Building an end-to-end ETL pipeline with Python and PostgreSQL.
- Creating interactive analytics dashboards with Streamlit and Plotly.
- Containerizing database services with Docker.
- Managing credentials securely with environment variables.

---

## 🔮 Future Improvements

- [ ] Add real-time streaming ingestion with Apache Kafka.
- [ ] Train and serve a fraud classification model (e.g. Random Forest, XGBoost) on Gold features.
- [ ] Orchestrate the pipeline with Apache Airflow.
- [ ] Deploy to cloud (Azure or AWS) with infrastructure-as-code.
- [ ] Implement CI/CD with GitHub Actions.

---

## 🤝 Contributing

Contributions, suggestions, and feedback are welcome! To contribute:

1. Fork the repository.
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Commit your changes: `git commit -m "Add your feature"`
4. Push to the branch: `git push origin feature/your-feature-name`
5. Open a Pull Request.

Please open an issue first for major changes so we can discuss the approach.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## 📬 Contact

**Abdulmalik Sanusi**
- 🔗 [LinkedIn](https://www.linkedin.com/in/abdulmalik-sanusi-b3a0813a3)
- 💻 [GitHub](https://github.com/Sanusi-Abdulmalik)

---

*Built with ❤️ as part of my data engineering portfolio.*
