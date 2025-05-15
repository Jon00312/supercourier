# 📦 SuperCourier - Mini ETL Pipeline

This project implements a **mini ETL (Extract, Transform, Load) pipeline** for a fictional logistics company, *SuperCourier*. It simulates delivery data, enriches it with weather conditions, calculates delivery performance, and saves the results for analysis.

---

## 🔄 Pipeline Overview

### 1. **Extraction**
- Creates a **SQLite database** with 1,000 simulated deliveries over the last 90 days.
- Generates **hourly weather conditions** for the same period and stores them in a JSON file.

### 2. **Transformation**
- Enriches delivery data with:
  - **Weekday and hour** of pickup.
  - **Weather condition** at pickup time.
  - Randomized **distance (km)** and **actual delivery time (minutes)**.
- Calculates a **theoretical delivery time** using a set of **adjustment factors** based on:
  - Package type
  - Delivery zone
  - Weather
  - Peak hours
  - Day of the week
- Determines delivery **status** (On-time or Delayed) using a threshold.

### 3. **Loading**
- Outputs a final CSV file (`deliveries.csv`) with cleaned and enriched delivery data, ready for analysis or reporting.

---

## 📁 Output Schema

The final dataset includes the following columns, in this order:

| Column                 | Description                               |
|------------------------|-------------------------------------------|
| `delivery_id`          | Unique identifier                         |
| `pickup_datetime`      | Date and time of pickup                   |
| `weekday`              | Day of the week                           |
| `hour`                 | Hour of the day (0-23)                    |
| `package_type`         | Package size/category                     |
| `distance`             | Distance in kilometers                    |
| `delivery_zone`        | Area type (Urban, Suburban, etc.)         |
| `WeatherCondition`     | Weather at pickup time                    |
| `actual_delivery_time` | Time taken to complete delivery (min)     |
| `status`               | Final status: On-time / Delayed           |

---

## 🛠️ How to Run

```bash
python de-code-snippet.py
