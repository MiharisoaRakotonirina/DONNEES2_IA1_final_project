# 🌦️ WEATHER PROJECT

## 📌 Theme: *Climate and Tourism – When to Travel?*

---

### ✅ Prerequisites

To run this project, make sure the following tools are installed:

- 🐍 **Python**: `v3.9.12`
- 📦 **Pip**: `v22.0.4`
- 🌬️ **Apache Airflow**: `v3.0.2`

Install Airflow using the official constraints file:

```bash
pip install "apache-airflow==3.0.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.1/constraints-3.9.txt"
```

- 🔗 **Other packages**: `requests` and `pandas`

```bash
pip install requests pandas
```

- 🔑 **OpenWeather API key**:  
  Create a free account at [https://openweathermap.org/](https://openweathermap.org/) and obtain your API key.  
  Then create a `.env` file in the project root and add:

```
OPENWEATHER_API_KEY=your_api_key_here
```

---

### 🧪 Running the Project (3 Terminals)

Start the following Airflow components in **three separate terminals**:

#### 📡 Terminal 1 – Airflow API Server
```bash
airflow api-server
```
> Launches the REST API to interact with Airflow programmatically.

#### 🔄 Terminal 2 – DAG Processor
```bash
airflow dag-processor
```
> Continuously parses and updates DAG definitions in the background.

#### ⏰ Terminal 3 – Scheduler
```bash
airflow scheduler
```
> Triggers tasks and DAGs according to their schedule (`@daily`, `@hourly`, etc.).

---

You're now ready to run the pipeline and visualize the DAGs!
