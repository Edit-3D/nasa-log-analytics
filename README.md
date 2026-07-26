# NASA Web Log Analytics

A data engineering project that parses NASA Apache web server logs and analyzes
them using both standard Python and PySpark. It demonstrates how an in-memory
workflow can be translated into distributed DataFrame operations.

## Features

- Apache Common Log Format parsing
- Graceful handling of malformed records
- Request, status-code, daily traffic, and response-size analytics
- Standard-Python CLI and PySpark implementations
- Jupyter notebook with tables and charts
- Automated tests

## Project Structure

```text
nasa-log-analytics/
├── data/
│   └── nasa_access_log_aug95_sample.txt  # downloaded locally; ignored by Git
├── notebooks/
│   └── analysis_demo.ipynb
├── outputs/
│   └── python_summary.json               # generated output; ignored by Git
├── src/
│   ├── nasa_log_analytics/
│   │   ├── __init__.py
│   │   ├── analytics.py
│   │   ├── cli.py
│   │   └── parser.py
│   └── spark_job.py
├── tests/
│   └── test_analysis.py
├── .gitignore
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Dataset

This project uses the NASA August 1995 Web Server Access Log dataset, containing
HTTP requests recorded by the NASA Kennedy Space Center web server.

Download it here: https://drive.google.com/file/d/1ZiyXLVDyirV_2OivNVdTqeUPQ5ez7M2a/view

After downloading, save it locally as:

```text
data/
└── nasa_access_log_aug95_sample.txt
```

The dataset file is intentionally ignored by Git and must be downloaded
separately.

## Installation

Clone the repository:

```bash
git clone <repository-url>
cd nasa-log-analytics
```

```bash
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

macOS/Linux:

```bash
source .venv/bin/activate
```

```bash
python -m pip install -r requirements.txt
```

## Running the Python Analysis

```bash
python -m nasa_log_analytics.cli
```

The CLI prints the summary and writes it to `outputs/python_summary.json`.
Use `--help` to see input, output, and top-result options.

## Running the Spark Analysis

```bash
python src/spark_job.py
```

The Spark job displays its aggregations and writes parsed records to
`outputs/parsed_logs.parquet`. Use `--help` to see available options.

## Running Tests

```bash
pytest
```

## Running the Notebook

```bash
jupyter notebook notebooks/analysis_demo.ipynb
```

The notebook reuses the standard-Python parser and analytics functions, then
presents the results with pandas and matplotlib.

## License

This project is provided for educational and portfolio purposes. Refer to the
original dataset source for licensing or redistribution restrictions affecting
the NASA access log.
