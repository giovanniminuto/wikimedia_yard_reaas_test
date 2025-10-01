# Wikimedia Yard Reaas – Churn Prediction Test

[![CI](https://github.com/giovanniminuto/my-py-template/actions/workflows/ci.yml/badge.svg)](https://github.com/giovanniminuto/my-py-template/actions/workflows/ci.yml)

This repository was created as part of the Data Scientist Take-Home Test (see assignment PDF).
It implements an end-to-end Spark + ML pipeline to ingest, process, and model Wikimedia pageview logs (Jan 2025) for early churn prediction.

## Overview
The project follows a bronze → silver → gold → train data engineering and modeling pipeline.
- Bronze: Raw ingestion of Wikimedia dumps.
- Silver: Cleansed and structured dataset with proper schema.
- Gold: Aggregated features (DAU, MAU, stickiness, content diversity, entropy, etc.).
- Train: Binary classification model for churn prediction, with hyperparameter tuning and explainability.


## How to Run the Pipeline
Clone the repository and make sure you have Python 3.10+ and Apache Spark 3.4+ installed.
All main scripts are provided under `examples/` and call the reusable modules inside the `wikimedia_yard_reaas_test/` package.
1. Download Data
```
python example_file_download.py
```
Downloads the Wikimedia pageviews dump (Jan 2025) from
https://dumps.wikimedia.org/other/pageviews/2025/2025-01/

2. Bronze Layer
```
python example_bronze_step.py
```
Parses raw logs into a Delta/Parquet bronze table.
3. Silver Layer
```
python example_silver_step.py
```
Cleans and structures the bronze table into daily pageview records.
4. Gold Layer
```
python example_gold_step.py
```
Builds aggregated features (DAU/MAU, stickiness, entropy, diversity, etc.) for ML.
5. Train Model
```
python example_train_model.py
```
Trains a churn prediction model using HistGradientBoostingClassifier, with hyperparameter tuning and feature explainability.
## Repository Structure
```
.
├── examples/
│   ├── example_file_download.py
│   ├── example_bronze_step.py
│   ├── example_silver_step.py
│   ├── example_gold_step.py
│   ├── example_train_model.py
│   ├── analysis_italian_table.ipynb  the notebook used to do the analysis on the italian in dataset
│   └── notebook_databricks_training.ipynb # the notebook used to train the model in databricks
│
├── wikimedia_yard_reaas_test/   # Source package with pipeline methods
│   ├── cleaning_pipeline.py
│   ├── feature_engineering.py
│   ├── modelling.py
│   ├── utils.py
│   └── train_and_evaluate_functions.py
│
├── DS_with_ML_takehome_test_1_rev0.2.pdf   # Assignment description
├── report.py # report of the work
├── .pre-commit-config.yml
├── README.md
└── pyproject.toml
```
## Environment Setup

### 1. Create a virtual environment

```bash
python -m venv .venv
```
The .venv/ folder is ignored by Git by default.
If you change the environment folder name, update .gitignore accordingly.
Activate the environment:
- Linux/macOS
```bash
source .venv/bin/activate
```

### 3. Install dependencies
Install project dependencies:
ℹRequires pip >= 21.3
```bash
pip install -e .
```
The default dependencies include Qadence ([link](https://github.com/pasqal-io/qadence)) and Jupyter Notebook support.
You can edit them under [project.dependencies] in pyproject.toml.

Install the optional dependencies to use pre-commit/pytests/mkdocs:
```bash
pip install -e ".[dev]"
```

### 4. Set up pre-commit hooks
Enable automatic checks on commit:
```bash
pre-commit install
```

## Here the last info to run properly the Pre-commit

### 🧹 Pre-commit Hooks

Run all pre-commit hooks manually:
```bash
pre-commit run --all-files
```
