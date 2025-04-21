# 🎮 Video Game Sales Data Analysis

This project analyzes video game sales data by merging and visualizing datasets to uncover industry trends. It includes data wrangling scripts, visual output, and AWS-compatible utilities for dataset management.

## 📁 Project Structure

```
video_game_sales_data-master/
├── data_analyser/            # Core Python logic for data reading, merging, and analysis
├── dataset_creator/          # Shell scripts to upload and update datasets in cloud
├── plotted_graphs/           # Pre-generated visualizations (PDFs) of analysis results
├── configs.yaml              # YAML configuration file for analysis settings
├── requirements.txt          # Python dependencies
```

## 🚀 Getting Started

1. **Clone the Repository**

```bash
git clone https://github.com/yourusername/video_game_sales_data.git
cd video_game_sales_data
```

2. **Install Requirements**

```bash
pip install -r requirements.txt
```

3. **Run Analysis**

```bash
cd data_analyser
python driver.py
```

## 🔧 Technical Workflow

### 🔹 Local Coding Logic

- `data_reader_functions.py`: Reads structured CSV datasets into pandas DataFrames.
- `data_merger_functions.py`: Merges different data sources into a unified schema.
- `data_analysis_functions.py`: Performs grouped aggregations and statistical summaries.
- `dataframe_models.py`: Defines reusable DataFrame models and schemas.
- `driver.py`: Acts as the orchestrator — loads configs, calls all the above functions, and generates outputs.

### ☁️ Cloud-Based Data Engineering (AWS)

This repo is built with a production-oriented data pipeline mindset:

- **AWS S3**: Storage layer to host raw datasets (triggered via `kaggle_to_s3.sh`)
- **AWS Glue Data Catalog**: Metadata layer to register structured datasets (`glue_catalog_updater.sh`)
- **(Optional)** Extendable for:
  - AWS Athena for querying datasets
  - AWS Lambda for event-driven pipeline automation
  - AWS Glue ETL Jobs for further transformation at scale

### 🛠️ Services Used

| Service       | Purpose                                                  |
|---------------|----------------------------------------------------------|
| `pandas`      | Data wrangling, transformation, and in-memory analytics |
| `matplotlib`  | Visualization and chart generation (saved as PDFs)      |
| `PyYAML`      | Configuration handling via `configs.yaml`               |
| `AWS S3`      | Cloud data storage (via CLI and shell scripts)          |
| `AWS Glue`    | Schema discovery and data cataloging                    |

---

## 📊 Visualizations

Pre-generated graphs include:
- Sales per genre
- Sales per platform
- Sales per publisher
- Scatter plots of total sales vs. gameplay metrics

Find them under `plotted_graphs/`.

## ⚙️ Configuration

Customize your analysis via `configs.yaml`.

## ☁️ Cloud Integration (Optional)

Upload dataset to AWS S3 and update Glue Catalog using:

```bash
cd dataset_creator
./kaggle_to_s3.sh
./glue_catalog_updater.sh
```

---

## 📌 Dependencies

All dependencies are listed in `requirements.txt`.

---

## 📬 Contact

For questions or collaborations, feel free to reach out or fork the repo!
