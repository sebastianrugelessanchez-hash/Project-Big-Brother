# 🧠 Project Big Brother

## 📊 Project Overview
Project Big Brother was developed to automate and monitor the reconciliation process of daily invoices generated across the United States. The project identifies inconsistencies, duplicates, and missing records in large datasets, providing a consolidated view of data quality and operational performance.

The goal is to replace manual reconciliation with an automated, scalable solution using **Python, Pandas, and chunk-based processing** for efficiency and memory optimization.

---

## 🔧 Technical Architecture

### ETL Pipeline
**Extract → Transform → Load** implemented in Python:

1. **Extract:** Reads Excel, CSV, or Parquet files, supports multi-sheet Excel inputs, and auto-detects headers.
2. **Transform:** Cleans column names, detects duplicates/missing values, and applies tolerance thresholds.
3. **Load:** Exports multi-sheet Excel reports optimized for **Looker Studio** dashboards.

### Core Modules
- `cli.py`: Command-line entry point for running the ETL pipeline.
- `pipeline.py`: Central orchestration logic connecting all steps.
- `processing.py`: Column cleaning, feature validation, and chunk-based transformations.
- `io.py`: Input/output layer for Excel, CSV, and Parquet.
- `production_system.py`: Production-level configuration manager.
- `series.py`: Time-based segmentation and tolerance management.

---

## ⚙️ Usage

### Setup
```bash
conda create -n recon python=3.11
conda activate recon
pip install -r requirements.txt
```

### Run Pipeline
```bash
python cli.py --in data/invoices_2025-10.csv --out out/reconciliation_report.xlsx --tolerance 100 --huge 101 --engine pandas --chunksize 5000
```

**Sheets generated:**
- Resumen
- Detalle_por_Serie
- Faltantes_intra
- Duplicados_por_Serie

---

## 📂 Project Structure
Project-Big-Brother/
├── cli.py
├── pipeline.py
├── processing.py
├── io.py
├── series.py
├── production_system.py
├── requirements.txt
├── README.txt
├── data/
└── out/

---

## 🧰 Tech Stack
- **Language:** Python 3.11  
- **Libraries:** Pandas, NumPy, Dask, OpenPyXL, PyArrow  
- **Visualization:** Looker Studio  
- **Processing:** Chunk-based and memory-safe design  

---

## 👨‍💻 Author
Developed by **Sebastian Rugeles Sanchez**  
Contact: [GitHub Profile](https://github.com/sebastianrugelessanchez-hash)

