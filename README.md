# Dreyfus - Development Impact Analysis Tool

This tool analyzes the impact of development changes by processing CSAT surveys, support tickets, and development backlog data.

## Setup

### 1. Create Virtual Environment

First, create and activate a Python virtual environment:

\\\
# Create virtual environment
python -m venv .venv

# Activate virtual environment (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Activate virtual environment (Mac/Linux)
source .venv/bin/activate
\\\

### 2. Install Dependencies

With the virtual environment activated, install the required packages:

\\\
pip install -e .
\\\

This will install:
- pandas
- numpy
- scikit-learn
- transformers
- plotly

## Generate Sample Data

Before running the main analysis, you need some data to analyze. Use the sample data generator:

\\\
python scripts/generate_sample_data.py
\\\

This will create:
- \data/raw/dev_backlog.csv\: Development backlog items
- \data/raw/csat_2023_q1.csv\: Q1 CSAT survey responses
- \data/raw/csat_2023_q2.csv\: Q2 CSAT survey responses
- \data/raw/csat_2023_q3.csv\: Q3 CSAT survey responses
- \data/raw/csat_2023_q4.csv\: Q4 CSAT survey responses
- \data/raw/support_tickets.csv\: Support ticket data

## Run Analysis

Run the complete analysis pipeline with:

\\\
python main.py
\\\

This will:
1. Preprocess the raw data
2. Load and ingest the data
3. Run sentiment analysis on CSAT and ticket text
4. Analyze development impact
5. Generate visualizations

## Project Structure

\\\
dreyfus/
├── data/
│   ├── raw/          # Raw input data
│   └── output/       # Processed data
├── src/
│   ├── preprocessing.py
│   ├── data_ingestion.py
│   ├── sentiment_analysis.py
│   ├── impact_analysis.py
    visualise.py
 scripts/
    generate_sample_data.py
 main.py
 setup.py
\\\

## Output

The analysis will generate:
- Processed data files in \data/output/\
- Impact analysis visualizations
- Priority-ranked development items
- Theme and sentiment analysis results

## Notes

- Ensure the \data/raw\ directory exists before running the sample data generator
- The virtual environment must be activated for all commands
- All paths are relative to the project root directory
