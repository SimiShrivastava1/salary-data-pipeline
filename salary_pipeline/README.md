# Salary Data Processing Pipeline

Production-ready pipeline for processing salary data with statistical outlier detection, file validation, and comprehensive analytics generation.

## Project Structure

```
salary_pipeline/
├── main.py                   # Pipeline entry point and orchestration
├── config.py                 # Configuration management and data classes
├── config.yaml              # Environment settings and parameters
├── data_processor.py         # Data loading, validation and processing
├── outlier_detector.py       # Statistical outlier detection and BLS validation
├── statistics_generator.py   # Analytics generation and clean dataset creation
├── utils.py                 # Logging and memory management utilities
└── README.md               # Documentation
```

## Configuration Overview

Configure data sources, date ranges, and processing parameters in `config.yaml`.

### config.yaml Settings
```yaml
development:
  # Data Sources
  indeed_path: "/path/to/indeed/parquet/files"
  simplyhired_path: "/path/to/simplyhired/parquet/files"  
  glassdoor_path: "/path/to/glassdoor.csv"
  bls_path: "/path/to/BLS_data.csv"
  
  # Date Range Processing
  start_date: "2025-06-01"
  end_date: "2025-07-31"
  
  # Source Control
  enable_indeed: true      # Process Indeed data
  enable_simplyhired: true # Process SimplyHired data
  enable_glassdoor: true   # Process Glassdoor data
  enable_linkedin: true   # Process LinkedIn data
  
  # Quality Parameters
  salary_min: 15000.0      # Minimum valid salary
  salary_max: 50000000.0   # Maximum valid salary
  zscore_threshold: 3.0    # Outlier detection sensitivity
```

### File Validation Thresholds
- **Minimum Records**: 500 per file
- **Data Coverage**: 30% of required fields must be non-null
- **Required Fields**:
  - Job Sites: salary, job_title, company, city, state, seniority
  - Glassdoor: total_pay, job_title, company, location, years_of_exp

## Quick Start

### Prerequisites
- Custom `salary.py` module at `/content/drive/MyDrive/salary.py`
- Required data files in specified paths

### Basic Usage
```bash
# Run with default settings
python salary_pipeline/main.py

# Custom parameters
python salary_pipeline/main.py --zscore-threshold 2.8 --output-dir ./custom_output
```

## Data Processing Flow

### 1. Data Loading and Validation (data_processor.py)
- **File Discovery**: Finds files in date range
- **Quality Validation**: Checks required fields coverage
- **Source Processing**: Handles parquet files for job sites, CSV for Glassdoor

### 2. Data Enhancement
- **Glassdoor Processing**: Parses locations and standardizes seniority levels
- **Weighting System**: Applies source and temporal weights
- **Filtering**: Removes invalid salary ranges and incomplete records

### 3. Outlier Detection (outlier_detector.py)
- **Z-Score Analysis**: Detects statistical outliers
- **BLS Validation**: Cross-references with Bureau of Labor Statistics
- **Quality Control**: Validates outlier detection accuracy

### 4. Statistics Generation (statistics_generator.py)
- **Grouping**: By job_title, city, state, seniority_level
- **Metrics**: Mean, median, confidence intervals
- **Output**: Clean CSV with essential columns

## Output Files

1. **clean_salary_dataset.parquet**: Complete processed dataset
2. **salary_statistics_granular.csv**: Client-ready statistics with columns:
   - job_title_normalized, city, state, seniority_level
   - record_count, mean_salary, median_salary
   - mean_salary_95ci, sample_confidence, data_sources
3. **dataset_summary.json**: Processing summary and quality metrics

## Usage for New Team Members

1. **Setup**: Update paths in `config.yaml` for your environment
2. **Dependencies**: Ensure `salary.py` module is available
3. **Run**: Execute `python salary_pipeline/main.py`
4. **Monitor**: Check logs for validation results and processing status
5. **Output**: Find results in configured output directory
