# Salary Data Processing Pipeline

A production-ready, enterprise-grade data processing pipeline for comprehensive salary analysis, featuring advanced outlier detection, cost-of-living adjustments, and statistical validation against Bureau of Labor Statistics benchmarks.

## Overview

This pipeline provides automated processing of multi-source salary data with sophisticated data quality controls, statistical outlier detection, and enhanced analytics generation. The system integrates industry standards, geographic cost-of-living adjustments, and occupation-specific validation to produce reliable, actionable salary intelligence.

## Architecture & Components

```
salary_pipeline/
├── main.py                    # Pipeline orchestration and workflow management
├── config.py                  # Configuration classes and data validation schemas
├── config.yaml               # Environment configuration and processing parameters
├── data_processor.py          # ETL operations and data quality validation
├── outlier_detector.py        # Statistical analysis and BLS validation engine
├── statistics_generator.py    # Analytics engine and statistical computations
├── cola_processor.py          # Geographic cost-of-living adjustment algorithms
├── utils.py                   # Logging infrastructure and memory optimization
└── README.md                  # Technical documentation
```

## Data Requirements & Dependencies

### Primary Salary Data Sources (Parquet Format)

The pipeline expects parquet files organized in date-named directories under `/content/drive/MyDrive/SalaryDataFiles/parquet_exports/`:

#### Job Board Sources
- **indeed_us** / **indeed_ca** - Indeed job postings
- **linkedin_us** / **linkedin_ca** - LinkedIn job postings
- **simplyhired_us** - SimplyHired job postings
- **jora_us** / **jora_ca** - Jora job postings

#### ATS/Company Sources
- **jobvite** - Jobvite ATS data
- **icims** - iCIMS ATS data
- **greenhouse** - Greenhouse ATS data
- **gem** - Gem recruiting data
- **dayforce** - Dayforce/Ceridian data
- **avature** - Avature ATS data
- **ultipro** - UltiPro/UKG data
- **smartrecruiter** - SmartRecruiters data
- **lever** - Lever recruiting data
- **myworkdayjobs** - Workday jobs data
- **applytojob** - ApplyToJob platform data
- **adpworkforce** - ADP Workforce data
- **pjf_us** - PJF (Public Job Feed) data

**Minimum Required Fields in Job Board Data:**
- **Salary field** (any one of): `parsed_annual_salary_avg`, `salary_annual`, or `salary`
- **Job title field**: `nlp_norm_title`

**Optional Fields** (enhance analysis when available):
- `company_name` - Company name
- `final_city` or `city` - Job location city
- `final_state` or `state` - Job location state
- `nlp_seniority`, `seniority_level`, or `seniority` - Seniority classification
- `nlp_soc_code` - Standard Occupational Classification code
- `industry` - Industry classification
- `db_insert_timestamp` - Data timestamp

### Glassdoor Data (CSV Format)

- **File Path**: `/content/drive/MyDrive/SalaryDataFiles/glassdoor_soc_code_mapping.csv`
- **Minimum Required Fields**:
  - `total_pay` - Total compensation amount
  - `normalizedTitle` - Standardized job title

**Optional Glassdoor Fields**:
- `company_name` - Company name
- `location` - Job location (city, state format)
- `years_of_exp` - Years of experience (e.g., "1-3 years")
- `industry` - Industry classification
- `mapped_soc_code` - SOC code mapping
- `submitted_date` - Submission timestamp

### Required Reference Files

#### BLS Data (Required for Statistical Validation)
- **File Path**: `/content/drive/MyDrive/SalaryDataFiles/BLS_all_data_M_2024.csv`
- **Purpose**: Government salary benchmark validation
- **Required Columns**:
  - SOC/occupation code column (column name contains "OCC_CODE")
  - 10th percentile salary column (column name contains "PCT10" or "P10" and "A_")
  - 90th percentile salary column (column name contains "PCT90" or "P90" and "A_")

#### Cost-of-Living (COLA) Files (Required for Geographic Analysis)
- **`/content/drive/MyDrive/SalaryDataFiles/county_economic_stat.csv`** - County economic indicators
  - Required columns: `GeoFIPS`, `avg_salary`, `gdp_per_capita`
- **`/content/drive/MyDrive/SalaryDataFiles/uscities.csv`** - US cities reference
  - Required columns: `city`, `state_id`, `county_fips`

#### COLA Enhancement Files
- **`/content/drive/MyDrive/SalaryDataFiles/uszips.csv`** - US zip codes (enables zip-based COLA lookup)
  - Required columns: `zip`, `county_fips`
- **`/content/drive/MyDrive/SalaryDataFiles/uscounties.csv`** - US counties reference
- **`/content/drive/MyDrive/SalaryDataFiles/zip_code_database.csv`** - Additional zip code data

### Additional Reference Files
- **`SAEMP27N_ALL_AREAS_1998_2021.csv`** - BLS employment statistics by area
- **`SAINC7N_ALL_AREAS_1998_2022.csv`** - BLS income statistics by area
- **`state_industry_income.csv`** - State-level industry income benchmarks

### Custom Module Dependency

- **`salary.py`** - Must be accessible at `salary_pipeline/salary.py`
  - Required function: `normalize_salary(salary_text)`
  - Returns: `(min_salary, max_salary, avg_salary)` tuple

### Centralized File Location

All required data files are now centrally located in:
**`/content/drive/MyDrive/SalaryDataFiles/`**

This includes:
- Parquet export directories (`parquet_exports/`)
- Glassdoor data (`glassdoor_soc_code_mapping.csv`)
- BLS benchmark data (`BLS_all_data_M_2024.csv`)
- COLA reference files (`county_economic_stat.csv`, `uscities.csv`, etc.)
- Custom salary parsing module (`salary.py`)
- All other supporting CSV files

Ensure your `config.yaml` paths point to this central location as shown in the configuration examples.

### File Organization Structure

```
/content/drive/MyDrive/SalaryDataFiles/
├── parquet_exports/
│   ├── indeed_us/
│   │   ├── 2025-06-01.parquet
│   │   ├── 2025-06-02.parquet
│   │   └── ...
│   ├── simplyhired_us/
│   └── [other sources]/
├── glassdoor_soc_code_mapping.csv
├── BLS_all_data_M_2024.csv
├── salary.py
├── county_economic_stat.csv
├── uscities.csv
├── uszips.csv (optional)
├── uscounties.csv (optional)
├── zip_code_database.csv (optional)
├── SAEMP27N_ALL_AREAS_1998_2021.csv
├── SAINC7N_ALL_AREAS_1998_2022.csv
└── state_industry_income.csv
```

### Data Quality Requirements

- **Minimum Records**: 500 records per input file
- **Salary Coverage**: ≥30% non-null salary values
- **Title Coverage**: ≥50% non-null job titles
- **Date Range**: Files must be named `YYYY-MM-DD.parquet` within configured date range
- **File Validation**: Files undergo automated quality checks before processing

## Component Architecture

### 1. Pipeline Orchestrator (`main.py`)
**Responsibilities**: Workflow coordination, error handling, and execution monitoring
- Manages multi-stage processing pipeline with checkpoint recovery
- Implements comprehensive error handling and rollback capabilities
- Provides execution monitoring and performance metrics collection
- Supports both batch and incremental processing modes

### 2. Configuration Management (`config.py`)
**Responsibilities**: Centralized parameter management and validation schemas
- Implements data classes for type-safe configuration management
- Provides environment-specific configuration loading
- Validates input parameters and data quality thresholds
- Manages file path resolution and dependency checking

### 3. Data Processing Engine (`data_processor.py`)
**Responsibilities**: ETL operations and data quality assurance
- **Data Discovery**: Automated file detection with date-range filtering
- **Quality Validation**: Multi-dimensional data quality assessment
- **Industry Classification**: Automated industry taxonomy assignment using NLP
- **Geographic Enrichment**: Location standardization and demographic enhancement
- **Schema Normalization**: Cross-source data harmonization

### 4. Outlier Detection System (`outlier_detector.py`)
**Responsibilities**: Statistical anomaly detection and validation
- **Multi-Method Detection**: Z-score analysis, IQR-based detection, and domain-specific rules
- **SOC Code Validation**: Occupation-specific outlier detection using Standard Occupational Classification
- **BLS Cross-Validation**: Government benchmark validation for statistical accuracy
- **Industry-Specific Thresholds**: Dynamic outlier boundaries based on industry standards
- **Confidence Scoring**: Probabilistic outlier classification with confidence intervals

### 5. Analytics Generation Engine (`statistics_generator.py`)
**Responsibilities**: Advanced statistical computation and reporting
- **Hierarchical Aggregation**: Multi-dimensional grouping (title, location, industry, seniority)
- **Statistical Metrics**: Comprehensive descriptive statistics with confidence intervals
- **Reliability Scoring**: Sample size-based confidence and reliability indicators
- **Trend Analysis**: Time-series analysis and seasonal adjustment capabilities
- **Benchmarking**: Industry and geographic comparative analysis

### 6. Cost-of-Living Processor (`cola_processor.py`)
**Responsibilities**: Geographic salary normalization and adjustment
- **Tier Classification**: Automated cost-of-living tier assignment using economic indicators
- **Adjustment Algorithms**: Regional purchasing power parity calculations
- **Geographic Analysis**: Location-specific salary intelligence and market analysis
- **Dynamic Updates**: Real-time cost-of-living index integration

## How It Works

The pipeline processes data through these components in sequence:
1. **Data Processor**: Loads files, validates quality, extracts industry information
2. **Outlier Detector**: Removes suspicious salary data using statistical analysis and BLS validation
3. **Statistics Generator**: Creates aggregated salary statistics with confidence intervals
4. **COLA Processor**: Adjusts salaries for geographic cost-of-living differences

## Output Files

**Location**: Files are created in the directory specified by `output_dir` in the config (default: `/content/output`)

### 1. Clean Dataset (`clean_salary_dataset.parquet`)
- All salary data with outliers removed and quality validation applied
- Cost-of-living adjustments included
- Industry classifications added
- Geographic standardization completed

### 2. Statistical Summary (`salary_statistics.csv`)
Comprehensive salary statistics including:
- **Segmentation**: job_title_normalized, city, state, industry, seniority_level
- **Core Metrics**: record_count, mean_salary_original, mean_salary_adjusted, median_salary_original, median_salary_adjusted, min_salary_original, max_salary_original
- **Quality Indicators**: mean_salary_original_95ci, sample_confidence, data_sources
- **COLA Analysis**: cola_multiplier_avg, cola_tier_primary

## Usage
### One-time setup
1. Open the shared folder link : https://drive.google.com/drive/folders/1Hx26iPQNjLo3cEDNxq5P4HQmKBXAPwJH?usp=sharing
2. Click Add shortcut to Drive.
3. Choose My Drive (root) and do not rename the shortcut — keep it exactly SalaryDataFiles.
4. To verify if the folder is added to your drive, run the below snippet -

```python
from google.colab import drive
drive.mount('/content/drive', force_remount=True)

import os
DATA_DIR = "/content/drive/MyDrive/SalaryDataFiles"
assert os.path.isdir(DATA_DIR), (
    "Could not find 'SalaryDataFiles'. Make sure you added a shortcut to *My Drive* root "
    "with the exact name 'SalaryDataFiles'."
)
print("Data directory:", DATA_DIR)
```
5. Common pitfall: If the assert fails, the shortcut was renamed (e.g., SalaryDataFiles (1)) or placed inside another folder. Move it to My Drive root and keep the exact name SalaryDataFiles.

## Installation and Setup
### To run this pipeline in Google Colab, follow these steps:

1. **Open** [Google Colab](https://colab.research.google.com/)
2. **Create** a new notebook
3. **Run** these commands in order:

```python
# Clone the repository
!git clone https://github.com/SimiShrivastava1/salary-data-pipeline.git

# Navigate to the project directory
%cd salary-data-pipeline

# Switch to the feature branch
!git checkout feature/salary-pipeline

# Install required packages
!pip install pandas==2.2.2 numpy==1.26.4 duckdb==1.0.0 pyarrow fastparquet PyYAML

# Navigate to the salary directory
%cd salary_pipeline

# Run the pipeline
!python main.py

#Alternatively you can run it without changing the directory
!python /content/salary-data-pipeline/salary_pipeline/main.py
```

**Ensure all '.py' files are in the correct directory**

**Verify `config.yaml` is properly configured with your file paths**


### Configuration

1. **Update Configuration File (`config.yaml`)**
   - Set date range: `start_date` and `end_date`
   - Configure output directory: `output_dir`
   - Verify file paths for all data sources match your file organization
   - Enable/disable data sources as needed using the `enabled: true/false` flags

2. **Data Source Configuration**
   ```yaml
   data_sources:
     - name: "indeed_us"
       path: "/content/drive/MyDrive/SalaryDataFiles/parquet_exports/indeed_us"
       enabled: true
       type: "job_board"
   ```

3. **COLA Settings Configuration**
   ```yaml
   cola_settings:
     salary_weight: 0.7    # Weight for salary-based COLA calculations
     gdp_weight: 0.3       # Weight for GDP-based COLA calculations
     enable_zip_lookup: true
     enable_city_lookup: true
   ```

### Data Preparation

1. **Organize Salary Data**
   - Place parquet files in dated subdirectories under each source folder
   - File naming convention: `YYYY-MM-DD.parquet`
   - Ensure minimum required fields (salary + job title) are present

2. **Verify Reference Files**
   - Confirm all required CSV files are in the correct locations
   - Check that BLS data contains the required salary percentile columns
   - Validate COLA files have necessary geographic mapping columns

3. **Test Data Quality**
   - Each parquet file should have ≥500 records
   - Salary fields should have ≥30% coverage
   - Job title fields should have ≥50% coverage

### Performance Optimization

1. **Memory Management**
   - Pipeline automatically manages memory usage with chunked processing
   - Configurable batch sizes in `config.yaml`
   - Automatic garbage collection between processing stages

2. **Processing Optimization**
   - Enable only required data sources to reduce processing time
   - Adjust date ranges to focus on specific time periods
   - Configure outlier detection parameters based on data quality needs

### Troubleshooting

**Common Issues:**
- **"No files in date range"**: Check parquet file naming and date range configuration
- **"No salary data"**: Verify salary field names match expected columns
- **"File loading error"**: Check file permissions and corruption
- **"BLS validation failed"**: Confirm BLS file path and column structure

**Log Analysis:**
- LOADED messages indicate successful data source processing
- SKIPPED messages show why sources were excluded
- Processing statistics help identify data quality issues
- Final record counts validate pipeline success

**Output Issues:**
- **"No output folder created"**: Check file permissions and disk space. Pipeline creates folder automatically on successful completion.
- **"No module named salary"**: Ensure you're using the latest version of the repository that includes `salary.py`

