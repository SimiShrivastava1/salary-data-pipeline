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

### Input Data Sources

#### Primary Salary Data
- **Job Sites Dataset**: Required fields include `salary`, `title`, `company`, `city`, `state`, `seniority`
- **Glassdoor Dataset**: Required fields include `total_pay`, `job_title`, `company`, `location`, `years_of_exp`

#### Reference Data Files
- `county_economic_stat.csv` - County-level economic indicators
- `state_industry_income.csv` - State-level industry income benchmarks
- `zip_code_database.csv` - Geographic reference and demographic data
- `uscities.csv` / `uscounties.csv` / 'uszips.csv' - Comprehensive US geographic taxonomy
- `SAEMP27N_ALL_AREAS_1998_2021.csv` - BLS employment statistics by area
- `SAINC7N_ALL_AREAS_1998_2022.csv` - BLS income statistics by area

#### Custom Modules
- `salary.py` - Proprietary salary parsing and normalization module

### Data Quality Thresholds
- **Minimum Record Count**: 500 records per input file
- **Field Coverage Requirements**: 30% non-null values for critical fields
- **Statistical Significance**: Minimum sample sizes enforced for reliability metrics

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

### Prerequisites
1. Update `config.yaml` with your file paths and settings
2. Ensure all required data files are available (see Data Requirements section)
3. Verify `salary.py` module is accessible
4. Create directory structure:

    ```bash
    import os
    pipeline_dir = "/content/salary_pipeline"  
    os.makedirs(pipeline_dir, exist_ok=True)
    ```

### Run the Pipeline
```bash
python salary_pipeline/main.py
```

The pipeline will process all data and generate the output files in the specified output directory.
