# Salary Data Processing Pipeline

Production-ready pipeline for processing salary data with statistical outlier detection and comprehensive analytics generation.

## Prerequisites

Ensure `salary.py` is available at `/content/drive/MyDrive/salary.py` with the `normalize_salary` function.
Ensure the correct path for the job sources parquet files.

## Quick Start

```bash
# Run with default configuration
python /content/salary_pipeline/main.py

# Use production environment
python /content/salary_pipeline/main.py --env production

# Custom Z-score threshold
python /content/salary_pipeline/main.py --zscore-threshold 2.8

# Validate configuration
python /content/salary_pipeline/main.py --validate-only
```

## Architecture

```
├── main.py                   # Pipeline entry point
├── config.py                 # Configuration management
├── config.yaml              # Environment settings
├── data_processor.py         # Data loading and preprocessing 
├── outlier_detector.py       # Statistical outlier detection
├── statistics_generator.py   # Analytics and clean dataset generation 
├── utils.py                 # Logging and memory management
└── README.md                # Documentation
```

## Key Enhancements

### Streamlined Output
- **Client CSV**: Contains only requested columns for direct use
- **Professional Logging**: Clean, production-appropriate output
- **Memory Optimized**: Efficient processing of large datasets

## Dependencies

The pipeline requires:
- Standard Python packages
- Custom `salary.py` module at `/content/drive/MyDrive/salary.py`

## Configuration

Update `config.yaml` with your environment-specific settings:

- **Data paths**: Parquet file locations and BLS reference data
- **Processing parameters**: Date ranges, salary bounds, detection thresholds
- **Output settings**: Result file locations

## Features

- **Multi-source data integration**: Indeed, SimplyHired, Glassdoor, LinkedIn
- **Enhanced Glassdoor processing**: Location parsing and seniority mapping
- **Memory-efficient processing**: Optimized for large datasets with DuckDB
- **Statistical outlier detection**: Z-score and MAD-based methods
- **BLS validation**: Cross-reference against Bureau of Labor Statistics data
- **Streamlined analytics**: Clean client CSV with only essential columns

## Output Files

1. **clean_salary_dataset.parquet**: Processed dataset with outliers removed
2. **salary_statistics_granular.csv**: Streamlined statistics with only requested columns:
   - job_title_normalized, city, state, seniority_level
   - record_count, mean_salary, median_salary
   - mean_salary_95ci, sample_confidence, data_sources
3. **dataset_summary.json**: High-level processing summary and data quality metrics

