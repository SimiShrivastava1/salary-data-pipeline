
#!/usr/bin/env python3

import sys
import os
import pandas as pd
import numpy as np
import traceback
import time
from datetime import datetime
from typing import Dict, List, Any

sys.path.append('/content/salary_pipeline')
sys.path.append('/content/drive/MyDrive')

from config import PipelineConfig
from utils import Logger, SmartMemoryManager
from data_processor import EnhancedSmartFileProcessor, calculate_percentile_weights, apply_source_weights
from outlier_detector import MinimalOutlierDetector
from statistics_generator import EnhancedStatisticsGenerator


class FileLoadingError(Exception):
    """Custom exception for file loading failures"""
    pass


class EnhancedSalaryPipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = Logger("salary_pipeline")
        self.memory_mgr = SmartMemoryManager()
        self._verify_salary_module()
        self.cached_raw_data = None
        self.cached_processed_data = None

    def _verify_salary_module(self):
        try:
            from salary import normalize_salary
        except ImportError as e:
            self.logger.error(f"Failed to import salary.py: {e}")
            raise

    def run(self):
        self.logger.info("Starting Salary Data Pipeline")
        self.logger.info(f"Environment: {self.config.environment}")
        self.logger.info(f"Z-score threshold: {self.config.zscore_threshold}")

        start_time = datetime.now()

        try:
            df = self._load_and_process_data()
            
            # CHECK: If no data loaded, raise error for retry
            if df is None or len(df) == 0:
                raise FileLoadingError("Data loading failed - no files were loaded")

            outlier_detector = MinimalOutlierDetector(self.config, self.logger)
            outlier_indices, outlier_results = outlier_detector.detect_all_outliers(df)

            outlier_results['total_records'] = len(df)
            self._log_outlier_results(outlier_results)

            bls_results = outlier_detector.bls_validate(df, outlier_indices)
            self._log_bls_results(bls_results)

            stats_generator = EnhancedStatisticsGenerator(self.config, self.logger)
            clean_results = stats_generator.generate_clean_dataset_and_stats(df, outlier_indices, bls_results)

            if not clean_results:
                self.logger.error("Statistics generation failed")
                return False

            runtime = (datetime.now() - start_time).total_seconds() / 60

            self.logger.info(f"Pipeline completed successfully in {runtime:.1f} minutes")
            self.logger.info(f"Final clean records: {len(clean_results['clean_df']):,}")
            self.logger.info(f"Output files: {len(clean_results.get('files_created', []))}")

            return True

        except FileLoadingError:
            # Re-raise file loading errors for retry logic
            raise
            
        except Exception as e:
            runtime = (datetime.now() - start_time).total_seconds() / 60
            self.logger.error(f"Pipeline failed after {runtime:.1f} minutes: {e}")
            return False

    def _load_and_process_data(self):
        if self.cached_processed_data is not None:
            return self.cached_processed_data.copy()

        if self.cached_raw_data is not None:
            df = self.cached_raw_data.copy()
        else:
            processor = EnhancedSmartFileProcessor(self.config, self.logger)

            try:
                df = processor.load_all_data_sources()
                
                # CHECK: If processor returns None or empty data
                if df is None or len(df) == 0:
                    raise FileLoadingError("No data loaded from file processor")

                self.cached_raw_data = df.copy()

            finally:
                processor.close()

        initial_count = len(df)

        df = df[df['salary_annual'].between(self.config.salary_min, self.config.salary_max)]
        df = df[df['job_title_normalized'].notna() & (df['job_title_normalized'] != '')]
        df = df[df['company'].notna() & (df['company'] != '')].copy()

        df = calculate_percentile_weights(df, 0.05)
        df = apply_source_weights(df)

        weight_threshold = df['final_weight'].quantile(0.01)
        df = df[df['final_weight'] >= weight_threshold].copy()

        # CHECK: If all data filtered out
        if len(df) == 0:
            raise FileLoadingError("All data was filtered out during processing")

        self.cached_processed_data = df.copy()
        return df

    def _log_outlier_results(self, outlier_results: Dict):
        total_outliers = outlier_results.get('zscore', 0)
        total_records = outlier_results.get('total_records', 1)
        outlier_percentage = (total_outliers / total_records * 100) if total_records > 0 else 0

        self.logger.info(f"Z-score outliers detected: {total_outliers:,} ({outlier_percentage:.2f}%)")

    def _log_bls_results(self, bls_results: Dict):
        if 'error' in bls_results:
            self.logger.warning(f"BLS validation failed: {bls_results['error']}")
            return

        if 'validation_skipped' in bls_results:
            self.logger.warning(f"BLS validation skipped: {bls_results.get('reason', 'Unknown reason')}")
            return

        validation_rate = bls_results.get('validation_rate', 0)
        legitimate_outliers = bls_results.get('legitimate_outliers', 0)

        self.logger.info(f"BLS validation completed: {legitimate_outliers:,} legitimate outliers ({validation_rate:.1f}%)")


def run_pipeline_once():
    config = PipelineConfig.from_yaml('/content/salary_pipeline/config.yaml', 'development')
    config.validate()

    pipeline = EnhancedSalaryPipeline(config)
    success = pipeline.run()
    
    if not success:
        raise Exception("Pipeline execution returned False")
    
    return success


def main():
    """Main function with auto-retry logic"""
    
    max_retries = 2
    retry_delay = 30
    
    for attempt in range(max_retries):
        try:
            print(f"Pipeline attempt {attempt + 1}/{max_retries}")
            
            # Run your existing pipeline
            success = run_pipeline_once()
            
            if success:
                print("Pipeline completed successfully")
                break
            
        except FileLoadingError as e:
            print(f"File loading failed: {e}")
            
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Pipeline failed permanently.")
                sys.exit(1)
                
        except Exception as e:
            print(f"Pipeline error: {e}")
            
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Pipeline failed permanently.")
                sys.exit(1)


if __name__ == "__main__":
    main()
    