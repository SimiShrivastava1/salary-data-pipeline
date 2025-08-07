#!/usr/bin/env python3
"""
Production Salary Pipeline
"""

import sys
import os
import argparse
from datetime import datetime

# Add paths for imports
sys.path.append('/content/salary_pipeline')
sys.path.append('/content/drive/MyDrive')  # For salary.py

from config import PipelineConfig
from utils import Logger, SmartMemoryManager
from data_processor import EnhancedSmartFileProcessor, calculate_percentile_weights, apply_source_weights
from outlier_detector import OutlierDetector
from statistics_generator import StatisticsGenerator

def create_cli():
    """Command line interface"""
    parser = argparse.ArgumentParser(description="Production Salary Data Pipeline")
    
    parser.add_argument('--config', default='/content/salary_pipeline/config.yaml',
                       help='Configuration file path')
    parser.add_argument('--env', choices=['development', 'production'],
                       help='Environment override')
    parser.add_argument('--zscore-threshold', type=float,
                       help='Z-score threshold override')
    parser.add_argument('--output-dir', help='Output directory override')
    parser.add_argument('--validate-only', action='store_true',
                       help='Validate configuration only')
    
    return parser.parse_args()

class SalaryPipeline:
    """Main salary data processing pipeline"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = Logger("salary_pipeline")
        self.memory_mgr = SmartMemoryManager()
        
        # Verify salary.py is available
        self._verify_salary_module()
        
        # Data caching
        self.cached_raw_data = None
        self.cached_processed_data = None
    
    def _verify_salary_module(self):
        """Verify salary.py module is accessible"""
        try:
            from salary import normalize_salary
            self.logger.info("Salary parsing module loaded successfully")
        except ImportError as e:
            self.logger.error(f"Failed to import salary.py: {e}")
            self.logger.error("Please ensure salary.py is available at /content/drive/MyDrive/salary.py")
            raise
        
    def run(self):
        """Execute complete pipeline"""
        
        self.logger.info("Starting salary data pipeline")
        self.logger.info(f"Environment: {self.config.environment}")
        self.logger.info(f"Z-score threshold: {self.config.zscore_threshold}")
        
        start_time = datetime.now()
        
        try:
            # Data loading and processing
            df = self._load_and_process_data()
            if df is None or len(df) == 0:
                self.logger.error("Data loading failed")
                return False
            
            self.logger.info(f"Data processed: {len(df):,} records")
            
            # Outlier detection
            outlier_detector = OutlierDetector(self.config, self.logger)
            outlier_indices = outlier_detector.detect_zscore_outliers(df)
            
            # BLS validation
            bls_results = outlier_detector.bls_validate(df, outlier_indices)
            
            # Clean dataset generation
            stats_generator = StatisticsGenerator(self.config, self.logger)
            clean_results = stats_generator.generate_clean_dataset_and_stats(df, outlier_indices, bls_results)
            
            if not clean_results:
                self.logger.error("Clean dataset generation failed")
                return False
            
            # Pipeline completion
            runtime = (datetime.now() - start_time).total_seconds() / 60
            self.logger.info(f"Pipeline completed successfully in {runtime:.1f} minutes")
            self.logger.info(f"Final clean records: {len(clean_results['clean_df']):,}")
            self.logger.info(f"Output files: {len(clean_results['files_created'])}")
            
            return True
            
        except Exception as e:
            runtime = (datetime.now() - start_time).total_seconds() / 60
            self.logger.error(f"Pipeline failed after {runtime:.1f} minutes: {e}")
            return False
    
    def _load_and_process_data(self):
        """Load and process salary data"""
        
        # Check processed data cache
        if self.cached_processed_data is not None:
            self.logger.info("Using cached processed data")
            return self.cached_processed_data.copy()
        
        # Load raw data
        if self.cached_raw_data is not None:
            self.logger.info("Using cached raw data")
            df = self.cached_raw_data.copy()
        else:
            processor = EnhancedSmartFileProcessor(self.config, self.logger)
            
            try:
                df = processor.load_all_data_sources()
                if df is None:
                    return None
                
                self.cached_raw_data = df.copy()
                
            finally:
                processor.close()
        
        # Data processing
        initial_count = len(df)
        
        # Apply filters
        df = df[df['salary_annual'].between(self.config.salary_min, self.config.salary_max)]
        df = df[df['job_title_normalized'].notna() & (df['job_title_normalized'] != '')]
        df = df[df['company'].notna() & (df['company'] != '')].copy()
        
        # Apply weighting system
        df = calculate_percentile_weights(df, 0.05)
        df = apply_source_weights(df)
        
        # Weight-based filtering
        weight_threshold = df['final_weight'].quantile(0.01)
        df = df[df['final_weight'] >= weight_threshold].copy()
        
        final_count = len(df)
        self.logger.info(f"Data filtering: {initial_count:,} → {final_count:,} records")
        
        # Cache processed data
        self.cached_processed_data = df.copy()
        
        # SOC code availability
        if 'soc_code' in df.columns:
            soc_available = df['soc_code'].notna().sum()
            soc_percentage = (soc_available / len(df)) * 100
            self.logger.info(f"SOC codes: {soc_available:,} available ({soc_percentage:.1f}%)")
        
        return df

def main():
    """Main entry point"""
    
    args = create_cli()
    
    try:
        # Load configuration
        config = PipelineConfig.from_yaml(args.config, args.env)
        
        # Apply CLI overrides
        if args.zscore_threshold:
            config.zscore_threshold = args.zscore_threshold
        if args.output_dir:
            config.output_dir = args.output_dir
        
        # Validate configuration
        config.validate()
        
        if args.validate_only:
            print("Configuration validation successful")
            return
        
    except Exception as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    # Run pipeline
    pipeline = SalaryPipeline(config)
    success = pipeline.run()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
