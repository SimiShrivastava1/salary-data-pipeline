import os
import gc
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional, Tuple, Dict
import duckdb
import sys

from utils import Logger, SmartMemoryManager
from config import PipelineConfig

def parse_salary_with_salary_py(salary_text):
    """Parse salary using salary.py normalize_salary function"""
    try:
        sys.path.append('/content/drive/MyDrive')
        from salary import normalize_salary
        min_sal, max_sal, avg_sal = normalize_salary(str(salary_text))

        if avg_sal is not None:
            return avg_sal
        elif min_sal is not None:
            return min_sal
        else:
            return None
    except Exception:
        return None

def parse_glassdoor_salary_with_per_year(salary_text):
    """Parse Glassdoor salary by adding per year context"""
    if not salary_text or pd.isna(salary_text):
        return None

    salary_with_context = f"{salary_text} per year"
    return parse_salary_with_salary_py(salary_with_context)

def parse_glassdoor_location(location_str) -> Tuple[str, str]:
    """
    Parse Glassdoor location string to extract city and state
    Simple split by comma approach

    Examples:
    - "Seattle, WA" -> ("Seattle", "WA")
    - "New York, NY" -> ("New York", "NY")
    - "San Francisco, CA" -> ("San Francisco", "CA")
    - "Los Angeles, California" -> ("Los Angeles", "California")
    """
    if not location_str or pd.isna(location_str):
        return 'Unknown', 'Unknown'

    location_clean = str(location_str).strip()
    parts = [part.strip() for part in location_clean.split(',')]

    if len(parts) >= 2:
        city = parts[0]
        state = parts[1]
        return city, state
    elif len(parts) == 1:
        return parts[0], 'Unknown'

    return 'Unknown', 'Unknown'

def map_glassdoor_seniority(years_exp_str) -> str:
    """
    Map Glassdoor years of experience to standard seniority levels

    Mapping:
    - '0-1 year' -> 'Entry level'
    - '1-3 years' -> 'Junior level'
    - '4-6 years' -> 'Mid level'
    - '7-9 years' -> 'Mid-Senior level'
    - '10-14 years' -> 'Senior level'
    - '15+ years' -> 'Director'
    """
    if not years_exp_str or pd.isna(years_exp_str):
        return 'Unknown'

    years_exp_clean = str(years_exp_str).strip().lower()

    seniority_mapping = {
        '0-1 year': 'Entry level',
        '1-3 years': 'Junior level',
        '4-6 years': 'Mid level',
        '7-9 years': 'Mid-Senior level',
        '10-14 years': 'Senior level',
        '15+ years': 'Director'
    }

    for key, value in seniority_mapping.items():
        if key.lower() == years_exp_clean:
            return value

    if '0-1' in years_exp_clean or years_exp_clean in ['0-1', '0 to 1', '0 - 1']:
        return 'Entry level'
    elif '1-3' in years_exp_clean or years_exp_clean in ['1-3', '1 to 3', '1 - 3']:
        return 'Junior level'
    elif '4-6' in years_exp_clean or years_exp_clean in ['4-6', '4 to 6', '4 - 6']:
        return 'Mid level'
    elif '7-9' in years_exp_clean or years_exp_clean in ['7-9', '7 to 9', '7 - 9']:
        return 'Mid-Senior level'
    elif '10-14' in years_exp_clean or years_exp_clean in ['10-14', '10 to 14', '10 - 14']:
        return 'Senior level'
    elif '15+' in years_exp_clean or '15 plus' in years_exp_clean or years_exp_clean in ['15+', '15 plus', '15 or more']:
        return 'Director'

    return 'Unknown'

def validate_file_quality(df: pd.DataFrame, source_name: str, file_path: str, logger: Logger, silent: bool = True) -> bool:
    """
    Validate if a file has sufficient data quality to be processed
    
    Args:
        df: DataFrame to validate
        source_name: Source type (indeed, simplyhired, glassdoor, linkedin)
        file_path: Path to file being validated
        logger: Logger instance
        silent: If True, only log on success, not failures
    
    Returns:
        bool: True if file should be processed, False if should be skipped
    """
    
    # Define required fields for each source
    required_fields = {
        'indeed': ['parsed_annual_salary_avg', 'nlp_norm_title', 'company_name', 'final_city', 'final_state', 'nlp_seniority'],
        'simplyhired': ['parsed_annual_salary_avg', 'nlp_norm_title', 'company_name', 'final_city', 'final_state', 'nlp_seniority'],
        'linkedin': ['parsed_annual_salary_avg', 'nlp_norm_title', 'company_name', 'final_city', 'final_state', 'nlp_seniority'],
        'glassdoor': ['total_pay', 'normalizedTitle', 'company_name', 'location', 'years_of_exp']
    }
    
    # Quality thresholds
    min_records = 500  # Minimum records to consider file useful
    min_coverage = 0.30  # 30% of required fields must be non-null
    
    file_name = os.path.basename(file_path)
    
    # Check minimum record count
    if len(df) < min_records:
        if not silent:
            logger.info(f"Skipping {file_name}: insufficient records ({len(df)} < {min_records})")
        return False
    
    # Check if source is supported
    if source_name not in required_fields:
        if not silent:
            logger.warning(f"Skipping {file_name}: unsupported source type '{source_name}'")
        return False
    
    # Check field coverage
    fields = required_fields[source_name]
    missing_fields = []
    low_coverage_fields = []
    
    for field in fields:
        if field not in df.columns:
            missing_fields.append(field)
        else:
            non_null_count = df[field].notna().sum()
            coverage = non_null_count / len(df)
            
            if coverage < min_coverage:
                low_coverage_fields.append(f"{field} ({coverage*100:.1f}%)")
    
    # Log issues and make decision
    if missing_fields:
        if not silent:
            logger.info(f"Skipping {file_name}: missing required fields: {', '.join(missing_fields)}")
        return False
    
    if low_coverage_fields:
        if not silent:
            logger.info(f"Skipping {file_name}: insufficient data coverage: {', '.join(low_coverage_fields)}")
        return False
    
    # File passes validation
    if not silent:
        logger.debug(f"Processing {file_name}: {len(df):,} records, all quality checks passed")
    return True

# Updated method in EnhancedSmartFileProcessor class:
def _load_source_data_with_validation(self, source_name: str, folder_path: str):
    """Load data from parquet source with validation"""
    all_files = self.find_files_in_date_range(folder_path)
    if not all_files:
        return None

    # Validate files before processing
    validated_files = []
    
    for file_path in all_files:
        try:
            # Load file for validation
            if self.conn:
                df = self.conn.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
            else:
                df = pd.read_parquet(file_path)

            # Validate file quality (silently)
            if validate_file_quality(df, source_name, file_path, self.logger, silent=True):
                validated_files.append(file_path)

        except Exception:
            continue

    if not validated_files:
        self.logger.warning(f"No valid {source_name} files found - all {len(all_files)} files failed validation")
        return None

    self.logger.info(f"{source_name}: processing {len(validated_files)}/{len(all_files)} files after validation")

    # Process validated files
    chunk_size = self.memory_mgr.calculate_optimal_chunk_size(len(validated_files))
    all_chunks = []

    for i in range(0, len(validated_files), chunk_size):
        chunk_files = validated_files[i:i+chunk_size]

        if not self.memory_mgr.memory_check(f"{source_name} processing"):
            break

        chunk_data = self._load_file_chunk(chunk_files, source_name)
        if chunk_data is not None and len(chunk_data) > 0:
            all_chunks.append(chunk_data)

        del chunk_data
        self.memory_mgr.force_cleanup()

    if all_chunks:
        combined = pd.concat(all_chunks, ignore_index=True)
        del all_chunks
        self.memory_mgr.force_cleanup()
        return combined

    return None

def calculate_percentile_weights(df, decay_factor=0.05):
    """Calculate volume-based weights for temporal data"""
    if 'date' in df.columns and not df['date'].isna().all():
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date_only'] = df['date'].dt.date
        dc = df['date_only'].value_counts().reset_index()
        dc.columns = ['date','count']
        dc['pct'] = dc['count'].rank(pct=True, method='min')
        dc['w'] = dc['pct']**decay_factor
        m = dict(zip(dc['date'], dc['w']))
        df['density_weight'] = df['date_only'].map(m).fillna(1.0)
        df.drop('date_only', axis=1, inplace=True)
    else:
        df['density_weight'] = 1.0
    return df

def apply_source_weights(df):
    """Apply source-based quality weights"""
    weights = {'indeed': 1.0, 'simplyhired': 0.8, 'glassdoor': 0.9, 'linkedin': 0.3}
    df['source_weight'] = df['source'].map(weights).fillna(0.5)
    df['final_weight'] = df['density_weight'] * df['source_weight']
    return df

class EnhancedSmartFileProcessor:
    """Data processing with DuckDB integration, Glassdoor enhancements, and file validation"""

    def __init__(self, config: PipelineConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self.conn = duckdb.connect()
        self.memory_mgr = SmartMemoryManager()

        self.start_date = datetime.strptime(config.start_date, config.date_format)
        self.end_date = datetime.strptime(config.end_date, config.date_format)

    def apply_weighting(self, df):
        """Apply weighting system to dataframe"""
        df = calculate_percentile_weights(df, 0.05)
        df = apply_source_weights(df)
        return df

    def find_files_in_date_range(self, folder_path: str) -> List[str]:
        """Find parquet files within date range"""
        if not os.path.exists(folder_path):
            return []

        all_files = glob.glob(os.path.join(folder_path, "*.parquet"))
        if not all_files:
            return []

        date_files = []
        for file_path in all_files:
            filename = os.path.basename(file_path)
            try:
                date_str = filename.replace('.parquet', '')
                file_date = datetime.strptime(date_str, "%Y-%m-%d")

                if self.start_date <= file_date <= self.end_date:
                    date_files.append(file_path)
            except ValueError:
                continue

        return sorted(date_files)

    def load_all_data_sources(self):
        """Load data from all enabled sources"""
        all_data = []

        if self.config.enable_indeed:
            indeed_data = self._load_source_data_with_validation('indeed', self.config.indeed_path)
            if indeed_data is not None and len(indeed_data) > 0:
                all_data.append(indeed_data)
                self.logger.info(f"Indeed: {len(indeed_data):,} records")
                self.memory_mgr.force_cleanup()

        if self.config.enable_simplyhired:
            sh_data = self._load_source_data_with_validation('simplyhired', self.config.simplyhired_path)
            if sh_data is not None and len(sh_data) > 0:
                all_data.append(sh_data)
                self.logger.info(f"SimplyHired: {len(sh_data):,} records")
                self.memory_mgr.force_cleanup()

        if self.config.enable_linkedin:
            linkedin_data = self._load_source_data_with_validation('linkedin', self.config.linkedin_path)
            if linkedin_data is not None and len(linkedin_data) > 0:
                all_data.append(linkedin_data)
                self.logger.info(f"LinkedIn: {len(linkedin_data):,} records")
                self.memory_mgr.force_cleanup()

        if self.config.enable_glassdoor:
            glassdoor_data = self._load_glassdoor_data()
            if glassdoor_data is not None and len(glassdoor_data) > 0:
                all_data.append(glassdoor_data)
                self.logger.info(f"Glassdoor: {len(glassdoor_data):,} records")
                self.memory_mgr.force_cleanup()

        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            del all_data
            self.memory_mgr.force_cleanup()

            self.logger.info(f"Total combined records: {len(combined_data):,}")

            if 'soc_code' in combined_data.columns:
                total_soc_count = combined_data['soc_code'].notna().sum()
                self.logger.info(f"SOC codes available: {total_soc_count:,} ({total_soc_count/len(combined_data)*100:.1f}%)")

            return combined_data
        else:
            self.logger.error("No data loaded from any source")
            return None

    def _load_source_data_with_validation(self, source_name: str, folder_path: str):
        """Load data from parquet source with validation"""
        all_files = self.find_files_in_date_range(folder_path)
        if not all_files:
            return None

        # Validate files before processing
        validated_files = []
        
        self.logger.info(f"Validating {len(all_files)} {source_name} files...")
        
        for file_path in all_files:
            try:
                # Load file for validation
                if self.conn:
                    df = self.conn.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
                else:
                    df = pd.read_parquet(file_path)

                # Validate file quality (with logging)
                if validate_file_quality(df, source_name, file_path, self.logger):
                    validated_files.append(file_path)

            except Exception:
                continue

        if not validated_files:
            self.logger.warning(f"No valid {source_name} files found after validation")
            return None

        self.logger.info(f"Processing {len(validated_files)} validated {source_name} files")

        # Process validated files
        chunk_size = self.memory_mgr.calculate_optimal_chunk_size(len(validated_files))
        all_chunks = []

        for i in range(0, len(validated_files), chunk_size):
            chunk_files = validated_files[i:i+chunk_size]

            if not self.memory_mgr.memory_check(f"{source_name} processing"):
                break

            chunk_data = self._load_file_chunk(chunk_files, source_name)
            if chunk_data is not None and len(chunk_data) > 0:
                all_chunks.append(chunk_data)

            del chunk_data
            self.memory_mgr.force_cleanup()

        if all_chunks:
            combined = pd.concat(all_chunks, ignore_index=True)
            del all_chunks
            self.memory_mgr.force_cleanup()
            return combined

        return None

    def _load_file_chunk(self, file_list: List[str], source_name: str):
        """Load and process file chunk"""
        chunk_dataframes = []

        for file_path in file_list:
            try:
                if self.conn:
                    df = self.conn.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
                else:
                    df = pd.read_parquet(file_path)

                if len(df) > 0:
                    processed_df = self._process_source_dataframe(df, source_name)
                    if processed_df is not None and len(processed_df) > 0:
                        chunk_dataframes.append(processed_df)

                del df
                gc.collect()

            except Exception:
                continue

        if chunk_dataframes:
            combined_chunk = pd.concat(chunk_dataframes, ignore_index=True)
            del chunk_dataframes
            gc.collect()
            return combined_chunk

        return None

    def _process_source_dataframe(self, df: pd.DataFrame, source_name: str):
        """Process dataframe based on source type with enhanced location handling"""
        try:
            if source_name in ['indeed', 'simplyhired', 'linkedin']:
                # Standard processing for job sites
                # Files are already validated, so we know they have sufficient data
                result_df = pd.DataFrame({
                    'salary_annual': df.get('parsed_annual_salary_avg'),
                    'job_title_normalized': df.get('nlp_norm_title'),
                    'company': df.get('company_name'),
                    'city': df.get('final_city', 'Unknown'),
                    'state': df.get('final_state', 'Unknown'),
                    'seniority_level': df.get('nlp_seniority', 'Unknown'),
                    'soc_code': df.get('nlp_soc_code'),
                    'industry_placeholder': 'All Industries',  # TODO: INDUSTRY INTEGRATION
                    'source': source_name,
                    'date': df.get('db_insert_timestamp')
                })
            else:
                return None

            result_df = self._filter_and_clean_data(result_df)
            return result_df

        except Exception:
            return None

    def _load_glassdoor_data(self):
        """Load and process Glassdoor CSV data with enhanced location and seniority mapping"""
        csv_path = self.config.glassdoor_path

        if not os.path.exists(csv_path):
            self.logger.warning(f"Glassdoor file not found: {csv_path}")
            return None

        try:
            if self.conn:
                df = self.conn.execute(f"SELECT * FROM read_csv_auto('{csv_path}')").df()
            else:
                df = pd.read_csv(csv_path)

            # Validate Glassdoor data quality
            if not validate_file_quality(df, 'glassdoor', csv_path, self.logger):
                return None

            self.logger.info(f"Processing Glassdoor data: {len(df):,} records")

            if len(df) > self.config.records_per_chunk:
                return self._process_glassdoor_in_chunks(df)
            else:
                return self._process_glassdoor_dataframe(df)

        except Exception as e:
            self.logger.error(f"Error loading Glassdoor data: {e}")
            return None

    def _process_glassdoor_in_chunks(self, df: pd.DataFrame):
        """Process large Glassdoor dataset in chunks"""
        chunk_size = self.config.records_per_chunk
        all_chunks = []

        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size].copy()

            processed_chunk = self._process_glassdoor_dataframe(chunk)
            if processed_chunk is not None and len(processed_chunk) > 0:
                all_chunks.append(processed_chunk)

            del chunk, processed_chunk
            self.memory_mgr.force_cleanup()

            if not self.memory_mgr.memory_check("Glassdoor processing"):
                break

        if all_chunks:
            combined = pd.concat(all_chunks, ignore_index=True)
            del all_chunks
            self.memory_mgr.force_cleanup()
            return combined

        return None

    def _process_glassdoor_dataframe(self, df: pd.DataFrame):
        """Process Glassdoor dataframe with enhanced location and seniority parsing"""
        try:
            # Parse salary
            df['parsed_salary'] = df['total_pay'].apply(parse_glassdoor_salary_with_per_year)

            # Parse location - ENHANCED
            location_data = df['location'].apply(parse_glassdoor_location)
            df['parsed_city'] = location_data.apply(lambda x: x[0])
            df['parsed_state'] = location_data.apply(lambda x: x[1])

            # Map seniority - ENHANCED
            df['mapped_seniority'] = df['years_of_exp'].apply(map_glassdoor_seniority)

            result_df = pd.DataFrame({
                'salary_annual': df['parsed_salary'],
                'job_title_normalized': df.get('normalizedTitle'),
                'company': df.get('company_name'),
                'city': df['parsed_city'],
                'state': df['parsed_state'],
                'seniority_level': df['mapped_seniority'],
                'soc_code': df.get('mapped_soc_code'),
                'industry_placeholder': 'All Industries',  # TODO: INDUSTRY INTEGRATION
                'source': 'glassdoor',
                'date': df.get('submitted_date')
            })

            result_df = self._filter_and_clean_data(result_df)
            return result_df

        except Exception as e:
            self.logger.error(f"Error processing Glassdoor dataframe: {e}")
            return None

    def _filter_and_clean_data(self, df: pd.DataFrame):
        """Apply data filters and cleaning"""
        try:
            df_clean = df[df['salary_annual'].notna()].copy()

            df_clean = df_clean[
                (df_clean['salary_annual'] >= self.config.salary_min) &
                (df_clean['salary_annual'] <= self.config.salary_max)
            ].copy()

            df_clean = df_clean[
                (df_clean['job_title_normalized'].notna()) &
                (df_clean['job_title_normalized'] != '') &
                (df_clean['company'].notna()) &
                (df_clean['company'] != '')
            ].copy()

            return df_clean

        except Exception:
            return df

    def close(self):
        """Close database connections"""
        if self.conn:
            self.conn.close()
