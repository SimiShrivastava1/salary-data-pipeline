
import os
import gc
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional, Tuple, Dict
import duckdb
import sys
import re

from utils import Logger, SmartMemoryManager
from config import PipelineConfig
from cola_processor import COLAProcessor

def parse_salary_with_salary_py(salary_text):
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
    if not salary_text or pd.isna(salary_text):
        return None
    salary_with_context = f"{salary_text} per year"
    return parse_salary_with_salary_py(salary_with_context)

def parse_glassdoor_location(location_str) -> Tuple[str, str]:
    if not location_str or pd.isna(location_str):
        return 'Unknown', 'Unknown'

    location_clean = str(location_str).strip()

    if not location_clean or location_clean.lower() in ['unknown', 'n/a', 'na', '']:
        return 'Unknown', 'Unknown'

    if 'remote' in location_clean.lower():
        return 'Remote', 'Remote'

    parts = [part.strip() for part in location_clean.split(',') if part.strip()]

    if len(parts) >= 2:
        city = parts[0]
        state = parts[1]
        state_parts = state.split(',')
        state = state_parts[0].strip()
        return city, state

    elif len(parts) == 1:
        single_part = parts[0]

        metro_area_mapping = {
            'Dallas-Fort Worth': ('Dallas-Fort Worth', 'TX'),
            'Long Island-Queens': ('Long Island-Queens', 'NY'),
            'San Ramon Valley': ('San Ramon Valley', 'CA'),
            'Tri-Cities': ('Tri-Cities', 'WA'),
            'Oahu Island': ('Oahu Island', 'HI'),
            'Midtown New York': ('Midtown New York', 'NY'),
            'East New York': ('East New York', 'NY'),
            'Alderwood': ('Alderwood', 'WA'),
            'Saratoga': ('Saratoga', 'CA')
        }

        if single_part in metro_area_mapping:
            return metro_area_mapping[single_part]

        if 'coast guard' in single_part.lower() or 'air station' in single_part.lower():
            if 'sacramento' in single_part.lower():
                return ('Military - Sacramento', 'CA')
            else:
                return ('Military Location', 'Unknown')

        state_abbreviations = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
        }

        common_states = {
            'CALIFORNIA', 'NEW YORK', 'TEXAS', 'FLORIDA', 'ILLINOIS',
            'PENNSYLVANIA', 'OHIO', 'GEORGIA', 'NORTH CAROLINA', 'MICHIGAN'
        }

        if single_part.upper() in state_abbreviations or single_part.upper() in common_states:
            return 'Unknown', single_part
        else:
            return single_part, 'Unknown'

    return 'Unknown', 'Unknown'

class DataDrivenSeniorityInference:
    def __init__(self, logger):
        self.logger = logger
        self.salary_ranges = {}
        self.title_patterns = {}
        self.initialized = False
    
    def initialize_from_indeed_data(self, df: pd.DataFrame):
        """Learn patterns from Indeed's existing seniority data"""
        indeed_with_seniority = df[
            (df['source'] == 'indeed') & 
            (df['seniority_level'] != 'Unknown') &
            (df['seniority_level'].notna())
        ]
        
        if len(indeed_with_seniority) < 1000:
            return False
        
        # 1. Calculate salary ranges for each seniority level
        self._calculate_salary_ranges(indeed_with_seniority)
        
        # 2. Extract common title patterns
        self._extract_title_patterns(indeed_with_seniority)
        
        self.initialized = True
        return True
    
    def _calculate_salary_ranges(self, df: pd.DataFrame):
        """Calculate salary ranges based on actual Indeed data"""
        for seniority in df['seniority_level'].unique():
            seniority_data = df[df['seniority_level'] == seniority]['salary_annual']
            
            if len(seniority_data) >= 50:  # Minimum sample size
                self.salary_ranges[seniority] = {
                    'q25': seniority_data.quantile(0.25),
                    'q50': seniority_data.quantile(0.50),
                    'q75': seniority_data.quantile(0.75),
                    'min': seniority_data.quantile(0.10),
                    'max': seniority_data.quantile(0.90),
                    'count': len(seniority_data)
                }
    
    def _extract_title_patterns(self, df: pd.DataFrame):
        """Extract common words/patterns for each seniority level"""
        
        for seniority in df['seniority_level'].unique():
            titles = df[df['seniority_level'] == seniority]['job_title_normalized'].astype(str)
            
            # Extract individual words, focusing on position indicators
            all_words = []
            for title in titles:
                # Clean and split title
                words = re.findall(r'\w+', title.lower())
                all_words.extend(words)
            
            # Count word frequency
            word_counts = pd.Series(all_words).value_counts()
            
            # Filter for meaningful position indicators (not common words like 'and', 'the')
            position_indicators = []
            common_words = {'and', 'the', 'of', 'in', 'at', 'for', 'with', 'to', 'a', 'an', 'is', 'are', 'or'}
            
            for word, count in word_counts.head(20).items():
                if len(word) > 2 and word not in common_words:
                    # Only include words that appear in at least 1% of titles for this seniority
                    if count >= len(titles) * 0.01:
                        position_indicators.append((word, count))
            
            self.title_patterns[seniority] = position_indicators[:10]  # Top 10 indicators
    
    def infer_seniority_from_salary(self, salary: float) -> str:
        """Infer seniority based on learned salary ranges"""
        if not self.initialized or not self.salary_ranges:
            return 'Unknown'
        
        # Calculate how well the salary fits each seniority level
        best_match = 'Unknown'
        best_score = 0
        
        for seniority, ranges in self.salary_ranges.items():
            score = 0
            
            # High score if within IQR (25th-75th percentile)
            if ranges['q25'] <= salary <= ranges['q75']:
                score = 0.8
            # Medium score if within broader range (10th-90th percentile)
            elif ranges['min'] <= salary <= ranges['max']:
                score = 0.4
            # Low score if close to median
            else:
                distance_from_median = abs(salary - ranges['q50']) / ranges['q50']
                if distance_from_median < 0.5:  # Within 50% of median
                    score = 0.2
            
            if score > best_score:
                best_score = score
                best_match = seniority
        
        # Only return if we have reasonable confidence
        return best_match if best_score >= 0.4 else 'Unknown'
    
    def infer_seniority_from_title(self, job_title: str) -> str:
        """Infer seniority based on learned title patterns"""
        if not self.initialized or not self.title_patterns or not job_title:
            return 'Unknown'
        
        title_lower = str(job_title).lower()
        
        # Score each seniority level based on matching patterns
        scores = {}
        
        for seniority, patterns in self.title_patterns.items():
            score = 0
            for word, frequency in patterns:
                if word in title_lower:
                    # Weight by how common this word is for this seniority level
                    score += frequency
            
            if score > 0:
                scores[seniority] = score
        
        if not scores:
            return 'Unknown'
        
        # Return the seniority with highest score
        best_match = max(scores.keys(), key=scores.get)
        
        # Only return if the score is significantly higher than others
        sorted_scores = sorted(scores.values(), reverse=True)
        if len(sorted_scores) > 1 and sorted_scores[0] > sorted_scores[1] * 1.5:
            return best_match
        elif len(sorted_scores) == 1:
            return best_match
        
        return 'Unknown'

def enhanced_map_glassdoor_seniority(years_exp_str, job_title=None, salary_annual=None, 
                                   seniority_learner=None) -> str:
    """
    Enhanced seniority mapping using learned patterns from Indeed data
    """
    
    # Method 1: Years of experience 
    if years_exp_str and not pd.isna(years_exp_str):
        years_exp_clean = str(years_exp_str).strip().lower()
        
        # Direct mapping for standard Glassdoor format
        direct_mapping = {
            '0-1 year': 'Entry level',
            '1-3 years': 'Junior level', 
            '4-6 years': 'Mid level',
            '7-9 years': 'Mid-Senior level',
            '10-14 years': 'Senior level',
            '15+ years': 'Director'
        }
        
        for key, value in direct_mapping.items():
            if key in years_exp_clean:
                return value
        
        # Pattern matching for variations
        if any(pattern in years_exp_clean for pattern in ['0-1', '0 to 1', 'less than 1']):
            return 'Entry level'
        elif any(pattern in years_exp_clean for pattern in ['1-3', '1 to 3', '2 year']):
            return 'Junior level'
        elif any(pattern in years_exp_clean for pattern in ['4-6', '4 to 6', '5 year']):
            return 'Mid level'
        elif any(pattern in years_exp_clean for pattern in ['7-9', '7 to 9', '8 year']):
            return 'Mid-Senior level'
        elif any(pattern in years_exp_clean for pattern in ['10-14', '10 to 14', '12 year']):
            return 'Senior level'
        elif any(pattern in years_exp_clean for pattern in ['15+', '15 or more', '20 year']):
            return 'Director'
    
    # Method 2: Use learned patterns from Indeed data
    if seniority_learner and seniority_learner.initialized:
        
        # Try title-based inference first 
        if job_title:
            title_inference = seniority_learner.infer_seniority_from_title(job_title)
            if title_inference != 'Unknown':
                return title_inference
        
        # Try salary-based inference as fallback
        if salary_annual and not pd.isna(salary_annual):
            try:
                salary_inference = seniority_learner.infer_seniority_from_salary(float(salary_annual))
                if salary_inference != 'Unknown':
                    return salary_inference
            except (ValueError, TypeError):
                pass
    
    # Method 3: Simple fallback patterns 
    if job_title and not pd.isna(job_title):
        title_lower = str(job_title).lower()
        
        # Only the most obvious patterns as final fallback
        if any(word in title_lower for word in ['director', 'vp', 'chief', 'head of']):
            return 'Director'
        elif any(word in title_lower for word in ['senior', 'lead', 'principal']):
            return 'Senior level'
        elif any(word in title_lower for word in ['junior', 'entry', 'intern', 'trainee']):
            return 'Entry level'
    
    return 'Unknown'

def validate_file_quality(df: pd.DataFrame, source_name: str, file_path: str, logger: Logger, silent: bool = True) -> bool:
    required_fields = {
        'indeed': ['parsed_annual_salary_avg', 'nlp_norm_title', 'company_name', 'final_city', 'final_state', 'nlp_seniority'],
        'simplyhired': ['parsed_annual_salary_avg', 'nlp_norm_title', 'company_name', 'final_city', 'final_state', 'nlp_seniority'],
        'linkedin': ['parsed_annual_salary_avg', 'nlp_norm_title', 'company_name', 'final_city', 'final_state', 'nlp_seniority'],
        'glassdoor': ['total_pay', 'normalizedTitle', 'company_name', 'location', 'years_of_exp']
    }

    min_records = 500
    min_coverage = 0.30

    if len(df) < min_records:
        return False

    if source_name not in required_fields:
        return False

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

    if missing_fields or low_coverage_fields:
        return False

    return True

def calculate_percentile_weights(df, decay_factor=0.05):
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
    weights = {'indeed': 1.0, 'simplyhired': 0.8, 'glassdoor': 0.9, 'linkedin': 0.3}
    df['source_weight'] = df['source'].map(weights).fillna(0.5)
    df['final_weight'] = df['density_weight'] * df['source_weight']
    return df

class IndustryProcessor:
    def __init__(self, config: PipelineConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self.enabled = config.industry_analysis['enabled']
        self.standardize = config.industry_analysis['standardize_names']

        self.industry_mapping = {
            'Information Technology': 'Technology',
            'IT': 'Technology',
            'Software': 'Technology',
            'Computer Software': 'Technology',
            'Internet': 'Technology',
            'Health Care': 'Healthcare',
            'Healthcare': 'Healthcare',
            'Medical': 'Healthcare',
            'Hospital & Health Care': 'Healthcare',
            'Financial Services': 'Finance',
            'Banking': 'Finance',
            'Investment Banking': 'Finance',
            'Insurance': 'Finance',
            'Real Estate': 'Real Estate',
            'Construction': 'Construction',
            'Manufacturing': 'Manufacturing',
            'Retail': 'Retail',
            'Education': 'Education',
            'Government': 'Government',
            'Non-profit': 'Non-profit',
            'Consulting': 'Consulting',
            'Legal': 'Legal',
            'Media': 'Media',
            'Entertainment': 'Media',
            'Telecommunications': 'Telecommunications',
            'Energy': 'Energy',
            'Utilities': 'Utilities',
            'Transportation': 'Transportation',
            'Logistics': 'Transportation',
            'Automotive': 'Automotive',
            'Aerospace': 'Aerospace',
            'Defense': 'Defense'
        }

    def extract_industry_data(self, df: pd.DataFrame, source_name: str) -> pd.Series:
        if not self.enabled:
            return pd.Series(['Unknown'] * len(df), index=df.index)

        industry_columns = {
            'indeed': ['industry', 'Industry', 'company_industry'],
            'glassdoor': ['industry', 'company_industry', 'Industry'],
            'simplyhired': ['industry', 'Industry', 'company_industry'],
            'linkedin': ['industry', 'Industry', 'company_industry']
        }

        source_columns = industry_columns.get(source_name, ['industry'])

        for col in source_columns:
            if col in df.columns:
                industry_series = df[col].fillna('Unknown').astype(str)
                if self.standardize:
                    industry_series = self.standardize_industry_names(industry_series)
                return industry_series

        return pd.Series(['Unknown'] * len(df), index=df.index)

    def standardize_industry_names(self, industry_series: pd.Series) -> pd.Series:
        cleaned = industry_series.str.strip().str.title()
        standardized = cleaned.replace(self.industry_mapping)
        standardized = standardized.str.replace(r'\s+', ' ', regex=True)
        standardized = standardized.str.replace(r'[^\w\s&-]', '', regex=True)
        return standardized

    def assess_industry_coverage(self, df: pd.DataFrame) -> Dict:
        if 'industry' not in df.columns:
            return {'coverage_rate': 0.0, 'unique_industries': 0, 'sufficient_coverage': False}

        total_records = len(df)
        known_industry = (df['industry'] != 'Unknown').sum()
        coverage_rate = known_industry / total_records
        unique_industries = df['industry'].nunique()

        min_coverage = self.config.industry_analysis['min_coverage_rate']
        sufficient_coverage = coverage_rate >= min_coverage

        return {
            'coverage_rate': coverage_rate,
            'unique_industries': unique_industries,
            'sufficient_coverage': sufficient_coverage,
            'records_with_industry': known_industry,
            'total_records': total_records
        }

class EnhancedGeographicProcessor:
    def __init__(self, config: PipelineConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self.enabled = config.geographic_context['enabled']
        self.use_dynamic_cola = config.geographic_context.get('use_dynamic_cola', False)

        if self.enabled and self.use_dynamic_cola:
            try:
                self.cola_processor = COLAProcessor(config, logger)
            except Exception as e:
                self.logger.error(f"Failed to initialize COLA processor: {e}")
                self.cola_processor = None
        else:
            self.cola_processor = None

    def add_geographic_context(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.enabled:
            df['geographic_tier'] = 'Unknown'
            df['cola_multiplier'] = 1.0
            df['salary_national_equivalent'] = df['salary_annual']
            return df

        if self.cola_processor:
            df = self.cola_processor.add_cola_to_dataframe(df)
            df['geographic_tier'] = df['cola_tier']
        else:
            df['geographic_tier'] = 'Medium Cost'
            df['cola_multiplier'] = 1.0
            df['salary_national_equivalent'] = df['salary_annual']

        return df

class EnhancedSeniorityProcessor:
    def __init__(self, logger):
        self.logger = logger
        self.seniority_learner = DataDrivenSeniorityInference(logger)
    
    def process_combined_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the combined dataset to improve seniority coverage"""
        
        # First, learn patterns from Indeed data
        self.seniority_learner.initialize_from_indeed_data(df)
        
        # Focus on Glassdoor records with Unknown seniority
        glassdoor_unknown_mask = (df['source'] == 'glassdoor') & (df['seniority_level'] == 'Unknown')
        glassdoor_unknown_count = glassdoor_unknown_mask.sum()
        
        if glassdoor_unknown_count == 0:
            return df
        
        # Apply enhanced inference (silent processing)
        inferred_seniority = df.loc[glassdoor_unknown_mask].apply(
            lambda row: enhanced_map_glassdoor_seniority(
                row.get('years_of_exp'),
                row.get('job_title_normalized'),
                row.get('salary_annual'),
                self.seniority_learner
            ), axis=1
        )
        
        # Update the dataframe
        df.loc[glassdoor_unknown_mask, 'seniority_level'] = inferred_seniority
        
        return df

class EnhancedSmartFileProcessor:
    def __init__(self, config: PipelineConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self.conn = duckdb.connect()
        self.memory_mgr = SmartMemoryManager()

        self.start_date = datetime.strptime(config.start_date, config.date_format)
        self.end_date = datetime.strptime(config.end_date, config.date_format)

        self.industry_processor = IndustryProcessor(config, logger)
        self.geo_processor = EnhancedGeographicProcessor(config, logger)

    def apply_weighting(self, df):
        df = calculate_percentile_weights(df, 0.05)
        df = apply_source_weights(df)
        return df

    def find_files_in_date_range(self, folder_path: str) -> List[str]:
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
        all_data = []

        if self.config.enable_indeed:
            indeed_data = self._load_source_data_with_validation('indeed', self.config.indeed_path)
            if indeed_data is not None and len(indeed_data) > 0:
                all_data.append(indeed_data)
                self.memory_mgr.force_cleanup()

        if self.config.enable_simplyhired:
            sh_data = self._load_source_data_with_validation('simplyhired', self.config.simplyhired_path)
            if sh_data is not None and len(sh_data) > 0:
                all_data.append(sh_data)
                self.memory_mgr.force_cleanup()

        if self.config.enable_linkedin:
            linkedin_data = self._load_source_data_with_validation('linkedin', self.config.linkedin_path)
            if linkedin_data is not None and len(linkedin_data) > 0:
                all_data.append(linkedin_data)
                self.memory_mgr.force_cleanup()

        if self.config.enable_glassdoor:
            glassdoor_data = self._load_glassdoor_data()
            if glassdoor_data is not None and len(glassdoor_data) > 0:
                all_data.append(glassdoor_data)
                self.memory_mgr.force_cleanup()

        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            del all_data
            self.memory_mgr.force_cleanup()

            self.logger.info(f"Total combined records: {len(combined_data):,}")
            
            # Apply seniority inference before geographic processing
            seniority_processor = EnhancedSeniorityProcessor(self.logger)
            combined_data = seniority_processor.process_combined_data(combined_data)
            
            # Apply geographic context
            combined_data = self.geo_processor.add_geographic_context(combined_data)

            return combined_data
        else:
            self.logger.error("No data loaded from any source")
            return None

    def _load_source_data_with_validation(self, source_name: str, folder_path: str):
        all_files = self.find_files_in_date_range(folder_path)
        if not all_files:
            self.logger.warning(f"No {source_name} files found in date range")
            return None

        self.logger.info(f"Validating {len(all_files)} {source_name} files")
        validated_files = []

        for file_path in all_files:
            try:
                if self.conn:
                    df = self.conn.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
                else:
                    df = pd.read_parquet(file_path)

                if validate_file_quality(df, source_name, file_path, self.logger, silent=True):
                    validated_files.append(file_path)

            except Exception:
                continue

        if not validated_files:
            self.logger.warning(f"No valid {source_name} files found after validation; Fields missing or are null")
            return None

        self.logger.info(f"Processing {len(validated_files)} validated {source_name} files")

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
            
            self.logger.info(f"{source_name.title()}: {len(combined):,} records")
            return combined

        return None

    def _load_file_chunk(self, file_list: List[str], source_name: str):
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
        try:
            if source_name in ['indeed', 'simplyhired', 'linkedin']:
                industry_data = self.industry_processor.extract_industry_data(df, source_name)

                result_df = pd.DataFrame({
                    'salary_annual': df.get('parsed_annual_salary_avg'),
                    'job_title_normalized': df.get('nlp_norm_title'),
                    'company': df.get('company_name'),
                    'city': df.get('final_city', 'Unknown'),
                    'state': df.get('final_state', 'Unknown'),
                    'seniority_level': df.get('nlp_seniority', 'Unknown'),
                    'soc_code': df.get('nlp_soc_code'),
                    'industry': industry_data,
                    'source': source_name,
                    'date': df.get('db_insert_timestamp'),
                    'years_of_exp': None  # Not available for these sources
                })
            else:
                return None

            result_df = self._filter_and_clean_data(result_df)
            return result_df

        except Exception:
            return None

    def _load_glassdoor_data(self):
        csv_path = self.config.glassdoor_path

        if not os.path.exists(csv_path):
            self.logger.warning("Glassdoor file not found")
            return None

        try:
            if self.conn:
                df = self.conn.execute(f"SELECT * FROM read_csv_auto('{csv_path}')").df()
            else:
                df = pd.read_csv(csv_path)

            self.logger.info(f"Processing Glassdoor data: {len(df):,} records")

            if not validate_file_quality(df, 'glassdoor', csv_path, self.logger):
                self.logger.warning("Glassdoor file failed validation")
                return None

            if len(df) > self.config.records_per_chunk:
                processed_data = self._process_glassdoor_in_chunks(df)
            else:
                processed_data = self._process_glassdoor_dataframe(df)

            if processed_data is not None and len(processed_data) > 0:
                self.logger.info(f"Glassdoor: {len(processed_data):,} records")
                return processed_data
            else:
                self.logger.warning("No valid Glassdoor records after processing; Fields missing or are null")
                return None

        except Exception as e:
            self.logger.error(f"Error loading Glassdoor data: {e}")
            return None

    def _process_glassdoor_in_chunks(self, df: pd.DataFrame):
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
        try:
            df['parsed_salary'] = df['total_pay'].apply(parse_glassdoor_salary_with_per_year)

            location_data = df['location'].apply(parse_glassdoor_location)
            df['parsed_city'] = location_data.apply(lambda x: x[0])
            df['parsed_state'] = location_data.apply(lambda x: x[1])

            # Basic seniority mapping
            df['mapped_seniority'] = df['years_of_exp'].apply(
                lambda x: enhanced_map_glassdoor_seniority(x, None, None, None)
            )

            industry_data = self.industry_processor.extract_industry_data(df, 'glassdoor')

            result_df = pd.DataFrame({
                'salary_annual': df['parsed_salary'],
                'job_title_normalized': df.get('normalizedTitle'),
                'company': df.get('company_name'),
                'city': df['parsed_city'],
                'state': df['parsed_state'],
                'seniority_level': df['mapped_seniority'],
                'soc_code': df.get('mapped_soc_code'),
                'industry': industry_data,
                'source': 'glassdoor',
                'date': df.get('submitted_date'),
                'years_of_exp': df.get('years_of_exp')  # Keep for enhanced inference
            })

            result_df = self._filter_and_clean_data(result_df)
            return result_df

        except Exception as e:
            self.logger.error(f"Error processing Glassdoor dataframe: {e}")
            return None

    def _filter_and_clean_data(self, df: pd.DataFrame):
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
        if self.conn:
            self.conn.close()
