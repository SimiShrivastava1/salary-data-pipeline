
# Loads and normalizes multiple job-source datasets (parquet/csv) into one schema.

import os
import gc
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional, Tuple, Dict
import duckdb
import re

from utils import (Logger, SmartMemoryManager, UNKNOWN, as_text_with_unknown, to_nullable_float,
                  parse_salary_with_salary_py, parse_glassdoor_salary_with_per_year,
                  DataValidationError, FileProcessingError, validate_data_coverage,
                  get_best_column, load_and_validate_file)
from config import PipelineConfig, COLUMN_MAPPINGS
from cola_processor import COLAProcessor

def parse_glassdoor_location(location_str) -> Tuple[str, str]:
    """Normalize a Glassdoor location string into (city, state). Handles 'Remote', metro aliases, state-only inputs, and unknowns gracefully."""
    if not location_str or pd.isna(location_str):
        return UNKNOWN, UNKNOWN

    location_clean = str(location_str).strip()
    if not location_clean or location_clean.lower() in ['unknown', 'n/a', 'na', '']:
        return UNKNOWN, UNKNOWN

    if 'remote' in location_clean.lower():
        return 'Remote', 'Remote'

    parts = [part.strip() for part in location_clean.split(',') if part.strip()]

    if len(parts) >= 2:
        city = parts[0]
        state = parts[1].split(',')[0].strip()
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
                return ('Military Location', UNKNOWN)

        state_abbrev = {
            'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
            'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
            'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'
        }
        common_states = {'CALIFORNIA','NEW YORK','TEXAS','FLORIDA','ILLINOIS','PENNSYLVANIA','OHIO','GEORGIA','NORTH CAROLINA','MICHIGAN'}

        if single_part.upper() in state_abbrev or single_part.upper() in common_states:
            return UNKNOWN, single_part
        else:
            return single_part, UNKNOWN

    return UNKNOWN, UNKNOWN

class DataDrivenSeniorityInference:
    """Learn ranges + title patterns from Indeed-like data and infer missing seniority."""
    def __init__(self, logger: Logger):
        self.logger = logger
        self.salary_ranges = {}
        self.title_patterns = {}
        self.initialized = False

    def initialize_from_indeed_data(self, df: pd.DataFrame):
        """Learn salary ranges and common title patterns from Indeed rows with known seniority."""
        indeed = df[(df['source'].astype(str).str.startswith('indeed')) & (df['seniority_level'] != UNKNOWN) & (df['seniority_level'].notna())]
        if len(indeed) < 1000:
            return False
        self._calculate_salary_ranges(indeed)
        self._extract_title_patterns(indeed)
        self.initialized = True
        return True

    def _calculate_salary_ranges(self, df: pd.DataFrame):
        for lvl in df['seniority_level'].unique():
            s = df[df['seniority_level'] == lvl]['salary_annual']
            if len(s) >= 50:
                self.salary_ranges[lvl] = {
                    'q25': s.quantile(0.25),
                    'q50': s.quantile(0.50),
                    'q75': s.quantile(0.75),
                    'min': s.quantile(0.10),
                    'max': s.quantile(0.90),
                    'count': len(s)
                }

    def _extract_title_patterns(self, df: pd.DataFrame):
        for lvl in df['seniority_level'].unique():
            titles = df[df['seniority_level'] == lvl]['job_title_normalized'].astype(str)
            words = []
            for t in titles:
                words.extend(re.findall(r'\b\w+\b', t.lower()))
            counts = pd.Series(words).value_counts()
            keep = []
            stop = {'and','the','of','in','at','for','with','to','a','an','is','are','or'}
            for w, c in counts.head(50).items():
                if len(w) > 2 and w not in stop and c >= len(titles) * 0.01:
                    keep.append((w, c))
            self.title_patterns[lvl] = keep[:10]

    def infer_seniority_from_salary(self, salary: float) -> str:
        if not self.initialized or not self.salary_ranges or pd.isna(salary):
            return UNKNOWN
        best, score_best = UNKNOWN, 0
        for lvl, rng in self.salary_ranges.items():
            score = 0
            if rng['q25'] <= salary <= rng['q75']:
                score = 0.8
            elif rng['min'] <= salary <= rng['max']:
                score = 0.4
            else:
                d = abs(salary - rng['q50']) / rng['q50'] if rng['q50'] else 9e9
                if d < 0.5:
                    score = 0.2
            if score > score_best:
                score_best, best = score, lvl
        return best if score_best >= 0.4 else UNKNOWN

    def infer_seniority_from_title(self, job_title: str) -> str:
        if not self.initialized or not self.title_patterns or not job_title:
            return UNKNOWN
        tl = str(job_title).lower()
        scores = {}
        for lvl, patterns in self.title_patterns.items():
            s = 0
            for w, freq in patterns:
                if w in tl:
                    s += freq
            if s > 0:
                scores[lvl] = s
        if not scores:
            return UNKNOWN
        best = max(scores.keys(), key=scores.get)
        ordered = sorted(scores.values(), reverse=True)
        if len(ordered) == 1 or (len(ordered) > 1 and ordered[0] > 1.5 * ordered[1]):
            return best
        return UNKNOWN

def enhanced_map_glassdoor_seniority(years_exp_str, job_title=None, salary_annual=None, seniority_learner: Optional[DataDrivenSeniorityInference] = None) -> str:
    """Map Glassdoor-like 'years_of_exp' to a seniority bucket; fall back to learner/title/salary."""
    if years_exp_str and not pd.isna(years_exp_str):
        y = str(years_exp_str).strip().lower()
        direct = {
            '0-1 year': 'Entry level',
            '1-3 years': 'Junior level',
            '4-6 years': 'Mid level',
            '7-9 years': 'Mid-Senior level',
            '10-14 years': 'Senior level',
            '15+ years': 'Director'
        }
        for k, v in direct.items():
            if k in y:
                return v
        if any(p in y for p in ['0-1', '0 to 1', 'less than 1']):
            return 'Entry level'
        if any(p in y for p in ['1-3', '1 to 3', '2 year']):
            return 'Junior level'
        if any(p in y for p in ['4-6', '4 to 6', '5 year']):
            return 'Mid level'
        if any(p in y for p in ['7-9', '7 to 9', '8 year']):
            return 'Mid-Senior level'
        if any(p in y for p in ['10-14', '10 to 14', '12 year']):
            return 'Senior level'
        if any(p in y for p in ['15+', '15 or more', '20 year']):
            return 'Director'

    if seniority_learner and seniority_learner.initialized:
        if job_title:
            t = seniority_learner.infer_seniority_from_title(job_title)
            if t != UNKNOWN:
                return t
        if salary_annual and not pd.isna(salary_annual):
            s = seniority_learner.infer_seniority_from_salary(float(salary_annual))
            if s != UNKNOWN:
                return s

    if job_title and not pd.isna(job_title):
        tl = str(job_title).lower()
        if any(w in tl for w in ['director', 'vp', 'chief', 'head of']):
            return 'Director'
        if any(w in tl for w in ['senior', 'lead', 'principal']):
            return 'Senior level'
        if any(w in tl for w in ['junior', 'entry', 'intern', 'trainee']):
            return 'Entry level'

    return UNKNOWN

class IndustryProcessor:
    """Extract and standardize industry from whichever column each source has."""
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
        """Pick the best available industry column for a source and optionally standardize names."""
        if not self.enabled:
            return pd.Series([UNKNOWN] * len(df), index=df.index)

        source_cols = COLUMN_MAPPINGS['industry_columns'].get(
            'glassdoor' if source_name == 'glassdoor' else 'default',
            ['industry']
        )

        for col in source_cols:
            if col in df.columns:
                s = df[col].fillna(UNKNOWN).astype(str)
                if self.standardize:
                    s = self.standardize_industry_names(s)
                return s

        return pd.Series([UNKNOWN] * len(df), index=df.index)

    def standardize_industry_names(self, s: pd.Series) -> pd.Series:
        """Tidy capitalization, remove noise, and map common variants to a standard label."""
        cleaned = s.str.strip().str.title()
        standardized = cleaned.replace(self.industry_mapping)
        standardized = standardized.str.replace(r'\s+', ' ', regex=True)
        standardized = standardized.str.replace(r'[^\w\s&-]', '', regex=True)
        return standardized

class EnhancedGeographicProcessor:
    """Attach COLA-based geographic context if enabled in config."""
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
        """Add COLA fields (tier, multiplier, national-equivalent) if enabled; otherwise set neutral defaults."""
        if not self.enabled:
            df['geographic_tier'] = UNKNOWN
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
    """Run seniority learning once and fill missing for Glassdoor rows."""
    def __init__(self, logger: Logger):
        self.logger = logger
        self.seniority_learner = DataDrivenSeniorityInference(logger)

    def process_combined_data(self, df: pd.DataFrame) -> pd.DataFrame:
        self.seniority_learner.initialize_from_indeed_data(df)
        mask = (df['source'] == 'glassdoor') & (df['seniority_level'] == UNKNOWN)
        if not mask.any():
            return df

        inferred = df.loc[mask].apply(lambda row: enhanced_map_glassdoor_seniority(row.get('years_of_exp'),row.get('job_title_normalized'),row.get('salary_annual'),self.seniority_learner), axis=1)
        df.loc[mask, 'seniority_level'] = inferred
        return df

def _series_or_default(df: pd.DataFrame, col: str, default_value) -> pd.Series:
    """Return df[col] if present, else a Series(len=df) filled with default_value."""
    if col in df.columns:
        return df[col]
    return pd.Series([default_value] * len(df))

class EnhancedSmartFileProcessor:
    """
    Loads, validates, and normalizes parquet/CSV sources across many job sites.
    Keeps logging concise: only LOADED / SKIPPED (with reason).
    """
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
        from utils import calculate_percentile_weights, apply_source_weights
        df = calculate_percentile_weights(df, 0.05)
        df = apply_source_weights(df)
        return df

    def find_files_in_date_range(self, folder_path: str) -> List[str]:
        """Return *.parquet files whose filename date lies within config range."""
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
        """Load all enabled sources (dynamic list from config) + Glassdoor CSV."""
        all_data = []

        enabled_sources = self.config.get_enabled_job_board_sources()
        if not enabled_sources:
            self.logger.warning("No job board sources enabled in configuration")
        else:
            for source_cfg in enabled_sources:
                source_name = source_cfg['name']
                source_path = source_cfg['path']
                try:
                    df_src = self._load_source_data_with_validation(source_name, source_path)
                    if df_src is not None and len(df_src) > 0:
                        all_data.append(df_src)
                        self.logger.info(f"LOADED   {source_name}: {len(df_src):,} records")
                        self.memory_mgr.force_cleanup()
                except DataValidationError as e:
                    self.logger.warning(f"SKIPPED {source_name}: {str(e).split(': ', 1)[-1]}")
                except FileProcessingError as e:
                    self.logger.warning(f"SKIPPED {source_name}: {str(e).split(': ', 1)[-1]}")

        if self.config.enable_glassdoor:
            try:
                gd = self._load_glassdoor_data()
                if gd is not None and len(gd) > 0:
                    all_data.append(gd)
                    self.logger.info(f"LOADED   glassdoor: {len(gd):,} records")
                    self.memory_mgr.force_cleanup()
            except (DataValidationError, FileProcessingError) as e:
                self.logger.warning(f"SKIPPED glassdoor: {str(e).split(': ', 1)[-1]}")

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            del all_data
            self.memory_mgr.force_cleanup()

            self.logger.info(f"Total combined records: {len(combined):,}")

            seniority_proc = EnhancedSeniorityProcessor(self.logger)
            combined = seniority_proc.process_combined_data(combined)
            combined = self.geo_processor.add_geographic_context(combined)

            return combined

        self.logger.error("No data loaded from any source")
        return None

    def _validate_file_quality(self, df: pd.DataFrame, source_name: str) -> None:
        """Gate a raw source file on row count and coverage. Requires ≥500 rows, ≥30% salary coverage, and ≥50% title coverage; returns True if usable."""
        schema = self.config.get_source_schema(source_name)

        # Check minimum records
        if len(df) < schema.min_records:
            raise DataValidationError(f"{source_name}: Too few records ({len(df)} < {schema.min_records})")

        # Get appropriate column mappings
        source_type = 'glassdoor' if source_name == 'glassdoor' else 'job_board'
        salary_cols = COLUMN_MAPPINGS['salary_columns'][source_type]
        title_cols = COLUMN_MAPPINGS['title_columns'][source_type]

        # Check salary coverage
        salary_col = get_best_column(df, salary_cols, source_name, required=False)
        if not salary_col:
            raise DataValidationError(f"{source_name}: No salary data (0% coverage)")

        validate_data_coverage(df, salary_col, schema.min_coverage.get('salary', 0.3), source_name)

        # Check title coverage
        title_col = get_best_column(df, title_cols, source_name, required=False)
        if not title_col:
            raise DataValidationError(f"{source_name}: No job titles (0% coverage)")

        validate_data_coverage(df, title_col, schema.min_coverage.get('job_title', 0.5), source_name)

    def _load_source_data_with_validation(self, source_name: str, folder_path: str):
        files = self.find_files_in_date_range(folder_path)
        if not files:
            raise DataValidationError(f"{source_name}: No files in date range")

        validated_files = []

        # Validate each file
        for file_path in files:
            try:
                df = load_and_validate_file(file_path, source_name, self.conn)
                self._validate_file_quality(df, source_name)
                validated_files.append(file_path)
            except (DataValidationError, FileProcessingError):
                continue

        if not validated_files:
            raise DataValidationError(f"{source_name}: No valid files found")

        # Process validated files
        chunk_size = self.memory_mgr.calculate_optimal_chunk_size(len(validated_files))
        chunks = []

        for i in range(0, len(validated_files), chunk_size):
            chunk_files = validated_files[i:i+chunk_size]
            if not self.memory_mgr.memory_check(f"{source_name} processing"):
                break
            df_chunk = self._load_file_chunk(chunk_files, source_name)
            if df_chunk is not None and len(df_chunk) > 0:
                chunks.append(df_chunk)
            del df_chunk
            self.memory_mgr.force_cleanup()

        if chunks:
            combined = pd.concat(chunks, ignore_index=True)
            del chunks
            self.memory_mgr.force_cleanup()
            return combined

        raise DataValidationError(f"{source_name}: No data after processing")

    def _load_file_chunk(self, file_list: List[str], source_name: str):
      dfs = []
      for file_path in file_list:
          df = None
          processed = None
          try:
              df = load_and_validate_file(file_path, source_name, self.conn)
              if len(df) > 0:
                  processed = self._process_source_dataframe(df, source_name)
                  if processed is not None and len(processed) > 0:
                      dfs.append(processed)
          except (DataValidationError, FileProcessingError):
              continue
          finally:
              # Force cleanup in every loop iteration
              del df
              del processed
              gc.collect()

      if dfs:
          out = pd.concat(dfs, ignore_index=True)
          del dfs
          gc.collect()
          return out
      return None

    def _process_source_dataframe(self, df: pd.DataFrame, source_name: str):
        """Normalize per-source schema to our unified schema."""
        try:
            if source_name == 'glassdoor':
                return self._process_glassdoor_dataframe(df)

            industry = self.industry_processor.extract_industry_data(df, source_name)

            # Use improved column selection
            source_type = 'job_board'
            salary_field = get_best_column(df, COLUMN_MAPPINGS['salary_columns'][source_type], source_name)
            city_field = get_best_column(df, COLUMN_MAPPINGS['city_columns'], source_name)
            state_field = get_best_column(df, COLUMN_MAPPINGS['state_columns'], source_name)
            seniority_field = get_best_column(df, COLUMN_MAPPINGS['seniority_columns'], source_name)

            result = pd.DataFrame({
                'salary_annual': df[salary_field] if salary_field else pd.Series([pd.NA] * len(df)),
                'job_title_normalized': _series_or_default(df, 'nlp_norm_title', pd.NA),
                'company': _series_or_default(df, 'company_name', UNKNOWN),
                'city': (_series_or_default(df, city_field, UNKNOWN) if city_field else pd.Series([UNKNOWN]*len(df))).astype("string").fillna(UNKNOWN),
                'state': (_series_or_default(df, state_field, UNKNOWN) if state_field else pd.Series([UNKNOWN]*len(df))).astype("string").fillna(UNKNOWN),
                'seniority_level': (_series_or_default(df, seniority_field, UNKNOWN) if seniority_field else pd.Series([UNKNOWN]*len(df))).astype("string").fillna(UNKNOWN),
                'soc_code': _series_or_default(df, 'nlp_soc_code', pd.NA).astype("string"),
                'industry': industry,
                'source': pd.Series([source_name] * len(df), dtype="string"),
                'date': _series_or_default(df, 'db_insert_timestamp', pd.NA),
                'years_of_exp': pd.Series([pd.NA] * len(df))
            })

            result = self._filter_and_clean_data(result)
            return result

        except Exception as e:
            raise FileProcessingError(f"{source_name}: Processing failed - {str(e)}")

    def _load_glassdoor_data(self):
        csv_path = self.config.glassdoor_path
        if not os.path.exists(csv_path):
            raise FileProcessingError("glassdoor: file not found")

        try:
            df = load_and_validate_file(csv_path, 'glassdoor', self.conn)
            self.logger.info(f"Processing Glassdoor data: {len(df):,} records")

            self._validate_file_quality(df, 'glassdoor')

            processed = self._process_glassdoor_in_chunks(df) if len(df) > self.config.records_per_chunk else self._process_glassdoor_dataframe(df)
            if processed is not None and len(processed) > 0:
                return processed
            raise DataValidationError("glassdoor: No valid records after processing")

        except (DataValidationError, FileProcessingError):
            raise
        except Exception as e:
            raise FileProcessingError(f"glassdoor: Unexpected error - {str(e)}")

    def _process_glassdoor_in_chunks(self, df: pd.DataFrame):
        chunk_size = self.config.records_per_chunk
        chunks = []
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size].copy()
            processed = self._process_glassdoor_dataframe(chunk)
            if processed is not None and len(processed) > 0:
                chunks.append(processed)
            del chunk, processed
            self.memory_mgr.force_cleanup()
            if not self.memory_mgr.memory_check("Glassdoor processing"):
                break

        if chunks:
            out = pd.concat(chunks, ignore_index=True)
            del chunks
            self.memory_mgr.force_cleanup()
            return out
        return None

    def _process_glassdoor_dataframe(self, df: pd.DataFrame):
        """Parse Glassdoor CSV slice into unified schema; avoid SettingWithCopy."""
        try:
            df = df.copy()

            df.loc[:, 'parsed_salary'] = df['total_pay'].apply(parse_glassdoor_salary_with_per_year)

            loc = df['location'].apply(parse_glassdoor_location)
            df.loc[:, 'parsed_city'] = loc.apply(lambda x: x[0]).astype("string").fillna(UNKNOWN)
            df.loc[:, 'parsed_state'] = loc.apply(lambda x: x[1]).astype("string").fillna(UNKNOWN)

            df.loc[:, 'mapped_seniority'] = df['years_of_exp'].apply(lambda x: enhanced_map_glassdoor_seniority(x, None, None, None)).astype("string").fillna(UNKNOWN)

            industry = self.industry_processor.extract_industry_data(df, 'glassdoor')

            result = pd.DataFrame({
                'salary_annual': df['parsed_salary'],
                'job_title_normalized': _series_or_default(df, 'normalizedTitle', pd.NA),
                'company': _series_or_default(df, 'company_name', UNKNOWN),
                'city': df['parsed_city'],
                'state': df['parsed_state'],
                'seniority_level': df['mapped_seniority'],
                'soc_code': _series_or_default(df, 'mapped_soc_code', pd.NA).astype("string"),
                'industry': industry,
                'source': pd.Series(['glassdoor'] * len(df), dtype="string"),
                'date': _series_or_default(df, 'submitted_date', pd.NA),
                'years_of_exp': _series_or_default(df, 'years_of_exp', pd.NA)
            })

            result = self._filter_and_clean_data(result)
            return result

        except Exception as e:
            raise FileProcessingError(f"glassdoor: DataFrame processing failed - {str(e)}")

    def _filter_and_clean_data(self, df: pd.DataFrame):
        """
        Keep rows with salary within configured range and a non-empty title.
        City/state/company are optional; keep them as text with 'Unknown'.
        """
        try:
            df = df[df['salary_annual'].notna()].copy()
            df = df[(df['salary_annual'] >= self.config.salary_min) & (df['salary_annual'] <= self.config.salary_max)].copy()

            df = df[(df['job_title_normalized'].notna()) & (df['job_title_normalized'] != '')].copy()

            for col in ['job_title_normalized', 'company', 'city', 'state', 'seniority_level', 'source']:
                if col in df.columns:
                    df[col] = as_text_with_unknown(df[col])

            if 'soc_code' in df.columns:
                df['soc_code'] = df['soc_code'].astype('string')

            if 'salary_annual' in df.columns:
                df['salary_annual'] = to_nullable_float(df['salary_annual'])

            return df

        except Exception as e:
            self.logger.warning(f"Data filtering failed: {e}")
            return df

    def close(self):
        if self.conn:
            self.conn.close()

def calculate_percentile_weights(df, decay_factor=0.05):
    from utils import calculate_percentile_weights as _c
    return _c(df, decay_factor)

def apply_source_weights(df):
    from utils import apply_source_weights as _a
    return _a(df)

