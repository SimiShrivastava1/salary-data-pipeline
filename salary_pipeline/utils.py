
# Minimal, self-contained utilities used across the pipeline.

import logging
import gc
from typing import Optional, List, Dict
import re
import pandas as pd
import numpy as np

class Logger:
    """Light wrapper around Python logging with a clear, consistent format."""
    _configured = False

    def __init__(self, name: str = "salary_pipeline", level: int = logging.INFO):
        if not Logger._configured:
            logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=level)
            Logger._configured = True
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

    def info(self, msg: str): self._logger.info(msg)
    def warning(self, msg: str): self._logger.warning(msg)
    def error(self, msg: str): self._logger.error(msg)
    def debug(self, msg: str): self._logger.debug(msg)

class SmartMemoryManager:
    """
    Very lightweight memory manager:
    - choose chunk sizes for file lists
    - allow external memory checks (stubbed here)
    - expose a cleanup hook
    """
    def calculate_optimal_chunk_size(self, n_files: int) -> int:
        # Heuristic: 4 chunks, cap at 50 files per chunk, min 1
        if n_files <= 0:
            return 1
        return max(1, min(50, int(np.ceil(n_files / 4))))

    def memory_check(self, context: str = "") -> bool:
        # Stubbed as always true; you can wire psutil if needed.
        return True

    def force_cleanup(self):
        gc.collect()

# Custom Exceptions
class DataValidationError(Exception):
    """Raised when data doesn't meet validation requirements"""
    pass

class FileProcessingError(Exception):
    """Raised when file processing fails"""
    pass

UNKNOWN = "Unknown"

# Fixed regex warning
_SOC_RE = re.compile(r'(\d{2}-?\d{4})')

_UNKNOWN_MARKERS = {"unknown", "Unknown", "UNKNOWN", "n/a", "N/A", "na", "Na", "NA", "", None, "null", "Null", "NULL", "–", "-"}

def clean_unknowns(s: pd.Series) -> pd.Series:
    """Replace common unknown-like values with <NA>."""
    return s.replace(list(_UNKNOWN_MARKERS), pd.NA)

def to_nullable_int(s: pd.Series) -> pd.Series:
    """Convert Series to pandas Int32 safely (non-numeric → <NA>)."""
    s = clean_unknowns(s)
    s = pd.to_numeric(s, errors="coerce")
    return s.astype("Int32")

def to_nullable_float(s: pd.Series) -> pd.Series:
    """Convert Series to pandas Float64 safely (non-numeric → <NA>)."""
    s = clean_unknowns(s)
    s = pd.to_numeric(s, errors="coerce")
    return s.astype("Float64")

def as_text_with_unknown(s: pd.Series) -> pd.Series:
    """Ensure text dtype and fill missing with 'Unknown'."""
    return s.astype("string").fillna(UNKNOWN)

def safe_numeric_casts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Safely cast common numeric fields (salary to Float64).
    Keep SOC columns as text to preserve hyphens used in BLS join.
    """
    if "salary_annual" in df.columns:
        df["salary_annual"] = to_nullable_float(df["salary_annual"])
    for col in ["soc_code", "nlp_soc_code", "mapped_soc_code", "occupation_code"]:
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df

def parse_numeric_salary(s: Optional[str]) -> Optional[float]:
    """Parse currency-like string ('$120,000') into float; None if it can't."""
    if s is None or pd.isna(s):
        return None
    cleaned = str(s).replace(',', '').replace('$', '').replace(' ', '')
    cleaned = ''.join(c for c in cleaned if (c.isdigit() or c == '.'))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None

def extract_soc_norm(series: pd.Series) -> pd.Series:
    """Extract SOC then normalize to digits-only (e.g., '15-1252' → '151252')."""
    s = series.astype(str).str.extract(_SOC_RE)[0]
    if s is not None:
        s = s.str.replace('-', '', regex=False)
    return s

def parse_salary_with_salary_py(salary_text):
    """Parse a raw salary string to annual numeric using salary.py. Tries /content/drive/MyDrive/SalaryDataFiles/salary.py::normalize_salary; returns avg or min if available, else None."""
    try:
        import sys
        sys.path.append('/content/drive/MyDrive/SalaryDataFiles')
        from salary import normalize_salary
        min_sal, max_sal, avg_sal = normalize_salary(str(salary_text))
        if avg_sal is not None:
            return avg_sal
        elif min_sal is not None:
            return min_sal
        else:
            return None
    except (ImportError, AttributeError) as e:
        raise FileProcessingError(f"Failed to import salary module: {e}")
    except Exception:
        return None

def parse_glassdoor_salary_with_per_year(salary_text):
    """Glassdoor helper: append 'per year' context and reuse the standard salary parser."""
    if not salary_text or pd.isna(salary_text):
        return None
    return parse_salary_with_salary_py(f"{salary_text} per year")

def calculate_percentile_weights(df: pd.DataFrame, decay_factor: float = 0.05) -> pd.DataFrame:
    """Density weight by collection date. Later/heavier dates get slightly higher weight via ranked percentiles^decay_factor."""
    if 'date' in df.columns and not df['date'].isna().all():
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date_only'] = df['date'].dt.date
        dc = df['date_only'].value_counts().reset_index()
        dc.columns = ['date', 'count']
        dc['pct'] = dc['count'].rank(pct=True, method='min')
        dc['w'] = dc['pct'] ** decay_factor
        m = dict(zip(dc['date'], dc['w']))
        df['density_weight'] = df['date_only'].map(m).fillna(1.0)
        df.drop('date_only', axis=1, inplace=True)
    else:
        df['density_weight'] = 1.0
    return df

def apply_source_weights(df: pd.DataFrame) -> pd.DataFrame:
    """Apply per-source trust weights and combine with density weights into final_weight."""
    base_weights = {'indeed': 1.0, 'simplyhired': 0.8, 'glassdoor': 0.9, 'linkedin': 0.3, 'jora': 0.7, 'jobvite': 0.6, 'icims': 0.6, 'greenhouse': 0.6, 'gem': 0.5, 'dayforce': 0.5, 'avature': 0.5}
    def get_source_weight(source_name):
        base_name = source_name.split('_')[0] if '_' in source_name else source_name
        return base_weights.get(base_name, 0.5)

    df['source_weight'] = df['source'].apply(get_source_weight)
    df['final_weight'] = df['density_weight'] * df['source_weight']
    return df

# Data Validation Utilities
def validate_required_columns(df: pd.DataFrame, required_cols: List[str], source_name: str):
    """Validate that all required columns exist"""
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise DataValidationError(f"{source_name}: Missing required columns: {missing}")

def validate_data_coverage(df: pd.DataFrame, col: str, min_coverage: float, source_name: str) -> bool:
    """Validate that column has minimum data coverage"""
    if col not in df.columns:
        return False
    coverage = df[col].notna().sum() / len(df)
    if coverage < min_coverage:
        raise DataValidationError(f"{source_name}: {col} coverage {coverage:.1%} below minimum {min_coverage:.1%}")
    return True

def get_best_column(df: pd.DataFrame, column_options: List[str], source_name: str, required: bool = False):
    """Find the best available column from options"""
    for col in column_options:
        if col in df.columns and df[col].notna().sum() > 0:
            return col
    if required:
        raise DataValidationError(f"{source_name}: None of required columns found: {column_options}")
    return None

def load_and_validate_file(file_path: str, source_name: str, conn=None) -> pd.DataFrame:
    """Single method for loading any file with consistent error handling"""
    try:
        if file_path.endswith('.parquet'):
            if conn:
                df = conn.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
            else:
                df = pd.read_parquet(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            raise FileProcessingError(f"Unsupported file format: {file_path}")

        # Common validation
        if len(df) == 0:
            raise DataValidationError(f"{source_name}: Empty file")

        return df
    except (FileNotFoundError, PermissionError) as e:
        raise FileProcessingError(f"{source_name}: File access error - {str(e)}")
    except pd.errors.EmptyDataError:
        raise DataValidationError(f"{source_name}: Empty data file")
    except Exception as e:
        raise FileProcessingError(f"{source_name}: File loading failed - {str(e)}")
