import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict
from utils import Logger
from config import PipelineConfig

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

class OutlierDetector:
    """Statistical outlier detection for salary data"""

    def __init__(self, config: PipelineConfig, logger: Logger):
        self.config = config
        self.logger = logger

    def detect_zscore_outliers(self, df: pd.DataFrame) -> List[int]:
        """Detect outliers using Z-score on log-transformed salaries"""
        z_threshold = self.config.zscore_threshold

        try:
            log_salaries = np.log(df['salary_annual'])
            mean_log, std_log = log_salaries.mean(), log_salaries.std()
            z_scores = (log_salaries - mean_log).abs() / std_log

            outlier_mask = z_scores > z_threshold
            outlier_indices = df.index[outlier_mask].tolist()

            self.logger.info(f"Z-score outliers detected: {len(outlier_indices):,} ({len(outlier_indices)/len(df)*100:.2f}%)")
            return outlier_indices

        except Exception as e:
            self.logger.error(f"Z-score detection failed: {e}")
            return []

    def detect_mad_outliers(self, df: pd.DataFrame) -> Tuple[List[int], Dict]:
        """Detect outliers using MAD across hierarchical groupings"""
        grouping_levels = self.config.grouping_levels

        remaining = df.index.tolist()
        all_outliers = set()
        level_results = {}

        for level in grouping_levels:
            level_name = level['name']
            cols = level['columns']
            min_size = level['min_size']
            threshold = level['threshold']

            available_cols = [c for c in cols if c in df.columns]
            if not available_cols:
                level_results[level_name] = 0
                continue

            level_outliers = set()

            try:
                for group_key, group in df.loc[remaining].groupby(available_cols, observed=True):
                    if len(group) < min_size:
                        continue

                    salaries = group['salary_annual'].dropna()
                    if len(salaries) < min_size:
                        continue

                    median_sal = salaries.median()
                    deviations = (salaries - median_sal).abs()
                    mad = deviations.median()

                    if mad == 0:
                        q75, q25 = salaries.quantile(0.75), salaries.quantile(0.25)
                        mad = (q75 - q25) / 2
                        if mad == 0:
                            continue

                    outlier_mask = deviations / mad > threshold
                    group_outliers = salaries[outlier_mask].index
                    level_outliers.update(group_outliers)

                all_outliers.update(level_outliers)
                level_results[level_name] = len(level_outliers)

                remaining = [i for i in remaining if i not in level_outliers]

            except Exception as e:
                self.logger.error(f"MAD detection error in {level_name}: {e}")
                level_results[level_name] = 0

        return sorted(all_outliers), level_results

    def bls_validate(self, df: pd.DataFrame, outlier_indices: List[int]) -> Dict:
        """Validate outliers against Bureau of Labor Statistics data"""
        bls_path = self.config.bls_path

        try:
            if not os.path.exists(bls_path):
                return {'error': 'BLS file not found'}

            bls = pd.read_csv(bls_path, dtype=str, low_memory=False)

            # Find required BLS columns
            code_col = p10_col = p90_col = None
            for col in bls.columns:
                col_upper = col.upper()
                if 'OCC_CODE' in col_upper and not code_col:
                    code_col = col
                elif 'A_PCT10' in col_upper or 'PCT10' in col_upper:
                    p10_col = col
                elif 'A_PCT90' in col_upper or 'PCT90' in col_upper:
                    p90_col = col

            if not all([code_col, p10_col, p90_col]):
                return {'error': 'Required BLS columns not found'}

            # Process BLS data
            bls['soc_code_clean'] = bls[code_col].astype(str).str.extract(r'(\d{2}-?\d{4})')[0]
            bls['bls_p10'] = pd.to_numeric(
                bls[p10_col].astype(str).str.replace(r'[^\d.]', '', regex=True),
                errors='coerce'
            )
            bls['bls_p90'] = pd.to_numeric(
                bls[p90_col].astype(str).str.replace(r'[^\d.]', '', regex=True),
                errors='coerce'
            )

            lookup = bls[['soc_code_clean', 'bls_p10', 'bls_p90']].dropna()

            # Find SOC column in data
            soc_col = None
            for col in ['soc_code', 'soc_clean', 'SOC_CODE', 'occupation_code']:
                if col in df.columns:
                    soc_col = col
                    break

            if not soc_col:
                return {'validation_skipped': True, 'reason': 'No SOC codes available'}

            # Validate outliers
            categories = {'below_p10': 0, 'above_p90': 0, 'within_range': 0, 'no_bls_match': 0}

            for idx in outlier_indices:
                try:
                    soc_value = df.at[idx, soc_col]
                    salary = df.at[idx, 'salary_annual']

                    if pd.isna(soc_value) or pd.isna(salary):
                        categories['no_bls_match'] += 1
                        continue

                    soc_clean = str(soc_value).strip()
                    bls_match = lookup[lookup['soc_code_clean'] == soc_clean]

                    if bls_match.empty:
                        categories['no_bls_match'] += 1
                    else:
                        p10, p90 = bls_match.iloc[0]['bls_p10'], bls_match.iloc[0]['bls_p90']

                        if pd.isna(p10) or pd.isna(p90):
                            categories['no_bls_match'] += 1
                        elif salary < p10:
                            categories['below_p10'] += 1
                        elif salary > p90:
                            categories['above_p90'] += 1
                        else:
                            categories['within_range'] += 1

                except Exception:
                    categories['no_bls_match'] += 1

            total_validated = sum(categories.values())
            legitimate_outliers = categories['below_p10'] + categories['above_p90']
            validation_rate = (legitimate_outliers / total_validated * 100) if total_validated > 0 else 0

            self.logger.info(f"BLS validation completed - {legitimate_outliers:,} legitimate outliers ({validation_rate:.1f}%)")

            return {
                'categories': categories,
                'validation_rate': validation_rate,
                'legitimate_outliers': legitimate_outliers,
                'total_processed': len(outlier_indices)
            }

        except Exception as e:
            self.logger.error(f"BLS validation failed: {e}")
            return {'error': str(e)}
