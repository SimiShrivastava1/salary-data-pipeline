
import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict
from utils import Logger
from config import PipelineConfig
from data_processor import calculate_percentile_weights, apply_source_weights  

class MinimalOutlierDetector:
    def __init__(self, config: PipelineConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self.soc_min_samples = config.soc_min_samples
        self.soc_iqr_scale = config.soc_iqr_scale

    def detect_zscore_outliers(self, df: pd.DataFrame) -> List[int]:
        z_threshold = self.config.zscore_threshold

        try:
            log_salaries = np.log(df['salary_annual'])
            mean_log, std_log = log_salaries.mean(), log_salaries.std()
            z_scores = (log_salaries - mean_log).abs() / std_log

            outlier_mask = z_scores > z_threshold
            outlier_indices = df.index[outlier_mask].tolist()

            return outlier_indices

        except Exception as e:
            self.logger.error(f"Z-score detection failed: {e}")
            return []

    def detect_soc_based_outliers(self, df: pd.DataFrame) -> List[int]:
        if 'soc_code' not in df.columns:
            return []

        soc_outliers = []
        groups_processed = 0
        total_groups = 0

        try:
            for soc_code, group in df.groupby('soc_code'):
                total_groups += 1

                if pd.isna(soc_code) or len(group) < self.soc_min_samples:
                    continue

                groups_processed += 1
                salaries = group['salary_annual']

                Q1 = salaries.quantile(0.25)
                Q3 = salaries.quantile(0.75)
                IQR = Q3 - Q1

                if IQR == 0:
                    continue

                lower_bound = Q1 - self.soc_iqr_scale * IQR
                upper_bound = Q3 + self.soc_iqr_scale * IQR

                outlier_mask = (salaries < lower_bound) | (salaries > upper_bound)
                group_outliers = group[outlier_mask].index.tolist()
                soc_outliers.extend(group_outliers)

            return soc_outliers

        except Exception as e:
            self.logger.error(f"SOC-based detection failed: {e}")
            return []

    def detect_all_outliers(self, df: pd.DataFrame) -> Tuple[List[int], Dict]:
        all_outliers = set()
        detection_results = {}

        zscore_outliers = self.detect_zscore_outliers(df)
        all_outliers.update(zscore_outliers)
        detection_results['zscore'] = len(zscore_outliers)

        soc_outliers = self.detect_soc_based_outliers(df)
        all_outliers.update(soc_outliers)
        detection_results['soc_based'] = len(soc_outliers)

        final_outliers = sorted(all_outliers)
        outlier_rate = len(final_outliers) / len(df) * 100

        zscore_set = set(zscore_outliers)
        soc_set = set(soc_outliers)
        overlap = len(zscore_set.intersection(soc_set))
        zscore_only = len(zscore_set - soc_set)
        soc_only = len(soc_set - zscore_set)

        detection_results.update({
            'total_unique_outliers': len(final_outliers),
            'outlier_rate': outlier_rate,
            'methods_used': ['zscore', 'soc_based'],
            'overlap_analysis': {
                'zscore_only': zscore_only,
                'soc_only': soc_only,
                'both_methods': overlap
            }
        })

        return final_outliers, detection_results

    def bls_validate(self, df: pd.DataFrame, outlier_indices: List[int]) -> Dict:
        bls_path = self.config.bls_path

        try:
            if not os.path.exists(bls_path):
                return {'error': 'BLS file not found'}

            original_count = len(outlier_indices)
            if len(outlier_indices) > 50000:
                import random
                sampled_indices = random.sample(outlier_indices, 50000)
                outlier_indices = sampled_indices

            bls = pd.read_csv(bls_path, dtype=str, low_memory=False)

            code_col = p10_col = p90_col = None
            for col in bls.columns:
                col_upper = col.upper()
                if 'OCC_CODE' in col_upper and not code_col:
                    code_col = col
                elif ('PCT10' in col_upper or 'P10' in col_upper) and 'A_' in col_upper:
                    p10_col = col
                elif ('PCT90' in col_upper or 'P90' in col_upper) and 'A_' in col_upper:
                    p90_col = col

            if not all([code_col, p10_col, p90_col]):
                return {
                    'error': 'Required BLS columns not found',
                    'found_columns': {'code_col': code_col, 'p10_col': p10_col, 'p90_col': p90_col}
                }

            bls['soc_code_clean'] = bls[code_col].astype(str).str.extract(r'(\d{2}-?\d{4})')[0]

            def clean_salary(salary_str):
                if pd.isna(salary_str):
                    return None
                cleaned = str(salary_str).replace(',', '').replace('$', '').replace(' ', '')
                cleaned = ''.join(c for c in cleaned if c.isdigit() or c == '.')
                try:
                    return float(cleaned) if cleaned else None
                except:
                    return None

            bls['bls_p10'] = bls[p10_col].apply(clean_salary)
            bls['bls_p90'] = bls[p90_col].apply(clean_salary)

            valid_bls = bls.dropna(subset=['soc_code_clean', 'bls_p10', 'bls_p90'])
            lookup_dict = {}
            for _, row in valid_bls.iterrows():
                soc_code = row['soc_code_clean']
                if soc_code and not pd.isna(soc_code):
                    lookup_dict[soc_code] = {
                        'p10': row['bls_p10'],
                        'p90': row['bls_p90']
                    }

            soc_col = None
            for col in ['soc_code', 'soc_clean', 'SOC_CODE', 'occupation_code']:
                if col in df.columns:
                    soc_col = col
                    break

            if not soc_col:
                return {'validation_skipped': True, 'reason': 'No SOC codes available'}

            outlier_df = df.loc[outlier_indices].copy()
            outlier_df['soc_clean'] = outlier_df[soc_col].astype(str).str.extract(r'(\d{2}-?\d{4})')[0]

            categories = {'below_p10': 0, 'above_p90': 0, 'within_range': 0, 'no_bls_match': 0}

            for idx, row in outlier_df.iterrows():
                try:
                    soc_code = row['soc_clean']
                    salary = row['salary_annual']

                    if pd.isna(soc_code) or soc_code not in lookup_dict:
                        categories['no_bls_match'] += 1
                        continue

                    p10 = lookup_dict[soc_code]['p10']
                    p90 = lookup_dict[soc_code]['p90']

                    if salary < p10:
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

            return {
                'categories': categories,
                'validation_rate': validation_rate,
                'legitimate_outliers': legitimate_outliers,
                'total_processed': len(outlier_indices),
                'original_outlier_count': original_count,
                'sampled': len(outlier_indices) < original_count
            }

        except Exception as e:
            self.logger.error(f"BLS validation failed: {e}")
            return {'error': str(e)}
