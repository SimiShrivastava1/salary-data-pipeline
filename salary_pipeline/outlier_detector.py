
# Outlier detection (robust z-score + per-SOC IQR) and BLS validation in batches.

import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict

from utils import Logger, extract_soc_norm
from config import PipelineConfig

def _robust_log_zscores(s: pd.Series) -> pd.Series:
    """
    Compute robust z-scores in log space (good for heavy-tailed salaries).
    Falls back to mean/std if MAD is zero.
    """
    s = pd.to_numeric(s, errors='coerce').where(lambda x: x > 0)
    logs = np.log(s)
    med = np.nanmedian(logs)
    mad = np.nanmedian(np.abs(logs - med))
    if not np.isfinite(mad) or mad == 0:
        mu = np.nanmean(logs)
        sd = np.nanstd(logs)
        if not np.isfinite(sd) or sd == 0:
            return pd.Series(np.zeros(len(s)), index=s.index)
        return (logs - mu) / sd
    return 0.6745 * (logs - med) / mad

def _pick_soc_column(df: pd.DataFrame) -> str:
    """Pick the best available SOC column from the dataframe."""
    for c in ['soc_code', 'nlp_soc_code', 'mapped_soc_code', 'occupation_code', 'soc_clean']:
        if c in df.columns:
            return c
    return None

def _salary_for_validation(df: pd.DataFrame) -> pd.Series:
    """Prefer national-equivalent salary if present; otherwise raw annual."""
    if 'salary_national_equivalent' in df.columns and df['salary_national_equivalent'].notna().any():
        return pd.to_numeric(df['salary_national_equivalent'], errors='coerce')
    return pd.to_numeric(df['salary_annual'], errors='coerce')

class MinimalOutlierDetector:
    def __init__(self, config: PipelineConfig, logger: Logger):
        """Configure z-score and SOC thresholds and hold a logger for progress/errors."""
        self.config = config
        self.logger = logger
        self.soc_min_samples = config.soc_min_samples
        self.soc_iqr_scale = config.soc_iqr_scale

    def detect_zscore_outliers(self, df: pd.DataFrame) -> List[int]:
        """Global outliers in log-salary space using the configured z-score threshold."""
        z_threshold = self.config.zscore_threshold
        try:
            z = _robust_log_zscores(df['salary_annual'])
            mask = z.abs() > z_threshold
            return df.index[mask.fillna(False)].tolist()
        except Exception as e:
            self.logger.error(f"Z-score detection failed: {e}")
            return []

    def detect_soc_based_outliers(self, df: pd.DataFrame) -> List[int]:
        """Within-SOC outliers via IQR; only runs for SOC groups with enough samples."""
        soc_col = _pick_soc_column(df)
        if not soc_col:
            return []

        tmp = df[[soc_col, 'salary_annual']].copy()
        tmp['soc_norm'] = extract_soc_norm(tmp[soc_col])
        tmp['salary_annual'] = pd.to_numeric(tmp['salary_annual'], errors='coerce')

        soc_outliers = []
        try:
            for soc, group in tmp.groupby('soc_norm', dropna=True):
                if pd.isna(soc) or len(group) < self.soc_min_samples:
                    continue
                s = group['salary_annual'].dropna()
                if len(s) < self.soc_min_samples:
                    continue
                Q1, Q3 = s.quantile(0.25), s.quantile(0.75)
                IQR = Q3 - Q1
                if not np.isfinite(IQR) or IQR == 0:
                    continue
                lb = Q1 - self.soc_iqr_scale * IQR
                ub = Q3 + self.soc_iqr_scale * IQR
                mask = (group['salary_annual'] < lb) | (group['salary_annual'] > ub)
                if mask.any():
                    soc_outliers.extend(group.index[mask].tolist())
            return soc_outliers
        except Exception as e:
            self.logger.error(f"SOC-based detection failed: {e}")
            return []

    def detect_all_outliers(self, df: pd.DataFrame) -> Tuple[List[int], Dict]:
        """Run z-score + SOC detectors, de-duplicate indices, and return both the list and simple method stats."""
        all_outliers = set()
        results = {}

        soc_outliers = self.detect_soc_based_outliers(df)
        all_outliers.update(soc_outliers)
        results['soc_based'] = len(soc_outliers)

        z_outliers = self.detect_zscore_outliers(df)
        all_outliers.update(z_outliers)
        results['zscore_robust'] = len(z_outliers)

        final = sorted(all_outliers)
        rate = len(final) / max(len(df), 1) * 100

        zs, ss = set(z_outliers), set(soc_outliers)
        results.update({
            'total_unique_outliers': len(final),
            'outlier_rate': rate,
            'methods_used': ['soc_iqr', 'zscore_robust_log'],
            'overlap_analysis': {
                'zscore_only': len(zs - ss),
                'soc_only': len(ss - zs),
                'both_methods': len(zs & ss),
            }
        })
        return final, results

    def bls_validate(self, df: pd.DataFrame, outlier_indices: List[int]) -> Dict:
        """Batch-validate outliers against BLS P10/P90 annual earnings. Processes all outliers in memory-safe chunks; returns category counts and validation rate."""
        bls_path = self.config.bls_path
        BATCH = getattr(self.config, "bls_batch_size", 20000)

        try:
            if not os.path.exists(bls_path):
                return {'error': 'BLS file not found'}

            original_count = len(outlier_indices)
            if original_count == 0:
                return {
                    'categories': {'below_p10': 0, 'above_p90': 0, 'within_range': 0, 'no_bls_match': 0},
                    'validation_rate': 0.0, 'legitimate_outliers': 0,
                    'total_processed': 0, 'original_outlier_count': 0,
                    'sampled': False, 'bls_matches': 0, 'batch_size': BATCH
                }

            # Ensure unique indices
            if len(outlier_indices) != len(set(outlier_indices)):
                outlier_indices = list(dict.fromkeys(outlier_indices))

            # Load BLS
            bls = pd.read_csv(bls_path, dtype=str, low_memory=True)

            # Find columns dynamically
            code_col = p10_col = p90_col = None
            for col in bls.columns:
                cu = col.upper()
                if 'OCC_CODE' in cu and code_col is None:
                    code_col = col
                elif ('PCT10' in cu or 'P10' in cu) and 'A_' in cu:
                    p10_col = col
                elif ('PCT90' in cu or 'P90' in cu) and 'A_' in cu:
                    p90_col = col

            if not all([code_col, p10_col, p90_col]):
                return {
                    'error': 'Required BLS columns not found',
                    'found_columns': {'code_col': code_col, 'p10_col': p10_col, 'p90_col': p90_col}
                }

            # Normalize SOC and clean salaries
            bls['soc_code_clean'] = bls[code_col].astype(str).str.extract(r'(\d{2}-?\d{4})')[0]
            bls['soc_code_norm']  = bls['soc_code_clean'].str.replace('-', '', regex=False)

            def _clean_currency(s):
                if s is None or pd.isna(s):
                    return None
                cleaned = str(s).replace(',', '').replace('$', '').replace(' ', '')
                cleaned = ''.join(c for c in cleaned if (c.isdigit() or c == '.'))
                try:
                    return float(cleaned) if cleaned else None
                except Exception:
                    return None

            bls['bls_p10'] = bls[p10_col].apply(_clean_currency)
            bls['bls_p90'] = bls[p90_col].apply(_clean_currency)

            valid = bls.dropna(subset=['soc_code_norm', 'bls_p10', 'bls_p90'])
            if valid.empty:
                return {'validation_skipped': True, 'reason': 'No usable rows in BLS file after cleaning'}

            agg = (valid.groupby('soc_code_norm', as_index=False).agg(bls_p10=('bls_p10', 'median'), bls_p90=('bls_p90', 'median')))

            p10_map = pd.Series(agg['bls_p10'].values, index=agg['soc_code_norm'].values)
            p90_map = pd.Series(agg['bls_p90'].values, index=agg['soc_code_norm'].values)

            soc_col = _pick_soc_column(df)
            if not soc_col:
                return {'validation_skipped': True, 'reason': 'No SOC codes available'}

            salary_series = _salary_for_validation(df)

            cats_total = {'below_p10': 0, 'above_p90': 0, 'within_range': 0, 'no_bls_match': 0}
            matched_total = 0
            processed_total = 0

            for start in range(0, original_count, BATCH):
                batch_idx = outlier_indices[start:start + BATCH]

                batch_soc = df.loc[batch_idx, soc_col].copy()
                batch_soc_norm = extract_soc_norm(batch_soc)

                batch_p10 = batch_soc_norm.map(p10_map)
                batch_p90 = batch_soc_norm.map(p90_map)
                batch_sal = salary_series.loc[batch_idx]

                match_mask = batch_p10.notna() & batch_p90.notna() & batch_sal.notna()
                matched = int(match_mask.sum())

                below_mask = match_mask & (batch_sal < batch_p10)
                above_mask = match_mask & (batch_sal > batch_p90)
                within_mask = match_mask & ~(below_mask | above_mask)

                cats_total['below_p10']    += int(below_mask.sum())
                cats_total['above_p90']    += int(above_mask.sum())
                cats_total['within_range'] += int(within_mask.sum())
                cats_total['no_bls_match'] += int(len(batch_idx) - matched)

                matched_total   += matched
                processed_total += int(len(batch_idx))

            legitimate_outliers = cats_total['below_p10'] + cats_total['above_p90']
            validation_rate = (legitimate_outliers / matched_total * 100.0) if matched_total > 0 else 0.0

            self.logger.info(
                f"BLS match stats — matched: {matched_total:,} / {processed_total:,}; "
                f"below_p10: {cats_total['below_p10']}, above_p90: {cats_total['above_p90']}, "
                f"within: {cats_total['within_range']}, no_match: {cats_total['no_bls_match']}"
            )

            return {
                'categories': cats_total,
                'validation_rate': validation_rate,
                'legitimate_outliers': legitimate_outliers,
                'total_processed': processed_total,
                'original_outlier_count': original_count,
                'sampled': False,
                'bls_matches': matched_total,
                'used_salary': 'salary_national_equivalent' if 'salary_national_equivalent' in df.columns else 'salary_annual',
                'batch_size': BATCH
            }

        except Exception as e:
            self.logger.error(f"BLS validation failed: {e}")
            return {'error': str(e)}

