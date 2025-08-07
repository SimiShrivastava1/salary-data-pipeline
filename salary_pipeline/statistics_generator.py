import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List
from utils import Logger
from config import PipelineConfig

class StatisticsGenerator:
    """Generate clean dataset and streamlined statistics"""
    
    def __init__(self, config: PipelineConfig, logger: Logger):
        self.config = config
        self.logger = logger

    def generate_clean_dataset_and_stats(self, df: pd.DataFrame, outlier_indices: List[int], 
                                        bls_results: Dict) -> Dict[str, Any]:
        """Generate clean dataset by removing outliers and create statistics"""
        
        os.makedirs(self.config.output_dir, exist_ok=True)

        # Determine outliers to remove based on BLS validation
        legitimate_outlier_indices = self._determine_outliers_to_remove(outlier_indices, bls_results)

        # Create clean dataset
        clean_df = df[~df.index.isin(legitimate_outlier_indices)].copy()
        
        original_count = len(df)
        clean_count = len(clean_df)
        removed_count = len(legitimate_outlier_indices)
        removal_rate = (removed_count / original_count) * 100

        self.logger.info(f"Clean dataset: {clean_count:,} records ({removed_count:,} outliers removed, {removal_rate:.2f}%)")

        # Generate streamlined statistics
        stats_df = self._generate_statistics(clean_df)
        
        if stats_df is None:
            self.logger.error("Statistics generation failed")
            return None

        # Save outputs
        files_created = self._save_outputs(clean_df, stats_df, {
            'total_clean_records': clean_count,
            'outliers_removed': removed_count,
            'outlier_removal_rate': removal_rate
        })

        self.logger.info(f"Generated {len(stats_df):,} statistical groups")

        return {
            'clean_df': clean_df,
            'stats_df': stats_df,
            'outliers_removed': removed_count,
            'files_created': files_created
        }

    def _determine_outliers_to_remove(self, outlier_indices: List[int], bls_results: Dict) -> set:
        """Determine which outliers to remove based on BLS validation"""
        legitimate_outlier_indices = set()

        if 'categories' in bls_results:
            validation_rate = bls_results['validation_rate']

            if validation_rate >= 70:
                num_to_remove = int(len(outlier_indices) * 0.8)
                legitimate_outlier_indices.update(outlier_indices[:num_to_remove])
            elif validation_rate >= 40:
                num_to_remove = int(len(outlier_indices) * 0.5)
                legitimate_outlier_indices.update(outlier_indices[:num_to_remove])
            else:
                num_to_remove = int(len(outlier_indices) * 0.1)
                legitimate_outlier_indices.update(outlier_indices[:num_to_remove])
        else:
            num_to_remove = int(len(outlier_indices) * 0.2)
            legitimate_outlier_indices.update(outlier_indices[:num_to_remove])

        return legitimate_outlier_indices

    def _generate_statistics(self, clean_df: pd.DataFrame) -> pd.DataFrame:
        """Generate streamlined salary statistics by groupings"""
        
        grouping_columns = ['job_title_normalized', 'city', 'state', 'seniority_level']
        available_cols = [col for col in grouping_columns if col in clean_df.columns]

        if not available_cols:
            return None

        stats_list = []

        for group_key, group_data in clean_df.groupby(available_cols):
            if len(group_data) < 3:
                continue

            salaries = group_data['salary_annual']
            weights = group_data.get('final_weight', pd.Series([1] * len(group_data)))

            # Group identifiers
            stats_dict = {}
            for i, col in enumerate(available_cols):
                if isinstance(group_key, tuple):
                    stats_dict[col] = group_key[i]
                else:
                    stats_dict[col] = group_key

            # Core statistics
            stats_dict['record_count'] = len(group_data)
            stats_dict['mean_salary'] = (salaries * weights).sum() / weights.sum()
            stats_dict['median_salary'] = salaries.median()

            # Confidence intervals
            n = len(group_data)
            if n >= 10:
                std_salary = salaries.std()
                std_error = std_salary / np.sqrt(n)
                margin_error = 1.96 * std_error if n >= 30 else 2.045 * std_error
                
                ci_lower = max(0, stats_dict['mean_salary'] - margin_error)
                ci_upper = stats_dict['mean_salary'] + margin_error
                stats_dict['mean_salary_95ci'] = f"${ci_lower:,.0f} - ${ci_upper:,.0f}"
                stats_dict['sample_confidence'] = 'High' if n >= 30 else 'Medium'
            else:
                stats_dict['mean_salary_95ci'] = 'Insufficient data'
                stats_dict['sample_confidence'] = 'Low'

            # Data sources
            source_counts = group_data['source'].value_counts()
            data_sources = []
            for source in ['indeed', 'simplyhired', 'glassdoor', 'linkedin']:
                count = source_counts.get(source, 0)
                if count > 0:
                    data_sources.append(f"{source}({count})")
            
            stats_dict['data_sources'] = '; '.join(data_sources) if data_sources else 'Unknown'

            stats_list.append(stats_dict)

        if not stats_list:
            return None

        stats_df = pd.DataFrame(stats_list)
        return stats_df.sort_values('record_count', ascending=False).reset_index(drop=True)

    def _save_outputs(self, clean_df: pd.DataFrame, stats_df: pd.DataFrame, summary_info: Dict) -> List[str]:
        """Save output files with streamlined client CSV"""
        files_created = []

        # Clean dataset
        clean_data_path = os.path.join(self.config.output_dir, "clean_salary_dataset.parquet")
        clean_df.to_parquet(clean_data_path, index=False)
        files_created.append(clean_data_path)

        # Streamlined client statistics - ONLY requested columns
        client_columns = [
            'job_title_normalized', 'city', 'state', 'seniority_level', 
            'record_count', 'mean_salary', 'median_salary', 
            'mean_salary_95ci', 'sample_confidence', 'data_sources'
        ]
        
        # Filter to only include available columns
        available_client_cols = [col for col in client_columns if col in stats_df.columns]
        client_stats_df = stats_df[available_client_cols].copy()
        
        client_stats_path = os.path.join(self.config.output_dir, "salary_statistics_granular.csv")
        client_stats_df.to_csv(client_stats_path, index=False)
        files_created.append(client_stats_path)

        # Summary statistics
        summary_stats = {
            'total_clean_records': summary_info['total_clean_records'],
            'total_groups_analyzed': len(stats_df),
            'outliers_removed': summary_info['outliers_removed'],
            'outlier_removal_rate': summary_info['outlier_removal_rate'],
            'data_sources': clean_df['source'].value_counts().to_dict(),
            'salary_range': {
                'min': float(clean_df['salary_annual'].min()),
                'max': float(clean_df['salary_annual'].max()),
                'median': float(clean_df['salary_annual'].median()),
                'mean': float(clean_df['salary_annual'].mean())
            },
            'processing_date': datetime.now().isoformat(),
            'client_csv_columns': available_client_cols
        }

        summary_path = os.path.join(self.config.output_dir, "dataset_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(summary_stats, f, indent=2, default=str)
        files_created.append(summary_path)

        return files_created
