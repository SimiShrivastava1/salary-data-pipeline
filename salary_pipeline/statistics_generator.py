
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List
from utils import Logger
from config import PipelineConfig

class EnhancedStatisticsGenerator:
    def __init__(self, config: PipelineConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self.include_industry = config.industry_analysis['include_in_statistics']
        self.include_geographic = config.geographic_context['enabled']

    def _format_salary(self, amount):
        """Format salary amounts as currency strings"""
        if pd.isna(amount) or amount == 0:
            return "$0"
        return f"${amount:,.0f}"

    def _format_multiplier(self, multiplier):
        """Format COLA multiplier to 2 decimal places"""
        if pd.isna(multiplier):
            return "1.00"
        return f"{multiplier:.2f}"

    def generate_clean_dataset_and_stats(self, df: pd.DataFrame, outlier_indices: List[int], bls_results: Dict) -> Dict[str, Any]:
        """Create the cleaned output dataset and optional grouped statistics, then write them to disk."""
        os.makedirs(self.config.output_dir, exist_ok=True)

        legitimate_outlier_indices = self._determine_outliers_to_remove(outlier_indices, bls_results)

        clean_df = df[~df.index.isin(legitimate_outlier_indices)].copy()

        original_count = len(df)
        clean_count = len(clean_df)
        removed_count = len(legitimate_outlier_indices)
        removal_rate = (removed_count / original_count) * 100

        stats_df = self._generate_enhanced_statistics(clean_df)

        if stats_df is None:
            self.logger.error("Statistics generation failed")
            return None

        files_created = self._save_essential_outputs_only(clean_df, stats_df, {
            'total_clean_records': clean_count,
            'outliers_removed': removed_count,
            'outlier_removal_rate': removal_rate
        })

        return {
            'clean_df': clean_df,
            'stats_df': stats_df,
            'outliers_removed': removed_count,
            'files_created': files_created
        }

    def _determine_outliers_to_remove(self, outlier_indices: List[int], bls_results: Dict) -> set:
        """Use the provided detector to compute outlier indices and return a set for fast filtering."""
        if 'legitimate_outliers' in bls_results:
          legitimate_count = bls_results['legitimate_outliers']
          num_to_remove = min(legitimate_count, len(outlier_indices))
          return set(outlier_indices[:num_to_remove])
        else:
          return set()

    def _generate_enhanced_statistics(self, clean_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate by title/location/industry/seniority and compute counts, means/medians, and simple CIs."""
        # Create smart sub-groups for Unknown seniority records
        enhanced_df = self._create_smart_unknown_groups(clean_df)

        # Preserve original salaries before any processing
        enhanced_df = self._preserve_original_salaries(enhanced_df)

        base_grouping_columns = ['job_title_normalized', 'city', 'state', 'seniority_level']

        if self.include_industry and 'industry' in enhanced_df.columns:
            industry_coverage = (enhanced_df['industry'] != 'Unknown').sum() / len(enhanced_df)
            if industry_coverage >= self.config.industry_analysis['min_coverage_rate']:
                base_grouping_columns.insert(-1, 'industry')

        available_cols = [col for col in base_grouping_columns if col in enhanced_df.columns]

        if not available_cols:
            self.logger.error("No suitable columns available for grouping")
            return None

        stats_list = []
        min_group_size = self.config.outlier_detection['min_group_size']

        groups_processed = 0
        groups_kept = 0

        for group_key, group_data in enhanced_df.groupby(available_cols):
            groups_processed += 1

            if len(group_data) < min_group_size:
                continue

            groups_kept += 1

            # Get both original and adjusted salaries
            original_salaries = group_data['salary_original']
            adjusted_salaries = group_data['salary_annual']
            cola_multipliers = group_data.get('cola_multiplier', pd.Series([1.0] * len(group_data)))

            weights = group_data.get('final_weight', pd.Series([1] * len(group_data)))

            stats_dict = {}
            for i, col in enumerate(available_cols):
                if isinstance(group_key, tuple):
                    stats_dict[col] = group_key[i]
                else:
                    stats_dict[col] = group_key

            if 'industry' not in stats_dict:
                stats_dict['industry'] = 'Unknown'

            stats_dict['record_count'] = len(group_data)

            # Calculate both original and adjusted salary statistics
            stats_dict['mean_salary_original'] = (original_salaries * weights).sum() / weights.sum()
            stats_dict['mean_salary_adjusted'] = (adjusted_salaries * weights).sum() / weights.sum()
            stats_dict['median_salary_original'] = original_salaries.median()
            stats_dict['median_salary_adjusted'] = adjusted_salaries.median()
            stats_dict['min_salary_original'] = original_salaries.min()
            stats_dict['max_salary_original'] = original_salaries.max()

            # COLA-related statistics
            stats_dict['cola_multiplier_avg'] = cola_multipliers.mean()

            if 'cola_tier' in group_data.columns:
                tier_counts = group_data['cola_tier'].value_counts()
                stats_dict['cola_tier_primary'] = tier_counts.index[0] if len(tier_counts) > 0 else 'National Average'
            else:
                stats_dict['cola_tier_primary'] = 'National Average'

            n = len(group_data)
            if n >= 10:
                # Calculate confidence intervals for original salary
                std_salary = original_salaries.std()
                std_error = std_salary / np.sqrt(n)
                margin_error = 1.96 * std_error if n >= 30 else 2.045 * std_error

                ci_lower = max(0, stats_dict['mean_salary_original'] - margin_error)
                ci_upper = stats_dict['mean_salary_original'] + margin_error
                stats_dict['mean_salary_original_95ci'] = f"${ci_lower:,.0f} - ${ci_upper:,.0f}"
                stats_dict['sample_confidence'] = 'High' if n >= 30 else 'Medium'
            else:
                # Show actual salary range for small samples
                min_sal = stats_dict['min_salary_original']
                max_sal = stats_dict['max_salary_original']
                stats_dict['mean_salary_original_95ci'] = f"${min_sal:,.0f} - ${max_sal:,.0f}"
                stats_dict['sample_confidence'] = 'Low'

            source_counts = group_data['source'].value_counts()
            data_sources = []
            for source in ['indeed', 'simplyhired', 'glassdoor', 'linkedin']:
                count = source_counts.get(source, 0)
                if count > 0:
                    data_sources.append(f"{source}({count})")

            stats_dict['data_sources'] = '; '.join(data_sources) if data_sources else 'Unknown'

            stats_list.append(stats_dict)

        if not stats_list:
            self.logger.error("No statistical groups generated")
            return None

        stats_df = pd.DataFrame(stats_list)

        required_columns = [
            'job_title_normalized', 'city', 'state', 'industry', 'seniority_level',
            'record_count', 'mean_salary_original', 'mean_salary_adjusted',
            'median_salary_original', 'median_salary_adjusted',
            'min_salary_original', 'max_salary_original',
            'cola_multiplier_avg', 'cola_tier_primary',
            'mean_salary_original_95ci', 'sample_confidence', 'data_sources'
        ]

        for col in required_columns:
            if col not in stats_df.columns:
                if col == 'industry':
                    stats_df[col] = 'Unknown'
                elif col == 'cola_tier_primary':
                    stats_df[col] = 'National Average'
                elif col in ['cola_multiplier_avg']:
                    stats_df[col] = 1.0

        stats_df = stats_df.sort_values(['record_count', 'mean_salary_original'], ascending=[False, False]).reset_index(drop=True)

        return stats_df

    def _preserve_original_salaries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure we have original salaries preserved before COLA adjustments
        """
        if 'salary_original' not in df.columns:
            # If we don't have original salaries preserved, calculate them from adjusted salaries
            if 'cola_multiplier' in df.columns:
                df['salary_original'] = df['salary_annual'] * df['cola_multiplier']
            else:
                # Fallback: assume current salary_annual is the original
                df['salary_original'] = df['salary_annual'].copy()

        return df

    def _create_smart_unknown_groups(self, df):
        """
        Create intelligent sub-groups for Unknown seniority records
        """
        # Separate known vs unknown seniority
        known_seniority = df[df['seniority_level'] != 'Unknown'].copy()
        unknown_seniority = df[df['seniority_level'] == 'Unknown'].copy()

        if len(unknown_seniority) == 0:
            return df

        # Process unknown seniority records by job title
        result_records = [known_seniority]  # Start with known seniority records

        unknown_groups_created = 0

        for job_title, title_group in unknown_seniority.groupby('job_title_normalized'):
            if len(title_group) < 10:
                # Too few records - keep as single Unknown group
                result_records.append(title_group)
                continue

            # Create salary quartiles for this job title
            try:
                title_group = title_group.copy()

                # Create quartiles based on unique salary values
                unique_salaries = len(title_group['salary_annual'].unique())
                n_quartiles = min(4, unique_salaries)

                if n_quartiles >= 2:
                    title_group['salary_quartile'] = pd.qcut(title_group['salary_annual'], q=n_quartiles, labels=False, duplicates='drop')

                    # Create descriptive seniority labels
                    if n_quartiles == 4:
                        quartile_labels = {
                            0: 'Unknown - Low Salary Range',
                            1: 'Unknown - Lower-Mid Salary Range',
                            2: 'Unknown - Upper-Mid Salary Range',
                            3: 'Unknown - High Salary Range'
                        }
                    elif n_quartiles == 3:
                        quartile_labels = {
                            0: 'Unknown - Low Salary Range',
                            1: 'Unknown - Mid Salary Range',
                            2: 'Unknown - High Salary Range'
                        }
                    else:  # n_quartiles == 2
                        quartile_labels = {
                            0: 'Unknown - Low Salary Range',
                            1: 'Unknown - High Salary Range'
                        }

                    title_group['seniority_level'] = title_group['salary_quartile'].map(quartile_labels)
                    title_group = title_group.drop('salary_quartile', axis=1)
                    unknown_groups_created += 1

                result_records.append(title_group)

            except Exception as e:
                # If quartile creation fails, keep as Unknown
                result_records.append(title_group)

        # Combine all records
        enhanced_df = pd.concat(result_records, ignore_index=True)

        return enhanced_df

    def _save_essential_outputs_only(self, clean_df: pd.DataFrame, stats_df: pd.DataFrame, summary_info: Dict) -> List[str]:
        """Save only the two essential files: clean dataset and statistics with COLA data"""
        files_created = []

        # Clean dataset parquet
        clean_data_path = os.path.join(self.config.output_dir, "clean_salary_dataset.parquet")
        clean_df.to_parquet(clean_data_path, index=False)
        files_created.append(clean_data_path)

        client_stats_df = stats_df.copy()

        # Format salary columns
        salary_columns = ['mean_salary_original', 'mean_salary_adjusted', 'median_salary_original', 'median_salary_adjusted', 'min_salary_original', 'max_salary_original']

        for col in salary_columns:
            if col in client_stats_df.columns:
                client_stats_df[col] = client_stats_df[col].apply(self._format_salary)

        # Format COLA multiplier
        if 'cola_multiplier_avg' in client_stats_df.columns:
            client_stats_df['cola_multiplier_avg'] = client_stats_df['cola_multiplier_avg'].apply(self._format_multiplier)

        # Rename columns for client readability
        column_renames = {
            'job_title_normalized': 'Job Title',
            'city': 'City',
            'state': 'State',
            'industry': 'Industry',
            'seniority_level': 'Seniority Level',
            'record_count': 'Record Count',
            'mean_salary_original': 'Average Salary (Original)',
            'mean_salary_adjusted': 'Average Salary (COLA)',
            'median_salary_original': 'Median Salary (Original)',
            'median_salary_adjusted': 'Median Salary (COLA)',
            'min_salary_original': 'Min Salary (Original)',
            'max_salary_original': 'Max Salary (Original)',
            'cola_multiplier_avg': 'COLA Multiplier',
            'cola_tier_primary': 'COLA Tier',
            'mean_salary_original_95ci': 'Salary Range (95% CI)',
            'sample_confidence': 'Sample Confidence',
            'data_sources': 'Data Sources'
        }

        # Apply renames only for columns that exist
        existing_renames = {old: new for old, new in column_renames.items() if old in client_stats_df.columns}
        client_stats_df = client_stats_df.rename(columns=existing_renames)

        client_columns = [
            'Job Title', 'City', 'State', 'Industry', 'Seniority Level', 'Record Count',
            'Average Salary (Original)', 'Average Salary (COLA)',
            'Median Salary (Original)', 'Median Salary (COLA)',
            'Min Salary (Original)', 'Max Salary (Original)',
            'COLA Multiplier', 'COLA Tier', 'Salary Range (95% CI)',
            'Sample Confidence', 'Data Sources'
        ]

        available_client_cols = [col for col in client_columns if col in client_stats_df.columns]
        final_client_df = client_stats_df[available_client_cols].copy()

        client_stats_path = os.path.join(self.config.output_dir, "salary_statistics.csv")
        final_client_df.to_csv(client_stats_path, index=False)
        files_created.append(client_stats_path)

        return files_created
