
import os
import yaml
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class PipelineConfig:
    start_date: str = "2025-06-01"
    end_date: str = "2025-07-31"
    date_format: str = "%Y-%m-%d"
    max_chunk_size: int = 8
    records_per_chunk: int = 150000
    indeed_path: str = ""
    simplyhired_path: str = ""
    linkedin_path: str = ""
    glassdoor_path: str = ""
    bls_path: str = ""
    output_dir: str = "./output"
    cola_data_paths: Dict = None
    enable_indeed: bool = True
    enable_simplyhired: bool = True
    enable_linkedin: bool = True
    enable_glassdoor: bool = True
    industry_analysis: Dict = None
    soc_detection: Dict = None
    geographic_context: Dict = None
    outlier_detection: Dict = None
    cola_settings: Dict = None
    salary_min: float = 15000.0
    salary_max: float = 50000000.0
    zscore_threshold: float = 3.5
    environment: str = "development"
    grouping_levels: List[Dict] = None

    def __post_init__(self):
        if self.industry_analysis is None:
            self.industry_analysis = {
                'enabled': True,
                'include_in_outlier_detection': False,
                'min_coverage_rate': 0.30,
                'include_in_statistics': True,
                'standardize_names': True
            }

        if self.soc_detection is None:
            self.soc_detection = {
                'enabled': True,
                'min_samples': 15,
                'iqr_scale': 3.0,
                'sd_scale': 3.5
            }

        if self.geographic_context is None:
            self.geographic_context = {
                'enabled': True,
                'use_dynamic_cola': True,
                'use_simple_state_tiers': False,
                'cola_cache_enabled': True
            }

        if self.outlier_detection is None:
            self.outlier_detection = {
                'min_group_size': 5,
                'enable_industry_grouping': False,
                'enable_soc_grouping': True,
                'fallback_without_industry': True
            }

        if self.cola_data_paths is None:
            self.cola_data_paths = {
                'county_economic': "",
                'cities': "",
                'zips': "",
                'counties': "",
                'zip_database': ""
            }

        if self.cola_settings is None:
            self.cola_settings = {
                'salary_weight': 0.7,
                'gdp_weight': 0.3,
                'min_records_for_custom': 10,
                'default_multiplier': 1.0,
                'enable_zip_lookup': True,
                'enable_city_lookup': True,
                'cache_lookups': True
            }

        if self.grouping_levels is None:
            self.grouping_levels = []

    @classmethod
    def from_yaml(cls, config_path: str, environment: str = None):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        env = environment or os.getenv('ENVIRONMENT', 'development')

        if env not in config_data:
            raise ValueError(f"Environment '{env}' not found in config file")

        env_config = config_data[env]
        env_config['environment'] = env

        return cls(**env_config)

    def validate(self):
        errors = []

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        paths_to_check = ['indeed_path', 'simplyhired_path', 'glassdoor_path', 'bls_path']
        for path_attr in paths_to_check:
            path_value = getattr(self, path_attr)
            if path_value and not os.path.exists(path_value):
                errors.append(f"{path_attr} does not exist: {path_value}")

        use_dynamic_cola = self.geographic_context.get('use_dynamic_cola', False)

        if use_dynamic_cola:
            cola_paths = ['county_economic', 'cities']
            for path_key in cola_paths:
                path_value = self.cola_data_paths.get(path_key, '')
                if path_value and not os.path.exists(path_value):
                    errors.append(f"COLA {path_key} file does not exist: {path_value}")

        if self.industry_analysis['min_coverage_rate'] < 0 or self.industry_analysis['min_coverage_rate'] > 1:
            errors.append("industry min_coverage_rate must be between 0 and 1")

        if use_dynamic_cola:
            if abs(self.cola_settings['salary_weight'] + self.cola_settings['gdp_weight'] - 1.0) > 0.01:
                errors.append("COLA salary_weight + gdp_weight must equal 1.0")

        if errors:
            raise ValueError(f"Configuration errors: {'; '.join(errors)}")

    @property
    def soc_min_samples(self):
        return self.soc_detection['min_samples']

    @property
    def soc_iqr_scale(self):
        return self.soc_detection['iqr_scale']

    @property
    def soc_sd_scale(self):
        return self.soc_detection['sd_scale']
