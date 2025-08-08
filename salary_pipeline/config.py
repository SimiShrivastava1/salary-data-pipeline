import os
import yaml
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class PipelineConfig:
    """Pipeline configuration management"""

    # Date range
    start_date: str = "2025-06-01"
    end_date: str = "2025-07-31"
    date_format: str = "%Y-%m-%d"

    # Processing settings
    max_chunk_size: int = 8
    records_per_chunk: int = 150000

    # Data paths
    indeed_path: str = ""
    simplyhired_path: str = ""
    linkedin_path: str = ""
    glassdoor_path: str = ""
    bls_path: str = ""
    output_dir: str = "./output"

    # Source settings
    enable_indeed: bool = True
    enable_simplyhired: bool = True
    enable_linkedin: bool = True
    enable_glassdoor: bool = True

    # Parameters
    salary_min: float = 15000.0
    salary_max: float = 50000000.0
    zscore_threshold: float = 3.0

    # Environment
    environment: str = "development"
    grouping_levels: List[Dict] = None

    def __post_init__(self):
        if self.grouping_levels is None:
            # TODO: INDUSTRY INTEGRATION - Current grouping levels ready for industry enhancement
            # When industry data becomes available, enhance these groupings to include industry dimension
            # This will enable more sophisticated outlier detection within industry contexts

            self.grouping_levels = [
                {
                    'name': 'most_granular',
                    # TODO: Add 'industry' when available: ['job_title_normalized', 'city', 'state', 'seniority_level', 'industry']
                    'columns': ['job_title_normalized', 'city', 'state', 'seniority_level'],
                    'min_size': 3,
                    'threshold': 1.5,
                    'description': 'Most specific grouping - ready for industry dimension'
                },
                {
                    'name': 'remove_seniority',
                    # TODO: Add 'industry' when available: ['job_title_normalized', 'city', 'state', 'industry']
                    'columns': ['job_title_normalized', 'city', 'state'],
                    'min_size': 5,
                    'threshold': 1.8,
                    'description': 'Remove seniority dimension - ready for industry'
                },
                {
                    'name': 'remove_city',
                    # TODO: Add 'industry' when available: ['job_title_normalized', 'state', 'seniority_level', 'industry']
                    'columns': ['job_title_normalized', 'state', 'seniority_level'],
                    'min_size': 4,
                    'threshold': 2.0,
                    'description': 'Remove city dimension - ready for industry'
                },
                {
                    'name': 'job_seniority',
                    # TODO: Consider industry-specific grouping: ['job_title_normalized', 'seniority_level', 'industry']
                    'columns': ['job_title_normalized', 'seniority_level'],
                    'min_size': 8,
                    'threshold': 2.2,
                    'description': 'Job and seniority only - ready for industry enhancement'
                },
                {
                    'name': 'location_seniority',
                    # TODO: Consider: ['city', 'state', 'seniority_level', 'industry']
                    'columns': ['city', 'state', 'seniority_level'],
                    'min_size': 10,
                    'threshold': 2.5,
                    'description': 'Location and seniority patterns - ready for industry'
                }
                # TODO: Add industry-specific outlier detection levels when data becomes available:
                # {
                #     'name': 'industry_job',
                #     'columns': ['industry', 'job_title_normalized'],
                #     'min_size': 15,
                #     'threshold': 2.8,
                #     'description': 'Industry and job title patterns'
                # },
                # {
                #     'name': 'industry_location',
                #     'columns': ['industry', 'state'],
                #     'min_size': 20,
                #     'threshold': 3.0,
                #     'description': 'Industry and location patterns'
                # },
                # {
                #     'name': 'industry_seniority',
                #     'columns': ['industry', 'seniority_level'],
                #     'min_size': 25,
                #     'threshold': 3.2,
                #     'description': 'Industry and seniority patterns'
                # }
            ]

    @classmethod
    def from_yaml(cls, config_path: str, environment: str = None):
        """Load configuration from YAML file"""
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
        """Validate configuration"""
        errors = []

        paths_to_check = ['indeed_path', 'simplyhired_path', 'glassdoor_path', 'bls_path']
        for path_attr in paths_to_check:
            path_value = getattr(self, path_attr)
            if path_value and not os.path.exists(path_value):
                errors.append(f"{path_attr} does not exist: {path_value}")

        os.makedirs(self.output_dir, exist_ok=True)

        if errors:
            raise ValueError(f"Configuration errors: {'; '.join(errors)}")
