
import os
import yaml
from dataclasses import dataclass
from typing import List, Dict, Any

# Define source categories for processing
JOB_BOARD_SOURCES = ['indeed_us', 'indeed_ca', 'linkedin_us', 'linkedin_ca', 'simplyhired_us', 'jora_us', 'jora_ca','jobvite', 'icims', 'greenhouse', 'gem', 'dayforce', 'avature', 'ultipro', 'smartrecruiter', 'lever', 'myworkdayjobs', 'applytojob', 'adpworkforce', 'pjf_us']

SPECIAL_PROCESSING = {'glassdoor': 'csv_processing'}

@dataclass
class SourceSchema:
    """Defines validation requirements for different source types"""
    required_fields: List[str]
    min_coverage: Dict[str, float]
    min_records: int = 500

# Schema registry for different source types
SCHEMA_REGISTRY = {'job_board': SourceSchema(required_fields=['salary', 'job_title'], min_coverage={'salary': 0.30, 'job_title': 0.50}), 'glassdoor': SourceSchema(required_fields=['total_pay', 'normalizedTitle'], min_coverage={'total_pay': 0.30, 'normalizedTitle': 0.50})}

# Column mapping for flexible field selection
COLUMN_MAPPINGS = {
    'salary_columns': {
        'job_board': ['parsed_annual_salary_avg', 'salary_annual', 'salary'],
        'glassdoor': ['total_pay']
    },
    'title_columns': {
        'job_board': ['nlp_norm_title'],
        'glassdoor': ['normalizedTitle']
    },
    'city_columns': ['final_city', 'city'],
    'state_columns': ['final_state', 'state'],
    'seniority_columns': ['nlp_seniority', 'seniority_level', 'seniority'],
    'industry_columns': {
        'glassdoor': ['industry', 'company_industry', 'Industry'],
        'default': ['industry', 'Industry', 'company_industry']
    }
}

@dataclass
class PipelineConfig:
    start_date: str = "2025-06-01"
    end_date: str = "2025-07-31"
    date_format: str = "%Y-%m-%d"
    max_chunk_size: int = 8
    records_per_chunk: int = 150000

    # Legacy paths
    indeed_path: str = ""
    simplyhired_path: str = ""
    linkedin_path: str = ""
    glassdoor_path: str = ""
    bls_path: str = ""
    output_dir: str = "./output"

    # Dynamic data sources configuration
    data_sources: List[Dict[str, Any]] = None

    # Legacy enables
    enable_indeed: bool = True
    enable_simplyhired: bool = True
    enable_linkedin: bool = True
    enable_glassdoor: bool = True

    cola_data_paths: Dict = None
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
        # Initialize default configurations
        if self.industry_analysis is None:
            self.industry_analysis = {'enabled': True, 'include_in_outlier_detection': False, 'min_coverage_rate': 0.30, 'include_in_statistics': True, 'standardize_names': True}

        if self.soc_detection is None:
            self.soc_detection = {'enabled': True, 'min_samples': 15, 'iqr_scale': 3.0, 'sd_scale': 3.5}

        if self.geographic_context is None:
            self.geographic_context = {'enabled': True, 'use_dynamic_cola': True, 'use_simple_state_tiers': False, 'cola_cache_enabled': True}

        if self.outlier_detection is None:
            self.outlier_detection = {'min_group_size': 5, 'enable_industry_grouping': False, 'enable_soc_grouping': True, 'fallback_without_industry': True}

        if self.cola_data_paths is None:
            self.cola_data_paths = {'county_economic': "", 'cities': "", 'zips': "", 'counties': "", 'zip_database': ""}

        if self.cola_settings is None:
            self.cola_settings = {'salary_weight': 0.7, 'gdp_weight': 0.3, 'min_records_for_custom': 10, 'default_multiplier': 1.0, 'enable_zip_lookup': True, 'enable_city_lookup': True, 'cache_lookups': True}

        if self.grouping_levels is None:
            self.grouping_levels = []

        # Initialize data_sources as empty list if None
        if self.data_sources is None:
            self.data_sources = []

    @classmethod
    def from_yaml(cls, config_path: str, environment: str = None):
        """Load a PipelineConfig from a YAML file and apply defaults."""
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

    def get_enabled_job_board_sources(self) -> List[Dict[str, Any]]:
        """Return configured job-board/ATS sources as a list of {name, path} dicts, in order."""
        if not self.data_sources:
            return []

        return [
            source for source in self.data_sources
            if source.get('enabled', False) and source.get('type') == 'job_board'
        ]

    def get_source_by_name(self, source_name: str) -> Dict[str, Any]:
        """Get source configuration by name"""
        if not self.data_sources:
            return None

        for source in self.data_sources:
            if source.get('name') == source_name:
                return source
        return None

    def is_source_enabled(self, source_name: str) -> bool:
        """Check if a specific source is enabled"""
        source = self.get_source_by_name(source_name)
        return source.get('enabled', False) if source else False

    def get_source_schema(self, source_name: str) -> SourceSchema:
        """Get validation schema for a source"""
        if source_name == 'glassdoor':
            return SCHEMA_REGISTRY['glassdoor']
        return SCHEMA_REGISTRY['job_board']

    def validate(self):
        """Sanity-check key config fields (dates, bounds, paths) and raise on invalid settings."""
        errors = []

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Validate enabled data sources exist
        for source in self.get_enabled_job_board_sources():
            source_path = source.get('path', '')
            source_name = source.get('name', 'unknown')

            if source_path and not os.path.exists(source_path):
                errors.append(f"Data source '{source_name}' path does not exist: {source_path}")

        # Legacy path validation (for backward compatibility)
        if self.enable_glassdoor and self.glassdoor_path:
            if not os.path.exists(self.glassdoor_path):
                errors.append(f"glassdoor_path does not exist: {self.glassdoor_path}")

        if self.bls_path and not os.path.exists(self.bls_path):
            errors.append(f"bls_path does not exist: {self.bls_path}")

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
        """Minimum rows required in a SOC group before applying IQR outlier logic."""
        return self.soc_detection['min_samples']

    @property
    def soc_iqr_scale(self):
        """IQR multiplier for SOC outlier bounds (lower=Q1-k*IQR, upper=Q3+k*IQR)."""
        return self.soc_detection['iqr_scale']

    @property
    def soc_sd_scale(self):
        return self.soc_detection['sd_scale']
