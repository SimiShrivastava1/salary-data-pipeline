
import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional, List
import os
from utils import Logger
from config import PipelineConfig

class COLAProcessor:
    def __init__(self, config: PipelineConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self.cola_lookup = {}
        self.coverage_stats = {}
        self.lookup_cache = {}
        self._build_cola_system()

    def _build_cola_system(self) -> None:
        try:
            county_econ_path = self.config.cola_data_paths['county_economic']
            if not os.path.exists(county_econ_path):
                raise FileNotFoundError(f"County economic data not found: {county_econ_path}")

            county_econ = pd.read_csv(county_econ_path)

            national_avg_salary = county_econ['avg_salary'].mean()
            national_gdp_per_capita = county_econ['gdp_per_capita'].mean()

            county_econ['salary_multiplier'] = county_econ['avg_salary'] / national_avg_salary
            county_econ['gdp_multiplier'] = county_econ['gdp_per_capita'] / national_gdp_per_capita

            salary_weight = self.config.cola_settings['salary_weight']
            gdp_weight = self.config.cola_settings['gdp_weight']

            county_econ['cola_multiplier'] = (county_econ['salary_multiplier'] * salary_weight + county_econ['gdp_multiplier'] * gdp_weight)

            cities = self._load_geographic_file('cities')
            zips = self._load_geographic_file('zips') if self.config.cola_settings['enable_zip_lookup'] else None

            self._build_county_lookup(county_econ)
            if self.config.cola_settings['enable_city_lookup'] and cities is not None:
                self._build_city_lookup(cities, county_econ)
            if zips is not None:
                self._build_zip_lookup(zips, county_econ)

            self._calculate_coverage_stats(county_econ, cities, zips)

            self.logger.info(f"COLA system built successfully: {len(self.cola_lookup)} total lookup entries")

        except Exception as e:
            self.logger.warning(f"COLA system build failed: {e}, using fallback")
            self._build_fallback_system()

    def _load_geographic_file(self, file_type: str) -> Optional[pd.DataFrame]:
        file_path = self.config.cola_data_paths.get(file_type, '')
        if not file_path or not os.path.exists(file_path):
            return None

        try:
            df = pd.read_csv(file_path)
            return df
        except Exception:
            return None

    def _build_county_lookup(self, county_econ: pd.DataFrame) -> None:
        for _, row in county_econ.iterrows():
            fips = int(row['GeoFIPS'])
            self.cola_lookup[f"county_{fips}"] = {'multiplier': row['cola_multiplier'], 'tier': self._get_cola_tier(row['cola_multiplier']), 'avg_salary': row['avg_salary'], 'gdp_per_capita': row['gdp_per_capita'], 'lookup_type': 'county'}

    def _build_city_lookup(self, cities: pd.DataFrame, county_econ: pd.DataFrame) -> None:
        county_fips_set = set(int(fips) for fips in county_econ['GeoFIPS'])
        city_count = 0

        for _, row in cities.iterrows():
            try:
                city = str(row['city']).strip()
                state = str(row['state_id']).strip()
                county_fips = int(row['county_fips'])

                if not city or not state or pd.isna(county_fips):
                    continue

                if county_fips in county_fips_set:
                    city_key = f"{city}, {state}"
                    county_data = self.cola_lookup.get(f"county_{county_fips}")
                    if county_data:
                        self.cola_lookup[city_key] = county_data.copy()
                        self.cola_lookup[city_key]['lookup_type'] = 'city'
                        city_count += 1
            except Exception:
                continue

    def _build_zip_lookup(self, zips: pd.DataFrame, county_econ: pd.DataFrame) -> None:
        """Prepare ZIP → COLA tier/multiplier lookup from reference files."""
        county_fips_set = set(int(fips) for fips in county_econ['GeoFIPS'])
        zip_count = 0

        for _, row in zips.iterrows():
            try:
                zip_code = str(row['zip']).zfill(5)
                county_fips = int(row['county_fips'])

                if pd.isna(county_fips) or not zip_code.isdigit():
                    continue

                if county_fips in county_fips_set:
                    county_data = self.cola_lookup.get(f"county_{county_fips}")
                    if county_data:
                        self.cola_lookup[f"zip_{zip_code}"] = county_data.copy()
                        self.cola_lookup[f"zip_{zip_code}"]["lookup_type"] = 'zip'
                        zip_count += 1
            except Exception:
                continue

    def _calculate_coverage_stats(self, county_econ: pd.DataFrame, cities: pd.DataFrame, zips: pd.DataFrame) -> None:
        self.coverage_stats = {
            'counties_with_data': len(county_econ),
            'cities_mapped': len([k for k in self.cola_lookup.keys() if not k.startswith(('county_', 'zip_', 'state_'))]),
            'zips_mapped': len([k for k in self.cola_lookup.keys() if k.startswith('zip_')]),
            'multiplier_range': {'min': float(county_econ['cola_multiplier'].min()), 'max': float(county_econ['cola_multiplier'].max()), 'mean': float(county_econ['cola_multiplier'].mean()), 'std': float(county_econ['cola_multiplier'].std())}
        }

    def _get_cola_tier(self, multiplier: float) -> str:
        if multiplier >= 1.50:      return 'Very High Cost'
        elif multiplier >= 1.25:    return 'High Cost'
        elif multiplier >= 1.10:    return 'Medium High Cost'
        elif multiplier >= 0.95:    return 'Medium Cost'
        elif multiplier >= 0.85:    return 'Low Medium Cost'
        else:                       return 'Low Cost'

    def _build_fallback_system(self) -> None:
        self.logger.warning("COLA multiplier files not found or failed to load - switching to default multiplier system")
        self.cola_lookup = {}

        # Set minimal coverage stats for fallback
        self.coverage_stats = {'counties_with_data': 0, 'cities_mapped': 0, 'zips_mapped': 0, 'multiplier_range': {'min': 1.0, 'max': 1.0, 'mean': 1.0, 'std': 0.0}}

    def get_cola_info(self, city: str, state: str, zip_code: str = None) -> Dict:
        cache_key = f"{city}|{state}|{zip_code}"
        if self.config.cola_settings.get('cache_lookups', True) and cache_key in self.lookup_cache:
            return self.lookup_cache[cache_key].copy()

        # If no COLA data available, return default values
        if not self.cola_lookup:
            result = {'multiplier': 1.0, 'tier': 'National Average', 'avg_salary': 0, 'gdp_per_capita': 0, 'lookup_type': 'disabled', 'lookup_method': 'COLA Disabled'}
            self.lookup_cache[cache_key] = result
            return result

        if zip_code and self.config.cola_settings['enable_zip_lookup']:
            zip_key = f"zip_{str(zip_code).zfill(5)}"
            if zip_key in self.cola_lookup:
                result = self.cola_lookup[zip_key].copy()
                result['lookup_method'] = 'ZIP Code'
                self.lookup_cache[cache_key] = result
                return result

        if city and state and self.config.cola_settings['enable_city_lookup']:
            city_variations = [f"{city}, {state}", f"{city.title()}, {state}", f"{city.upper()}, {state}", f"{city.lower()}, {state}"]

            for city_key in city_variations:
                if city_key in self.cola_lookup:
                    result = self.cola_lookup[city_key].copy()
                    result['lookup_method'] = 'City, State'
                    self.lookup_cache[cache_key] = result
                    return result

        if state:
            state_key = f"state_{state}"
            if state_key in self.cola_lookup:
                result = self.cola_lookup[state_key].copy()
                result['lookup_method'] = 'State Fallback'
                self.lookup_cache[cache_key] = result
                return result

        # Default fallback when COLA is enabled but no match found
        result = { 'multiplier': self.config.cola_settings['default_multiplier'], 'tier': 'National Average', 'avg_salary': 0, 'gdp_per_capita': 0, 'lookup_type': 'default', 'lookup_method': 'Default'}

        self.lookup_cache[cache_key] = result
        return result

    def add_cola_to_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Attach COLA multiplier, tier, and national-equivalent salary using the internal lookup tables."""
        cola_results = []
        lookup_method_counts = {}

        for _, row in df.iterrows():
            cola_info = self.get_cola_info(row.get('city', 'Unknown'), row.get('state', 'Unknown'), row.get('zip_code'))

            method = cola_info['lookup_method']
            lookup_method_counts[method] = lookup_method_counts.get(method, 0) + 1
            cola_results.append(cola_info)

        df['cola_multiplier'] = [r['multiplier'] for r in cola_results]
        df['cola_tier'] = [r['tier'] for r in cola_results]
        df['cola_lookup_method'] = [r['lookup_method'] for r in cola_results]
        df['local_avg_salary'] = [r.get('avg_salary', 0) for r in cola_results]
        df['salary_national_equivalent'] = df['salary_annual'] / df['cola_multiplier']

        return df

    def get_coverage_report(self) -> Dict:
        return {
            'system_status': 'Active' if self.cola_lookup else 'Disabled',
            'coverage_stats': self.coverage_stats,
            'lookup_entries': len(self.cola_lookup),
            'cache_entries': len(self.lookup_cache),
            'data_sources': {
                'county_economic_data': bool(self.config.cola_data_paths.get('county_economic')),
                'geographic_mappings': bool(self.config.cola_data_paths.get('cities')),
                'zip_mappings': bool(self.config.cola_data_paths.get('zips'))
            }
        }
