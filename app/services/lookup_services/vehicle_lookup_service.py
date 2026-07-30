import pandas as pd
import logging
from typing import List, Optional
from app.utils.data_loader import VEHICLE_RATES
from app.services.storage_service import StorageService

logger = logging.getLogger(__name__)

# Collation for case-insensitive string equality (mirrors the old `.upper()`
# comparisons that filtered results in Python).
CASE_INSENSITIVE = {'locale': 'en', 'strength': 2}


class VehicleLookupService:
    """
    Service for providing vehicle data for cascading dropdowns.

    Each level queries MongoDB directly for the distinct values at that level,
    scoped by the selections already made, instead of loading the whole
    collection into memory.
    """

    def _distinct_text(self, field: str, query: dict) -> List[str]:
        """
        Return the sorted distinct values of a text field, normalizing null /
        NaN entries to '' (mirrors the previous pandas ``fillna('')`` behavior,
        so a "blank" option is offered when some rows have no value).
        """
        values = StorageService().distinct(field, query, VEHICLE_RATES)
        normalized = {'' if pd.isna(v) else v for v in values}
        return sorted(normalized)

    def get_years(self) -> List[int]:
        """Get all available years."""
        years = StorageService().distinct('YEAR', {}, VEHICLE_RATES)
        return sorted(int(y) for y in years if not pd.isna(y))

    def get_makes(self, year: int) -> List[str]:
        """Get all makes for a given year."""
        makes = StorageService().distinct('MAKE', {'YEAR': year}, VEHICLE_RATES)
        return sorted(m for m in makes if not pd.isna(m))

    def get_models(self, year: int, make: str) -> List[str]:
        """Get all models for a given year and make."""
        models = StorageService().distinct(
            'MODEL', {'YEAR': year, 'MAKE': make}, VEHICLE_RATES
        )
        return sorted(m for m in models if not pd.isna(m))

    def get_series(self, year: int, make: str, model: str) -> List[str]:
        """Get all series for a given year, make, and model."""
        return self._distinct_text(
            'SERIES', {'YEAR': year, 'MAKE': make, 'MODEL': model}
        )

    def get_packages(self, year: int, make: str, model: str, series: str) -> List[str]:
        """Get all packages for a given year, make, model, and series."""
        return self._distinct_text('OPTIONPACKAGE', {
            'YEAR': year,
            'MAKE': make,
            'MODEL': model,
            'SERIES': self._exact_or_blank(series),
        })

    def get_styles(self, year: int, make: str, model: str, series: str, package: str) -> List[str]:
        """Get all styles for a given year, make, model, series, and package."""
        return self._distinct_text('BODYSTYLE', {
            'YEAR': year,
            'MAKE': make,
            'MODEL': model,
            'SERIES': self._exact_or_blank(series),
            'OPTIONPACKAGE': self._exact_or_blank(package),
        })

    def get_engines(self, year: int, make: str, model: str, series: str, package: str, style: str) -> List[str]:
        """Get all engines for a given year, make, model, series, package, and style."""
        return self._distinct_text('ENGINE', {
            'YEAR': year,
            'MAKE': make,
            'MODEL': model,
            'SERIES': self._exact_or_blank(series),
            'OPTIONPACKAGE': self._exact_or_blank(package),
            'BODYSTYLE': self._exact_or_blank(style),
        })

    # Maps the caller-facing option name to its underlying column.
    _OPTION_FIELDS = {
        'series': 'SERIES',
        'package': 'OPTIONPACKAGE',
        'style': 'BODYSTYLE',
        'engine': 'ENGINE',
    }

    def get_distinct_options(self, option_type: str, year: int, make: str, model: str) -> List[str]:
        """
        Distinct values of one option field across a year/make/model, ignoring
        any deeper selection. Returns [] for an unknown option_type.
        """
        field = self._OPTION_FIELDS.get(option_type)
        if field is None:
            return []
        values = self._distinct_text(field, {'YEAR': year, 'MAKE': make, 'MODEL': model})
        # Callers of this method don't want the synthetic blank option.
        return [v for v in values if v]
    
    def find_rating_groups(self, year: int, make: str, model: str, series: str, package: str, style: str, engine: str) -> Optional[dict]:
        """
        Keyed lookup of a single vehicle's rating groups (DRG/GRG/VSD/LRG).

        Fully-specified exact match: queries MongoDB for the single matching
        document instead of loading and scanning the entire collection. Returns
        None on a miss so callers can decide their own fallback.
        """
        query = {
            'YEAR': year,
            'MAKE': make,
            'MODEL': model,
            'SERIES': self._exact_or_blank(series),
            'OPTIONPACKAGE': self._exact_or_blank(package),
            'BODYSTYLE': self._exact_or_blank(style),
            'ENGINE': self._exact_or_blank(engine),
        }
        documents = StorageService().find(query, VEHICLE_RATES)
        if documents:
            row = documents[0]
            return {
                'drg': int(row['DRG']),
                'grg': int(row['GRG']),
                'vsd': str(row['VSD']),
                'lrg': int(row['LRG'])
            }
        return None

    def get_rating_groups(self, year: int, make: str, model: str, series: str, package: str, style: str, engine: str) -> dict:
        """Rating groups for a complete vehicle spec, or defaults if not found."""
        return self.find_rating_groups(
            year, make, model, series, package, style, engine
        ) or {'drg': 1, 'grg': 1, 'vsd': '1', 'lrg': 1}

    @staticmethod
    def _exact_or_blank(value: str):
        """
        Build a MongoDB match that mirrors pandas' ``fillna('') == value``:
        an empty string matches blank, null, or missing fields; a non-empty
        value matches exactly.
        """
        if value == '' or value is None:
            return {'$in': ['', None]}
        return value
    
    def search_vehicles(
        self,
        make: str = None,
        model: str = None,
        year: int = None,
        series: str = None,
        package: str = None,
        style: str = None,
        engine: str = None,
    ) -> List[dict]:
        """
        Search for vehicles based on the provided criteria.

        Builds a MongoDB query from whatever criteria are provided and fetches
        only the matching documents, rather than loading the entire collection
        into memory and filtering with pandas.

        make/model are matched as case-insensitive substrings; series/package/
        style/engine are matched as case-insensitive exact values (only applied
        when non-empty).

        Returns a list of matching vehicles with their details.
        """
        query = {}
        if year is not None:
            query['YEAR'] = year
        if make is not None:
            # Case-insensitive substring match.
            query['MAKE'] = {'$regex': make, '$options': 'i'}
        if model is not None:
            query['MODEL'] = {'$regex': model, '$options': 'i'}

        # Exact (case-insensitive) filters, applied only when supplied.
        exact_filters = False
        for value, field in ((series, 'SERIES'), (package, 'OPTIONPACKAGE'),
                             (style, 'BODYSTYLE'), (engine, 'ENGINE')):
            if value:
                query[field] = value
                exact_filters = True

        # Collation only matters for the exact string filters above.
        collation = CASE_INSENSITIVE if exact_filters else None
        documents = StorageService().find(query, VEHICLE_RATES, collation=collation)

        def _text(value):
            # Missing values may be stored as null or float NaN; normalize to ''.
            return value if pd.notna(value) else ''

        def _int(value):
            return int(value) if pd.notna(value) else None

        results = []
        for doc in documents:
            vehicle = {
                'year': int(doc['YEAR']),
                'make': doc.get('MAKE'),
                'model': doc.get('MODEL'),
                'series': _text(doc.get('SERIES')),
                'package': _text(doc.get('OPTIONPACKAGE')),
                'style': _text(doc.get('BODYSTYLE')),
                'engine': _text(doc.get('ENGINE')),
                'wheelbase': _text(doc.get('Wheelbase')),
                'grg': _int(doc.get('GRG')),
                'drg': _int(doc.get('DRG')),
                'vsd': doc.get('VSD') if pd.notna(doc.get('VSD')) else None,
                'lrg': _int(doc.get('LRG')),
            }
            results.append(vehicle)

        logger.info(f"Found {len(results)} vehicles matching criteria: make={make}, model={model}, year={year}")
        return results
