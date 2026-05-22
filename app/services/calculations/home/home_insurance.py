import pandas as pd
from dataclasses import dataclass, field
from typing import Optional, List

from app.services.storage_service import StorageService

_VALID_COVERAGE_TYPES = {
    "HOMEOWNERS",
    "CONDOMINIUM",
    "MOBILEHOME",
    "RENTERS",
    "EARTHQUAKE_SINGLE_FAMILY",
    "EARTHQUAKE_CONDOMINIUM",
    "EARTHQUAKE_MOBILEHOME",
    "EARTHQUAKE_RENTERS",
}

_AGE_APPLIES_TO = {"HOMEOWNERS", "MOBILEHOME"}

_DEDUCTIBLE_FACTORS: dict[int, float] = {
    500:  1.110,   # +11% premium penalty vs $1000 baseline
    1000: 1.000,   # Standard baseline
    2500: 0.875,   # -12.5% discount vs $1000 baseline
    5000: 0.725,   # -27.5% discount vs $1000 baseline
}

_BASE_RATES: dict[str, float] = {
    "HOMEOWNERS":               2.5853,  # Single family detached frame construction
    "CONDOMINIUM":              9.4537,  # Condo unit owners (interior + personal property)
    "MOBILEHOME":               5.8969,  # Manufactured/mobilehome
    "RENTERS":                  7.1449,  # Renters/tenants personal property
    "EARTHQUAKE_SINGLE_FAMILY": 2.5,     # Earthquake standalone single family
    "EARTHQUAKE_CONDOMINIUM":   1.5,     # Earthquake standalone condo unit
    "EARTHQUAKE_MOBILEHOME":    3.5,     # Earthquake standalone mobilehome
    "EARTHQUAKE_RENTERS":       1.0,     # Earthquake standalone renters
}

@dataclass(frozen=True)
class _EndorsementDef:
    name: str
    pricing_method: str   # percent_of_base | percent_of_coverage_c | flat
    rate: float
    applies_to: str       # pipe-delimited coverage types, or "ALL"

_ENDORSEMENTS: dict[str, _EndorsementDef] = {
    "ERC_110":                    _EndorsementDef("Extended Replacement Cost 110%",               "percent_of_base",        0.01,  "HOMEOWNERS|MOBILEHOME"),
    "ERC_125":                    _EndorsementDef("Extended Replacement Cost 125%",               "percent_of_base",        0.03,  "HOMEOWNERS|MOBILEHOME"),
    "ERC_150":                    _EndorsementDef("Extended Replacement Cost 150%",               "percent_of_base",        0.06,  "HOMEOWNERS|MOBILEHOME"),
    "ERC_200":                    _EndorsementDef("Extended Replacement Cost 200%",               "percent_of_base",        0.10,  "HOMEOWNERS|MOBILEHOME"),
    "GRC":                        _EndorsementDef("Guaranteed Replacement Cost",                  "percent_of_base",        0.15,  "HOMEOWNERS|MOBILEHOME"),
    "BUILDING_CODE_UPGRADE":      _EndorsementDef("Building Code Upgrade — Ordinance & Law",      "flat",                  66.00,  "HOMEOWNERS|MOBILEHOME"),
    "INFLATION_GUARD_4":          _EndorsementDef("Inflation Guard 4%",                           "percent_of_base",        0.02,  "HOMEOWNERS|CONDOMINIUM|MOBILEHOME"),
    "INFLATION_GUARD_6":          _EndorsementDef("Inflation Guard 6%",                           "percent_of_base",        0.03,  "HOMEOWNERS|CONDOMINIUM|MOBILEHOME"),
    "REPLACEMENT_PERSONAL_PROPERTY": _EndorsementDef("Replacement Value — Personal Property",    "percent_of_coverage_c",  0.15,  "HOMEOWNERS|CONDOMINIUM|MOBILEHOME|RENTERS"),
    "WATER_BACKUP_5K":            _EndorsementDef("Water Backup and Sump Overflow ($5K)",         "flat",                  60.00,  "HOMEOWNERS|CONDOMINIUM|MOBILEHOME"),
    "WATER_BACKUP_25K":           _EndorsementDef("Water Backup and Sump Overflow ($25K)",        "flat",                 120.00,  "HOMEOWNERS|CONDOMINIUM|MOBILEHOME"),
    "SERVICE_LINE":               _EndorsementDef("Service Line Coverage ($10K)",                 "flat",                  55.00,  "HOMEOWNERS|MOBILEHOME"),
    "IDENTITY_FRAUD":             _EndorsementDef("Identity Fraud Expense ($15K)",                "flat",                  40.00,  "ALL"),
    "PERSONAL_INJURY_LIABILITY":  _EndorsementDef("Personal Injury Liability",                    "flat",                  25.00,  "ALL"),
    "WORKERS_COMP":               _EndorsementDef("Workers Compensation",                         "flat",                   4.00,  "ALL"),
    "LOSS_ASSESSMENT_10K":        _EndorsementDef("Loss Assessment Coverage ($10K)",              "flat",                  25.00,  "HOMEOWNERS|CONDOMINIUM"),
    "LOSS_ASSESSMENT_EARTHQUAKE": _EndorsementDef("Earthquake Loss Assessment",                   "flat",                  50.00,  "HOMEOWNERS|CONDOMINIUM"),
    "EQUIPMENT_BREAKDOWN":        _EndorsementDef("Equipment Breakdown",                          "flat",                  50.00,  "HOMEOWNERS|CONDOMINIUM|MOBILEHOME"),
    "MOLD_COVERAGE":              _EndorsementDef("Mold and Fungi Coverage",                      "flat",                  50.00,  "HOMEOWNERS|CONDOMINIUM|MOBILEHOME"),
    "HOME_SHARING":               _EndorsementDef("Home-Sharing Host Activities",                 "flat",                 125.00,  "HOMEOWNERS"),
    "HOME_BUSINESS":              _EndorsementDef("Home Business Insurance (Comprehensive)",      "flat",                 500.00,  "HOMEOWNERS"),
    "GREEN_UPGRADES":             _EndorsementDef("Green and Sustainable Building Upgrades",      "percent_of_base",        0.02,  "HOMEOWNERS|MOBILEHOME"),
    "ASSISTED_LIVING":            _EndorsementDef("Assisted Living Care Coverage",                "flat",                  35.00,  "HOMEOWNERS|CONDOMINIUM"),
}

_AGE_FACTORS: dict[str, float] = {
    "New":      1.0,      # New construction baseline — CDI 2025 market anchor
    "3 Years":  1.1412,   # +14% vs new; early wear begins — CDI avg $1063 vs $931
    "6 Years":  1.298,    # +30% vs new; systems aging — CDI avg $1209
    "15 Years": 1.6522,   # +65% vs new; peak mid-life risk; roof/HVAC/plumbing exposure — CDI avg $1539
    "25 Years": 1.8019,   # +80% vs new; major systems near end-of-life — CDI avg $1678
    "40 Years": 1.8191,   # +82% vs new; PEAK market rate tier — CDI avg $1694
    "70 Years": 1.7889,   # +79% vs new; slightly below 40yr peak due to survivor bias — CDI avg $1666
}


@dataclass
class HomeInsuranceInput:
    coverage_type: str        # e.g. "HOMEOWNERS"
    county: str               # e.g. "SANTA CLARA"
    coverage_amount: float    # e.g. 500000
    deductible: int           # e.g. 1000
    age_of_home: Optional[str] = None          # e.g. "15 Years" — required for HOMEOWNERS/MOBILEHOME
    endorsements: List[str] = field(default_factory=list)  # e.g. ["ERC_150", "WATER_BACKUP"]


@dataclass
class HomeInsuranceQuote:
    annual_premium: float
    monthly_premium: float
    base_rate_per_1k: float
    age_factor: float
    deductible_factor: float
    county_factor: float
    county_risk_tier: str
    base_annual_premium: float
    endorsement_premium: float
    endorsements_applied: List[dict]
    coverage_package: dict   # coverage_a 
    breakdown: dict


class HomeInsuranceCalculator:
    def __init__(self):
        self._county_factors: pd.DataFrame = None

    def _load(self):
        self._county_factors = (
            StorageService()
            .get_collection_as_dataframe("home_county_factors")
            .set_index("county")
        )

    def _ensure_loaded(self):
        if self._county_factors is None:
            self._load()

    def calculate(self, input: HomeInsuranceInput) -> HomeInsuranceQuote:
        self._ensure_loaded()

        coverage_type = input.coverage_type.upper()
        county = input.county.upper().strip()

        if coverage_type not in _VALID_COVERAGE_TYPES:
            raise ValueError(
                f"Invalid coverage_type '{coverage_type}'. "
                f"Valid options: {sorted(_VALID_COVERAGE_TYPES)}"
            )

        # Base rate
        if coverage_type not in _BASE_RATES:
            raise ValueError(f"No base rate found for coverage type '{coverage_type}'")
        base_rate_per_1k = _BASE_RATES[coverage_type]

        # Age factor — only for HOMEOWNERS and MOBILEHOME
        age_factor = 1.0
        if coverage_type in _AGE_APPLIES_TO:
            if not input.age_of_home:
                raise ValueError(
                    f"age_of_home is required for coverage type '{coverage_type}'"
                )
            age_key = input.age_of_home.strip()
            if age_key not in _AGE_FACTORS:
                raise ValueError(
                    f"Invalid age_of_home '{age_key}'. "
                    f"Valid options: {list(_AGE_FACTORS)}"
                )
            age_factor = _AGE_FACTORS[age_key]

        # Deductible factor
        if input.deductible not in _DEDUCTIBLE_FACTORS:
            raise ValueError(
                f"Invalid deductible '{input.deductible}'. "
                f"Valid options: {list(_DEDUCTIBLE_FACTORS)}"
            )
        deductible_factor = _DEDUCTIBLE_FACTORS[input.deductible]

        # County factor
        if county not in self._county_factors.index:
            raise ValueError(
                f"County '{county}' not found. "
                f"Ensure it matches a California county name (e.g. 'LOS ANGELES')."
            )
        county_factor = float(self._county_factors.loc[county, "factor"])
        county_risk_tier = str(self._county_factors.loc[county, "risk_tier"])

        # CDI coverage package — input is Coverage A (Dwelling); B/C/D derived by standard ratios
        coverage_a = input.coverage_amount 

        # Premium = (coverage_a / 1000) × base_rate × age × deductible × county
        coverage_units = coverage_a / 1000.0
        base_annual_premium = round(
            coverage_units * base_rate_per_1k * age_factor * deductible_factor * county_factor,
            2,
        )

        # Endorsements
        endorsement_premium = 0.0
        endorsements_applied = []
        for code in input.endorsements:
            code_upper = code.upper()
            if code_upper not in _ENDORSEMENTS:
                raise ValueError(
                    f"Unknown endorsement code '{code_upper}'. "
                    f"Valid codes: {sorted(_ENDORSEMENTS)}"
                )
            endt = _ENDORSEMENTS[code_upper]
            applies_to_set = (
                _VALID_COVERAGE_TYPES
                if endt.applies_to.upper() == "ALL"
                else {t.strip() for t in endt.applies_to.split("|")}
            )
            if coverage_type not in applies_to_set:
                raise ValueError(
                    f"Endorsement '{code_upper}' does not apply to coverage type '{coverage_type}'. "
                    f"Applies to: {sorted(applies_to_set)}"
                )
            pricing_method = endt.pricing_method
            rate = endt.rate
            if pricing_method == "percent_of_base":
                add_on = round(base_annual_premium * rate, 2)
            elif pricing_method == "percent_of_coverage_c":
                # Price on Coverage C (personal property) at the same per-$1K rate as dwelling
                coverage_c_units = coverage_c / 1000.0
                add_on = round(
                    coverage_c_units * base_rate_per_1k * age_factor * deductible_factor * county_factor * rate,
                    2,
                )
            elif pricing_method == "flat":
                add_on = round(rate, 2)
            else:
                raise ValueError(
                    f"Unknown pricing_method '{pricing_method}' for endorsement '{code_upper}'"
                )
            endorsement_premium = round(endorsement_premium + add_on, 2)
            endorsements_applied.append({
                "code": code_upper,
                "name": endt.name,
                "pricing_method": pricing_method,
                "rate": rate,
                "premium": add_on,
            })

        annual_premium = round(base_annual_premium + endorsement_premium, 2)
        monthly_premium = round(annual_premium / 12, 2)

        return HomeInsuranceQuote(
            annual_premium=annual_premium,
            monthly_premium=monthly_premium,
            base_rate_per_1k=base_rate_per_1k,
            age_factor=age_factor,
            deductible_factor=deductible_factor,
            county_factor=county_factor,
            county_risk_tier=county_risk_tier,
            base_annual_premium=base_annual_premium,
            endorsement_premium=endorsement_premium,
            endorsements_applied=endorsements_applied,
            coverage_package={
                "coverage_a_dwelling": coverage_a
            },
            breakdown={
                "coverage_a": coverage_a,
                "coverage_units": coverage_units,
                "base_rate_per_1k": base_rate_per_1k,
                "age_factor": age_factor,
                "deductible_factor": deductible_factor,
                "county_factor": county_factor,
                "base_annual_premium": base_annual_premium,
                "endorsement_premium": endorsement_premium,
                "annual_premium": annual_premium,
                "monthly_premium": monthly_premium,
            },
        )


_calculator = HomeInsuranceCalculator()
_calculator._ensure_loaded()


def calculate_home_insurance(
    coverage_type: str,
    county: str,
    coverage_amount: float,
    deductible: int,
    age_of_home: Optional[str] = None,
    endorsements: Optional[List[str]] = None,
) -> HomeInsuranceQuote:
    """
    Calculate an estimated California home insurance annual premium.

    Args:
        coverage_type:  One of HOMEOWNERS, CONDOMINIUM, MOBILEHOME, RENTERS,
                        EARTHQUAKE_SINGLE_FAMILY, EARTHQUAKE_CONDOMINIUM,
                        EARTHQUAKE_MOBILEHOME, EARTHQUAKE_RENTERS
        county:         California county name (e.g. "LOS ANGELES", "SANTA CLARA")
        coverage_amount: Dollar amount of coverage (e.g. 500000)
        deductible:     Deductible amount — 500, 1000, 2500, or 5000
        age_of_home:    Required for HOMEOWNERS and MOBILEHOME.
                        One of: "New", "3 Years", "6 Years", "15 Years",
                        "25 Years", "40 Years", "70 Years"

    Returns:
        HomeInsuranceQuote with annual_premium, monthly_premium, and full breakdown.
    """
    return _calculator.calculate(
        HomeInsuranceInput(
            coverage_type=coverage_type,
            county=county,
            coverage_amount=coverage_amount,
            deductible=deductible,
            age_of_home=age_of_home,
            endorsements=endorsements or [],
        )
    )


def get_deductible_factor(deductible: int) -> float:
    """Return the deductible factor for a given deductible amount (1000 = 1.000 baseline)."""
    if deductible not in _DEDUCTIBLE_FACTORS:
        raise ValueError(
            f"Invalid deductible '{deductible}'. "
            f"Valid options: {list(_DEDUCTIBLE_FACTORS)}"
        )
    return _DEDUCTIBLE_FACTORS[deductible]


if __name__ == "__main__":
    examples = [
        dict(coverage_type="HOMEOWNERS", county="SANTA CLARA", coverage_amount=500_000, deductible=1000, age_of_home="15 Years"),
        dict(coverage_type="HOMEOWNERS", county="BUTTE",       coverage_amount=300_000, deductible=500,  age_of_home="40 Years"),
        dict(coverage_type="RENTERS",    county="LOS ANGELES", coverage_amount=30_000,  deductible=500),
        dict(coverage_type="CONDOMINIUM",county="SAN FRANCISCO",coverage_amount=75_000, deductible=1000),
        dict(coverage_type="EARTHQUAKE_SINGLE_FAMILY", county="LOS ANGELES", coverage_amount=500_000, deductible=500),
    ]

    for ex in examples:
        q = calculate_home_insurance(**ex)
        print(
            f"{ex['coverage_type']:30s} | {ex['county']:20s} | "
            f"${ex['coverage_amount']:>10,.0f} | deductible ${ex['deductible']:>5} | "
            f"age: {ex.get('age_of_home', 'N/A'):10s} | "
            f"annual: ${q.annual_premium:>8,.2f} | monthly: ${q.monthly_premium:>7,.2f}"
        )
