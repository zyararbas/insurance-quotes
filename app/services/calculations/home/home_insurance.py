import os
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional, List

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

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
    breakdown: dict


class HomeInsuranceCalculator:
    def __init__(self):
        self._base_rates: pd.DataFrame = None
        self._age_factors: pd.DataFrame = None
        self._deductible_factors: pd.DataFrame = None
        self._county_factors: pd.DataFrame = None
        self._endorsements: pd.DataFrame = None

    def _load(self):
        self._base_rates = pd.read_csv(
            os.path.join(_DATA_DIR, "base_rates.csv"), index_col="coverage_type"
        )
        self._age_factors = pd.read_csv(
            os.path.join(_DATA_DIR, "age_factors.csv"), index_col="age_of_home"
        )
        self._deductible_factors = pd.read_csv(
            os.path.join(_DATA_DIR, "deductible_factors.csv"), index_col="deductible"
        )
        self._county_factors = pd.read_csv(
            os.path.join(_DATA_DIR, "county_factors.csv"), index_col="county"
        )
        self._endorsements = pd.read_csv(
            os.path.join(_DATA_DIR, "endorsements.csv"), index_col="code"
        )

    def _ensure_loaded(self):
        if self._base_rates is None:
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
        if coverage_type not in self._base_rates.index:
            raise ValueError(f"No base rate found for coverage type '{coverage_type}'")
        base_rate_per_1k = float(self._base_rates.loc[coverage_type, "base_rate_per_1k"])

        # Age factor — only for HOMEOWNERS and MOBILEHOME
        age_factor = 1.0
        if coverage_type in _AGE_APPLIES_TO:
            if not input.age_of_home:
                raise ValueError(
                    f"age_of_home is required for coverage type '{coverage_type}'"
                )
            age_key = input.age_of_home.strip()
            if age_key not in self._age_factors.index:
                raise ValueError(
                    f"Invalid age_of_home '{age_key}'. "
                    f"Valid options: {list(self._age_factors.index)}"
                )
            age_factor = float(self._age_factors.loc[age_key, "factor"])

        # Deductible factor
        if input.deductible not in self._deductible_factors.index:
            raise ValueError(
                f"Invalid deductible '{input.deductible}'. "
                f"Valid options: {list(self._deductible_factors.index)}"
            )
        deductible_factor = float(
            self._deductible_factors.loc[input.deductible, "factor"]
        )

        # County factor
        if county not in self._county_factors.index:
            raise ValueError(
                f"County '{county}' not found. "
                f"Ensure it matches a California county name (e.g. 'LOS ANGELES')."
            )
        county_factor = float(self._county_factors.loc[county, "factor"])
        county_risk_tier = str(self._county_factors.loc[county, "risk_tier"])

        # Premium = (coverage / 1000) × base_rate × age × deductible × county
        coverage_units = input.coverage_amount / 1000.0
        base_annual_premium = round(
            coverage_units * base_rate_per_1k * age_factor * deductible_factor * county_factor,
            2,
        )

        # Endorsements
        endorsement_premium = 0.0
        endorsements_applied = []
        for code in input.endorsements:
            code_upper = code.upper()
            if code_upper not in self._endorsements.index:
                raise ValueError(
                    f"Unknown endorsement code '{code_upper}'. "
                    f"Valid codes: {sorted(self._endorsements.index.tolist())}"
                )
            row = self._endorsements.loc[code_upper]
            applies_to_raw = str(row["applies_to"])
            applies_to_set = (
                _VALID_COVERAGE_TYPES
                if applies_to_raw.upper() == "ALL"
                else {t.strip() for t in applies_to_raw.split("|")}
            )
            if coverage_type not in applies_to_set:
                raise ValueError(
                    f"Endorsement '{code_upper}' does not apply to coverage type '{coverage_type}'. "
                    f"Applies to: {sorted(applies_to_set)}"
                )
            pricing_method = str(row["pricing_method"])
            rate = float(row["rate"])
            if pricing_method == "percent_of_base":
                add_on = round(base_annual_premium * rate, 2)
            elif pricing_method == "flat":
                add_on = round(rate, 2)
            else:
                raise ValueError(
                    f"Unknown pricing_method '{pricing_method}' for endorsement '{code_upper}'"
                )
            endorsement_premium = round(endorsement_premium + add_on, 2)
            endorsements_applied.append({
                "code": code_upper,
                "name": str(row["name"]),
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
            breakdown={
                "coverage_amount": input.coverage_amount,
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
