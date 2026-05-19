"""
Live CDI market rate lookup.
Fetches per-company rates from interactive.web.insurance.ca.gov and returns
statistical summaries alongside raw company data.
"""
from __future__ import annotations

import re
import time
import urllib.parse
from dataclasses import dataclass, field
from statistics import mean, median
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://interactive.web.insurance.ca.gov/apex_extprd"

# Map internal coverage_type keys to CDI form values
_COVERAGE_TYPE_MAP = {
    "HOMEOWNERS": "HOMEOWNERS",
    "CONDOMINIUM": "CONDOMINIUM",
    "RENTERS": "RENTERS",
    "MOBILEHOME": "MOBILEHOME",
    "EARTHQUAKE_SINGLE_FAMILY": "EARTHQUAKE - Single Family",
    "EARTHQUAKE_CONDOMINIUM": "EARTHQUAKE - Condominium",
    "EARTHQUAKE_MOBILEHOME": "EARTHQUAKE - Mobilehome",
    "EARTHQUAKE_RENTERS": "EARTHQUAKE - Renters",
}

# Coverage types that require an age_of_home value
_AGE_REQUIRED = {"HOMEOWNERS", "MOBILEHOME"}


@dataclass
class CompanyRate:
    company: str
    annual_premium: int
    monthly_premium: float


@dataclass
class CDIMarketStats:
    count: int
    minimum: int
    maximum: int
    mean: float
    median: float
    percentile_80: float
    percentile_90: float
    percentile_95: float


@dataclass
class CDILookupResult:
    location: str
    coverage_type: str
    home_age: Optional[str]
    coverage_amount: int
    deductible: int
    companies: List[CompanyRate] = field(default_factory=list)
    stats: Optional[CDIMarketStats] = None


def _percentile(sorted_values: list, pct: float) -> float:
    """Linear interpolation percentile on a sorted list."""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    if n == 1:
        return float(sorted_values[0])
    idx = (pct / 100) * (n - 1)
    lo = int(idx)
    hi = lo + 1
    if hi >= n:
        return float(sorted_values[-1])
    frac = idx - lo
    return round(sorted_values[lo] + frac * (sorted_values[hi] - sorted_values[lo]), 2)


class CDILookupService:
    """Fetches live market rates from the CDI interactive tool."""

    def __init__(self, delay: float = 0.3):
        self.delay = delay
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "Mozilla/5.0"
        self._hidden: dict = {}
        self._session_id: str = ""
        self._refresh_session()

    def _refresh_session(self) -> None:
        self._session.get(f"{BASE_URL}/f?p=111:1", timeout=15)
        r = self._session.get(f"{BASE_URL}/f?p=111:21", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        self._hidden = {
            i["name"]: i.get("value", "")
            for i in soup.find_all("input", type="hidden")
            if i.get("name")
        }
        self._session_id = self._hidden.get("p_instance", "")

    def _prefill_page(self, location: str, cdi_type: str, home_age: str, amount: str) -> dict:
        items = "P21_LOCATION,P21_TYPE,P21_HOME_AGE,P21_INSURANCE_AMOUNT"
        values = urllib.parse.quote(f"{location},{cdi_type},{home_age},{amount}")
        url = f"{BASE_URL}/f?p=111:21:{self._session_id}::NO::{items}:{values}"
        r = self._session.get(url, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        return {
            i["name"]: i.get("value", "")
            for i in soup.find_all("input", type="hidden")
            if i.get("name")
        }

    def _submit_and_parse(
        self, location: str, cdi_type: str, home_age: str, amount: str
    ) -> List[CompanyRate]:
        hidden = self._prefill_page(location, cdi_type, home_age, amount)
        payload = dict(hidden)
        payload.update({
            "p_request": "SUBMIT",
            "P21_LOCATION": location,
            "P21_TYPE": cdi_type,
            "P21_HOME_AGE": home_age,
            "P21_INSURANCE_AMOUNT": amount,
        })
        resp = self._session.post(
            f"{BASE_URL}/wwv_flow.accept", data=payload, timeout=20
        )
        soup = BeautifulSoup(resp.text, "html.parser")
        return self._parse(soup)

    def _parse(self, soup: BeautifulSoup) -> List[CompanyRate]:
        seen: set = set()
        rates: List[CompanyRate] = []
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 5:
                continue
            for row in rows:
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) < 4:
                    continue
                company = cells[2]
                price_cell = cells[3]
                m = re.search(r"\$([\d,]+)", price_cell)
                if m and len(company) > 3 and "Company" not in company:
                    annual = int(m.group(1).replace(",", ""))
                    key = (company, annual)
                    if key not in seen:
                        seen.add(key)
                        rates.append(CompanyRate(
                            company=company,
                            annual_premium=annual,
                            monthly_premium=round(annual / 12, 2),
                        ))
        return rates

    def lookup(
        self,
        location: str,
        coverage_type: str,
        coverage_amount: int,
        home_age: Optional[str] = None,
        retries: int = 2,
    ) -> CDILookupResult:
        """
        Fetch live CDI market rates and compute statistics.

        Args:
            location:        CDI location string, e.g. "ALAMEDA ALAMEDA"
            coverage_type:   Internal key, e.g. "HOMEOWNERS", "EARTHQUAKE_SINGLE_FAMILY"
            coverage_amount: Coverage in dollars, e.g. 375000
            home_age:        Required for HOMEOWNERS/MOBILEHOME, e.g. "3 Years"
            retries:         Number of retry attempts on failure

        Returns:
            CDILookupResult with per-company rates and market statistics
        """
        cdi_type = _COVERAGE_TYPE_MAP.get(coverage_type.upper(), coverage_type)
        age_str = home_age or ""

        result = CDILookupResult(
            location=location,
            coverage_type=coverage_type,
            home_age=home_age,
            coverage_amount=coverage_amount,
            deductible=1000,
        )

        for attempt in range(retries + 1):
            try:
                rates = self._submit_and_parse(
                    location, cdi_type, age_str, str(coverage_amount)
                )
                if rates:
                    result.companies = sorted(rates, key=lambda r: r.annual_premium)
                    result.stats = _compute_stats(result.companies)
                    return result
                # empty result — retry with session refresh
                if attempt < retries:
                    time.sleep(1)
                    self._refresh_session()
            except Exception as e:
                if attempt < retries:
                    time.sleep(2)
                    self._refresh_session()
                else:
                    raise RuntimeError(
                        f"CDI lookup failed after {retries + 1} attempts: {e}"
                    ) from e

        return result


def _compute_stats(companies: List[CompanyRate]) -> CDIMarketStats:
    premiums = sorted(r.annual_premium for r in companies)
    return CDIMarketStats(
        count=len(premiums),
        minimum=premiums[0],
        maximum=premiums[-1],
        mean=round(mean(premiums), 2),
        median=round(median(premiums), 2),
        percentile_80=_percentile(premiums, 80),
        percentile_90=_percentile(premiums, 90),
        percentile_95=_percentile(premiums, 95),
    )
