"""
CDI Homeowners Insurance Rate Scraper
Fetches real market rates from interactive.web.insurance.ca.gov for calibration.
"""
import requests
import time
import csv
import re
import urllib.parse
import os
from bs4 import BeautifulSoup
from statistics import mean, median

BASE_URL = "https://interactive.web.insurance.ca.gov/apex_extprd"
DATA_DIR = os.path.dirname(__file__)

HOMEOWNERS_AGES    = ["New", "3 Years", "6 Years", "15 Years", "25 Years", "40 Years", "70 Years"]
HOMEOWNERS_AMOUNTS = ["250000", "375000", "500000", "625000", "750000", "1000000"]
CONDO_AMOUNTS      = ["25000", "50000", "75000", "100000"]
RENTERS_AMOUNTS    = ["15000", "25000", "30000", "50000", "70000"]
MOBILEHOME_AGES    = ["New", "15 Years", "30 Years"]
MOBILEHOME_AMOUNTS = ["50000", "100000"]


class CDIScraper:
    def __init__(self, delay=0.4):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0"
        self._hidden: dict = {}
        self._session_id: str = ""
        self._refresh_session()

    def _refresh_session(self):
        self.session.get(f"{BASE_URL}/f?p=111:1", timeout=15)
        r = self.session.get(f"{BASE_URL}/f?p=111:21", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        self._hidden = {
            i["name"]: i.get("value", "")
            for i in soup.find_all("input", type="hidden")
            if i.get("name")
        }
        self._session_id = self._hidden.get("p_instance", "")

    def _get_prefilled_page(self, location, coverage_type, home_age, amount):
        items  = "P21_LOCATION,P21_TYPE,P21_HOME_AGE,P21_INSURANCE_AMOUNT"
        values = urllib.parse.quote(f"{location},{coverage_type},{home_age},{amount}")
        url = f"{BASE_URL}/f?p=111:21:{self._session_id}::NO::{items}:{values}"
        r = self.session.get(url, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        hidden = {
            i["name"]: i.get("value", "")
            for i in soup.find_all("input", type="hidden")
            if i.get("name")
        }
        return hidden

    def fetch_rates(self, location, coverage_type, home_age, amount, retries=2):
        for attempt in range(retries + 1):
            try:
                hidden = self._get_prefilled_page(location, coverage_type, home_age, amount)
                payload = dict(hidden)
                payload.update({
                    "p_request": "SUBMIT",
                    "P21_LOCATION": location,
                    "P21_TYPE": coverage_type,
                    "P21_HOME_AGE": home_age,
                    "P21_INSURANCE_AMOUNT": amount,
                })
                resp = self.session.post(
                    f"{BASE_URL}/wwv_flow.accept", data=payload, timeout=20
                )
                soup = BeautifulSoup(resp.text, "html.parser")
                return self._parse_rates(soup, location, coverage_type, home_age, amount)
            except Exception as e:
                print(f"    Attempt {attempt+1} failed: {e}")
                if attempt < retries:
                    time.sleep(2)
                    self._refresh_session()
        return []

    def _parse_rates(self, soup, location, coverage_type, home_age, amount):
        rows = []
        county = location.split()[0]
        city = " ".join(location.split()[1:])
        for table in soup.find_all("table"):
            trows = table.find_all("tr")
            if len(trows) < 5:
                continue
            for row in trows:
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) >= 4:
                    company = cells[2] if len(cells) > 2 else cells[0]
                    price_cell = cells[3] if len(cells) > 3 else cells[1]
                    price_match = re.search(r"\$([\d,]+)", price_cell)
                    if price_match and len(company) > 3 and "Company" not in company:
                        annual = int(price_match.group(1).replace(",", ""))
                        rows.append({
                            "county": county,
                            "city": city,
                            "location": location,
                            "coverage_type": coverage_type,
                            "home_age": home_age,
                            "coverage_amount": amount,
                            "deductible": 1000,
                            "company": company,
                            "annual_premium": annual,
                        })
        return rows


def run_county_calibration(scraper, locations, out_path):
    """Fixed profile: HOMEOWNERS / 3 Years / $375K — vary all 270 locations."""
    print(f"\n{'='*60}")
    print(f"County calibration: {len(locations)} locations")
    print(f"Profile: HOMEOWNERS / 3 Years / $375,000 / $1K deductible")
    print(f"{'='*60}")

    all_rows = []
    for idx, loc in enumerate(locations):
        print(f"  [{idx+1}/{len(locations)}] {loc} ... ", end="", flush=True)
        rows = scraper.fetch_rates(loc, "HOMEOWNERS", "3 Years", "375000")
        if rows:
            avg = mean(r["annual_premium"] for r in rows)
            print(f"{len(rows)} companies | avg ${avg:,.0f}")
            all_rows.extend(rows)
        else:
            print("no data")
        time.sleep(scraper.delay)

    _write_csv(all_rows, out_path)
    print(f"\nSaved {len(all_rows)} rows to {out_path}")
    return all_rows


def run_age_calibration(scraper, location, out_path):
    """Fixed profile: HOMEOWNERS / ALAMEDA ALAMEDA / $375K — vary all 7 ages."""
    print(f"\n{'='*60}")
    print(f"Age calibration: {len(HOMEOWNERS_AGES)} ages @ {location}")
    print(f"{'='*60}")

    all_rows = []
    for age in HOMEOWNERS_AGES:
        print(f"  Age: {age:12s} ... ", end="", flush=True)
        rows = scraper.fetch_rates(location, "HOMEOWNERS", age, "375000")
        if rows:
            avg = mean(r["annual_premium"] for r in rows)
            print(f"{len(rows)} companies | avg ${avg:,.0f}")
            all_rows.extend(rows)
        else:
            print("no data")
        time.sleep(scraper.delay)

    _write_csv(all_rows, out_path, append=True)
    return all_rows


def run_amount_calibration(scraper, location, out_path):
    """Fixed profile: HOMEOWNERS / ALAMEDA ALAMEDA / 3 Years — vary all 6 amounts."""
    print(f"\n{'='*60}")
    print(f"Amount calibration: {len(HOMEOWNERS_AMOUNTS)} amounts @ {location}")
    print(f"{'='*60}")

    all_rows = []
    for amount in HOMEOWNERS_AMOUNTS:
        print(f"  Amount: ${int(amount):>9,} ... ", end="", flush=True)
        rows = scraper.fetch_rates(location, "HOMEOWNERS", "3 Years", amount)
        if rows:
            avg = mean(r["annual_premium"] for r in rows)
            print(f"{len(rows)} companies | avg ${avg:,.0f}")
            all_rows.extend(rows)
        else:
            print("no data")
        time.sleep(scraper.delay)

    _write_csv(all_rows, out_path, append=True)
    return all_rows


def run_coverage_type_calibration(scraper, location, out_path):
    """Vary coverage type with appropriate defaults per type."""
    print(f"\n{'='*60}")
    print(f"Coverage type calibration @ {location}")
    print(f"{'='*60}")

    combos = [
        ("CONDOMINIUM",             None,       "75000"),
        ("RENTERS",                 None,       "30000"),
        ("MOBILEHOME",              "New",      "100000"),
        ("EARTHQUAKE - Single Family", None,    "375000"),
        ("EARTHQUAKE - Condominium",   None,    "75000"),
        ("EARTHQUAKE - Renters",       None,    "30000"),
    ]
    all_rows = []
    for ctype, age, amount in combos:
        label = f"{ctype} / {age or 'N/A'} / ${int(amount):,}"
        print(f"  {label:55s} ... ", end="", flush=True)
        rows = scraper.fetch_rates(location, ctype, age or "", amount)
        if rows:
            avg = mean(r["annual_premium"] for r in rows)
            print(f"{len(rows)} companies | avg ${avg:,.0f}")
            all_rows.extend(rows)
        else:
            print("no data")
        time.sleep(scraper.delay)

    _write_csv(all_rows, out_path, append=True)
    return all_rows


def _write_csv(rows, path, append=False):
    if not rows:
        return
    mode = "a" if append and os.path.exists(path) else "w"
    fieldnames = ["county","city","location","coverage_type","home_age",
                  "coverage_amount","deductible","company","annual_premium"]
    with open(path, mode, newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
            w.writeheader()
        w.writerows(rows)


def compute_county_factors(scraped_path, county_factors_path, base_rate, age_factor, ded_factor):
    """Back-calculate county factors from scraped market averages."""
    import pandas as pd
    from statistics import mean

    df = pd.read_csv(scraped_path)
    # Use only the county calibration profile
    df_ho = df[(df["coverage_type"] == "HOMEOWNERS") &
               (df["home_age"] == "3 Years") &
               (df["coverage_amount"].astype(str) == "375000")]

    location_avgs = (
        df_ho.groupby("location")["annual_premium"]
        .mean()
        .reset_index()
        .rename(columns={"annual_premium": "market_avg"})
    )
    location_avgs["county"] = location_avgs["location"].str.split().str[0]

    # county_factor = market_avg / (coverage_units * base_rate * age_factor * ded_factor)
    denominator = 375 * base_rate * age_factor * ded_factor
    location_avgs["implied_factor"] = location_avgs["market_avg"] / denominator

    # Average across cities within same county
    county_avgs = location_avgs.groupby("county")["implied_factor"].mean().reset_index()

    # Load existing county_factors.csv and update
    existing = pd.read_csv(county_factors_path)
    updated = existing.merge(county_avgs.rename(columns={"implied_factor": "new_factor"}),
                             on="county", how="left")
    updated["factor"] = updated["new_factor"].combine_first(updated["factor"]).round(4)
    updated.drop(columns=["new_factor"], inplace=True)
    updated.to_csv(county_factors_path, index=False)
    print(f"\nUpdated {len(county_avgs)} county factors in {county_factors_path}")
    return county_avgs


def compute_age_factors(scraped_path, age_factors_path, base_rate, ded_factor, county_factor):
    """Back-calculate age factors from scraped market averages."""
    import pandas as pd

    df = pd.read_csv(scraped_path)
    df_ages = df[(df["coverage_type"] == "HOMEOWNERS") &
                 (df["coverage_amount"].astype(str) == "375000")]

    age_avgs = df_ages.groupby("home_age")["annual_premium"].mean().reset_index()

    denominator_base = 375 * base_rate * ded_factor * county_factor
    age_avgs["implied_factor"] = (age_avgs["annual_premium"] / denominator_base).round(4)

    # Normalize so "New" = 1.000
    new_factor = age_avgs.loc[age_avgs["home_age"] == "New", "implied_factor"].values
    if len(new_factor):
        age_avgs["implied_factor"] = (age_avgs["implied_factor"] / new_factor[0]).round(4)

    existing = pd.read_csv(age_factors_path)
    updated = existing.merge(
        age_avgs[["home_age","implied_factor"]].rename(
            columns={"implied_factor":"new_factor","home_age":"age_of_home"}),
        on="age_of_home", how="left"
    )
    updated["factor"] = updated["new_factor"].combine_first(updated["factor"]).round(4)
    updated.drop(columns=["new_factor"], inplace=True)
    updated.to_csv(age_factors_path, index=False)
    print(f"Updated age factors: {age_avgs[['home_age','implied_factor']].values.tolist()}")
    return age_avgs


if __name__ == "__main__":
    OUT = os.path.join(DATA_DIR, "cdi_scraped_rates.csv")

    scraper = CDIScraper(delay=0.4)

    # --- Step 1: All locations (county calibration) ---
    # Get all locations from the form
    r = scraper.session.get(f"{BASE_URL}/f?p=111:21", timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    ALL_LOCATIONS = [
        o.get("value") for o in soup.find("select", id="P21_LOCATION").find_all("option")
        if o.get("value")
    ]
    county_rows = run_county_calibration(scraper, ALL_LOCATIONS, OUT)

    # --- Step 2: Age calibration ---
    age_rows = run_age_calibration(scraper, "ALAMEDA ALAMEDA", OUT)

    # --- Step 3: Amount calibration ---
    amount_rows = run_amount_calibration(scraper, "ALAMEDA ALAMEDA", OUT)

    # --- Step 4: Coverage type calibration ---
    type_rows = run_coverage_type_calibration(scraper, "ALAMEDA ALAMEDA", OUT)

    print(f"\nTotal rows collected: {sum(len(x) for x in [county_rows, age_rows, amount_rows, type_rows])}")

    # --- Step 5: Recalibrate factors ---
    BASE_RATE   = 2.86
    AGE_FACTOR  = 1.030   # 3 Years
    DED_FACTOR  = 0.915   # $1K deductible
    ALAMEDA_CF  = 1.05    # current county factor

    county_avgs = compute_county_factors(
        OUT,
        os.path.join(DATA_DIR, "data", "home_county_factors.csv"),
        BASE_RATE, AGE_FACTOR, DED_FACTOR
    )
    age_avgs = compute_age_factors(
        OUT,
        os.path.join(DATA_DIR, "age_factors.csv"),
        BASE_RATE, DED_FACTOR, ALAMEDA_CF
    )

    print("\nDone. Run home_insurance.py to verify.")
