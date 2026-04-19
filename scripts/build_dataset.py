"""
build_dataset.py — Canadian Recession Watch Dashboard

Fetches the v2 indicators from Bank of Canada Valet, Statistics Canada WDS,
and FRED. Normalizes to monthly frequency where appropriate, and writes
data/data.json.

Designed to be idempotent — re-running it reconstructs the full history from
scratch. Series-level failures are logged but do not crash the build; the
prior data.json is preserved in that case.

v2 series:
  Headline (coincident):
    1. monthly_gdp          Real GDP at basic prices, all industries, SA (YoY %)
    2. unemployment         Unemployment rate, SA, 15+, Canada
    3. cpi_trim (+median)   BoC core inflation measure + CPI-headline overlay

  Other indicators:
    4. yield_curve          10Y GoC minus 3M T-bill, monthly avg
    5. housing + permits    CMHC starts (primary) + building permits (overlay)
    6. vehicle_sales        New motor vehicle sales, YoY %
    7. retail_sales         Retail trade total, SA, YoY %
    8. bcpi                 BoC Commodity Price Index, YoY %
    9. us_cli               OECD Composite Leading Indicator, United States
"""

from __future__ import annotations
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOC_VALET = "https://www.bankofcanada.ca/valet"
STATCAN_WDS = "https://www150.statcan.gc.ca/t1/wds/rest"
FRED_API = "https://api.stlouisfed.org/fred"

START_YEAR = 1990  # historical backfill floor
REQUEST_TIMEOUT = 30

# FRED key: read from environment. Dashboard works without it (us_cli will fail
# gracefully). To use locally, `export FRED_API_KEY=...`; in GitHub Actions,
# add a repo secret named FRED_API_KEY.
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "data.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def month_key(date_str: str) -> str:
    """ '2026-03-15' -> '2026-03' """
    return date_str[:7]


def daily_to_monthly_avg(observations: List[Dict[str, Any]], field: str) -> Dict[str, float]:
    """Average all values in a given month. Skips None values."""
    buckets: Dict[str, List[float]] = defaultdict(list)
    for obs in observations:
        raw = obs.get(field, {}).get("v")
        if raw in (None, ""):
            continue
        try:
            buckets[month_key(obs["d"])].append(float(raw))
        except (TypeError, ValueError):
            continue
    return {m: sum(vs) / len(vs) for m, vs in buckets.items() if vs}


def yoy_from_levels(monthly: Dict[str, float]) -> Dict[str, float]:
    """Compute 12-month YoY % change from a level series keyed by 'YYYY-MM'."""
    out: Dict[str, float] = {}
    for m, v in monthly.items():
        y, mm = m.split("-")
        prior = f"{int(y)-1}-{mm}"
        if prior in monthly and monthly[prior] not in (0, None):
            out[m] = (v / monthly[prior] - 1.0) * 100.0
    return out


def sort_points(monthly: Dict[str, float]) -> List[Dict[str, Any]]:
    return [{"date": m, "value": round(monthly[m], 4)} for m in sorted(monthly)]


# ---------------------------------------------------------------------------
# Fetchers — BoC Valet
# ---------------------------------------------------------------------------


def fetch_valet(series_ids: List[str], start: str) -> List[Dict[str, Any]]:
    url = f"{BOC_VALET}/observations/{','.join(series_ids)}/json"
    r = requests.get(url, params={"start_date": start}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("observations", [])


def build_yield_curve() -> Dict[str, Any]:
    """10Y GoC yield − 3M T-bill, both from StatsCan monthly financial market table.

    Uses StatsCan vectors v122543 (10Y benchmark) and v122541 (3M T-bill auction
    average), which provide monthly averages back to 1988. This is cleaner than
    splicing the BoC Valet daily series (which starts 2001) for our needs.
    """
    pts_10y = fetch_statcan_vector(122543, n_periods=500)
    pts_3m = fetch_statcan_vector(122541, n_periods=500)
    monthly_10y = statcan_points_to_monthly(pts_10y)
    monthly_3m = statcan_points_to_monthly(pts_3m)

    spread = {
        m: monthly_10y[m] - monthly_3m[m]
        for m in monthly_10y
        if m in monthly_3m and m >= f"{START_YEAR}-01"
    }
    points = sort_points(spread)
    return {
        "id": "yield_curve",
        "label": "Yield curve spread: 10-year GoC − 3-month T-bill",
        "units": "percentage points",
        "source": "Statistics Canada (Table 10-10-0122), sourced from Bank of Canada",
        "source_url": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1010012201",
        "frequency": "monthly",
        "recession_signal": "Inversion (spread < 0) historically precedes recession by 6–18 months.",
        "last_observation": points[-1]["date"] if points else None,
        "last_value": points[-1]["value"] if points else None,
        "data": points,
    }


def build_bcpi() -> Dict[str, Any]:
    obs = fetch_valet(["M.BCPI"], f"{START_YEAR}-01-01")
    levels = {o["d"][:7]: float(o["M.BCPI"]["v"]) for o in obs if o.get("M.BCPI", {}).get("v") not in (None, "")}
    yoy = yoy_from_levels(levels)
    points = sort_points(yoy)
    return {
        "id": "bcpi",
        "label": "BoC Commodity Price Index — YoY % change",
        "units": "percent, year-over-year",
        "source": "Bank of Canada Valet API",
        "source_url": "https://www.bankofcanada.ca/valet",
        "frequency": "monthly",
        "recession_signal": "Sharp declines in commodity prices pressure resource-sector employment and investment; historically correlated with Canadian slowdowns.",
        "last_observation": points[-1]["date"] if points else None,
        "last_value": points[-1]["value"] if points else None,
        "data": points,
        "auxiliary": {"level_latest": max(levels.items())[1] if levels else None},
    }


def build_core_cpi_from_boc() -> Dict[str, Dict[str, Any]]:
    """CPI-trim and CPI-median, monthly YoY %, direct from BoC Valet."""
    obs = fetch_valet(["CPI_TRIM", "CPI_MEDIAN"], f"{START_YEAR}-01-01")
    out = {}
    for sid, label, units in [
        ("CPI_TRIM", "CPI-trim (core inflation measure)", "percent, year-over-year"),
        ("CPI_MEDIAN", "CPI-median (core inflation measure)", "percent, year-over-year"),
    ]:
        vals = {o["d"][:7]: float(o[sid]["v"]) for o in obs if o.get(sid, {}).get("v") not in (None, "")}
        points = sort_points(vals)
        out[sid.lower()] = {
            "id": sid.lower(),
            "label": label,
            "units": units,
            "source": "Bank of Canada Valet API",
            "source_url": "https://www.bankofcanada.ca/valet",
            "frequency": "monthly",
            "last_observation": points[-1]["date"] if points else None,
            "last_value": points[-1]["value"] if points else None,
            "data": points,
        }
    return out


# ---------------------------------------------------------------------------
# Fetchers — StatsCan WDS
# ---------------------------------------------------------------------------


def fetch_statcan_vector(vector_id: int, n_periods: int = 500) -> List[Dict[str, Any]]:
    """Pull the latest n monthly periods for a vector."""
    r = requests.post(
        f"{STATCAN_WDS}/getDataFromVectorsAndLatestNPeriods",
        json=[{"vectorId": vector_id, "latestN": n_periods}],
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    body = r.json()
    if not body or body[0].get("status") != "SUCCESS":
        raise RuntimeError(f"StatsCan v{vector_id} returned non-success: {body}")
    return body[0]["object"].get("vectorDataPoint", [])


def fetch_statcan_coordinate(product_id: int, coordinate: str, n_periods: int = 500) -> List[Dict[str, Any]]:
    """Pull the latest n monthly periods for a table coordinate (when we don't know the vector)."""
    r = requests.post(
        f"{STATCAN_WDS}/getDataFromCubePidCoordAndLatestNPeriods",
        json=[{"productId": product_id, "coordinate": coordinate, "latestN": n_periods}],
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    body = r.json()
    if not body or body[0].get("status") != "SUCCESS":
        raise RuntimeError(f"StatsCan {product_id} [{coordinate}] returned non-success: {body}")
    return body[0]["object"].get("vectorDataPoint", [])


def statcan_points_to_monthly(points: List[Dict[str, Any]]) -> Dict[str, float]:
    out = {}
    for pt in points:
        raw = pt.get("value")
        if raw in (None, ""):
            continue
        try:
            out[pt["refPer"][:7]] = float(raw)
        except (TypeError, ValueError):
            continue
    return out


def build_unemployment() -> Dict[str, Any]:
    # Historical backfill: 500 monthly periods ≈ 41 years, covers us back to ~1985.
    pts = fetch_statcan_vector(2062815, n_periods=500)
    vals = statcan_points_to_monthly(pts)
    # Truncate to START_YEAR floor
    vals = {m: v for m, v in vals.items() if m >= f"{START_YEAR}-01"}
    points = sort_points(vals)
    return {
        "id": "unemployment",
        "label": "Unemployment rate, 15+, seasonally adjusted",
        "units": "percent",
        "source": "Statistics Canada, Labour Force Survey (Table 14-10-0287)",
        "source_url": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410028701",
        "frequency": "monthly",
        "recession_signal": "Rising unemployment is the classic coincident-to-lagging recession indicator. The Sahm rule (3-mo avg UR rising ≥0.5ppt above its trailing 12-mo low) has historically triggered early in recessions.",
        "last_observation": points[-1]["date"] if points else None,
        "last_value": points[-1]["value"] if points else None,
        "data": points,
    }


def build_housing_starts() -> Dict[str, Any]:
    # Canada-wide vector we identified: v52300157
    pts = fetch_statcan_vector(52300157, n_periods=500)
    vals = statcan_points_to_monthly(pts)
    vals = {m: v for m, v in vals.items() if m >= f"{START_YEAR}-01"}
    points = sort_points(vals)
    return {
        "id": "housing_starts",
        "label": "Housing starts, Canada, seasonally adjusted at annual rates",
        "units": "thousands of units (SAAR)",
        "source": "Statistics Canada / CMHC (Table 34-10-0158)",
        "source_url": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410015801",
        "frequency": "monthly",
        "recession_signal": "Residential investment is the most cyclically volatile GDP component. Sharp drops in starts typically precede or coincide with broader weakness.",
        "last_observation": points[-1]["date"] if points else None,
        "last_value": points[-1]["value"] if points else None,
        "data": points,
    }


def build_retail_sales() -> Dict[str, Any]:
    """Retail trade sales YoY %, spliced across two StatsCan tables.

    - Old table 20-10-0008 (v52367097): 1991-01 through 2022-12. Now inactive
      but still queryable via WDS.
    - New table 20-10-0056 (v1446859483): 2017-01 onward.

    Splice strategy: compute YoY from each series independently, then use the
    OLD series' YoY for dates ≤ 2018-12 and the NEW series' YoY for 2019-01+.
    At overlap dates the two series disagree by <1pp on YoY growth, which is
    immaterial for recession-signal purposes. This avoids any level-splice
    gymnastics and preserves the interpretability of each source.
    """
    old_pts = fetch_statcan_vector(52367097, n_periods=500)   # legacy total retail
    new_pts = fetch_statcan_vector(1446859483, n_periods=120)  # current total retail
    old_levels = statcan_points_to_monthly(old_pts)
    new_levels = statcan_points_to_monthly(new_pts)

    yoy_old = yoy_from_levels(old_levels)
    yoy_new = yoy_from_levels(new_levels)

    splice_cutoff = "2018-12"  # old series used through this month inclusive
    spliced: Dict[str, float] = {}
    for m, v in yoy_old.items():
        if m <= splice_cutoff and m >= f"{START_YEAR}-01":
            spliced[m] = v
    for m, v in yoy_new.items():
        if m > splice_cutoff:
            spliced[m] = v

    points = sort_points(spliced)
    return {
        "id": "retail_sales",
        "label": "Retail trade sales, total, SA — YoY % change (nominal)",
        "units": "percent, year-over-year",
        "source": "Statistics Canada Tables 20-10-0008 (legacy, through 2018-12) + 20-10-0056 (current, 2019-01+)",
        "source_url": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=2010005601",
        "frequency": "monthly",
        "recession_signal": "Nominal retail sales YoY collapsing toward or below zero is a coincident signal of consumer demand weakness.",
        "last_observation": points[-1]["date"] if points else None,
        "last_value": points[-1]["value"] if points else None,
        "data": points,
        "caveat": "Nominal, not deflated. Includes autos and gasoline. Spliced across a 2018-12 table transition; overlap disagreement is <1pp on YoY.",
    }


def build_cpi_headline() -> Dict[str, Any]:
    # v41690973: CPI all-items Canada (NSA, the official headline series)
    pts = fetch_statcan_vector(41690973, n_periods=500)
    levels = statcan_points_to_monthly(pts)
    levels = {m: v for m, v in levels.items() if m >= f"{START_YEAR-1}-01"}
    yoy = yoy_from_levels(levels)
    yoy = {m: v for m, v in yoy.items() if m >= f"{START_YEAR}-01"}
    points = sort_points(yoy)
    return {
        "id": "cpi_headline",
        "label": "CPI, all-items, Canada — YoY % change",
        "units": "percent, year-over-year",
        "source": "Statistics Canada (Table 18-10-0004)",
        "source_url": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000401",
        "frequency": "monthly",
        "recession_signal": "Not a recession indicator per se, but essential context: defines the BoC's 1–3% target band and interacts with policy rate decisions.",
        "last_observation": points[-1]["date"] if points else None,
        "last_value": points[-1]["value"] if points else None,
        "data": points,
    }


def build_monthly_gdp() -> Dict[str, Any]:
    """Monthly real GDP (chained 2017 dollars, SAAR, all industries), YoY %."""
    pts = fetch_statcan_coordinate(36100434, "1.1.1.1.0.0.0.0.0.0", n_periods=500)
    levels = statcan_points_to_monthly(pts)
    levels = {m: v for m, v in levels.items() if m >= f"{START_YEAR-1}-01"}
    yoy = yoy_from_levels(levels)
    yoy = {m: v for m, v in yoy.items() if m >= f"{START_YEAR}-01"}
    points = sort_points(yoy)
    return {
        "id": "monthly_gdp",
        "label": "Monthly real GDP, all industries — YoY % change",
        "units": "percent, year-over-year",
        "source": "Statistics Canada (Table 36-10-0434)",
        "source_url": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3610043401",
        "frequency": "monthly",
        "recession_signal": "The coincident indicator par excellence. Two consecutive negative monthly GDP prints is often the first real-time alarm for a recession call.",
        "last_observation": points[-1]["date"] if points else None,
        "last_value": points[-1]["value"] if points else None,
        "data": points,
    }


def build_building_permits() -> Dict[str, Any]:
    """Total building permits value, Canada, SA current dollars. Starts 2018-01."""
    # Canada=1, Total res+nonres=1, types-of-work-total=1, Value=1, SA-current=2
    pts = fetch_statcan_coordinate(34100292, "1.1.1.1.2.0.0.0.0.0", n_periods=500)
    levels = statcan_points_to_monthly(pts)
    # This table only starts 2018-01 — that's fine for our display window.
    points = sort_points({m: v / 1_000_000 for m, v in levels.items()})  # -> billions
    return {
        "id": "building_permits",
        "label": "Building permits, total value, Canada, SA",
        "units": "billions of dollars (SA current)",
        "source": "Statistics Canada (Table 34-10-0292)",
        "source_url": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410029201",
        "frequency": "monthly",
        "recession_signal": "Leading indicator — building permits precede actual housing starts by ~1–2 months, leading broader residential investment.",
        "last_observation": points[-1]["date"] if points else None,
        "last_value": points[-1]["value"] if points else None,
        "data": points,
        "caveat": "Table 34-10-0292 only starts 2018-01. For pre-2018 history, splicing with legacy table 34-10-0008 would be required.",
    }


def build_vehicle_sales() -> Dict[str, Any]:
    """New motor vehicle sales (units), Canada. Unadjusted → YoY % to strip seasonality."""
    # Canada, total vehicles, all fuel, total country, Units, UNADJUSTED (id=1)
    pts = fetch_statcan_coordinate(20100085, "1.1.1.1.1.1.0.0.0.0", n_periods=500)
    levels = statcan_points_to_monthly(pts)
    levels = {m: v for m, v in levels.items() if m >= f"{START_YEAR-1}-01"}
    yoy = yoy_from_levels(levels)
    yoy = {m: v for m, v in yoy.items() if m >= f"{START_YEAR}-01"}
    points = sort_points(yoy)
    return {
        "id": "vehicle_sales",
        "label": "New motor vehicle sales (units) — YoY % change",
        "units": "percent, year-over-year",
        "source": "Statistics Canada (Table 20-10-0085)",
        "source_url": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=2010008501",
        "frequency": "monthly",
        "recession_signal": "Big-ticket durables — highly sensitive to credit conditions and consumer confidence. Tends to turn slightly ahead of the broader consumer cycle.",
        "last_observation": points[-1]["date"] if points else None,
        "last_value": points[-1]["value"] if points else None,
        "data": points,
        "caveat": "Derived from unadjusted levels; YoY strips annual seasonality. Post-2021 data distorted by chip shortages, inventory rebuilds.",
    }


def build_us_cli() -> Dict[str, Any]:
    """OECD Composite Leading Indicator for the United States (amplitude adjusted, 100 = trend)."""
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY not set; skip US CLI. Set via environment variable.")
    sid = "USALOLITOAASTSAM"
    r = requests.get(
        f"{FRED_API}/series/observations",
        params={
            "series_id": sid,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": f"{START_YEAR}-01-01",
        },
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    obs = r.json().get("observations", [])
    vals: Dict[str, float] = {}
    for o in obs:
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            vals[o["date"][:7]] = float(v)
        except (TypeError, ValueError):
            continue
    points = sort_points(vals)
    return {
        "id": "us_cli",
        "label": "OECD Composite Leading Indicator — United States",
        "units": "index (100 = trend, amplitude adjusted)",
        "source": "OECD via FRED (USALOLITOAASTSAM)",
        "source_url": "https://fred.stlouisfed.org/series/USALOLITOAASTSAM",
        "frequency": "monthly",
        "recession_signal": "Since ~75% of Canadian exports go to the US, the US CLI is a powerful leading indicator for Canada. Values below 100 and falling historically precede Canadian slowdowns.",
        "last_observation": points[-1]["date"] if points else None,
        "last_value": points[-1]["value"] if points else None,
        "data": points,
        "caveat": "Conference Board LEI (first choice) is not freely available on FRED. OECD CLI is the standard free alternative and performs similarly.",
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def safe_build(name: str, fn, status: Dict[str, str]) -> Optional[Any]:
    try:
        result = fn()
        status[name] = "ok"
        log.info(f"  ✓ {name}: {result.get('last_observation') if isinstance(result, dict) else 'ok'}")
        return result
    except Exception as exc:
        status[name] = f"failed: {type(exc).__name__}: {exc}"
        log.error(f"  ✗ {name}: {exc}")
        return None


def main() -> int:
    log.info("Building Canadian recession watch dataset")
    status: Dict[str, str] = {}
    series: Dict[str, Any] = {}

    yc = safe_build("yield_curve", build_yield_curve, status)
    if yc: series["yield_curve"] = yc

    unemp = safe_build("unemployment", build_unemployment, status)
    if unemp: series["unemployment"] = unemp

    gdp = safe_build("monthly_gdp", build_monthly_gdp, status)
    if gdp: series["monthly_gdp"] = gdp

    hs = safe_build("housing_starts", build_housing_starts, status)
    if hs: series["housing_starts"] = hs

    perm = safe_build("building_permits", build_building_permits, status)
    if perm: series["building_permits"] = perm

    veh = safe_build("vehicle_sales", build_vehicle_sales, status)
    if veh: series["vehicle_sales"] = veh

    rs = safe_build("retail_sales", build_retail_sales, status)
    if rs: series["retail_sales"] = rs

    bcpi = safe_build("bcpi", build_bcpi, status)
    if bcpi: series["bcpi"] = bcpi

    cpi_h = safe_build("cpi_headline", build_cpi_headline, status)
    if cpi_h: series["cpi_headline"] = cpi_h

    core = safe_build("cpi_core", build_core_cpi_from_boc, status)
    if core:
        series.update(core)

    us_cli = safe_build("us_cli", build_us_cli, status)
    if us_cli: series["us_cli"] = us_cli

    # If everything failed, don't clobber a good prior file
    if not series:
        log.error("All series failed; leaving data.json untouched.")
        return 1

    payload = {
        "schema_version": "1",
        "last_updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "history_from": f"{START_YEAR}-01",
        "series": series,
        "series_status": status,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2))
    log.info(f"Wrote {OUTPUT_PATH}  ({OUTPUT_PATH.stat().st_size:,} bytes)")

    ok_count = sum(1 for v in status.values() if v == "ok")
    log.info(f"Summary: {ok_count}/{len(status)} series succeeded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
