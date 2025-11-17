"""
Location parsing / normalization helpers.

Usage:
    from location_cleaner import enrich_locations
    df = enrich_locations(df, loc_col="location", fill_unknown=True)

This module parses location strings like:
    "Remote, US", "New York, NY, US", "Seattle, WA, US", "US", None, ""
into structured columns:
    - location_city
    - location_state
    - location_country
    - is_remote (pandas nullable boolean)
    - location_missing (True when original raw value was missing or blank)
    - location_display (friendly label, never null)
"""
import pandas as pd
from typing import Dict, Optional
import re

# --- small country normalizer ---
COUNTRY_MAP = {
    "us": "US",
    "usa": "US",
    "united states": "US",
    "united states of america": "US",
    "uk": "UK",
    "gb": "UK",
    "united kingdom": "UK",
    "england": "UK",
    "canada": "CA",
    "ca": "CA",
    "australia": "AU",
    "au": "AU",
    "india": "IN",
    "in": "IN",
}

# Small state name -> abbrev map for US states
US_STATE_MAP = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    # DC, territories
    "district of columbia": "DC",
    "washington dc": "DC",
    "dc": "DC",
}

# reuse between runs
_state_abbrev_set = set(US_STATE_MAP.values())


def _normalize_country(token: str) -> Optional[str]:
    """
    Normalizes a country token to a 2-letter code where possible, using COUNTRY_MAP.
    Returns None if the token is empty after stripping.
    """
    if token is None:
        return None
    t = token.strip().lower()
    # drop punctuation
    t = re.sub(r"[^\w\s]", "", t).strip()
    if not t:
        return None
    if t in COUNTRY_MAP:
        return COUNTRY_MAP[t]
    # if token already uppercase two-letter, assume country code
    if len(t) == 2 and t.isalpha():
        return t.upper()
    # last resort: return uppercased original trimmed token
    return token.strip().upper() if token.strip() else None


def _normalize_state(token: str) -> Optional[str]:
    """
    Normalizes a US state token to a 2-letter abbreviation where possible.
    Returns None if not recognized.
    """
    if token is None:
        return None
    t = token.strip()
    if not t:
        return None
    # if already 2-letter abbrev
    up = t.upper()
    if up in _state_abbrev_set:
        return up
    # lowercase full name mapping
    key = t.lower()
    if key in US_STATE_MAP:
        return US_STATE_MAP[key]
    # some inputs like "WA" or "Wa" will be handled above; else None
    return None


def parse_location(cell: object) -> Dict[str, Optional[str]]:
    """
    Parse a location cell into components:
        { 'city': ..., 'state': ..., 'country': ..., 'is_remote': bool, 'raw': original_value }
    Conservative: does NOT guess city/state from single tokens, but will infer state/country when obvious.
    """
    out = {
        "city": None,
        "state": None,
        "country": None,
        "is_remote": False,
        "raw": (cell if pd.notna(cell) else None),
    }
    if pd.isna(cell):
        return out
    s = str(cell).strip()
    if s == "":
        return out

    # split on common separators (commas, slash, pipe)
    parts = [p.strip() for p in re.split(r"[,/|]+", s) if p and p.strip()]
    if not parts:
        return out

    # detect 'remote' anywhere -> mark and remove that token
    lowered = [p.lower() for p in parts]
    if any(p == "remote" or p.startswith("remote ") or p.endswith(" remote") for p in lowered):
        out["is_remote"] = True
        parts = [p for p in parts if p.lower() != "remote"]

    # after removing remote, handle remaining tokens
    # common formats:
    #  - "City, ST, Country"  => parts[-1] often country, parts[-2] state abbrev,
    #  - "City, ST, US" or "City, State Name, Country"
    #  - "US" or "UK"
    n = len(parts)

    # try last token as country
    last = parts[-1]
    country = _normalize_country(last)
    # If country recognized (either via mapping or 2-letter code), remove it from parts and set country
    if country is not None and (last.strip().lower() in COUNTRY_MAP or (len(last.strip()) == 2 and last.strip().isalpha()) or country != last.strip().upper()):
        out["country"] = country
        parts = parts[:-1]
        n -= 1
    else:
        # last token might still be a direct country token like "US" even if not in COUNTRY_MAP
        last_clean = last.strip().lower()
        if last_clean in COUNTRY_MAP or (len(last_clean) == 2 and last_clean.isalpha()):
            out["country"] = _normalize_country(last)
            parts = parts[:-1]
            n -= 1

    # try second-last as state (if exists) — accept 2-letter abbrev or full state name
    if n >= 1:
        candidate_state = parts[-1]  # after possibly popping country, the new last might be state
        state_abbrev = _normalize_state(candidate_state)
        if state_abbrev:
            out["state"] = state_abbrev
            parts = parts[:-1]
            n -= 1

    # if after that one or more tokens remain, treat the remainder as city (e.g., "New York")
    if n >= 1:
        out["city"] = ", ".join(parts).strip()

    # Special handling: if city token is actually a country (e.g., "US") and country wasn't set
    if out["city"] and out["city"].strip().upper() in {"US", "USA", "UNITED STATES", "UK", "UNITED KINGDOM"} and not out["country"]:
        out["country"] = _normalize_country(out["city"])

    return out


def enrich_locations(df: pd.DataFrame, loc_col: str = "location", fill_unknown: bool = True) -> pd.DataFrame:
    """
    Parse and enrich a DataFrame's location column into structured fields.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to mutate (in-place). Returns the same DataFrame for convenience.
    loc_col : str
        Name of the raw location column in df (default "location").
    fill_unknown : bool
        If True, replace nulls in location_city and location_state with the string 'Unknown'
        (this preserves a separate location_missing flag that reflects original missingness).

    Returns
    -------
    pd.DataFrame
        The same DataFrame with added/updated columns:
            - location_city, location_state, location_country
            - is_remote (pandas nullable boolean)
            - location_missing (True when the original raw location was missing or blank)
            - location_display (friendly display string, no nulls)
    """
    parsed = df[loc_col].apply(parse_location).apply(pd.Series)

    # preserve original missingness based on raw parsed value
    df["location_missing"] = parsed["raw"].isna() | (parsed["raw"].astype(str).str.strip() == "")

    # assign parsed columns (may contain None/NaN)
    df["location_city"] = parsed["city"]
    df["location_state"] = parsed["state"]
    df["location_country"] = parsed["country"]
    df["is_remote"] = parsed["is_remote"].astype("boolean")

    # If the user requested filling, replace nulls in city/state with 'Unknown' (but keep location_missing flag)
    if fill_unknown:
        df["location_city"] = df["location_city"].fillna("Unknown")
        df["location_state"] = df["location_state"].fillna("Unknown")

    # Build a user-friendly display column for dashboards (prefer "Remote" if is_remote)
    # Use filled city/state values where available so display contains no nulls.
    df["location_display"] = df["is_remote"].map({True: "Remote"})
    # For non-remote, join city and state only when they are not Unknown to avoid strings like "Unknown, Unknown"
    city_part = df["location_city"].where(df["location_city"].notna() & (df["location_city"] != "Unknown"), "")
    state_part = df["location_state"].where(df["location_state"].notna() & (df["location_state"] != "Unknown"), "")
    df["location_display"] = df["location_display"].fillna(
        (city_part.astype(str).replace("nan", "") + (", " + state_part.astype(str)).where(state_part != "", ""))
    )

    # fallback: if display is empty but country exists use country; otherwise use 'Unknown'
    df.loc[df["location_display"].str.strip() == "", "location_display"] = df["location_country"].fillna("Unknown")
    df["location_display"] = df["location_display"].replace("", "Unknown").fillna("Unknown")

    # Convert to categorical to save memory where appropriate
    df["location_country"] = df["location_country"].astype("category")
    df["location_state"] = df["location_state"].astype("category")
    df["location_city"] = df["location_city"].astype("category")
    df["location_display"] = df["location_display"].astype("category")

    return df


if __name__ == "__main__":
    # Quick self-test demonstrating behavior on typical inputs.
    example = pd.DataFrame(
        {
            "location": [
                "Remote, US",
                "New York, NY, US",
                "US",
                "Seattle, WA, US",
                "Washington, DC, US",
                None,
                "",
                "Desert Ridge, AZ, USA",
                "Ronks, PA, US",
                "Moore, SC, US",
            ]
        }
    )
    print("Before enrichment:")
    print(example)
    enrich_locations(example, "location", fill_unknown=True)
    print("\nAfter enrichment (showing parsed columns):")
    print(example[["location", "location_city", "location_state", "location_country", "is_remote", "location_missing", "location_display"]])