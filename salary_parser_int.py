"""
Salary parsing and integer-cleaning utilities.

Save this file as `salary_parser_int.py` in the same folder as your notebook
and import the main function:

from salary_parser_int import clean_salary_columns_int

The function will parse messy salary strings (ranges, "k"/"M" suffixes, currency
symbols, thousands separators, EU decimals, etc.), produce numeric min/max/mean
columns and (optionally) convert them to integer dtype suitable for export to
Tableau (prefer parquet export to preserve dtypes).

Example:
    df = clean_salary_columns_int(df,
                                min_col="min_amount",
                                max_col="max_amount",
                                mean_col="mean_salary",
                                overwrite=True,
                                fill_strategy="median",
                                enforce_int=True)
    # then export:
    df.to_parquet("./output/cleaned_all_jobs.parquet", index=False)
"""
from __future__ import annotations
import re
from typing import Tuple, Optional, List

import numpy as np
import pandas as pd


def _normalize_whitespace(s: str) -> str:
    if s is None:
        return ""
    return str(s).replace("\u00A0", " ").strip()


def _parse_single_number(s: str) -> Optional[float]:
    """
    Parse a single numeric expression to float. Returns np.nan on failure.
    Handles currency symbols, parentheses negative, k/m suffixes, and common formats.
    """
    if s is None:
        return np.nan
    s0 = _normalize_whitespace(str(s))
    if s0 == "" or s0.lower() in {"na", "n/a", "none", "-", "—", "[]"}:
        return np.nan

    # Remove verbose words and keep numeric-ish chars
    s1 = re.sub(r"(?i)\bper\s*year\b|\bper\s*annum\b|/yr\b|\byr\b|\bannually\b", "", s0)
    s1 = re.sub(r"[^\d\.,kKmM\-\+\(\)]", "", s1)  # keep digits, dots, commas, k/m, signs, parentheses

    # parentheses negative "(50000)" -> -50000
    if re.match(r'^\([\d\.,\skmKM]+\)$', s1):
        s1 = "-" + s1.strip().lstrip("(").rstrip(")")

    # suffix k/m (e.g. "50k", "1.2M")
    m = re.match(r"^([+-]?[\d\.,]+)\s*([kKmM])$", s1.strip())
    if m:
        num, suf = m.groups()
        num_clean = num.replace(",", "").replace(" ", "")
        try:
            val = float(num_clean)
            if suf.lower() == "k":
                return val * 1_000.0
            if suf.lower() == "m":
                return val * 1_000_000.0
        except Exception:
            return np.nan

    # Strategy A: remove spaces and thousands separators, keep dot as decimal
    a = s1.replace(" ", "").replace(",", "")
    a = re.sub(r"[^\d\.\-]", "", a)
    if a and re.search(r"\d", a):
        try:
            return float(a)
        except Exception:
            pass

    # Strategy B: European style "50.000,00" -> "50000.00"
    if "." in s1 and "," in s1 and s1.rfind(",") > s1.rfind("."):
        b = s1.replace(".", "").replace(",", ".")
        b = re.sub(r"[^\d\.\-]", "", b)
        try:
            return float(b)
        except Exception:
            pass

    # Strategy C: extract first numeric-looking token
    tokens = re.findall(r"[+-]?\d+[.,]?\d*[kKmM]?", s1)
    if tokens:
        t = tokens[0]
        # handle token decimal comma
        if "," in t and "." not in t:
            t = t.replace(",", ".")
        t = t.replace(",", "")
        try:
            return float(t)
        except Exception:
            return np.nan

    return np.nan


def parse_salary_field(cell: object) -> Tuple[float, float, float]:
    """
    Parse a salary-like cell and return (min_val, max_val, mean_val)
    Values are floats or np.nan.
    """
    if pd.isna(cell):
        return (np.nan, np.nan, np.nan)
    s = _normalize_whitespace(cell)
    if s == "":
        return (np.nan, np.nan, np.nan)

    # remove surrounding brackets/quotes often found in messy exports
    s_clean = re.sub(r"^[\[\(\{'\"]+|[\]\)\}'\"]+$", "", s).strip()

    # If contains explicit range separators (to, -, –)
    if re.search(r"\bto\b|[-–—]", s_clean):
        parts = re.split(r"\bto\b|[-–—]", s_clean)
        parts = [p.strip() for p in parts if p.strip()]
        nums = [_parse_single_number(p) for p in parts]
        nums = [float(x) for x in nums if pd.notna(x)]
        if len(nums) == 0:
            return (np.nan, np.nan, np.nan)
        if len(nums) == 1:
            return (nums[0], nums[0], nums[0])
        mn = min(nums)
        mx = max(nums)
        return (mn, mx, (mn + mx) / 2.0)

    # If multiple numeric tokens (e.g., list-like), take min/max
    tokens = re.findall(r"[+-]?\d+[.,]?\d*[kKmM]?", s_clean)
    if len(tokens) >= 2:
        nums = [_parse_single_number(t) for t in tokens]
        nums = [float(x) for x in nums if pd.notna(x)]
        if len(nums) >= 2:
            mn = min(nums)
            mx = max(nums)
            return (mn, mx, (mn + mx) / 2.0)

    # Otherwise parse single numeric token
    val = _parse_single_number(s_clean)
    if pd.isna(val):
        return (np.nan, np.nan, np.nan)
    return (val, val, val)


def clean_salary_columns_int(
    df: pd.DataFrame,
    min_col: str = "min_amount",
    max_col: str = "max_amount",
    mean_col: str = "mean_salary",
    overwrite: bool = True,
    fill_strategy: Optional[str] = "median",  # "median", "mean", or None
    enforce_int: bool = True,
    round_method: str = "round",  # "round" or "floor" or "ceil"
) -> pd.DataFrame:
    """
    Parse and clean salary columns and produce numeric columns (integers if enforce_int=True).

    Parameters
    ----------
    df : pd.DataFrame
    min_col, max_col, mean_col : source column names
    overwrite : if True, set output columns 'min_salary', 'max_salary', 'mean_salary'
                otherwise produce '*_salary_clean' columns
    fill_strategy : how to fill remaining NaNs: "median", "mean", or None
    enforce_int : if True, convert final columns to integer dtype (Int64) after filling
    round_method : how to convert floats to ints: "round" (nearest), "floor", "ceil"

    Returns
    -------
    pd.DataFrame (mutated and returned)
    """
    n = len(df)
    parsed_min = [np.nan] * n
    parsed_max = [np.nan] * n
    parsed_mean = [np.nan] * n

    def _fill_from_column(col_name: str):
        if col_name not in df.columns:
            return
        for i, raw in enumerate(df[col_name].astype(object)):
            mn, mx, me = parse_salary_field(raw)
            if pd.notna(mn) and pd.isna(parsed_min[i]):
                parsed_min[i] = mn
            if pd.notna(mx) and pd.isna(parsed_max[i]):
                parsed_max[i] = mx
            if pd.notna(me) and pd.isna(parsed_mean[i]):
                parsed_mean[i] = me

    # Try mean_col first (often clean), then min_col and max_col
    if mean_col in df.columns:
        _fill_from_column(mean_col)
    if min_col in df.columns:
        _fill_from_column(min_col)
    if max_col in df.columns:
        _fill_from_column(max_col)

    # Last-pass logic
    for i in range(n):
        if pd.isna(parsed_min[i]) and pd.notna(parsed_mean[i]):
            parsed_min[i] = parsed_mean[i]
        if pd.isna(parsed_max[i]) and pd.notna(parsed_mean[i]):
            parsed_max[i] = parsed_mean[i]
        if pd.isna(parsed_mean[i]) and pd.notna(parsed_min[i]) and pd.notna(parsed_max[i]):
            parsed_mean[i] = (parsed_min[i] + parsed_max[i]) / 2.0

    # Put temp numeric columns
    df["_min_salary_tmp"] = pd.to_numeric(pd.Series(parsed_min), errors="coerce")
    df["_max_salary_tmp"] = pd.to_numeric(pd.Series(parsed_max), errors="coerce")
    df["_mean_salary_tmp"] = pd.to_numeric(pd.Series(parsed_mean), errors="coerce")

    # Compute fill values if requested
    if fill_strategy == "median":
        med_min = df["_min_salary_tmp"].median(skipna=True)
        med_max = df["_max_salary_tmp"].median(skipna=True)
        mean_mean = df["_mean_salary_tmp"].mean(skipna=True)
        df["_min_salary_tmp"] = df["_min_salary_tmp"].fillna(med_min)
        df["_max_salary_tmp"] = df["_max_salary_tmp"].fillna(med_max)
        df["_mean_salary_tmp"] = df["_mean_salary_tmp"].fillna(mean_mean)
    elif fill_strategy == "mean":
        mean_min = df["_min_salary_tmp"].mean(skipna=True)
        mean_max = df["_max_salary_tmp"].mean(skipna=True)
        mean_mean = df["_mean_salary_tmp"].mean(skipna=True)
        df["_min_salary_tmp"] = df["_min_salary_tmp"].fillna(mean_min)
        df["_max_salary_tmp"] = df["_max_salary_tmp"].fillna(mean_max)
        df["_mean_salary_tmp"] = df["_mean_salary_tmp"].fillna(mean_mean)
    # else: leave NaNs (user can decide)

    # Convert to integer if requested (must have no NaNs for plain int; use pandas Int64 extension)
    def _to_int_series(s: pd.Series) -> pd.Series:
        # choice of rounding
        if round_method == "floor":
            s_int = np.floor(s)
        elif round_method == "ceil":
            s_int = np.ceil(s)
        else:  # default "round"
            s_int = s.round(0)
        # convert to pandas nullable integer (keeps NA if present)
        return s_int.astype("Int64")

    if overwrite:
        if enforce_int:
            df["min_salary"] = _to_int_series(df["_min_salary_tmp"])
            df["max_salary"] = _to_int_series(df["_max_salary_tmp"])
            df["mean_salary"] = _to_int_series(df["_mean_salary_tmp"])
        else:
            df["min_salary"] = df["_min_salary_tmp"].astype(float)
            df["max_salary"] = df["_max_salary_tmp"].astype(float)
            df["mean_salary"] = df["_mean_salary_tmp"].astype(float)
    else:
        if enforce_int:
            df["min_salary_clean"] = _to_int_series(df["_min_salary_tmp"])
            df["max_salary_clean"] = _to_int_series(df["_max_salary_tmp"])
            df["mean_salary_clean"] = _to_int_series(df["_mean_salary_tmp"])
        else:
            df["min_salary_clean"] = df["_min_salary_tmp"].astype(float)
            df["max_salary_clean"] = df["_max_salary_tmp"].astype(float)
            df["mean_salary_clean"] = df["_mean_salary_tmp"].astype(float)

    # Drop temp columns
    df.drop(columns=[c for c in ["_min_salary_tmp", "_max_salary_tmp", "_mean_salary_tmp"] if c in df.columns], inplace=True)

    return df


def export_for_tableau(df: pd.DataFrame, path: str, fmt: str = "parquet"):
    """
    Convenience export helper. fmt in {"parquet", "csv"}.
    Parquet is preferred (preserves dtypes). CSV will write integer values as numerics
    but watch locale/decimal separators in Tableau.
    """
    path = str(path)
    if fmt == "parquet":
        df.to_parquet(path, index=False)
    elif fmt == "csv":
        # ensure numerical columns are written without thousands separators
        df.to_csv(path, index=False, float_format="%.0f")
    else:
        raise ValueError("fmt must be 'parquet' or 'csv'")


if __name__ == "__main__":
    # Quick self-test
    sample = pd.DataFrame(
        {
            "min_amount": [
                "$50,000",
                "40k-60k",
                "50000",
                "€50.000,00",
                None,
                "NA",
                "(60,000)",
                "50000/year",
                "['40000','50000']",
                "100k",
            ],
            "max_amount": [
                "$80,000",
                "70k",
                "50000",
                "€60.000,00",
                "40,000",
                None,
                None,
                "60000",
                "['60000','70000']",
                "150k",
            ],
            "mean_salary": [
                None,
                None,
                "50000",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        }
    )
    print("Before cleaning:")
    print(sample)
    sample = clean_salary_columns_int(
        sample,
        min_col="min_amount",
        max_col="max_amount",
        mean_col="mean_salary",
        overwrite=True,
        fill_strategy="median",
        enforce_int=True,
        round_method="round",
    )
    print("\nAfter cleaning:")
    print(sample[["min_amount", "min_salary", "max_amount", "max_salary", "mean_salary"]])