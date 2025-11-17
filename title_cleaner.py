import re
from typing import List, Optional

import pandas as pd


# Patterns for detecting data roles (mapped to "Data Scientist").
DATA_PATTERNS: List[str] = [
    r"\bdata scientist\b",
    r"\bdata science\b",
    r"\bmachine learning\b",
    r"\bml engineer\b",
    r"\bml\b",
    r"\bdeep learning\b",
    r"\bcomputer vision\b",
    r"\bnlp\b",
    r"\bnatural language\b",
    r"\bpytorch\b",
    r"\btensorflow\b",
    r"\bscikit\b",
    r"\bpyspark\b",
    r"\bspark\b",
    r"\bdata engineer\b",
    r"\bdata analyst\b",
    r"\bstatistician\b",
    r"\bresearch scientist\b",
]

# Patterns for detecting software roles (mapped to "Software Engineer").
# Keep these more generic and after DATA_PATTERNS to avoid mapping "data engineer" -> Software Engineer.
SOFTWARE_PATTERNS: List[str] = [
    r"\bsoftware engineer\b",
    r"\bsoftware developer\b",
    r"\bdevops\b",
    r"\bsre\b",
    r"\bsite reliability\b",
    r"\bbackend\b",
    r"\bfront[- ]?end\b",
    r"\bfull[- ]?stack\b",
    r"\bmobile engineer\b",
    r"\bplatform engineer\b",
    r"\bapplication engineer\b",
    r"\bengineer\b",
    r"\bprogrammer\b",
    r"\bdeveloper\b",
    r"\bbackend\b",
    r"\bfrontend\b",
]


def _compile_patterns(patterns: List[str]):
    return [re.compile(p, flags=re.I) for p in patterns]


_DATA_RE = _compile_patterns(DATA_PATTERNS)
_SOFTWARE_RE = _compile_patterns(SOFTWARE_PATTERNS)


def classify_title(
    title: Optional[str],
    data_res: List[re.Pattern] = _DATA_RE,
    sw_res: List[re.Pattern] = _SOFTWARE_RE,
    unknown_label: str = "Unknown",
    other_label: str = "Other",
    coerce_other_to_sw: bool = True,
) -> str:
    """
    Classify a single job title into "Data Scientist" or "Software Engineer".
    - Titles matching data_res patterns -> "Data Scientist"
    - Else titles matching sw_res patterns -> "Software Engineer"
    - Else -> other_label (or coerced to "Software Engineer" if coerce_other_to_sw True)
    """
    if pd.isna(title) or title is None:
        return unknown_label

    t = str(title).strip()
    if t == "":
        return unknown_label

    # Check data-related patterns first
    for r in data_res:
        if r.search(t):
            return "Data Scientist"

    # Then software patterns
    for r in sw_res:
        if r.search(t):
            return "Software Engineer"

    # If nothing matched:
    if coerce_other_to_sw:
        return "Software Engineer"
    return other_label


def clean_job_titles(
    df: pd.DataFrame,
    title_col: str = "title",
    out_col: str = "title_clean",
    coerce_other_to_sw: bool = True,
    keep_top_n_other: Optional[int] = None,
) -> pd.DataFrame:
    """
    Add a column with cleaned job titles narrowed to "Software Engineer" and "Data Scientist".

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing job title column.
    title_col : str
        Name of the raw title column in df.
    out_col : str
        Name of the output cleaned title column (created or overwritten).
    coerce_other_to_sw : bool
        If True, any title not matched by data/software patterns will be labeled "Software Engineer".
        If False, unmatched titles will be labeled "Other".
    keep_top_n_other : Optional[int]
        If not None and coerce_other_to_sw is False, keep the top N most frequent unmatched raw titles as-is
        (useful if you want to preserve some common non-dev/data titles). Remaining unmatched are labeled "Other".
    """
    # If keeping top N other raw titles, compute them first
    top_other_set = set()
    if keep_top_n_other and not coerce_other_to_sw:
        top = (
            df[title_col]
            .astype(str)
            .str.strip()
            .replace({"nan": None})
            .value_counts(dropna=True)
            .head(keep_top_n_other)
        )
        top_other_set = set([str(x) for x in top.index if x and x.strip()])

    def _map_title(x):
        raw = x if pd.notna(x) else None
        # preserve explicit top-N other titles if requested
        if keep_top_n_other and not coerce_other_to_sw and raw and str(raw).strip() in top_other_set:
            return str(raw).strip()
        return classify_title(
            raw,
            unknown_label="Unknown",
            other_label="Other",
            coerce_other_to_sw=coerce_other_to_sw,
        )

    df[out_col] = df[title_col].apply(_map_title)
    df[out_col] = df[out_col].astype("category")
    return df