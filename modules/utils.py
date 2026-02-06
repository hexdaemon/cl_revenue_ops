"""
Small shared helpers used across cl-revenue-ops modules.

Keep this module dependency-light to avoid import cycles.
"""

from typing import Any, Optional


def normalize_scid(scid: Optional[str]) -> str:
    """
    Normalize a short_channel_id to the plugin's internal 'x' separator format.

    CLN APIs may return SCIDs with either 'x' or ':' separators depending on context/version.
    """
    return (scid or "").replace(":", "x")


def parse_msat(msat_val: Any) -> int:
    """
    Safely convert msat values to an integer number of millisatoshis.

    Handles:
    - ints/floats
    - strings like '1000msat' or '1000'
    - pyln Millisatoshi-like objects (has .millisatoshis)
    """
    if msat_val is None:
        return 0
    if hasattr(msat_val, "millisatoshis"):
        try:
            return int(msat_val.millisatoshis)
        except Exception:
            return 0
    if isinstance(msat_val, bool):
        return int(msat_val)
    if isinstance(msat_val, (int, float)):
        try:
            return int(msat_val)
        except Exception:
            return 0
    if isinstance(msat_val, str):
        s = msat_val.strip()
        if s.endswith("msat"):
            s = s[:-4]
        try:
            return int(s)
        except Exception:
            return 0
    try:
        return int(msat_val)
    except Exception:
        return 0

