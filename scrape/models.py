"""Anthropic model tier comparison for Soogle LLM reviews.

Used to decide whether a record was reviewed by a "lower" model and
should be re-reviewed when upgrading to a better one.

Tier ordering (lowest to highest):
    haiku < sonnet < opus

Usage:
    from scrape.models import is_upgrade

    is_upgrade("claude-haiku-4-5-20251001", "claude-opus-4-6")  # True
    is_upgrade("claude-opus-4-6", "claude-haiku-4-5-20251001")  # False
    is_upgrade("claude-opus-4-6", "claude-opus-4-6")            # False
    is_upgrade(None, "claude-haiku-4-5-20251001")               # True  (unreviewed)
"""

import re

# Family keywords mapped to tier number.  Higher = more capable.
_TIERS = {
    "haiku": 1,
    "sonnet": 2,
    "opus": 3,
}

_FAMILY_RE = re.compile(r"(haiku|sonnet|opus)", re.IGNORECASE)


def model_tier(model_id):
    """Return an integer tier for a model id string.

    Strips `:error` suffixes and extracts the family keyword.
    Returns 0 for unrecognised models so they sort below everything.
    """
    if not model_id:
        return 0
    # Strip error/status suffixes like "claude-haiku-4-5-20251001:error"
    clean = model_id.split(":")[0]
    m = _FAMILY_RE.search(clean)
    if m:
        return _TIERS[m.group(1).lower()]
    return 0


def is_upgrade(reviewed_by, target_model):
    """Return True if *target_model* is a higher tier than *reviewed_by*.

    Also returns True when reviewed_by is None/empty (unreviewed).
    """
    return model_tier(target_model) > model_tier(reviewed_by)
