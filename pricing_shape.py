"""
Price-plan pricing "shape" — the basis for de-duplication
=========================================================
A NetSuite Price Plan record carries NO item or line reference (confirmed by GET
on a live plan: its only fields are currency, pricePlanType, minimumAmount,
maximumAmount, priceTiers). The item↔plan link lives on the subscription line's
price interval, not on the plan. So two plans whose pricing-defining fields are
identical are the SAME plan as far as NS is concerned — the LINE_ITEM_ID and item
slug in the externalId (`MP_PP_<LINE_ITEM_ID>_<slug>`) are a naming convention,
not a requirement, and produce heavy duplication (the full price book: 20k rows,
~3k distinct shapes).

This module defines the canonical "shape" of a plan — the fields that actually
determine the price — and a stable, item-free externalId derived from it. Shared
by analyze_priceplan_dedup.py (report) and dedup_price_plans.py (rewrite) so the
two never drift. The eventual proper fix moves this same hashing into the pricing
+ subs DDLs; until then we apply it here as a CSV transform.
"""

import json
import hashlib

# The fields that DEFINE a price (everything on the NS pricePlan record except
# the externalId itself). Two plans equal on all of these are interchangeable.
SHAPE_FIELDS = (
    "currency",
    "pricePlanType",
    "minimumAmount",
    "maximumAmount",
    "priceTiers",
)


def shape_of(plan: dict) -> str:
    """Canonical JSON string of a plan's pricing-defining fields.

    ``sort_keys`` makes the string order-independent so logically equal plans
    hash identically regardless of key order in the source JSON. The externalId
    is deliberately excluded — it's the thing we're trying to collapse.
    """
    return json.dumps({k: plan.get(k) for k in SHAPE_FIELDS}, sort_keys=True)


def shape_hash(plan: dict) -> str:
    """Short, stable hash of the pricing shape (12 hex chars ≈ 48 bits).

    Plenty of headroom for the few thousand distinct shapes in the price book;
    collisions are negligible. Same shape → same hash, always.
    """
    return hashlib.sha1(shape_of(plan).encode("utf-8")).hexdigest()[:12]


def canonical_ext_id(plan: dict) -> str:
    """Item-free externalId for a plan, keyed only on its pricing shape.

    Format ``MP_PP_<shape_hash>`` — keeps the ``MP_PP_`` prefix the loaders and
    the reconciliation LIKE pattern expect, but carries no LINE_ITEM_ID or item
    slug. Every line that shares this pricing resolves to this one externalId.
    """
    return f"MP_PP_{shape_hash(plan)}"
