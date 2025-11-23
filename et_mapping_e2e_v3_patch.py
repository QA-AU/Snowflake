
TABLE_META

"date_filter": "order_date between ('2024-01-01' and '2024-12-31')",
"date_filter_token": "order_date",


. Add this helper (near your other helpers)

def _resolve_date_filter(meta, alias=None, for_target=False):
    """
    Replace the logical date token (e.g. 'order_date') with the
    actual column expression, e.g. 'o.order_date' for source,
    'order_date' for target. Handles multiple occurrences.
    """
    raw = (meta.get("date_filter") or "").strip()
    if not raw:
        return ""

    token = meta.get("date_filter_token", "order_date")

    if for_target:
        replacement = token                 # e.g. order_date
    else:
        replacement = f"{alias}.{token}" if alias else token  # e.g. o.order_date

    pattern = r"\b" + re.escape(token) + r"\b"
    return re.sub(pattern, replacement, raw)


3. In build_source_sql

Replace:

src_flt = (meta.get("source_filter_clause") or "").strip()
date_flt = (meta.get("date_filter") or "").strip()
where_combined = _combine_filters(src_flt, date_flt)

with

src_flt = (meta.get("source_filter_clause") or "").strip()
date_flt = _resolve_date_filter(meta, alias=alias, for_target=False)
where_combined = _combine_filters(src_flt, date_flt)

. In build_target_sql
Replace
tgt_flt = (meta.get("target_filter_clause") or "").strip()
date_flt = (meta.get("date_filter") or "").strip()
where_combined = _combine_filters(tgt_flt, date_flt)


with
tgt_flt = (meta.get("target_filter_clause") or "").strip()
date_flt = _resolve_date_filter(meta, alias=None, for_target=True)
where_combined = _combine_filters(tgt_flt, date_flt)


