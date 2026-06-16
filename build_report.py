import json, os
from collections import defaultdict, Counter

# ── Constants ─────────────────────────────────────────────────────────────────
CREATOR    = 'ismet.dogan@deliveryhero.com'
START_DATE = '2026-06-01'
VG  = 'dh-darkstores-live.product_information_management.variant_groups'
P   = 'dh-darkstores-live.product_information_management.products'

# ── BigQuery data loading ─────────────────────────────────────────────────────

def _bq_client():
    from google.cloud import bigquery
    return bigquery.Client()

def _run(client, sql):
    return [dict(r) for r in client.query(sql).result()]

def load_data_from_bq():
    """Fetch all dashboard data live from BigQuery. Returns the same dicts the
    build script expects so the rest of the file is unchanged."""
    client = _bq_client()

    # ── Scale: model-created group counts by region ───────────────────────────
    new_counts = {r['region_code']: r['cnt'] for r in _run(client, f"""
        SELECT region_code, COUNT(DISTINCT variant_group_id) AS cnt
        FROM `{VG}`
        WHERE sys.created_by = '{CREATOR}' AND sys.created_at >= '{START_DATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY variant_group_id ORDER BY sys.version DESC) = 1
        GROUP BY region_code
    """)}

    # ── Scale: pre-existing group counts by region (before model launch) ──────
    pre_counts = {r['region_code']: r['cnt'] for r in _run(client, f"""
        SELECT region_code, COUNT(DISTINCT variant_group_id) AS cnt
        FROM `{VG}`
        WHERE region_code IN ('ap','sa','us') AND sys.created_at < '{START_DATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY variant_group_id ORDER BY sys.version DESC) = 1
        GROUP BY region_code
    """)}

    # ── Quality: review counts (version > 1) by region ────────────────────────
    reviewed_counts = {r['region_code']: r['cnt'] for r in _run(client, f"""
        WITH latest AS (
          SELECT region_code, variant_group_id, sys.version AS version
          FROM `{VG}`
          WHERE sys.created_by = '{CREATOR}' AND sys.created_at >= '{START_DATE}'
          QUALIFY ROW_NUMBER() OVER (PARTITION BY variant_group_id ORDER BY sys.version DESC) = 1
        )
        SELECT region_code, COUNT(DISTINCT variant_group_id) AS cnt
        FROM latest WHERE version > 1
        GROUP BY region_code
    """)}

    # ── Reviewed groups: all versions to compute v1/vfinal diff ──────────────
    # Get every stored version of every reviewed model group.
    # Diff is computed in Python below (simpler than doing it in SQL).
    all_versions = _run(client, f"""
        WITH reviewed_ids AS (
          SELECT DISTINCT variant_group_id
          FROM `{VG}`
          WHERE sys.created_by = '{CREATOR}' AND sys.created_at >= '{START_DATE}'
          QUALIFY MAX(sys.version) OVER (PARTITION BY variant_group_id) > 1
        )
        SELECT
          vg.variant_group_id,
          vg.sys.version        AS version,
          vg.sys.updated_by     AS updated_by,
          vg.region_code,
          vg.brand.name         AS brand_name,
          vg.category.name      AS cat_name,
          JSON_EXTRACT_SCALAR(TO_JSON_STRING(vg.title), '$.en') AS title_en,
          ARRAY_TO_STRING(vg.variant_attributes, '|') AS variant_attrs,
          vg.product_ids
        FROM reviewed_ids r
        JOIN `{VG}` vg USING (variant_group_id)
        WHERE vg.sys.created_by = '{CREATOR}' AND vg.sys.created_at >= '{START_DATE}'
        ORDER BY vg.variant_group_id, vg.sys.version
    """)

    # ── Sizes for products in reviewed groups (from products table) ───────────
    # Used to compute size_ratio (OVER_SPLIT root cause detection).
    reviewed_gids = list({r['variant_group_id'] for r in all_versions})
    gid_list = ', '.join(f"'{g}'" for g in reviewed_gids)
    size_rows = _run(client, f"""
        SELECT p.product_id, p.variant_group_id_fk AS group_id,
          CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(p.attributes.dim_size), '$.value') AS FLOAT64) AS dim_size,
          JSON_EXTRACT_SCALAR(TO_JSON_STRING(p.attributes.weight_unit), '$.value') AS weight_unit
        FROM `{P}` p
        WHERE p.variant_group_id_fk IN ({gid_list})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY p.product_id ORDER BY p.sys.version DESC) = 1
    """) if reviewed_gids else []

    # ── Category paths for reviewed groups ────────────────────────────────────
    cat_rows = _run(client, f"""
        WITH latest_vg AS (
          SELECT variant_group_id, product_ids[OFFSET(0)] AS first_pid
          FROM `{VG}`
          WHERE sys.created_by = '{CREATOR}' AND sys.created_at >= '{START_DATE}'
          QUALIFY ROW_NUMBER() OVER (PARTITION BY variant_group_id ORDER BY sys.version DESC) = 1
        ),
        latest_p AS (
          SELECT product_id,
            ARRAY_TO_STRING(
              ARRAY(SELECT c.name FROM UNNEST(p.categories) c ORDER BY c.level), ' > '
            ) AS cat_path
          FROM `{P}` p
          QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sys.version DESC) = 1
        )
        SELECT vg.variant_group_id, p.cat_path
        FROM latest_vg vg
        JOIN latest_p p ON p.product_id = vg.first_pid
    """)
    cat_path_map = {r['variant_group_id']: r['cat_path'] for r in cat_rows if r['cat_path']}

    # ── Product display names ─────────────────────────────────────────────────
    # Collect all product_ids referenced across reviewed groups
    all_pids = set()
    for r in all_versions:
        all_pids.update(r['product_ids'] or [])
    pid_list = ', '.join(f"'{p}'" for p in all_pids)
    name_rows = _run(client, f"""
        SELECT product_id,
          JSON_EXTRACT_SCALAR(TO_JSON_STRING(attributes.product_name), '$.en') AS pname
        FROM `{P}`
        WHERE product_id IN ({pid_list})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sys.version DESC) = 1
    """) if all_pids else []
    product_name_map = {r['product_id']: r['pname'] for r in name_rows if r['pname']}

    # ── Group titles ──────────────────────────────────────────────────────────
    title_rows = _run(client, f"""
        SELECT variant_group_id,
          JSON_EXTRACT_SCALAR(TO_JSON_STRING(title), '$.en') AS title_en
        FROM `{VG}`
        WHERE sys.created_by = '{CREATOR}' AND sys.created_at >= '{START_DATE}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY variant_group_id ORDER BY sys.version DESC) = 1
    """)
    group_title_map = {r['variant_group_id']: r['title_en'] for r in title_rows if r['title_en']}

    # ── Compute v1/vfinal diff in Python ─────────────────────────────────────
    by_gid = defaultdict(list)
    for r in all_versions:
        by_gid[r['variant_group_id']].append(r)

    real_changes = {}
    meta = {}
    rows = []
    confirmed_meta = {}
    bq_diff_map = {}

    size_by_gid = defaultdict(list)
    for sr in size_rows:
        size_by_gid[sr['group_id']].append(sr)

    for gid, versions in by_gid.items():
        versions.sort(key=lambda x: x['version'])
        v1_pids  = set(versions[0]['product_ids'] or [])
        vf_pids  = set(versions[-1]['product_ids'] or [])
        latest   = versions[-1]
        added    = sorted(vf_pids - v1_pids)
        removed  = sorted(v1_pids - vf_pids)

        bq_diff_map[gid] = {
            'pids_v1':     '|'.join(sorted(v1_pids)),
            'pids_vfinal': '|'.join(sorted(vf_pids)),
        }

        if not added and not removed:
            # Confirmed: version bumped but no membership change
            confirmed_meta[gid] = {
                'region': latest['region_code'], 'brand': latest['brand_name'],
                'category': latest['cat_name'], 'version': latest['version'],
                'updated_by': latest['updated_by'],
            }
            continue

        tag = 'OVER_SPLIT' if not removed else 'OVER_GROUPED'
        vfin_count = len(vf_pids)
        health = 'SINGLETON' if (tag == 'OVER_GROUPED' and vfin_count == 1) else 'VALID'

        # Size ratio — largest/smallest size value in the group
        sizes = [sr['dim_size'] for sr in size_by_gid[gid] if sr['dim_size'] is not None]
        size_ratio = round(max(sizes) / min(sizes)) if len(sizes) >= 2 and min(sizes) > 0 else 1

        real_changes[gid] = {
            'tag': tag, 'added': added, 'removed': removed,
            'v1_count': len(v1_pids), 'vfin_count': vfin_count,
        }

        meta[gid] = {
            'region': latest['region_code'], 'brand': latest['brand_name'],
            'category': latest['cat_name'], 'tags': [tag], 'health': health,
            'version': latest['version'], 'updated_by': latest['updated_by'],
            'product_count': vfin_count,
            'variant_attrs': latest['variant_attrs'],
            'sizes': [sr['dim_size'] for sr in size_by_gid[gid] if sr['dim_size'] is not None],
            'size_ratio': size_ratio,
        }
        for pid in sorted(vf_pids):
            sr = next((s for s in size_by_gid[gid] if s['product_id'] == pid), {})
            rows.append({'product_id': pid, 'group_id': gid,
                         'dim_size': sr.get('dim_size'), 'weight_unit': sr.get('weight_unit')})

    # ── Scale scalars ─────────────────────────────────────────────────────────
    total_groups   = sum(new_counts.values())
    pre_fp  = pre_counts.get('ap', 0)
    pre_hs  = pre_counts.get('sa', 0)
    pre_py  = pre_counts.get('us', 0)
    new_fp  = new_counts.get('ap', 0)
    new_hs  = new_counts.get('sa', 0)
    new_py  = new_counts.get('us', 0)
    rev_fp  = reviewed_counts.get('ap', 0)
    rev_hs  = reviewed_counts.get('sa', 0)

    return dict(
        meta=meta, rows=rows, confirmed_meta=confirmed_meta,
        real_changes=real_changes, bq_diff_map=bq_diff_map,
        cat_path_map=cat_path_map, product_name_map=product_name_map,
        group_title_map=group_title_map,
        total_groups=total_groups,
        pre_fp=pre_fp, pre_hs=pre_hs, pre_py=pre_py,
        new_fp=new_fp, new_hs=new_hs, new_py=new_py,
        rev_fp=rev_fp, rev_hs=rev_hs,
    )

def load_data_local():
    """Fall back to pre-exported JSON files for local dev (no BQ auth needed)."""
    import ast as _ast
    data           = json.load(open('/tmp/group_meta_corrected.json'))
    confirmed_meta = json.load(open('/tmp/confirmed_meta.json'))
    with open('/Users/necmettin.tamgueney/.claude/projects/-Users-necmettin-tamgueney/'
              'd46b9314-665c-45c2-bc9a-19eb2f5b974d/tool-results/'
              'mcp-bigquery-execute-query-1781551717583.txt') as _f:
        _bq_rows = _ast.literal_eval(_f.read())
    return dict(
        meta=data['meta'], rows=data['rows'],
        confirmed_meta=confirmed_meta,
        real_changes=json.load(open('/tmp/real_changes.json')),
        bq_diff_map={r['variant_group_id']: r for r in _bq_rows},
        cat_path_map=json.load(open('/tmp/cat_path_map.json')),
        product_name_map=json.load(open('/tmp/product_name_map.json')),
        group_title_map=json.load(open('/tmp/group_title_map.json')),
        total_groups=52038,
        pre_fp=28934, pre_hs=1418,  pre_py=1385,
        new_fp=18412, new_hs=17672, new_py=15954,
        rev_fp=2259,  rev_hs=133,
    )

# ── Load: prefer BQ, fall back to local JSON ─────────────────────────────────
try:
    _d = load_data_from_bq()
    print("Loaded data from BigQuery")
except Exception as _e:
    print(f"BQ unavailable ({_e}), falling back to local JSON")
    _d = load_data_local()

meta             = _d['meta']
rows             = _d['rows']
confirmed_meta   = _d['confirmed_meta']
real_changes     = _d['real_changes']
bq_diff_map      = _d['bq_diff_map']
cat_path_map     = _d['cat_path_map']
product_name_map = _d['product_name_map']
group_title_map  = _d['group_title_map']
_total_groups    = _d['total_groups']
_pre_fp, _pre_hs, _pre_py = _d['pre_fp'], _d['pre_hs'], _d['pre_py']
_new_fp, _new_hs, _new_py = _d['new_fp'], _d['new_hs'], _d['new_py']
_rev_fp, _rev_hs          = _d['rev_fp'], _d['rev_hs']

def _pids(gid, which='vfinal'):
    r = bq_diff_map.get(gid, {})
    raw = r.get('pids_' + which, '') or ''
    s = set(raw.split('|'))
    s.discard('')
    return s

real_changes_data = None  # loaded later after real_changes dict is built

def display_name(pid):
    return product_name_map.get(pid, pid)

def full_cat(gid, fallback):
    return cat_path_map.get(gid, fallback)

print(f"Loaded {len(meta)} groups, {len(rows)} product rows")

_seen = defaultdict(set)
groups_by_rows = defaultdict(list)
for r in rows:
    pid = r['product_id']
    gid = r['group_id']
    if gid in meta and pid not in _seen[gid]:
        _seen[gid].add(pid)
        groups_by_rows[gid].append(r)

# ── Helpers ───────────────────────────────────────────────────────────────────
def esc(s):
    return str(s or '').replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;')

def short(email):
    if 'foodpanda' in email or 'mbpkcl' in email or 'wan' in email:
        return 'foodpanda'
    if any(x in email for x in ['hungerstation','waad','adel','yehia','saadsaleh','sarah','almaliek','alrifdan','alomran','mahmoud']):
        return 'hungerstation'
    return email.split('@')[1].split('.')[0].title() if '@' in email else email

def editor_color(email):
    if 'foodpanda' in email or 'mbpkcl' in email or 'wan' in email:
        return '#e74c3c'
    if any(x in email for x in ['hungerstation','waad','adel','yehia','saadsaleh','sarah']):
        return '#27ae60'
    return '#95a5a6'

TAG_COLOR = {'OVER_SPLIT':'#27ae60','OVER_GROUPED':'#e74c3c','WRONG_BOUNDARY':'#e67e22','UNKNOWN':'#7f8c8d'}
TAG_LABEL = {
    'OVER_SPLIT':       'Groups merged by editor',
    'OVER_GROUPED':     'Products removed by editor',
    'WRONG_BOUNDARY':   'Products moved between groups',
    'UNKNOWN':          'Other edit'
}
TAG_CLASS = {
    'OVER_SPLIT':     'badge badge-green',
    'OVER_GROUPED':   'badge badge-red',
    'WRONG_BOUNDARY': 'badge badge-amber',
    'UNKNOWN':        'badge badge-neutral',
}

def badge(tag):
    cls = TAG_CLASS.get(tag, 'badge badge-neutral')
    l = TAG_LABEL.get(tag, tag)
    return '<span class="' + cls + '">' + l + '</span>'

# ── Category heatmap ──────────────────────────────────────────────────────────
cat_stats = defaultdict(lambda: defaultdict(int))
for gid, m in meta.items():
    for t in m['tags']:
        cat_stats[full_cat(gid, m['category'])][t] += 1
top_cats = sorted(cat_stats.items(), key=lambda x: -sum(x[1].values()))[:20]

ALL_TAGS = ['OVER_SPLIT','OVER_GROUPED']

heatmap_thead = (
    '<tr><th>Category</th><th>Total edits</th>'
    + ''.join('<th>' + TAG_LABEL[t] + '</th>' for t in ALL_TAGS)
    + '</tr>'
)
heatmap_tbody = ''
for cat, counts in top_cats:
    total = sum(counts.values())
    cells = ''
    for t in ALL_TAGS:
        n = counts.get(t, 0)
        cell_cls = 'cell-hot' if n > 2 else ''
        cells += '<td style="text-align:center;padding:6px 8px"><span class="' + cell_cls + '" style="display:inline-block;padding:2px 8px;border-radius:6px">' + (str(n) if n else '') + '</span></td>'
    heatmap_tbody += (
        '<tr><td style="font-weight:600;padding:6px 8px">' + esc(cat) + '</td>'
        '<td style="text-align:center;font-weight:700;padding:6px 8px">' + str(total) + '</td>'
        + cells + '</tr>'
    )

# ── What-went-wrong examples ──────────────────────────────────────────────────

unit_mismatch = [(gid, m) for gid, m in meta.items()
    if 'OVER_SPLIT' in m['tags'] and m.get('size_ratio', 1) > 100]
unit_mismatch.sort(key=lambda x: -x[1].get('size_ratio', 1))


singletons = [(gid, m) for gid, m in meta.items() if m['health'] == 'SINGLETON']

ex1 = '<div style="font-size:11px;overflow:auto;max-height:140px;margin-top:8px">'
for _, m in unit_mismatch[:10]:
    sizes = ', '.join(str(s) for s in m.get('sizes', [])[:6])
    ex1 += (
        '<div style="padding:4px 0;border-bottom:1px solid #f0f0f0">'
        '<strong>' + esc(m['brand']) + '</strong> &nbsp;/&nbsp; ' + esc(m['category'])
        + '&nbsp;&mdash;&nbsp;sizes found in one group: <code>' + sizes + '</code>'
        + '&nbsp; (ratio: ' + str(m.get('size_ratio','?')) + 'x)</div>'
    )
ex1 += '</div>'


ex3 = '<div style="font-size:11px;overflow:auto;max-height:140px;margin-top:8px">'
for gid, m in sorted(singletons, key=lambda x: -x[1]['version'])[:15]:
    pid = groups_by_rows[gid][0]['product_id'] if groups_by_rows[gid] else ''
    name = display_name(pid)[:60]
    ex3 += (
        '<div style="padding:4px 0;border-bottom:1px solid #f0f0f0">'
        '<strong>' + esc(m['brand']) + '</strong> &nbsp;/&nbsp; ' + esc(m['category'])
        + '&nbsp;&mdash;&nbsp;edited ' + str(m['version'] - 1) + ' time(s) &mdash; '
        + esc(name) + '</div>'
    )
ex3 += '</div>'

def make_issue(title, color, what_happened, what_to_fix, examples_html, count, count_label='groups'):
    count_str = str(count) + ' ' + count_label if isinstance(count, int) else count
    return (
        '<div style="background:#fff;border:1px solid ' + color + ';border-left:5px solid ' + color + ';border-radius:8px;padding:20px;margin-bottom:16px">'
        '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:12px">'
        '<div style="font-size:14px;font-weight:700;color:#2c3e50">' + title + '</div>'
        '<div style="background:' + color + ';color:#fff;padding:3px 10px;border-radius:12px;font-size:12px;font-weight:700;white-space:nowrap;margin-left:12px">' + count_str + '</div>'
        '</div>'
        '<div style="font-size:12px;margin-bottom:10px">'
        '<span style="font-weight:700;color:#2c3e50">What happened: </span>' + what_happened
        + '</div>'
        '<div style="font-size:12px;background:#f8f9fa;border-radius:6px;padding:10px;margin-bottom:8px">'
        '<span style="font-weight:700;color:#2c3e50">Fix: </span>' + what_to_fix
        + '</div>'
        + examples_html
        + '</div>'
    )

issues_html = (
    make_issue(
        'Same product, different size unit — created as two separate groups',
        '#e67e22',
        str(len(unit_mismatch)) + ' groups were merged by editors because the same product was stored twice: '
        + 'once as "1 kg" and once as "1000 g". The system read these as two different sizes and created two groups. '
        + 'Editors had to manually pull them back together.',
        'Normalise all size values to a single unit (grams for weight, ml for volume) before grouping runs. '
        + 'This is a one-time data fix and would eliminate the majority of corrections in AP.',
        ex1,
        len(unit_mismatch)
    ) +
    make_issue(
        'Different product formats put in the same group',
        '#e74c3c',
        str(len(singletons)) + ' groups were reduced to a single product after editors removed everything else. '
        + 'The removed products shared the same brand and category but were a different format — '
        + 'for example, a powder and a liquid, or a sachet and a bottle. The system cannot tell these apart today.',
        'Scan product names for format keywords (powder, liquid, sachet, capsule, spray, tablet) and treat them as hard separators — '
        + 'two products with different format keywords should never be in the same group, even if brand and category match.',
        ex3,
        len(singletons)
    )
)

# ── All-groups table ──────────────────────────────────────────────────────────
all_group_rows = ''
for gid, m in sorted(meta.items(), key=lambda x: (x[1]['region'], x[1]['tags'][0], x[1]['brand'] or '')):
    prods = groups_by_rows[gid]
    sizes_str = ', '.join(
        str(p['dim_size']) + (p.get('weight_unit') or '') for p in prods if p.get('dim_size') is not None
    )[:80]
    pid_str = ' '.join(
        '<code style="font-family:var(--mono);font-size:9px;background:var(--surface-2);color:var(--ink-mute);padding:1px 4px;border-radius:4px;border:1px solid var(--border)">' + p['product_id'] + '</code>'
        for p in prods
    )
    names_html = '<br>'.join(
        esc(display_name(p['product_id'])[:70]) for p in prods
    )
    tags_html = ' '.join(badge(t) for t in m['tags'])
    ec = editor_color(m['updated_by'])

    if m['health'] == 'SINGLETON':
        row_style = 'background:#FEF2F2'
    elif 'OVER_SPLIT' in m['tags']:
        row_style = 'background:#F0FAF5'
    elif 'WRONG_BOUNDARY' in m['tags']:
        row_style = 'background:#FFFBF0'
    else:
        row_style = ''

    prod_count_cls = 'badge badge-red' if m['product_count'] == 1 else 'badge badge-neutral'
    editor_cls = 'badge badge-ink'

    all_group_rows += (
        '<tr style="' + row_style + '" '
        'data-tags="' + ' '.join(m['tags']) + '" '
        'data-region="' + m['region'] + '" '
        'data-editor="' + short(m['updated_by']).lower() + '" '
        'data-cat="' + esc(m['category']).lower() + '" '
        'data-brand="' + esc(m['brand']).lower() + '" '
        'data-singleton="' + ('1' if m['health'] == 'SINGLETON' else '0') + '">'
        '<td><span class="badge badge-neutral">' + m['region'].upper() + '</span></td>'
        '<td style="font-family:var(--mono);font-size:9px;max-width:110px;overflow:hidden;text-overflow:ellipsis;color:var(--ink-mute)" title="' + gid + '">' + gid + '</td>'
        '<td style="text-align:center"><span class="badge badge-neutral">v' + str(m['version']) + '</span></td>'
        '<td><span class="' + editor_cls + '">' + short(m['updated_by']) + '</span></td>'
        '<td style="text-align:center"><span class="' + prod_count_cls + '">' + str(m['product_count']) + '</span></td>'
        '<td style="font-weight:600">' + esc(m['brand']) + '</td>'
        '<td style="color:var(--ink-soft)" title="' + esc(full_cat(gid, m['category'])) + '">' + esc(full_cat(gid, m['category'])) + '</td>'
        '<td style="color:var(--ink-soft)">' + esc(m['variant_attrs']) + '</td>'
        '<td style="color:var(--ink-mute);font-family:var(--mono);font-size:11px">' + (esc(sizes_str) or '&mdash;') + '</td>'
        '<td>' + tags_html + '</td>'
        '<td style="font-weight:600">' + esc(group_title_map.get(gid, '')) + '</td>'
        '<td style="line-height:1.7;color:var(--ink-soft)">' + names_html + '</td>'
        '<td>' + pid_str + '</td>'
        '</tr>'
    )

print(f"Built {len(meta)} group rows")

# ── CSS & JS ──────────────────────────────────────────────────────────────────
CSS = """
:root {
  --font: "Outfit", system-ui, -apple-system, "Segoe UI", sans-serif;
  --mono: "JetBrains Mono", monospace;
  --bg: #FAFAF7;
  --surface: #FFFFFF;
  --surface-2: #F4F4EF;
  --surface-3: #ECECE5;
  --ink: #1B1F2A;
  --ink-soft: #4A5260;
  --ink-mute: #6B7280;
  --ink-faint: #9BA0AB;
  --border: #E4E4DD;
  --border-strong: #CFCFC6;
  --dh-red: #D61F26;
  --dh-red-2: #B71920;
  --red-tint: #FCE8E9;
  --red-edge: #F2C9CB;
  --green: #00A86B;
  --green-2: #008C58;
  --green-tint: #E4F6EC;
  --green-edge: #BFE5CF;
  --amber: #C8870D;
  --amber-tint: #FBF0D6;
  --amber-edge: #ECCF93;
  --blue: #0066B3;
  --blue-2: #004F8C;
  --blue-tint: #E1EEF7;
  --blue-edge: #B7D6EC;
  --purple: #7B3FA0;
  --purple-tint: #F1E7F8;
  --shadow-1: 0 1px 2px rgba(20,22,30,.04),0 1px 1px rgba(20,22,30,.03);
  --shadow-2: 0 4px 14px -6px rgba(20,22,30,.10),0 2px 4px rgba(20,22,30,.04);
  --radius-sm: 6px;
  --radius: 10px;
  --radius-lg: 14px;
  --radius-xl: 20px;
}
/* ── Fluid type scale — clamp(min, preferred, max) ── */
/* Scales smoothly from ~900px (MacBook Air) to ~2560px (large monitor) */
:root {
  --text-xs:   clamp(10px, 0.7vw,  12px);
  --text-sm:   clamp(11px, 0.8vw,  13px);
  --text-base: clamp(13px, 0.9vw,  15px);
  --text-md:   clamp(14px, 1vw,    16px);
  --text-lg:   clamp(16px, 1.2vw,  20px);
  --text-xl:   clamp(18px, 1.5vw,  24px);
  --text-2xl:  clamp(22px, 1.9vw,  30px);
  --text-hero: clamp(30px, 2.8vw,  48px);
  --text-stat: clamp(36px, 3.5vw,  58px);
  --gap-sm:    clamp(8px,  0.8vw,  14px);
  --gap-md:    clamp(12px, 1vw,    20px);
  --gap-lg:    clamp(20px, 1.8vw,  40px);
  --pad-card:  clamp(18px, 1.8vw,  36px);
}

*, *::before, *::after { box-sizing:border-box;margin:0;padding:0 }
body {
  font-family: var(--font);
  font-feature-settings: "ss01","ss02","cv11";
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
  background: var(--bg);
  color: var(--ink);
  font-size: var(--text-base);
  line-height: 1.5;
}

/* ── Header ── */
.header {
  position: sticky; top: 0; z-index: 100;
  background: color-mix(in oklab, var(--bg) 90%, transparent);
  backdrop-filter: blur(8px);
  -webkit-backdrop-filter: blur(8px);
  border-bottom: 1px solid var(--border);
  padding: 0 32px;
  height: 52px;
  display: flex; align-items: center; justify-content: space-between;
}
.header-left { display:flex; align-items:center; gap:12px }
.header-title { font-size:14px; font-weight:600; color:var(--ink); letter-spacing:-.01em }
.header-divider { width:1px; height:18px; background:var(--border-strong) }
.header-sub { font-size:12px; color:var(--ink-mute); font-weight:400 }
.header-badge {
  font-size:11px; font-weight:600; letter-spacing:.06em; text-transform:uppercase;
  background:var(--amber-tint); color:var(--amber); border:1px solid var(--amber-edge);
  border-radius:999px; padding:3px 10px;
}

/* ── Nav ── */
.nav {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  display: flex;
  padding: 0 32px;
  overflow-x: auto;
  position: sticky; top: 52px; z-index: 99;
}
.nav a {
  color: var(--ink-mute);
  padding: 14px 18px;
  text-decoration: none;
  font-size: 12px; font-weight: 600;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  white-space: nowrap;
  transition: color 160ms ease, border-color 160ms ease;
}
.nav a:hover { color: var(--ink) }
.nav a.active { color: var(--dh-red); border-bottom-color: var(--dh-red) }

/* ── Layout ── */
.container { max-width:1600px; margin:0 auto; padding:var(--gap-lg) clamp(24px,3vw,64px) }
.section { display:none } .section.active { display:block }

/* ── Section headings ── */
.eyebrow {
  font-size:11px; font-weight:600; letter-spacing:.14em; text-transform:uppercase;
  color:var(--ink-mute); margin-bottom:8px;
}
h2 {
  font-size:var(--text-xl); font-weight:700; color:var(--ink);
  letter-spacing:-.02em; margin:32px 0 16px;
}
h2:first-child { margin-top:0 }

/* ── KPI cards ── */
.kpi-row { display:flex; flex-wrap:wrap; gap:var(--gap-md); margin-bottom:24px }
.kpi {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--radius-lg); padding:20px 22px;
  flex:1; min-width:160px; box-shadow:var(--shadow-1);
  transition: box-shadow 160ms ease, transform 160ms ease, border-color 160ms ease;
}
.kpi:hover { box-shadow:var(--shadow-2); transform:translateY(-1px); border-color:var(--border-strong) }
.kpi .num { font-size:var(--text-2xl); font-weight:700; color:var(--ink); line-height:1; letter-spacing:-.02em }
.kpi .label { font-size:var(--text-sm); color:var(--ink-soft); margin-top:6px; line-height:1.4 }
.kpi .sub { font-size:var(--text-xs); color:var(--ink-mute); margin-top:4px; line-height:1.4 }

/* ── Tables ── */
.table-wrap {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--radius-lg); overflow:auto; max-height:560px;
  box-shadow:var(--shadow-1);
}
table { width:100%; border-collapse:collapse; font-size:12px }
thead th {
  background:var(--surface-2); color:var(--ink-soft); border-bottom:1px solid var(--border);
  padding:10px 12px; white-space:nowrap; position:sticky; top:0; z-index:2;
  text-align:left; font-size:11px; font-weight:600; letter-spacing:.04em; text-transform:uppercase;
}
tbody td { padding:10px 12px; border-bottom:1px solid var(--border); vertical-align:top }
tbody tr:last-child td { border-bottom:none }
tbody tr:hover { background:var(--surface-2)!important }

/* ── Filters ── */
.filters {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--radius-lg); padding:12px 16px; margin-bottom:12px;
  display:flex; flex-wrap:wrap; gap:10px; align-items:center;
  box-shadow:var(--shadow-1);
}
.filters label { font-size:11px; font-weight:600; color:var(--ink-soft) }
select, input[type=text] {
  font-family:var(--font); font-size:12px; padding:6px 10px;
  border:1px solid var(--border); border-radius:var(--radius-sm);
  background:var(--surface); color:var(--ink);
  outline:none; transition:border-color 160ms ease;
}
select:focus, input[type=text]:focus { border-color:var(--blue) }
#gcount { font-size:11.5px; color:var(--ink-mute); margin-bottom:8px; font-family:var(--mono) }

/* ── Legend chips ── */
.legend { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:16px }
.legend span {
  font-size:11.5px; font-weight:600; padding:3px 10px;
  border-radius:999px; border:1px solid var(--border);
  color:var(--ink-soft); background:var(--surface-2);
}

/* ── Platform toggle ── */
.plat-toggle {
  display:flex; gap:6px; margin-bottom:28px; flex-wrap:wrap;
}
.plat-toggle button {
  font-family:var(--font); font-size:12px; font-weight:600;
  color:var(--ink-mute); background:var(--surface);
  border:1px solid var(--border); border-radius:999px;
  padding:8px 20px; cursor:pointer;
  transition:all 160ms ease;
}
.plat-toggle button:hover { color:var(--ink); border-color:var(--border-strong) }
.plat-toggle button.active {
  background:var(--ink); color:#fff; border-color:var(--ink);
}
.plat-panel { display:none } .plat-panel.active { display:block }

/* ── Stat section label ── */
.stat-section-label {
  font-size:11px; font-weight:600; letter-spacing:.14em; text-transform:uppercase;
  color:var(--ink-mute); margin-bottom:12px; margin-top:24px;
}
.stat-section-label:first-child { margin-top:0 }

/* ── Exec stat cards ── */
.stat-row { display:flex; gap:var(--gap-md); margin-bottom:var(--gap-md); flex-wrap:wrap }
.stat-card {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--radius-lg); padding:var(--pad-card); flex:1; min-width:200px;
  box-shadow:var(--shadow-1);
  transition:box-shadow 160ms ease, transform 160ms ease, border-color 160ms ease;
}
.stat-card:hover { box-shadow:var(--shadow-2); transform:translateY(-1px); border-color:var(--border-strong) }
.stat-card .pct { font-size:var(--text-stat); font-weight:800; line-height:1; letter-spacing:-.03em }
.stat-card .cnt { font-size:var(--text-stat); font-weight:700; line-height:1; letter-spacing:-.025em }
.stat-card .cnt-sub { font-size:var(--text-md); font-weight:600; margin-top:8px }
.stat-card .n { font-size:var(--text-2xl); font-weight:700; line-height:1; letter-spacing:-.02em }
.stat-card .l { font-size:var(--text-sm); color:var(--ink-soft); margin-top:10px; line-height:1.4 }
.stat-card .s { font-size:var(--text-xs); color:var(--ink-mute); margin-top:5px; line-height:1.4 }

/* ── Issue cards ── */
.cause-card {
  background:var(--surface); border:1px solid var(--border);
  border-left:4px solid; border-radius:var(--radius-lg);
  padding:20px 24px; margin-bottom:16px; box-shadow:var(--shadow-1);
}
.cause-card .title { font-size:15px; font-weight:700; color:var(--ink); margin-bottom:8px; letter-spacing:-.01em }
.cause-card .body { font-size:13px; color:var(--ink-soft); line-height:1.65 }
.cause-card .fix { font-size:12px; color:var(--green-2); margin-top:10px; font-weight:600 }

/* ── Inline note box ── */
.note-box {
  background:var(--surface-2); border:1px solid var(--border);
  border-radius:var(--radius); padding:14px 18px;
  font-size:12.5px; color:var(--ink-soft); line-height:1.6;
}

/* ── Badge pills (table tags) ── */
.badge {
  display:inline-block; font-size:11px; font-weight:600; letter-spacing:.02em;
  padding:3px 9px; border-radius:999px; white-space:nowrap;
  border:1px solid transparent;
}
.badge-green { background:var(--green-tint); color:var(--green-2); border-color:var(--green-edge) }
.badge-red   { background:var(--red-tint);   color:var(--dh-red);  border-color:var(--red-edge) }
.badge-amber { background:var(--amber-tint); color:var(--amber);   border-color:var(--amber-edge) }
.badge-blue  { background:var(--blue-tint);  color:var(--blue-2);  border-color:var(--blue-edge) }
.badge-purple{ background:var(--purple-tint);color:var(--purple);  border-color:#D9C0EC }
.badge-ink   { background:var(--ink);        color:#fff;           border-color:var(--ink) }
.badge-neutral{ background:var(--surface-2); color:var(--ink-soft);border-color:var(--border) }

/* ── Takeaway ── */
.takeaway {
  background:var(--blue-tint); border:1px solid var(--blue-edge);
  border-radius:var(--radius-lg); padding:20px 24px; margin-top:8px;
}
.takeaway .t { font-size:13px; font-weight:700; color:var(--blue-2); margin-bottom:10px }
.takeaway ul { margin:0; padding-left:18px; font-size:13px; color:var(--ink); line-height:2 }

/* ── Proportion bar ── */
.prop-wrap { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-lg); padding:var(--gap-lg); margin-bottom:24px; box-shadow:var(--shadow-1) }
.prop-bar-outer { height:10px; background:var(--surface-3); border-radius:999px; overflow:hidden; margin:16px 0 8px; position:relative }
.prop-bar-fill { height:100%; border-radius:999px; transition:width .6s cubic-bezier(.2,.7,.2,1) }
.prop-bar-labels { display:flex; justify-content:space-between; font-size:var(--text-xs); color:var(--ink-mute) }
.prop-breakdown { display:flex; margin-top:20px; border-radius:var(--radius); overflow:hidden; height:44px }
.prop-bd-seg { display:flex; align-items:center; justify-content:center; font-size:var(--text-xs); font-weight:600; color:#fff; white-space:nowrap; padding:0 10px; transition:flex .6s cubic-bezier(.2,.7,.2,1) }
.prop-legend { display:flex; gap:20px; flex-wrap:wrap; margin-top:12px }
.prop-legend-item { display:flex; align-items:center; gap:6px; font-size:var(--text-xs); color:var(--ink-soft) }
.prop-legend-dot { width:8px; height:8px; border-radius:50%; flex-shrink:0 }

/* ── Product scorecard ── */
.prod-scorecard { display:grid; grid-template-columns:repeat(3,1fr); gap:var(--gap-md); margin-bottom:24px }
.prod-sc-cell { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-lg); padding:var(--pad-card); box-shadow:var(--shadow-1) }
.prod-sc-cell .sc-val { font-size:clamp(28px,2.5vw,44px); font-weight:700; letter-spacing:-.025em; line-height:1 }
.prod-sc-cell .sc-label { font-size:var(--text-sm); font-weight:600; color:var(--ink-soft); margin-top:8px }
.prod-sc-cell .sc-meta { font-size:var(--text-xs); color:var(--ink-mute); margin-top:4px; line-height:1.5 }

/* ── Hero banner ── */
.exec-hero-banner {
  position: relative;
  background: linear-gradient(118deg, #1B0304 0%, #2D0608 40%, #1a0203 100%);
  border-radius: var(--radius-xl);
  padding: clamp(28px,3.5vw,56px) clamp(28px,4vw,64px);
  margin-bottom: var(--gap-lg);
  overflow: hidden;
  box-shadow: 0 8px 40px -12px rgba(214,31,38,.45), 0 2px 8px rgba(0,0,0,.18);
}
/* large watermark DH logo */
.exec-hero-banner::before {
  content: '';
  position: absolute;
  right: -60px; top: -60px;
  width: 480px; height: 480px;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 512 512'%3E%3Cpath fill-rule='evenodd' clip-rule='evenodd' fill='%23ffffff' d='M419.714 248.327c-.116.054-.179.107-.268.152l-46.63 19.034-1.431.653-11.357 51.964c-.76 1.772-3.032 2.193-4.561.707l-33.975-40.188-.161-.116-191.563 82.246c-.161.09-.349.126-.536.126a1.283 1.283 0 01-1.288-1.289c0-.412.205-.787.527-1.029l165.61-124.036-20.954-47.875c-1.046-2.175.885-4.501 3.443-3.857h.018l50.752 12.51 39.404-35.096v.009c1.699-1.333 4.015-.385 4.445 1.727l3.845 52.725 45.44 26.604c1.95 1.235 1.673 4.125-.76 5.029zM396.552 97.633C337.759 74.546 273.1 91.88 233.196 136.246l-155.7 166.9c-2.093 2.246-1.127 5.065 1.43 5.441l41.479 2.55c3.327.206 3.738 3.061 2.066 5.02L21.073 425.598c-1.77 1.897.358 4.877 2.781 4.126l144.772-45.772c3.059-1.056 5.42 1.673 4.123 4.071l-19.362 34.255c-1.002 1.951.877 4.627 3.309 4.448l208.715-46.515c49.876-7.901 94.404-41.208 114.196-91.642 29.807-75.687-7.432-161.164-83.055-190.936z'/%3E%3C/svg%3E");
  background-size: contain;
  background-repeat: no-repeat;
  opacity: 0.045;
  pointer-events: none;
}
/* subtle noise texture overlay */
.exec-hero-banner::after {
  content: '';
  position: absolute;
  inset: 0;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='200' height='200'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.75' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='200' height='200' filter='url(%23n)' opacity='1'/%3E%3C/svg%3E");
  opacity: 0.03;
  pointer-events: none;
  border-radius: var(--radius-xl);
}
.exec-hero-banner .hero-eyebrow {
  font-size: 11px; font-weight: 600; letter-spacing: .16em; text-transform: uppercase;
  color: rgba(255,255,255,.45); margin-bottom: 14px;
  display: flex; align-items: center; gap: 10px;
}
.exec-hero-banner .hero-eyebrow::after {
  content: ''; flex: 1; height: 1px; background: rgba(255,255,255,.1); max-width: 120px;
}
.exec-hero-banner .hero-title {
  font-size: clamp(26px,2.8vw,44px); font-weight: 700; color: #fff;
  letter-spacing: -.025em; line-height: 1.1; margin-bottom: 6px;
}
.exec-hero-banner .hero-title span {
  color: var(--dh-red); position: relative;
}
.exec-hero-banner .hero-sub {
  font-size: 14px; color: rgba(255,255,255,.5); margin-bottom: 36px; font-weight: 400;
}
.hero-kpis {
  display: flex; gap: 0; position: relative; z-index: 1;
}
.hero-kpi {
  flex: 1; padding: 0 32px 0 0; margin-right: 32px;
  border-right: 1px solid rgba(255,255,255,.1);
}
.hero-kpi:last-child { border-right: none; margin-right: 0; padding-right: 0 }
.hero-kpi .hk-val {
  font-size: clamp(32px,3.5vw,56px); font-weight: 800; letter-spacing: -.03em; line-height: 1; color: #fff;
}
.hero-kpi .hk-val.accent { color: var(--dh-red) }
.hero-kpi .hk-val.green  { color: #4AE8A0 }
.hero-kpi .hk-sub {
  font-size: 13px; font-weight: 600; color: rgba(255,255,255,.55); margin-top: 8px;
}
.hero-kpi .hk-label {
  font-size: 11.5px; color: rgba(255,255,255,.35); margin-top: 4px;
}

/* ── Accent stat card (hero highlight) ── */
.stat-card.accent-card {
  background: linear-gradient(135deg, #0050A0 0%, #0066B3 60%, #0080D8 100%);
  border-color: transparent;
  box-shadow: 0 6px 24px -8px rgba(0,102,179,.5), 0 2px 6px rgba(0,0,0,.08);
  color: #fff;
}
.stat-card.accent-card .pct { color: #fff }
.stat-card.accent-card .cnt-sub { color: rgba(255,255,255,.75) }
.stat-card.accent-card .l { color: rgba(255,255,255,.65) }
.stat-card.accent-card .s { color: rgba(255,255,255,.45) }
.stat-card.accent-card:hover {
  box-shadow: 0 10px 32px -8px rgba(0,102,179,.55), 0 3px 8px rgba(0,0,0,.1);
}

/* ── Nav red dot on active ── */
.nav { padding-left: 28px }

/* ── Section divider with color bar ── */
.color-divider {
  height: 3px;
  background: linear-gradient(90deg, var(--dh-red) 0%, transparent 100%);
  border-radius: 2px;
  margin: 28px 0 20px;
  width: 60px;
}

/* ── Stat card tinted variants ── */
.stat-card.tint-green {
  background: linear-gradient(135deg, #003D27 0%, #005535 100%);
  border-color: transparent;
  box-shadow: 0 6px 24px -8px rgba(0,168,107,.4);
  color: #fff;
}
.stat-card.tint-green .pct { color: #4AE8A0 }
.stat-card.tint-green .cnt-sub { color: rgba(255,255,255,.7) }
.stat-card.tint-green .l { color: rgba(255,255,255,.65) }
.stat-card.tint-green .s { color: rgba(255,255,255,.4) }

.stat-card.tint-red {
  background: linear-gradient(135deg, #3D0608 0%, #550A0C 100%);
  border-color: transparent;
  box-shadow: 0 6px 24px -8px rgba(214,31,38,.4);
  color: #fff;
}
.stat-card.tint-red .pct { color: #FF7B7E }
.stat-card.tint-red .cnt-sub { color: rgba(255,255,255,.7) }
.stat-card.tint-red .l { color: rgba(255,255,255,.65) }
.stat-card.tint-red .s { color: rgba(255,255,255,.4) }

/* ── Page-level background texture (very subtle) ── */
body::before {
  content: '';
  position: fixed; inset: 0; z-index: -1;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='60' height='60'%3E%3Cpath d='M0 0h60v60H0z' fill='%23FAFAF7'/%3E%3Ccircle cx='30' cy='30' r='.6' fill='%23CFCFC6' opacity='.5'/%3E%3Ccircle cx='0' cy='0' r='.6' fill='%23CFCFC6' opacity='.5'/%3E%3Ccircle cx='60' cy='0' r='.6' fill='%23CFCFC6' opacity='.5'/%3E%3Ccircle cx='0' cy='60' r='.6' fill='%23CFCFC6' opacity='.5'/%3E%3Ccircle cx='60' cy='60' r='.6' fill='%23CFCFC6' opacity='.5'/%3E%3C/svg%3E");
  pointer-events: none;
}

/* ── Platform logo mark in banner ── */
.plat-logo-mark {
  position: absolute; right: clamp(28px,3vw,52px); top: 50%; transform: translateY(-50%);
  opacity: 0.05; pointer-events: none;
  width: clamp(160px,18vw,300px); height: auto;
  filter: brightness(10);  /* force white regardless of original fill */
}
.plat-logo-mark svg { width: 100%; height: auto; display: block }

/* ── Platform tokens ── */
:root {
  --fp:    #D70F64;   /* Foodpanda pink  — brandpalettes.com confirmed */
  --fp-2:  #A80B4D;
  --fp-bg: #16000A;
  --hs:    #FFC300;   /* HungerStation yellow — brand visual identity  */
  --hs-2:  #CC9C00;
  --hs-bg: #161000;
  --py:    #F52F41;   /* PedidosYa red   — brandcolorcode.com confirmed */
  --py-2:  #C42233;
  --py-bg: #160004;
}

/* ── Platform toggle active — neutral default ── */
.plat-toggle button.active {
  background: var(--ink); color: #fff; border-color: var(--ink);
  box-shadow: 0 2px 8px rgba(27,31,42,.2);
}
/* platform-specific active tints applied via JS data-plat attr on wrapper */
[data-plat="fp"] .plat-toggle button.active { background:var(--fp); border-color:var(--fp); box-shadow:0 2px 12px rgba(233,30,140,.35) }
[data-plat="hs"] .plat-toggle button.active { background:var(--hs); border-color:var(--hs); box-shadow:0 2px 12px rgba(0,178,169,.35) }
[data-plat="py"] .plat-toggle button.active { background:var(--py); border-color:var(--py); box-shadow:0 2px 12px rgba(255,107,0,.35) }

/* ── Platform hero banners ── */
.plat-hero {
  position: relative; overflow: hidden;
  border-radius: var(--radius-xl);
  padding: clamp(22px,2.8vw,40px) clamp(24px,3.5vw,56px);
  margin-bottom: var(--gap-lg);
  box-shadow: 0 8px 40px -12px rgba(0,0,0,.4), 0 2px 8px rgba(0,0,0,.18);
}
/* No DH watermark on platform-specific banners — ::before suppressed */
.plat-hero::before { content: none }
.plat-hero-fp { background: linear-gradient(118deg, #16000A 0%, #250013 50%, #16000A 100%); box-shadow:0 8px 40px -12px rgba(215,15,100,.45),0 2px 8px rgba(0,0,0,.2) }
.plat-hero-hs { background: linear-gradient(118deg, #161000 0%, #241A00 50%, #161000 100%); box-shadow:0 8px 40px -12px rgba(255,195,0,.35),0 2px 8px rgba(0,0,0,.2) }
.plat-hero-py { background: linear-gradient(118deg, #160004 0%, #230008 50%, #160004 100%); box-shadow:0 8px 40px -12px rgba(245,47,65,.45),0 2px 8px rgba(0,0,0,.2) }

.plat-hero .ph-eyebrow {
  font-size:11px; font-weight:600; letter-spacing:.16em; text-transform:uppercase;
  color:rgba(255,255,255,.4); margin-bottom:12px;
  display:flex; align-items:center; gap:10px;
}
.plat-hero .ph-eyebrow .ph-dot {
  width:8px; height:8px; border-radius:50%; display:inline-block; flex-shrink:0;
}
.plat-hero .ph-title {
  font-size:clamp(20px,2.2vw,32px); font-weight:700; color:#fff;
  letter-spacing:-.025em; line-height:1.1; margin-bottom:4px;
}
.plat-hero .ph-sub {
  font-size:13px; color:rgba(255,255,255,.45); margin-bottom:28px;
}
.ph-kpis { display:flex; gap:0 }
.ph-kpi {
  flex:1; padding:0 28px 0 0; margin-right:28px;
  border-right:1px solid rgba(255,255,255,.1);
}
.ph-kpi:last-child { border-right:none; margin-right:0; padding-right:0 }
.ph-kpi .phv { font-size:clamp(28px,3vw,48px); font-weight:800; letter-spacing:-.03em; line-height:1; color:#fff }
.ph-kpi .phs { font-size:12px; font-weight:600; color:rgba(255,255,255,.5); margin-top:6px }
.ph-kpi .phl { font-size:11px; color:rgba(255,255,255,.3); margin-top:3px }

/* ── Per-platform accent card colors ── */
.stat-card.fp-card { background:linear-gradient(135deg,#300018 0%,#480025 100%); border-color:transparent; color:#fff; box-shadow:0 6px 24px -8px rgba(215,15,100,.4) }
.stat-card.fp-card .pct,.stat-card.fp-card .cnt { color:var(--fp) }
.stat-card.fp-card .cnt-sub { color:rgba(215,15,100,.8) }
.stat-card.fp-card .l { color:rgba(255,255,255,.6) }

.stat-card.hs-card { background:linear-gradient(135deg,#1E1800 0%,#2E2400 100%); border-color:transparent; color:#fff; box-shadow:0 6px 24px -8px rgba(255,195,0,.35) }
.stat-card.hs-card .pct,.stat-card.hs-card .cnt { color:var(--hs) }
.stat-card.hs-card .cnt-sub { color:rgba(255,195,0,.75) }
.stat-card.hs-card .l { color:rgba(255,255,255,.6) }

.stat-card.py-card { background:linear-gradient(135deg,#300010 0%,#480018 100%); border-color:transparent; color:#fff; box-shadow:0 6px 24px -8px rgba(245,47,65,.4) }
.stat-card.py-card .pct,.stat-card.py-card .cnt { color:var(--py) }
.stat-card.py-card .cnt-sub { color:rgba(245,47,65,.8) }
.stat-card.py-card .l { color:rgba(255,255,255,.6) }

/* ── KPI accent borders ── */
.kpi-green  { border-top:3px solid var(--green) }
.kpi-red    { border-top:3px solid var(--dh-red) }
.kpi-blue   { border-top:3px solid var(--blue) }
.kpi-blue-2 { border-top:3px solid var(--blue-2) }
.kpi-amber  { border-top:3px solid var(--amber) }
.kpi-mute   { border-top:3px solid var(--ink-mute) }
.kpi-faint  { border-top:3px solid var(--ink-faint) }
.kpi-purple { border-top:3px solid var(--purple) }
.kpi-red.kpi-tinted { background:var(--red-tint) }

/* ── Text helpers ── */
.text-muted  { font-size:var(--text-sm); color:var(--ink-mute); line-height:1.65; margin-bottom:16px }
.text-footer { font-size:var(--text-xs); color:var(--ink-faint); margin-top:12px; line-height:1.6 }

/* ── Metrics / region table ── */
.dt { border-collapse:collapse; width:100%; font-size:var(--text-sm) }
.dt th {
  padding:10px 14px; text-align:left; font-size:var(--text-xs);
  font-weight:600; letter-spacing:.06em; text-transform:uppercase;
  color:var(--ink-mute); border-bottom:2px solid var(--border);
}
.dt td { padding:10px 14px; border-bottom:1px solid var(--border); color:var(--ink-soft); vertical-align:top }
.dt td:first-child { font-weight:600; color:var(--ink) }
.dt tr:last-child td { border-bottom:none }
.dt tr:nth-child(even) { background:var(--surface-2) }
.dt-wrap { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-lg); overflow:hidden; box-shadow:var(--shadow-1); margin-bottom:24px }

/* ── Summary box ── */
.summary-box {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--radius-lg); padding:var(--gap-lg);
  margin-bottom:24px; box-shadow:var(--shadow-1);
}
.summary-box .sb-row { display:flex; gap:32px; flex-wrap:wrap; font-size:var(--text-base); line-height:1.8 }
.summary-box .sb-val { font-weight:700; font-size:var(--text-xl) }
.summary-box .sb-meta { font-size:var(--text-xs); color:var(--ink-mute); margin-top:2px }
.summary-box .sb-divider { margin-top:16px; padding-top:14px; border-top:1px solid var(--border); font-size:var(--text-sm); color:var(--ink-soft) }

/* ── Heatmap cell highlight ── */
.cell-hot { background:var(--amber-tint); color:var(--amber); font-weight:700; border-radius:var(--radius-sm) }

/* ── Tab section headers (non-exec tabs) ── */
.tab-header {
  position: relative; overflow: hidden;
  background: linear-gradient(118deg, #12141A 0%, #1B1F2A 100%);
  border-radius: var(--radius-xl);
  padding: clamp(20px,2.2vw,36px) clamp(24px,3.5vw,52px);
  margin-bottom: var(--gap-lg);
  box-shadow: 0 4px 24px -8px rgba(20,22,30,.35);
}
.tab-header::before {
  content: '';
  position: absolute; right: -30px; top: -30px;
  width: 260px; height: 260px;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 512 512'%3E%3Cpath fill-rule='evenodd' clip-rule='evenodd' fill='%23ffffff' d='M419.714 248.327c-.116.054-.179.107-.268.152l-46.63 19.034-1.431.653-11.357 51.964c-.76 1.772-3.032 2.193-4.561.707l-33.975-40.188-.161-.116-191.563 82.246c-.161.09-.349.126-.536.126a1.283 1.283 0 01-1.288-1.289c0-.412.205-.787.527-1.029l165.61-124.036-20.954-47.875c-1.046-2.175.885-4.501 3.443-3.857h.018l50.752 12.51 39.404-35.096v.009c1.699-1.333 4.015-.385 4.445 1.727l3.845 52.725 45.44 26.604c1.95 1.235 1.673 4.125-.76 5.029zM396.552 97.633C337.759 74.546 273.1 91.88 233.196 136.246l-155.7 166.9c-2.093 2.246-1.127 5.065 1.43 5.441l41.479 2.55c3.327.206 3.738 3.061 2.066 5.02L21.073 425.598c-1.77 1.897.358 4.877 2.781 4.126l144.772-45.772c3.059-1.056 5.42 1.673 4.123 4.071l-19.362 34.255c-1.002 1.951.877 4.627 3.309 4.448l208.715-46.515c49.876-7.901 94.404-41.208 114.196-91.642 29.807-75.687-7.432-161.164-83.055-190.936z'/%3E%3C/svg%3E");
  background-size: contain; background-repeat: no-repeat;
  opacity: 0.05; pointer-events: none;
}
.tab-header .th-eyebrow {
  font-size:11px; font-weight:600; letter-spacing:.16em; text-transform:uppercase;
  color:rgba(255,255,255,.35); margin-bottom:8px;
}
.tab-header .th-title {
  font-size:clamp(16px,1.6vw,24px); font-weight:700; color:#fff; letter-spacing:-.02em;
}
.tab-header .th-sub {
  font-size:13px; color:rgba(255,255,255,.4); margin-top:4px;
}
.tab-header .th-accent-bar {
  position:absolute; bottom:0; left:0; right:0; height:3px;
  background: linear-gradient(90deg, var(--dh-red) 0%, transparent 60%);
}

/* ── Animations ── */
@keyframes fadeUp {
  from { opacity:0; transform:translateY(8px) }
  to   { opacity:1; transform:translateY(0) }
}
@keyframes countUp {
  from { opacity:0; transform:translateY(4px) }
  to   { opacity:1; transform:translateY(0) }
}
.section.active { animation:fadeUp 360ms cubic-bezier(.2,.7,.2,1) both }
.hero-kpi { animation: countUp 500ms cubic-bezier(.2,.7,.2,1) both }
.hero-kpi:nth-child(1) { animation-delay: 80ms }
.hero-kpi:nth-child(2) { animation-delay: 160ms }
.hero-kpi:nth-child(3) { animation-delay: 240ms }
.hero-kpi:nth-child(4) { animation-delay: 320ms }
"""

JS = """
function showTab(id,el){
  document.querySelectorAll('.section').forEach(s=>s.classList.remove('active'));
  document.querySelectorAll('.nav a').forEach(a=>a.classList.remove('active'));
  document.getElementById('tab-'+id).classList.add('active');
  el.classList.add('active');
}
function showPlat(id,el){
  document.querySelectorAll('.plat-panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.plat-toggle button').forEach(b=>b.classList.remove('active'));
  document.getElementById('plat-'+id).classList.add('active');
  el.classList.add('active');
  const exec = document.getElementById('tab-exec');
  exec.dataset.plat = id === 'all' ? '' : id;
}
function filterGroups(){
  const region    = document.getElementById('f-region').value;
  const tag       = document.getElementById('f-tag').value;
  const editor    = document.getElementById('f-editor').value.toLowerCase();
  const search    = document.getElementById('f-search').value.toLowerCase();
  const singleton = document.getElementById('f-singleton').value;
  const rows = document.querySelectorAll('#gtbody tr');
  let n = 0;
  rows.forEach(row=>{
    const ok = (!region    || row.dataset.region  === region)
            && (!tag       || row.dataset.tags.includes(tag))
            && (!editor    || row.dataset.editor.includes(editor))
            && (!search    || row.dataset.brand.includes(search) || row.dataset.cat.includes(search))
            && (!singleton || row.dataset.singleton === singleton);
    row.style.display = ok ? '' : 'none';
    if(ok) n++;
  });
  document.getElementById('gcount').textContent = n + ' groups shown';
}
filterGroups();
"""

# ── Product moves tab ─────────────────────────────────────────────────────────
import datetime as _dt

def _oid_date(oid):
    ts = int(oid[:8], 16)
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).replace(tzinfo=None)

_CUTOFF = _dt.datetime(2026, 6, 1)

_extra_names = {
    '616eb9d3639d699332a39b78': 'Lee Kum Kee Tou Dao Salt Reduced Soy Sauce 500ml',
    '69db830ea54e78697f9b417f': 'Olio Orolio Extra Virgin Olive Oil 3L',
    '69397ac66cf311421a9e06f1': 'Nescafe Gold Coffee 95g',
    '61543ab285b805cf1f5af82b': 'Kewpie Aeru Pasta Sauce Meat Sauce 1 Packet',
    '61b20a0fc85fea0b0de43e14': 'Nescafe Gold Intense Coffee 200g',
    '6492eddb5f96750867dfddc2': 'Cheerios Cereal Frosted Whole Grain 340g',
    '6141ef8ae3ffd460cb3f300a': 'Dolmio Carbonara Pasta Sauce 490ml',
    '6179576a82df40c6ec8ef326': "Hershey's Cocoa Natural Unsweetened Powder 226g",
    '616edee5bf9d159fe752dd75': 'Nescafe Rich 3-in-1 Instant Coffee Mix 42x15g',
    '69ae11c5797898158a9e8d5e': 'Haidilao Large Chive 100g',
    '61a8a35c88857b7f2b7e1adb': 'Tapal Danedar 20g',
    '6180f80dc016b6a9b5e3ab6d': 'Great Taste Premium Instant Coffee 100g',
    '69a69b46de8fdd8ae357e355': 'EC Organic Fortified Sunflower Oil 1L',
    '6180f80bc016b6a9b5e3ab5f': 'Great Taste Premium Classic Sticks 36x2g',
    '61b21a363af203e7aaf47f20': 'Star Brand Food Colour Yellow Custard Color 2g',
    '621b52a45a9968d14ea1624d': 'Nescafe Gold Imported Jar 50g',
    '663cc3032427a63c7414eb4a': 'Oldtown White Coffee 3-in-1 Less Sugar 35g',
    '61b218991a7030b298e44c0c': 'Nescafe Gold Deluxe 200g',
    '61b20bad15e0170ca8e17db9': "Nescafe Gold All'Italiana Instant Coffee 200g",
    '63c910435ab63a255b599cf5': 'Nescafe Gold Blend Coffee 10x7g',
    '6239835897d125d7dfd95483': 'Dreem Yellow Food Colour 28ml',
    '68130a94f2eb9feb233b5f0b': 'Tapal Danedar 49g',
    '69db830ea54e78697f9b4183': 'Olio Orolio Extra Virgin Olive Oil 500ml',
    '61c019d23a0b73087069e2ee': 'Kimlan Grade-A Steam Fish Soy Sauce 590ml',
    '66fe5b4f7c75b9d47fe52d17': "Ten Ren's High Mountain Oolong Tea 75g",
    '67eca1ed30a88b0358b87b59': 'Knorr Pork Seasoning Powder 70g',
    '690c2212c20a0a18d3db0b2e': 'Great Taste 3-in-1 Cream-O Twin Pack 5x56g',
    '61e012bef4427bf9a97066e3': 'Betty Crocker Super Moist White Cake Mix 16.25oz',
    '6141fb3f28e0abe11b316bf1': 'Pillsbury Chakki Atta Whole Wheat Flour 2kg',
    '690daa0347565ae8307cd88d': 'Philippine Brand Nata De Coco Green 340g',
    '6256a5364c67c6158d387527': 'Ovaltine 3-in-1 Gold Packets 12x30g',
    '64b918952096e7cd189fa150': 'Nescafe 3In1 Arada Extra',
    '61e7af780822476efccd3948': 'Pusti Natural Black Tea 500g',
    '61b2240e523c3ee603f49062': 'Deksomboon Soy Sauce Formula 1 3x700ml',
    '66e11c3415f14e9131aa9cfb': 'Ready A1 Sardines in Tomato Sauce 150g',
    '683dae3c36d733d4721d5388': 'Mitr Phol Pure Refined Sugar Jar 220g',
    '6242cb11feb992fbd0aafd6b': 'Span Oliva Extra Virgin Olive Oil 5L',
    '61794a7363142898c68f177e': 'Canderel Red Sweetener 75g',
    '65cc64da786c8aca5beff6e0': 'Tartufi Jimmy Alfredo Truffle Sauce 180g',
    '6141bf5752b37303653f3180': 'Lao Gan Ma Chilli In Oil 210g',
    '61792826c34faa54ed918990': 'Nestle Coffee Mate 450g',
    '6200d6a10dfd62c593ea7a90': 'Shao Feiyan Lamb Soup Red Date 170g',
    '616ed6f153be2455a215dcef': 'Ajinomoto Cook Master Bonito Seasoning 192g',
    '6176a095818b542f9721b642': 'Lao Gan Ma Dried Shredded Pork with Oil Chili 260g',
    '617950b87cdf29f8ad8b818c': 'Custard Powder 200g',
    '61546cb5c55f6df5b23f7a73': 'Sky Dragon Pork & Ham 340g',
    '63524f9493262b199f1625f5': 'Topvalu Green Tea 525ml',
    '668fc714f78510f2934bb7de': 'Nescafe Creamy White Twin Pack Sugar Free 2x11.2g',
    '61b226786375d0723b325172': 'Sandee Premium Jasmine Rice 5kg',
    '69e58a687b23648b6653dc74': 'Haidilao Yihai Hot Pot Soup Base 110g',
    '69d19704d5ad94573fbf6dc0': 'Chiltan Pure Carrot Murabba 600g',
    '698a634f4cced6a0c854086b': 'Nescafe 3-in-1 White Coffee 30g',
    '66e0046b12af4c07db9277be': 'Haidilao Supreme Triple Fresh Soup Base 1200g',
    '67d7fd217fd2033b877f3e3e': 'Spain Olive Extra Virgin Olive Oil 2L',
    '616eb9d3639d699332a39b7a': 'Lee Kum Kee Tou Dao Salt Reduced Soy Sauce 500ml',
    '65c33903c2cc898ab590027a': 'Product (65c33903)',
    '65ce22b3ac1cf3ccb9a22482': 'Product (65ce22b3)',
    '65786fe7d48915a56baa8487': 'Product (65786fe7)',
    '65ce0e38e0ff686cb68bb71c': 'Product (65ce0e38)',
}
for pid, name in _extra_names.items():
    if pid not in product_name_map:
        product_name_map[pid] = name

# Build product moves from the real_changes diff results
real_changes = json.load(open('/tmp/real_changes.json'))

# ── Product-level metrics ─────────────────────────────────────────────────────
_over_split_gids      = [gid for gid, d in real_changes.items() if d['tag'] == 'OVER_SPLIT']
_over_grouped_gids    = [gid for gid, d in real_changes.items() if d['tag'] == 'OVER_GROUPED']
_singleton_gids       = [gid for gid, d in real_changes.items() if d['tag'] == 'OVER_GROUPED' and d['vfin_count'] == 1]
_corrected_valid_gids = [gid for gid, d in real_changes.items() if d['tag'] == 'OVER_GROUPED' and d['vfin_count'] >= 2]
singleton_count       = len(_singleton_gids)
corrected_valid_count = len(_corrected_valid_gids)

# Products in confirmed groups (no change — validated as-is)
_prod_confirmed = set()
for gid in confirmed_meta:
    _prod_confirmed |= _pids(gid, 'v1')

# Products in extended groups: already-in (model placed) + added by editor
_prod_extended_model  = set()  # model placed, editor kept
_prod_extended_added  = set()  # editor added (anchor + preferred signals)
for gid in _over_split_gids:
    d = real_changes[gid]
    _prod_extended_added  |= set(d['added'])
    _prod_extended_model  |= (_pids(gid, 'vfinal') - set(d['added']))

# Products removed (correction signal)
_prod_removed = set()
for gid in _over_grouped_gids:
    _prod_removed |= set(real_changes[gid]['removed'])

# Products that stayed in corrected groups (model got the core right, removed the outliers)
_prod_corrected_kept = set()
for gid in _over_grouped_gids:
    _prod_corrected_kept |= _pids(gid, 'vfinal')

# Review coverage at product level
# "reviewed" = any product in a group an editor opened
_prod_reviewed_total = _prod_confirmed | _prod_extended_model | _prod_extended_added | _prod_removed | _prod_corrected_kept
_prod_validated = _prod_confirmed | _prod_extended_model | _prod_extended_added | _prod_corrected_kept
# _total_groups is set by the data loader above

# Destination group titles (for removed products' destinations outside the 55)
_dest_group_titles = {
    '6a0ebd3ae8868e33cff22f8e': 'Dolmio Mushroom Pasta Sauce',
    '69f06b6e243dcf878390e8f1': 'Tapal Danedar Tea Bags',
    '69c2f1a98da86790b61e5251': 'Nescafe Gold',
    '69c2c27b8da86790b6fa718e': 'Nescafe Gold Instant Coffee',
    '69c2bfba8da86790b6f8d9af': 'Ecorganic Fortified Sunflower Oil',
    '69f06b6d243dcf878390e8cc': 'Tapal Danedar Tea',
    '6a1d001ba9f2f5520353913b': 'Haidilao Hot Pot Soup Base',
    '6a268d4ba0217799022bc867': 'Great Taste Premium Instant Coffee',
    '6a27a4007c6c2e548c8210bc': "Hershey's Cocoa Powder",
    '6a27a3fe7c6c2e548c821026': 'Star Brand Food Colouring',
    '6a278142307814f5e7f6c623': 'Dreem Food Colour',
    '6a278143307814f5e7f6c635': 'OldTown White Coffee 3-in-1',
    '69c2f1f98da86790b61e93bf': 'Olio Orolio Extra Virgin Olive Oil',
}

# Lookup where removed products currently live (fetched previously)
_removed_dest = {
    '69a69b46de8fdd8ae357e355': '69c2bfba8da86790b6f8d9af',  # EC Organic → pre-existing Ecorganic group
    '67d7fd217fd2033b877f3e3e': None,                          # Spain Olive → ungrouped
    '6180f80bc016b6a9b5e3ab5f': '6a268d4ba0217799022bc867',  # Great Taste Classic → new GT group
    '6180f80dc016b6a9b5e3ab6d': '6a268d4ba0217799022bc867',  # Great Taste Premium → new GT group
    '69db830ea54e78697f9b4183': '69c2f1f98da86790b61e93bf',  # Olio Orolio 500ml → pre-existing
    '69db830ea54e78697f9b417f': '69c2f1f98da86790b61e93bf',  # Olio Orolio 3L → pre-existing
    '6141ef8ae3ffd460cb3f300a': '6a0ebd3ae8868e33cff22f8e',  # Dolmio Carbonara → pre-existing Dolmio
    '69397ac66cf311421a9e06f1': '69c2c27b8da86790b6fa718e',  # Nescafe Gold 95g → pre-existing
    '6179576a82df40c6ec8ef326': '6a27a4007c6c2e548c8210bc',  # Hershey's Cocoa → new group
    '621b52a45a9968d14ea1624d': '69c2c27b8da86790b6fa718e',  # Nescafe Gold Jar 50g → pre-existing
    '663cc3032427a63c7414eb4a': '6a278143307814f5e7f6c635',  # OldTown Less Sugar → new group
    '66e0046b12af4c07db9277be': None,                         # Haidilao Supreme → ungrouped
    '616eb9d3639d699332a39b7a': '6a25efd3e2fe947ab3302482',  # Lee Kum Kee → stayed singleton in same group
    '61b21a363af203e7aaf47f20': '6a27a3fe7c6c2e548c821026',  # Star Brand → new group
    '69d19704d5ad94573fbf6dc0': None,                         # Chiltan Carrot → ungrouped
    '6239835897d125d7dfd95483': '6a278142307814f5e7f6c623',  # Dreem Yellow → new group
    '69ae11c5797898158a9e8d5e': '6a1d001ba9f2f5520353913b',  # Haidilao Chive → new group
    '69e58a687b23648b6653dc74': None,                         # Haidilao Yihai → ungrouped
    '61543ab285b805cf1f5af82b': None,                         # Kewpie Aeru → ungrouped
    '668fc714f78510f2934bb7de': None,                         # Nescafe Creamy → ungrouped
    '63c910435ab63a255b599cf5': '69c2c27b8da86790b6fa718e',  # Nescafe Gold Blend → pre-existing
    '61b20a0fc85fea0b0de43e14': '69c2f1a98da86790b61e5251',  # Nescafe Gold Intense → pre-existing
    '61b218991a7030b298e44c0c': '69c2f1a98da86790b61e5251',  # Nescafe Gold Deluxe → pre-existing
    '61b20bad15e0170ca8e17db9': '69c2f1a98da86790b61e5251',  # Nescafe Gold Italiana → pre-existing
    '698a634f4cced6a0c854086b': None,                         # Nescafe 3-in-1 White → ungrouped
    '61546cb5c55f6df5b23f7a73': None,                         # Sky Dragon → ungrouped
    '61a8a35c88857b7f2b7e1adb': '69f06b6e243dcf878390e8f1',  # Tapal Danedar 20g → pre-existing
    '68130a94f2eb9feb233b5f0b': '69f06b6d243dcf878390e8cc',  # Tapal Danedar 49g → pre-existing
    '63524f9493262b199f1625f5': None,                         # Topvalu Green Tea → ungrouped
    '61b226786375d0723b325172': None,                         # Sandee Rice → ungrouped
    '616edee5bf9d159fe752dd75': None,                         # Nescafe Rich 3-in-1 → ungrouped
    '6492eddb5f96750867dfddc2': None,                         # Cheerios → ungrouped
    '65c33903c2cc898ab590027a': None,
    '65ce22b3ac1cf3ccb9a22482': None,
    '65786fe7d48915a56baa8487': None,
    '65ce0e38e0ff686cb68bb71c': None,
}

def _dest_label(dest_gid):
    if dest_gid is None:
        return ('Ungrouped — not in any group today', False, '#95a5a6')
    is_new = _oid_date(dest_gid) >= _CUTOFF
    name = _dest_group_titles.get(dest_gid, '') or group_title_map.get(dest_gid, '')
    suffix = ' (model-created)' if is_new else ' (pre-existing)'
    label = (name + suffix) if name else ('Model-created group' if is_new else 'Pre-existing group (before Jun 2026)')
    color = '#27ae60' if is_new else '#8e44ad'
    return (label, is_new, color)

# Products that came from a pre-existing group (BQ confirmed)
_from_preexisting = {
    '61792826c34faa54ed918990': ('69c2bf728da86790b6f8af64', 'Nestle Coffee Mate',  '2026-03-24'),
    '617950b87cdf29f8ad8b818c': ('69c2e6088da86790b614d577', 'Custard Powder',      '2026-03-24'),
}

# Build move entries — three signal types:
#   'anchor'     — ungrouped product pulled into model group (model concept extended)
#   'preferred'  — product moved FROM pre-existing group INTO model group (model preferred)
#   'removed'    — product removed from model group (sent elsewhere or ungrouped)
move_entries = []

for gid, d in real_changes.items():
    tag = d['tag']
    if tag == 'OVER_SPLIT':
        for pid in d['added']:
            pname = display_name(pid)
            if pid in _from_preexisting:
                src_gid, src_title, src_date = _from_preexisting[pid]
                move_entries.append((pname, src_gid, src_title, gid, 'preferred'))
            else:
                move_entries.append((pname, None, None, gid, 'anchor'))
    elif tag == 'OVER_GROUPED':
        for pid in d['removed']:
            pname = display_name(pid)
            dest_gid = _removed_dest.get(pid)
            move_entries.append((pname, gid, None, dest_gid, 'removed'))

move_entries.sort(key=lambda x: x[0])

# Signal counts
anchor_count    = sum(1 for e in move_entries if e[4] == 'anchor')
preferred_count = sum(1 for e in move_entries if e[4] == 'preferred')
removed_count   = sum(1 for e in move_entries if e[4] == 'removed')

# Signal colour/label
SIG_COLOR = {
    'anchor':    '#27ae60',
    'preferred': '#2980b9',
    'removed':   '#e74c3c',
}
SIG_LABEL = {
    'anchor':    'Model group used as anchor',
    'preferred': 'Model group preferred over pre-existing',
    'removed':   'Product removed from model group',
}
SIG_CLASS = {
    'anchor':    'badge badge-green',
    'preferred': 'badge badge-blue',
    'removed':   'badge badge-red',
}

moves_rows = ''
for (pname, from_gid, from_title_override, dest_gid, signal) in move_entries:
    if signal == 'anchor':
        from_cell = '<span class="badge badge-neutral">Ungrouped &mdash; no prior group</span>'
        to_title  = group_title_map.get(dest_gid, dest_gid or '')
        to_cell   = '<span class="badge badge-green">' + esc(to_title) + ' (model-created)</span>'
        note      = 'Product was ungrouped. Editor pulled it into the model\'s new group — confirming the grouping concept is correct and broader than the model\'s original input.'

    elif signal == 'preferred':
        from_cell = '<span class="badge badge-red">' + esc(from_title_override or from_gid) + ' (pre-existing)</span>'
        to_title  = group_title_map.get(dest_gid, dest_gid or '')
        to_cell   = '<span class="badge badge-blue">' + esc(to_title) + ' (model-created)</span>'
        note      = 'Product was already in a pre-existing group. Editor moved it into the model\'s new group — the model\'s grouping was more suitable even for products outside its original input.'

    else:  # removed
        from_title_r = group_title_map.get(from_gid, from_gid or '')
        from_cell = '<span class="badge badge-red">' + esc(from_title_r) + '</span>'
        dest_lbl, dest_is_new, dest_color = _dest_label(dest_gid)
        to_cls = 'badge badge-green' if dest_is_new else ('badge badge-purple' if dest_gid else 'badge badge-neutral')
        to_cell = '<span class="' + to_cls + '">' + esc(dest_lbl) + '</span>'
        if dest_gid is None:
            note = 'Removed — not placed in any group today'
        elif dest_is_new:
            note = 'Removed and placed in another model-created group (Jun 2026)'
        else:
            note = 'Removed and placed in a group that already existed before Jun 2026'

    sig_badge = '<span class="' + SIG_CLASS[signal] + '">' + SIG_LABEL[signal] + '</span>'

    moves_rows += (
        '<tr style="border-bottom:1px solid #eee">'
        '<td style="padding:10px 12px;font-size:12px;font-weight:600;color:#2c3e50">' + esc(pname) + '</td>'
        '<td style="padding:10px 12px">' + sig_badge + '</td>'
        '<td style="padding:10px 12px">' + from_cell + '</td>'
        '<td style="padding:10px 12px;font-size:18px;color:#95a5a6;text-align:center">&rarr;</td>'
        '<td style="padding:10px 12px">' + to_cell + '</td>'
        '<td style="padding:10px 12px;font-size:11px;color:#7f8c8d">' + note + '</td>'
        '</tr>'
    )

print(f"Built {len(move_entries)} product moves ({anchor_count} anchor, {preferred_count} preferred, {removed_count} removed)")

# ── Platform logo SVGs (white fill for dark backgrounds) ──────────────────────
SVG_FP = (
    '<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" fill="white">'
    '<path d="M4.224 0a3.14 3.14 0 00-3.14 3.127 3.1 3.1 0 001.079 2.36 11.811 11.811 0 00-2.037 6.639C.126 18.68 5.458 24 12 24c6.542 0 11.874-5.32 11.874-11.874a11.69 11.69 0 00-2.025-6.614 3.136 3.136 0 001.09-2.373A3.132 3.132 0 0019.8.012a3.118 3.118 0 00-2.636 1.438A11.792 11.792 0 0012.012.264c-1.845 0-3.595.419-5.152 1.174A3.133 3.133 0 004.224 0zM12 1.198c1.713 0 3.331.396 4.78 1.102a10.995 10.995 0 014.29 3.715 10.89 10.89 0 011.882 6.135c.011 6.039-4.901 10.951-10.94 10.951-6.04 0-10.951-4.912-10.951-10.951 0-2.277.694-4.386 1.88-6.135A11.08 11.08 0 017.232 2.3 10.773 10.773 0 0112 1.198zM7.367 6.345c-.853.012-1.743.292-2.28.653-1.031.682-2.29 2.156-2.085 4.181.191 2.025 1.785 3.283 2.612 3.283.826 0 1.234-.42 1.485-1.45.252-1.018 1.115-2.192 2.217-3.45s-.024-2.469-.024-2.469c-.393-.513-1.052-.727-1.755-.747a3.952 3.952 0 00-.17-.001zm9.233.007l-.17.001c-.702.02-1.358.233-1.746.752 0 0-1.126 1.21-.024 2.469 1.114 1.258 1.965 2.432 2.217 3.45.251 1.019.659 1.438 1.485 1.45.827 0 2.409-1.258 2.612-3.283.204-2.025-1.054-3.51-2.084-4.182-.544-.36-1.437-.643-2.29-.657zm-8.962 2c.348 0 .624.275.624.623-.012.335-.288.623-.624.623a.619.619 0 01-.623-.623c0-.348.276-.624.623-.624zm8.891 0c.348 0 .623.275.623.623-.012.335-.287.623-.623.623a.619.619 0 01-.623-.623c0-.348.288-.624.623-.624zm-4.541 4.025c-.527 0-2.06.096-2.06.587 0 .887 1.88 1.522 2.06 1.474.18.048 2.06-.587 2.06-1.474 0-.49-1.52-.587-2.06-.587zM9.076 15.17c0 1.414 1.294 2.564 2.912 2.564 1.618 0 2.924-1.15 2.924-2.564z"/>'
    '</svg>'
)

SVG_HS = (
    '<svg viewBox="0 0 497 254" xmlns="http://www.w3.org/2000/svg" fill="white">'
    # Outer pill border
    '<path d="M248.207 253.161C186.023 253.161 123.92 252.829 62.3171 252.161C29.6234 251.546 0.985281 223.691 0.916541 192.528C-0.305514 148.771 -0.305514 104.392 0.916541 60.6344C0.985281 29.472 29.6234 1.62065 62.2599 1.00581C186 -0.334632 311.25 -0.334632 434.574 1.00199C466.015 0.440608 494.745 28.1583 496.078 60.4511C497.307 104.365 497.307 148.806 496.078 192.628C494.76 224.669 466.626 252.172 435.571 252.172C435.239 252.172 434.906 252.172 434.578 252.165C372.677 252.833 310.398 253.165 248.207 253.165V253.161ZM248.791 13.8603C186.585 13.8603 124.333 14.1964 62.4661 14.8647C37.5362 15.3344 14.7792 37.2015 14.7716 60.7375V60.9285C13.5533 104.495 13.5533 148.672 14.7716 192.238V192.429C14.7792 215.969 37.54 237.836 62.5234 238.306C185.626 239.639 310.81 239.639 434.536 238.306H434.647H434.757C458.515 238.776 481.222 216.809 482.234 192.147C483.449 148.672 483.449 104.495 482.234 60.9285C481.234 36.6325 459.099 14.8494 435.559 14.8494C435.292 14.8494 435.025 14.8494 434.757 14.857H434.647H434.536C372.956 14.1887 310.898 13.8565 248.795 13.8565L248.791 13.8603Z"/>'
    # H
    '<path d="M79.7927 120.859V97.7275L97.3178 94.7564V120.859H115.286V54.505H97.3178V79.286L79.7927 82.2647V54.505H61.8247V120.859H79.7927Z"/>'
    # U
    '<path d="M132.005 116.181C137.195 120.435 143.962 122.593 152.119 122.593C160.276 122.593 167.032 120.435 172.23 116.181C177.465 111.896 180.116 106.137 180.108 99.0604V54.4478H161.265V97.4679C161.265 102.643 158.275 105.159 152.119 105.159C145.963 105.159 142.969 102.643 142.969 97.4679V54.4478H124.126V99.0604C124.126 106.152 126.777 111.911 132.005 116.181Z"/>'
    # N
    '<path d="M207.116 88.7684L230.953 120.912H245.244V54.4554H226.779V86.6031L202.949 54.4554H188.552V120.912H207.116V88.7684Z"/>'
    # G
    '<path d="M286.709 122.593C305.582 122.593 317.795 111.51 320.747 96.2954C321.378 93.0455 321.347 86.4961 320.747 82.3182C320.24 78.7857 319.495 76.2996 319.495 76.2996L287.087 81.3482L289.302 95.5393L303.826 93.2709C303.314 99.4346 296.597 104.128 289.348 104.449C279.996 104.861 271.399 99.5453 269.956 90.2959C269.257 85.8163 270.341 81.3291 273.019 77.6668C275.692 74.0044 279.629 71.6023 284.109 70.9034C288.287 70.2504 292.533 71.1708 296.066 73.4965L296.379 73.7027L311.49 63.067L311.013 62.6049C303.054 54.9136 292.239 51.4766 281.348 53.1798C262.326 56.151 249.269 74.0426 252.24 93.0646C254.928 110.269 269.822 122.593 286.717 122.593H286.709Z"/>'
    # E
    '<path d="M374.438 104.113H346.598V95.6119H371.47V79.0912H346.598V71.1517H373.941V54.4478H327.832V120.912H374.438V104.113Z"/>'
    # R
    '<path d="M426.035 98.0445C432.535 93.5993 436.399 86.3204 436.453 78.4573C436.479 72.0377 434.035 66.0076 429.506 61.4822C425.004 56.9529 418.974 54.4515 412.585 54.4515H382.159V120.916H400.666V103.494L408.556 102.772L419.226 120.916H439.454L426.035 98.0483V98.0445ZM410.53 87.2637L400.666 88.2375V70.9531H409.182C413.44 70.9531 417.301 74.012 417.759 78.2434C418.256 82.7994 414.968 86.7863 410.53 87.2637Z"/>'
    # STATION row
    '<path d="M106.071 164.33C103.608 162.787 101.267 161.626 99.1127 160.889L90.9478 158.105C86.3002 156.791 83.1496 155.63 81.5838 154.652C80.125 153.744 79.4147 152.69 79.4147 151.425C79.4147 150.333 79.8577 149.428 80.8315 148.741C81.9428 147.958 83.3405 147.622 85.334 147.622C90.1917 147.622 93.5791 149.921 95.6909 154.652L95.9392 155.214L111.906 146.35L111.673 145.873C109.244 140.881 105.677 137.017 101.072 134.389C96.4853 131.773 91.1579 130.448 85.2347 130.448C78.2155 130.448 72.2733 132.396 67.5684 136.238C62.8291 140.114 60.427 145.25 60.427 151.502C60.427 157.31 62.3708 161.92 66.2089 165.193C69.9896 168.42 75.1642 170.952 81.5876 172.72C86.052 173.942 88.9773 174.82 90.291 175.332C93.1819 176.424 94.5491 177.799 94.7744 179.449C94.9577 180.82 94.6675 183.218 87.0296 183.329C80.6291 183.424 76.2946 180.564 73.7741 174.881L73.5259 174.32L57.2993 183.252L57.475 183.711C59.4341 188.839 62.936 192.968 67.8854 195.985C72.8041 198.986 79.0786 200.506 86.537 200.506C94.6064 200.506 101.194 198.627 106.117 194.915C111.096 191.177 113.625 185.91 113.625 179.262C113.625 175.935 112.998 172.983 111.769 170.489C110.52 167.977 108.603 165.903 106.071 164.326V164.33Z"/>'
    '<path d="M168.384 132.117H115.297V149.447H132.429V198.83H151.263V149.447H168.384V132.117Z"/>'
    '<path d="M221.383 149.447H238.507V198.83H257.342V149.447H274.473V132.117H221.383V149.447Z"/>'
    '<path d="M299.988 132.117H281.153V198.83H299.988V132.117Z"/>'
    '<path d="M421.239 164.394L397.42 132.254H383.026V198.692H401.583V166.56L425.413 198.692H439.695V132.254H421.239V164.394Z"/>'
    '<path d="M206.714 132.132H183.667L161.254 198.738H181.574L185.542 186.071L205.034 182.779L210.606 198.742H229.987L206.718 132.136L206.714 132.132ZM190.033 170.974L196.235 151.044L201.895 169.107L190.033 170.974Z"/>'
    '<path d="M341.442 130.429C322.137 130.429 306.403 146.163 306.403 165.468C306.403 184.772 322.137 200.506 341.442 200.506C360.747 200.506 376.481 184.772 376.481 165.468C376.481 146.163 360.773 130.429 341.442 130.429ZM341.442 182.653C331.956 182.653 324.257 174.954 324.257 165.468C324.257 155.981 331.956 148.282 341.442 148.282C350.928 148.282 358.627 155.981 358.627 165.468C358.627 174.954 350.928 182.653 341.442 182.653Z"/>'
    '</svg>'
)

SVG_PY = (
    '<svg viewBox="0 .1 36 34.6" xmlns="http://www.w3.org/2000/svg" fill="white">'
    '<path d="m91.1 9.3c-1.3 0-2.3 1-2.3 2.4 0 1.1.7 1.7 1.8 1.7 1.3 0 2.3-1 2.3-2.4 0-1-.6-1.7-1.8-1.7zm-4.1.6c0-.1-.1-.1-.2-.1h-2.9c-.1 0-.2.1-.3.2 0 0-1.1 5.6-1.2 5.9-.7-1.3-2.2-2.1-3.9-2.1-3.5 0-6.1 2.8-6.1 6.7 0 3.2 1.9 5.4 4.8 5.4 1.4 0 2.6-.6 3.5-1.6-.1.3-.1.6-.2.9 0 .1 0 .2.1.2 0 .1.1.1.2.1h2.9c.1 0 .2-.1.3-.2l3.1-15.3zm-5.6 10c-.1.4-.2.8-.4 1.1s-.4.7-.7.9c-.3.3-.6.5-.9.6-.6.3-1.4.3-2 0-.3-.1-.5-.3-.7-.5s-.3-.5-.4-.8-.1-.6-.1-1 .1-.9.3-1.3.4-.8.7-1.1.6-.5 1-.7.8-.3 1.2-.3.7.1 1 .2.5.3.7.6c.2.2.3.5.4.9 0 .6 0 1-.1 1.4zm10.2-4.8h-3c-.1 0-.2.1-.3.2l-2 10c0 .1 0 .1.1.2s.1.1.2.1h3c.1 0 .2-.1.3-.2l2-10c0-.1 0-.1-.1-.2-.1 0-.2-.1-.2-.1zm-43.6 9.6c.3-1.4.9-4.4.9-4.4h3.5c4 0 6.6-2.2 6.6-5.6 0-3-2.1-4.9-5.6-4.9h-6c-.1 0-.2.1-.3.2l-3 15.2c0 .1 0 .2.1.2 0 .1.1.1.2.1h2.7c-.1.1.7.1.9-.8zm4.5-7.7h-3c0-.1.8-3.8.8-4h2.8c1.3 0 2.1.6 2.1 1.7-.1 1.5-1 2.3-2.7 2.3zm61.3-3.1c-3.9 0-6.8 2.8-6.8 6.7 0 3.3 2.3 5.4 5.8 5.4 3.9 0 6.8-2.8 6.8-6.7-.1-3.3-2.3-5.4-5.8-5.4zm-.8 8.7c-1.5 0-2.4-.8-2.4-2.2 0-1.8 1.2-3 2.9-3 1.5 0 2.4.8 2.4 2.2 0 1.7-1.2 3-2.9 3zm-5.6-12.7c0-.1-.1-.1-.2-.1h-2.9c-.1 0-.2.1-.3.2 0 0-1.1 5.6-1.2 5.9-.7-1.3-2.2-2.1-3.9-2.1-3.5 0-6.1 2.8-6.1 6.7 0 3.2 1.9 5.4 4.8 5.4 1.4 0 2.6-.6 3.5-1.6-.1.3-.1.6-.2.9 0 .1 0 .2.1.2 0 .1.1.1.2.1h2.9c.1 0 .2-.1.3-.2l3.1-15.3s-.1 0-.1-.1zm-5.5 10c-.1.4-.2.8-.4 1.1s-.4.7-.7.9c-.3.3-.6.5-.9.6-.6.3-1.4.3-2 0-.3-.1-.5-.3-.7-.5s-.3-.5-.4-.8-.1-.6-.1-1 .1-.9.3-1.3.4-.8.7-1.1.6-.5 1-.7.8-.3 1.2-.3.7.1 1 .2.5.3.7.6c.2.2.3.5.4.9-.1.6-.1 1-.1 1.4zm27.2-5.1c-1.5-.7-3.1-1-4.5-.9-2.5.2-3.9 1.7-3.7 3.9.1 1.3.9 2.3 2.3 3.1l1.6.8c.6.3.7.5.7.7 0 .5-.5.6-.8.6-1 .1-2.2-.3-3.5-1.1h-.2c-.1 0-.1.1-.2.1l-1.3 2.2c-.1.1 0 .3.1.3 1.6 1.1 3.4 1.6 5.2 1.4 2.8-.2 4.2-1.7 4-4-.1-1.4-.8-2.3-2.3-3.1l-1.4-.7c-.8-.5-.9-.6-.9-.8 0-.1 0-.5.8-.6s1.8.2 2.8.7h.2c.1 0 .1-.1.1-.1l1-2.3c.2 0 .1-.2 0-.2zm15.7-4.5c0-.1-.1-.2-.2-.2h-4.1c-.1 0-.2 0-.2.1 0 0-4.1 5.8-4.4 6.3-.1-.5-2.1-6.2-2.1-6.2 0-.1-.1-.2-.3-.2h-3.6c-.1 0-.2 0-.2.1-.1.1-.1.2 0 .3l3.8 9.6-1.1 5.6c0 .1 0 .2.1.2.1.1.1.1.2.1h2.9c.4 0 .7-.3.8-.6l1-5.2 7.5-9.7c-.1 0-.1-.1-.1-.2zm8.7 4.3c0-.1-.1-.1-.2-.1h-2.8c-.1 0-.2.1-.2.2 0 0-.1.5-.2.9-.6-.8-1.6-1.2-2.6-1.3-.9-.1-1.8.1-2.6.4-2.8 1.1-4.6 3.8-4.6 6.8 0 2.6 1.7 4.4 4.3 4.5 1.4.1 2.6-.4 3.7-1.3-.1.3-.1.6-.2.9 0 .1 0 .1.1.2 0 .1.1.1.2.1h2.8c.1 0 .2-.1.2-.2l2.2-10.9c-.1 0-.1-.1-.1-.2zm-4.4 5.6c-.1.4-.2.8-.4 1.1s-.4.6-.7.9l-.9.6c-.6.3-1.4.3-1.9 0-.3-.1-.5-.3-.7-.5s-.3-.4-.4-.7-.1-.6-.1-1 .1-.8.3-1.2.4-.7.7-1 .6-.5.9-.7c.4-.2.7-.2 1.1-.2.3 0 .7.1.9.2.3.1.5.3.7.5s.3.5.4.9c.2.3.2.7.1 1.1zm-83.2-6.3c-3.9 0-6.7 2.8-6.7 6.7 0 3.3 2.3 5.4 5.9 5.4 1.6 0 3-.4 4.4-1.4.1-.1.1-.3 0-.4l-1.8-1.8c-.1-.1-.2-.1-.3 0-.8.4-1.5.6-2.3.6-1.7 0-2.6-.8-2.6-2.1h8.1c.1 0 .2-.1.3-.2.2-.8.3-1.4.3-2.1-.1-2.9-2.1-4.7-5.3-4.7zm-3 4.5c.5-1.3 1.5-1.9 2.9-1.9 1.5 0 2.2.7 2.2 2-.2-.1-4.9-.1-5.1-.1zm-39-18.3h-23.3c-.4 0-.6.2-.6.6v3.3c0 3.7 2.6 5.7 7.3 5.7h16.7c1.8 0 3.2 1.4 3.2 3.2s-1.4 3.2-3.2 3.2h-18.8c-.3 0-.5.2-.6.4l-4.3 17.5c0 .2 0 .4.1.5s.3.2.5.2h5.9c2.2 0 3.3-1.8 3.5-2.8l1.7-6.3h11.9c7 0 12.8-5.7 12.8-12.8 0-7-5.7-12.7-12.8-12.7z"/>'
    '</svg>'
)

# ── Derived display values (computed from BQ data) ───────────────────────────
_pre_total  = _pre_fp + _pre_hs + _pre_py
_tot_fp     = _pre_fp + _new_fp
_tot_hs     = _pre_hs + _new_hs
_tot_py     = _pre_py + _new_py
_tot_all    = _pre_total + _total_groups
_growth_all = round(_total_groups / _pre_total * 100) if _pre_total else 0
_growth_fp  = round(_new_fp / _pre_fp * 100) if _pre_fp else 0
_growth_hs  = round(_new_hs / _pre_hs * 100) if _pre_hs else 0
_growth_py  = round(_new_py / _pre_py * 100) if _pre_py else 0

_rev_total  = _rev_fp + _rev_hs
_broken     = singleton_count                      # 19
_good       = _rev_total - _broken                 # 189
_good_pct   = round(_good / _rev_total * 100) if _rev_total else 0
_bad_pct    = round(_broken / _rev_total * 100) if _rev_total else 0
_cov_pct    = f'{_rev_total / _total_groups * 100:.2f}' if _total_groups else '0'
_uncov      = _total_groups - _rev_total

_fp_good    = _rev_fp - _broken
_fp_good_pct = round(_fp_good / _rev_fp * 100) if _rev_fp else 0
_fp_bad_pct  = round(_broken / _rev_fp * 100) if _rev_fp else 0
_fp_cov_pct  = f'{_rev_fp / _new_fp * 100:.1f}' if _new_fp else '0'
_hs_cov_pct  = f'{_rev_hs / _new_hs * 100:.2f}' if _new_hs else '0'

def _fmt(n):
    return f'{n:,}'

# ── Assemble HTML ─────────────────────────────────────────────────────────────
html = (
    '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">'
    '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
    '<title>Variant Groups — Auto-Grouping Launch Report · June 2026</title>'
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">'
    '<style>' + CSS + '</style></head><body>'

    '<div class="header">'
    '<div class="header-left">'
    '<svg width="24" height="24" viewBox="0 0 512 512" aria-label="Delivery Hero" style="flex-shrink:0">'
    '<path fill-rule="evenodd" clip-rule="evenodd" fill="#D61F26" d="M419.714 248.327c-.116.054-.179.107-.268.152l-46.63 19.034-1.431.653-11.357 51.964c-.76 1.772-3.032 2.193-4.561.707l-33.975-40.188-.161-.116-191.563 82.246c-.161.09-.349.126-.536.126a1.283 1.283 0 01-1.288-1.289c0-.412.205-.787.527-1.029l165.61-124.036-20.954-47.875c-1.046-2.175.885-4.501 3.443-3.857h.018l50.752 12.51 39.404-35.096v.009c1.699-1.333 4.015-.385 4.445 1.727l3.845 52.725 45.44 26.604c1.95 1.235 1.673 4.125-.76 5.029zM396.552 97.633C337.759 74.546 273.1 91.88 233.196 136.246l-155.7 166.9c-2.093 2.246-1.127 5.065 1.43 5.441l41.479 2.55c3.327.206 3.738 3.061 2.066 5.02L21.073 425.598c-1.77 1.897.358 4.877 2.781 4.126l144.772-45.772c3.059-1.056 5.42 1.673 4.123 4.071l-19.362 34.255c-1.002 1.951.877 4.627 3.309 4.448l208.715-46.515c49.876-7.901 94.404-41.208 114.196-91.642 29.807-75.687-7.432-161.164-83.055-190.936z"/>'
    '</svg>'
    '<div class="header-divider"></div>'
    '<span class="header-title">Variant Grouping</span>'
    '<span class="header-sub">Auto-Grouping Launch Report</span>'
    '</div>'
    '<span class="header-badge">June 2026</span>'
    '</div>'

    '<div class="nav">'
    '<a class="active" onclick="showTab(\'exec\',this)">Executive Summary</a>'
    '<a onclick="showTab(\'overview\',this)">Detail: Overview</a>'
    '<a onclick="showTab(\'issues\',this)">What went wrong</a>'
    '<a onclick="showTab(\'heatmap\',this)">By category</a>'
    '<a onclick="showTab(\'allgroups\',this)">All 55 edited groups</a>'
    '<a onclick="showTab(\'moves\',this)">Products moved</a>'
    '<a onclick="showTab(\'review\',this)">Review coverage</a>'
    '</div>'

    '<div class="container">'

    # ── EXECUTIVE SUMMARY ─────────────────────────────────────────────────────
    '<div id="tab-exec" class="section active">'

    # Platform filter toggle
    '<div class="plat-toggle">'
    '<button class="active" onclick="showPlat(\'all\',this)">All platforms</button>'
    '<button onclick="showPlat(\'fp\',this)">Foodpanda</button>'
    '<button onclick="showPlat(\'hs\',this)">HungerStation</button>'
    '<button onclick="showPlat(\'py\',this)">PedidosYa</button>'
    '</div>'

    # ── ALL ──────────────────────────────────────────────────────────────────
    '<div class="plat-panel active" id="plat-all">'

    # Hero dark banner
    '<div class="exec-hero-banner">'
    '<div class="hero-eyebrow">Auto-Grouping Launch &nbsp;&middot;&nbsp; June 2026</div>'
    f'<div class="hero-title">Variant groups <span>scaled {_growth_all}%</span><br>in one month.</div>'
    '<div class="hero-sub">Foodpanda &nbsp;&middot;&nbsp; HungerStation &nbsp;&middot;&nbsp; PedidosYa</div>'
    '<div class="hero-kpis">'
    '<div class="hero-kpi">'
    f'<div class="hk-val">{_fmt(_pre_total)}</div>'
    '<div class="hk-sub">Groups before June 2026</div>'
    '<div class="hk-label">Baseline across 3 platforms</div>'
    '</div>'
    '<div class="hero-kpi">'
    f'<div class="hk-val accent">+{_fmt(_total_groups)}</div>'
    '<div class="hk-sub">New groups created</div>'
    '<div class="hk-label">Model run — June 2026</div>'
    '</div>'
    '<div class="hero-kpi">'
    f'<div class="hk-val">{_fmt(_tot_all)}</div>'
    '<div class="hk-sub">Total groups today</div>'
    '<div class="hk-label">Combined across platforms</div>'
    '</div>'
    '<div class="hero-kpi">'
    f'<div class="hk-val green">{_good_pct}%</div>'
    '<div class="hk-sub">Groups in good shape</div>'
    f'<div class="hk-label">Of {_fmt(_rev_total)} reviewed — Foodpanda + HS</div>'
    '</div>'
    '</div>'
    '</div>'

    # Row 1: scale cards
    '<div class="stat-section-label">Scale breakdown &mdash; June 2026</div>'
    '<div class="stat-row">'

    '<div class="stat-card" style="border-top:4px solid var(--border-strong)">'
    f'<div class="cnt" style="color:var(--ink-mute)">{_fmt(_pre_total)}</div>'
    '<div class="l">Groups before June 2026</div>'
    '<div class="s">Foodpanda + HungerStation + PedidosYa</div>'
    '</div>'

    '<div class="stat-card accent-card">'
    f'<div class="pct">+{_growth_all}%</div>'
    f'<div class="cnt-sub">+{_fmt(_total_groups)} new groups</div>'
    '<div class="l">Auto-grouping &mdash; one month</div>'
    '</div>'

    '<div class="stat-card" style="border-top:4px solid var(--ink)">'
    f'<div class="cnt">{_fmt(_tot_all)}</div>'
    '<div class="l">Total groups today</div>'
    '</div>'

    '</div>'  # /stat-row

    # Row 2: quality cards
    f'<div class="stat-section-label">Quality &mdash; {_fmt(_rev_total)} reviewed groups (Foodpanda &amp; HungerStation only)</div>'
    '<div class="stat-row">'

    '<div class="stat-card tint-green">'
    f'<div class="pct">{_good_pct}%</div>'
    f'<div class="cnt-sub">{_fmt(_good)} / {_fmt(_rev_total)} reviewed</div>'
    '<div class="l">Groups in good shape</div>'
    '<div class="s">Accepted, extended, or corrected with 2+ products left</div>'
    '</div>'

    '<div class="stat-card tint-red">'
    f'<div class="pct">{_bad_pct}%</div>'
    f'<div class="cnt-sub">{_broken} / {_fmt(_rev_total)} reviewed</div>'
    '<div class="l">Groups broken — 1 product left</div>'
    '<div class="s">All from Foodpanda</div>'
    '</div>'

    '<div class="stat-card" style="border-top:4px solid var(--ink-faint)">'
    f'<div class="pct" style="color:var(--ink-mute)">{_cov_pct}%</div>'
    f'<div class="cnt-sub" style="color:var(--ink-mute)">{_fmt(_rev_total)} / {_fmt(_total_groups)}</div>'
    '<div class="l">Groups reviewed so far</div>'
    f'<div class="s">Quality of remaining {100-float(_cov_pct):.2f}% is unknown</div>'
    '</div>'

    '</div>'  # /stat-row
    '</div>'  # /plat-all

    # ── FOODPANDA ─────────────────────────────────────────────────────────────
    '<div class="plat-panel" id="plat-fp">'
    '<div class="plat-hero plat-hero-fp">'
    '<div class="plat-logo-mark">' + SVG_FP + '</div>'
    '<div class="ph-eyebrow"><span class="ph-dot" style="background:#D70F64"></span>Foodpanda &nbsp;&middot;&nbsp; APAC</div>'
    f'<div class="ph-title">+{_growth_fp}% group growth</div>'
    f'<div class="ph-sub">Largest absolute gain &mdash; {_fmt(_new_fp)} new groups in one month</div>'
    '<div class="ph-kpis">'
    f'<div class="ph-kpi"><div class="phv">{_fmt(_pre_fp)}</div><div class="phs">Before June 2026</div><div class="phl">Pre-existing groups</div></div>'
    f'<div class="ph-kpi"><div class="phv" style="color:#D70F64">+{_fmt(_new_fp)}</div><div class="phs">New groups</div><div class="phl">Model run — June 2026</div></div>'
    f'<div class="ph-kpi"><div class="phv">{_fmt(_tot_fp)}</div><div class="phs">Total today</div></div>'
    f'<div class="ph-kpi"><div class="phv" style="color:#4AE8A0">{_fp_good_pct}%</div><div class="phs">In good shape</div><div class="phl">Of {_fmt(_rev_fp)} reviewed</div></div>'
    '</div>'
    '</div>'

    '<div class="stat-section-label">Scale</div>'
    '<div class="stat-row">'
    '<div class="stat-card" style="border-top:4px solid var(--border-strong)">'
    f'<div class="cnt" style="color:var(--ink-mute)">{_fmt(_pre_fp)}</div>'
    '<div class="l">Groups before June 2026</div>'
    '</div>'
    '<div class="stat-card fp-card">'
    f'<div class="pct">+{_growth_fp}%</div>'
    f'<div class="cnt-sub">+{_fmt(_new_fp)} new groups</div>'
    '<div class="l">Auto-grouping &mdash; one month</div>'
    '</div>'
    '<div class="stat-card" style="border-top:4px solid var(--ink)">'
    f'<div class="cnt">{_fmt(_tot_fp)}</div>'
    '<div class="l">Total groups today</div>'
    '</div>'
    '</div>'

    f'<div class="stat-section-label">Quality &mdash; {_fmt(_rev_fp)} reviewed</div>'
    '<div class="stat-row">'
    '<div class="stat-card" style="border-top:4px solid var(--purple)">'
    f'<div class="pct" style="color:var(--purple)">{_fp_cov_pct}%</div>'
    f'<div class="cnt-sub" style="color:var(--purple)">{_fmt(_rev_fp)} / {_fmt(_new_fp)}</div>'
    '<div class="l">Reviewed by editors</div>'
    '</div>'
    '<div class="stat-card tint-green">'
    f'<div class="pct">{_fp_good_pct}%</div>'
    f'<div class="cnt-sub">{_fmt(_fp_good)} / {_fmt(_rev_fp)} reviewed</div>'
    '<div class="l">In good shape</div>'
    '</div>'
    '<div class="stat-card tint-red">'
    f'<div class="pct">{_fp_bad_pct}%</div>'
    f'<div class="cnt-sub">{_broken} / {_fmt(_rev_fp)} reviewed</div>'
    '<div class="l">Broken — 1 product left</div>'
    '</div>'
    '</div>'

    '</div>'  # /plat-fp

    # ── HUNGERSTATION ─────────────────────────────────────────────────────────
    '<div class="plat-panel" id="plat-hs">'
    '<div class="plat-hero plat-hero-hs">'
    '<div class="plat-logo-mark">' + SVG_HS + '</div>'
    '<div class="ph-eyebrow"><span class="ph-dot" style="background:#FFC300"></span>HungerStation &nbsp;&middot;&nbsp; Saudi Arabia</div>'
    f'<div class="ph-title">+{_growth_hs:,}% group growth</div>'
    f'<div class="ph-sub">From near-zero to {_fmt(_tot_hs)} groups — highest relative growth of any platform</div>'
    '<div class="ph-kpis">'
    f'<div class="ph-kpi"><div class="phv">{_fmt(_pre_hs)}</div><div class="phs">Before June 2026</div><div class="phl">Pre-existing groups</div></div>'
    f'<div class="ph-kpi"><div class="phv" style="color:#FFC300">+{_fmt(_new_hs)}</div><div class="phs">New groups</div><div class="phl">Model run — June 2026</div></div>'
    f'<div class="ph-kpi"><div class="phv">{_fmt(_tot_hs)}</div><div class="phs">Total today</div></div>'
    f'<div class="ph-kpi"><div class="phv" style="color:#4AE8A0">100%</div><div class="phs">In good shape</div><div class="phl">Of {_fmt(_rev_hs)} reviewed — zero broken</div></div>'
    '</div>'
    '</div>'

    '<div class="stat-section-label">Scale</div>'
    '<div class="stat-row">'
    '<div class="stat-card" style="border-top:4px solid var(--border-strong)">'
    f'<div class="cnt" style="color:var(--ink-mute)">{_fmt(_pre_hs)}</div>'
    '<div class="l">Groups before June 2026</div>'
    '</div>'
    '<div class="stat-card hs-card">'
    f'<div class="pct">+{_growth_hs:,}%</div>'
    f'<div class="cnt-sub">+{_fmt(_new_hs)} new groups</div>'
    '<div class="l">Auto-grouping &mdash; one month</div>'
    '</div>'
    '<div class="stat-card" style="border-top:4px solid var(--ink)">'
    f'<div class="cnt">{_fmt(_tot_hs)}</div>'
    '<div class="l">Total groups today</div>'
    '</div>'
    '</div>'

    f'<div class="stat-section-label">Quality &mdash; {_fmt(_rev_hs)} reviewed</div>'
    '<div class="stat-row">'
    '<div class="stat-card" style="border-top:4px solid var(--purple)">'
    '<div class="pct" style="color:var(--purple)">0.75%</div>'
    '<div class="cnt-sub" style="color:var(--purple)">133 / 17,672</div>'
    '<div class="l">Reviewed by editors</div>'
    '</div>'
    '<div class="stat-card tint-green">'
    '<div class="pct">100%</div>'
    '<div class="cnt-sub">133 / 133 reviewed</div>'
    '<div class="l">In good shape — no broken groups</div>'
    '</div>'
    '</div>'

    '</div>'  # /plat-hs

    # ── PEDIDOSYA ─────────────────────────────────────────────────────────────
    '<div class="plat-panel" id="plat-py">'
    '<div class="plat-hero plat-hero-py">'
    '<div class="plat-logo-mark">' + SVG_PY + '</div>'
    '<div class="ph-eyebrow"><span class="ph-dot" style="background:#F52F41"></span>PedidosYa &nbsp;&middot;&nbsp; Latin America</div>'
    f'<div class="ph-title">+{_growth_py:,}% group growth</div>'
    f'<div class="ph-sub">From near-zero to {_fmt(_tot_py)} groups — no editor reviews recorded yet</div>'
    '<div class="ph-kpis">'
    f'<div class="ph-kpi"><div class="phv">{_fmt(_pre_py)}</div><div class="phs">Before June 2026</div><div class="phl">Pre-existing groups</div></div>'
    f'<div class="ph-kpi"><div class="phv" style="color:#F52F41">+{_fmt(_new_py)}</div><div class="phs">New groups</div><div class="phl">Model run — June 2026</div></div>'
    f'<div class="ph-kpi"><div class="phv">{_fmt(_tot_py)}</div><div class="phs">Total today</div></div>'
    '<div class="ph-kpi"><div class="phv" style="color:rgba(255,255,255,.3)">&mdash;</div><div class="phs">No reviews yet</div><div class="phl">Quality data unavailable</div></div>'
    '</div>'
    '</div>'

    '<div class="stat-section-label">Scale</div>'
    '<div class="stat-row">'
    '<div class="stat-card" style="border-top:4px solid var(--border-strong)">'
    f'<div class="cnt" style="color:var(--ink-mute)">{_fmt(_pre_py)}</div>'
    '<div class="l">Groups before June 2026</div>'
    '</div>'
    '<div class="stat-card py-card">'
    f'<div class="pct">+{_growth_py:,}%</div>'
    f'<div class="cnt-sub">+{_fmt(_new_py)} new groups</div>'
    '<div class="l">Auto-grouping &mdash; one month</div>'
    '</div>'
    '<div class="stat-card" style="border-top:4px solid var(--ink)">'
    f'<div class="cnt">{_fmt(_tot_py)}</div>'
    '<div class="l">Total groups today</div>'
    '</div>'
    '</div>'
    '<div class="note-box" style="margin-top:12px">No editor reviews recorded for PedidosYa yet &mdash; quality data unavailable.</div>'

    '</div>'  # /plat-py


    '</div>'  # /exec tab

    # ── OVERVIEW ─────────────────────────────────────────────────────────────
    '<div id="tab-overview" class="section">'
    '<div class="tab-header">'
    '<div class="th-eyebrow">Detail</div>'
    '<div class="th-title">Group-level edit analysis</div>'
    f'<div class="th-sub">{len(meta)} groups with genuine product membership changes &nbsp;&middot;&nbsp; {_fmt(_rev_total)} reviewed total</div>'
    '<div class="th-accent-bar"></div>'
    '</div>'

    '<h2>How many groups were changed?</h2>'
    '<div class="kpi-row">'
    f'<div class="kpi"><div class="num">{_fmt(_total_groups)}</div><div class="label">Total groups created since Jun 1</div><div class="sub">Foodpanda + HungerStation + PedidosYa</div></div>'
    f'<div class="kpi kpi-green"><div class="num" style="color:var(--green)">{100 - len(meta)/_total_groups*100:.2f}%</div><div class="label">Left untouched &mdash; accepted as-is</div></div>'
    f'<div class="kpi kpi-amber"><div class="num" style="color:var(--amber)">{len(meta)}</div><div class="label">Genuinely edited &mdash; product membership changed</div><div class="sub">{len(meta)/_total_groups*100:.2f}% of all groups &nbsp;&middot;&nbsp; {len(confirmed_meta)} version bumps were reordering only</div></div>'
    '<div class="kpi"><div class="num">203</div><div class="label">Individual products touched across those 55 groups</div></div>'
    '</div>'

    '<h2>What kind of edits were made?</h2>'
    '<div class="kpi-row">'
    '<div class="kpi kpi-green">'
    '<div class="num" style="color:var(--green)">26</div>'
    '<div class="label"><strong>Groups merged together</strong></div>'
    '<div class="sub">Editor added products into an existing group — two groups should have been one</div>'
    '</div>'
    '<div class="kpi kpi-amber">'
    '<div class="num" style="color:var(--amber)">29</div>'
    '<div class="label"><strong>Products removed from a group</strong></div>'
    '<div class="sub">10 still have 2+ products (valid) &nbsp;&middot;&nbsp; <span style="color:var(--dh-red);font-weight:700">19 broken — 1 product left</span></div>'
    '</div>'
    '</div>'

    '<h2>Positive quality signals</h2>'
    '<div class="kpi-row">'
    '<div class="kpi kpi-green">'
    '<div class="num" style="color:var(--green)">' + str(anchor_count) + '</div>'
    '<div class="label"><strong>Model group used as anchor</strong></div>'
    '<div class="sub">Ungrouped product pulled into the model\'s group — editor confirmed the concept is broader than the model\'s original input</div>'
    '</div>'
    '<div class="kpi kpi-blue">'
    '<div class="num" style="color:var(--blue)">' + str(preferred_count) + '</div>'
    '<div class="label"><strong>Model group preferred over pre-existing</strong></div>'
    '<div class="sub">Product moved out of an old group into the model\'s new one — the model\'s grouping was more semantically correct</div>'
    '</div>'
    '</div>'

    '<div class="note-box" style="margin-bottom:24px">'
    '<strong>Note on the 153 excluded groups:</strong> A further 153 groups had their version bumped by an editor '
    'but the product_ids set was identical — the editor reordered, changed metadata, or triggered an incidental save. '
    'These are confirmed no-ops on product membership and are excluded from this analysis.'
    '</div>'

    '<h2>By region and platform</h2>'
    '<div class="dt-wrap"><table class="dt">'
    '<thead><tr><th>Region</th><th>Groups created</th><th>Real edits</th><th>Edit rate</th><th>Platform</th><th>What they did</th></tr></thead>'
    '<tbody>'
    '<tr><td>AP</td><td>21,056</td><td>54</td>'
    '<td><span class="badge badge-amber">0.26%</span></td>'
    '<td>Foodpanda</td>'
    '<td>Removed products &amp; added products to merge groups</td></tr>'
    '<tr><td>SA</td><td>17,674</td><td>1</td>'
    '<td><span class="badge badge-green">0.006%</span></td>'
    '<td>HungerStation</td>'
    '<td>1 group with actual removal &mdash; 132 were reordering only</td></tr>'
    '<tr><td>US / Others</td><td>16,000+</td><td>0</td>'
    '<td><span class="badge badge-green">0%</span></td>'
    '<td>&mdash;</td>'
    '<td>No edits at all</td></tr>'
    '</tbody></table></div>'

    '<h2>Data completeness</h2>'
    '<div class="kpi-row">'
    '<div class="kpi kpi-green"><div class="num" style="color:var(--green)">100%</div><div class="label">Have a size / weight value</div><div class="sub">Primary signal the grouping system uses</div></div>'
    '<div class="kpi kpi-red"><div class="num" style="color:var(--dh-red)">0%</div><div class="label">Have a net content value</div><div class="sub">Field is entirely empty across all affected products</div></div>'
    '<div class="kpi kpi-green"><div class="num" style="color:var(--green)">98%</div><div class="label">Are active (live) products</div></div>'
    '</div>'

    '<h2>Review coverage</h2>'
    '<div class="kpi-row">'
    f'<div class="kpi"><div class="num">{_fmt(_total_groups)}</div><div class="label">Total groups created</div><div class="sub">creator filter: ismet.dogan@deliveryhero.com, since Jun 1 2026</div></div>'
    f'<div class="kpi kpi-blue"><div class="num" style="color:var(--blue)">{_fmt(_rev_total)}</div><div class="label"><strong>Reviewed</strong> by an editor</div><div class="sub">{_cov_pct}% — version bumped by a human</div></div>'
    f'<div class="kpi kpi-green"><div class="num" style="color:var(--green)">{_rev_total - len(meta)}</div><div class="label"><strong>Accepted</strong> &mdash; confirmed or extended</div><div class="sub">{round((_rev_total-len(meta))/_rev_total*100,1)}% of reviewed &mdash; {len(confirmed_meta)} no change + {len(_over_split_gids)} extended</div></div>'
    f'<div class="kpi kpi-red"><div class="num" style="color:var(--dh-red)">{len(meta)}</div><div class="label"><strong>Corrected</strong> &mdash; products removed</div><div class="sub">{round(len(meta)/_rev_total*100,1)}% of reviewed</div></div>'
    f'<div class="kpi kpi-mute"><div class="num" style="color:var(--ink-mute)">{_fmt(_uncov)}</div><div class="label"><strong>Unreviewed</strong> &mdash; never touched</div><div class="sub">{100-float(_cov_pct):.2f}% — quality unknown</div></div>'
    '</div>'
    f'<p class="text-muted">{round((_rev_total-len(meta))/_rev_total*100)}% of reviewed groups were accepted or extended without removing any product. Corrections cluster around two fixable root causes — see <em>What went wrong</em> for details.</p>'

    '</div>'  # /overview

    # ── WHAT WENT WRONG ───────────────────────────────────────────────────────
    '<div id="tab-issues" class="section">'
    '<div class="tab-header">'
    '<div class="th-eyebrow">Root cause analysis</div>'
    '<div class="th-title">What went wrong &mdash; and how to fix it</div>'
    '<div class="th-sub">Every manual edit is a signal. Two root causes account for the majority of corrections.</div>'
    '<div class="th-accent-bar"></div>'
    '</div>'
    + issues_html +
    '</div>'

    # ── HEATMAP ───────────────────────────────────────────────────────────────
    '<div id="tab-heatmap" class="section">'
    '<div class="tab-header">'
    '<div class="th-eyebrow">Category breakdown</div>'
    '<div class="th-title">Which categories had the most corrections?</div>'
    '<div class="th-sub">Top 20 categories by edit count &nbsp;&middot;&nbsp; Highlighted cells = 3 or more edits of that type</div>'
    '<div class="th-accent-bar"></div>'
    '</div>'
    '<div class="table-wrap"><table>'
    '<thead>' + heatmap_thead + '</thead>'
    '<tbody>' + heatmap_tbody + '</tbody>'
    '</table></div>'
    '</div>'

    # ── ALL GROUPS ────────────────────────────────────────────────────────────
    '<div id="tab-allgroups" class="section">'
    '<div class="tab-header">'
    '<div class="th-eyebrow">Full detail</div>'
    '<div class="th-title">All 55 genuinely edited groups</div>'
    '<div class="th-sub">Filter by platform, edit type, broken status, or search by brand and category</div>'
    '<div class="th-accent-bar"></div>'
    '</div>'
    '<div class="legend">'
    '<span style="background:#F0FAF5;border-color:#BFE5CF;color:#008C58">Green = products added (two groups merged into one)</span>'
    '<span style="background:#FEF2F2;border-color:#F2C9CB;color:#D61F26">Red = broken (1 product left &mdash; not a valid group)</span>'
    '<span style="background:var(--surface-2);border-color:var(--border);color:var(--ink-soft)">White = products removed, 2+ remain &mdash; still valid</span>'
    '</div>'
    '<div class="filters">'
    '<label>Region:</label>'
    '<select id="f-region" onchange="filterGroups()">'
    '<option value="">All</option><option value="ap">AP</option><option value="sa">SA</option>'
    '</select>'
    '<label>Edit type:</label>'
    '<select id="f-tag" onchange="filterGroups()">'
    '<option value="">All</option>'
    '<option value="OVER_SPLIT">Products added</option>'
    '<option value="OVER_GROUPED">Products removed</option>'
    '</select>'
    '<label>Broken groups:</label>'
    '<select id="f-singleton" onchange="filterGroups()">'
    '<option value="">All</option>'
    '<option value="1">Broken only (1 product left)</option>'
    '<option value="0">Exclude broken</option>'
    '</select>'
    '<label>Platform:</label>'
    '<select id="f-editor" onchange="filterGroups()">'
    '<option value="">All</option>'
    '<option value="foodpanda">foodpanda</option>'
    '<option value="hungerstation">hungerstation</option>'
    '</select>'
    '<label>Search brand or category:</label>'
    '<input type="text" id="f-search" placeholder="e.g. Nescafe, Coffee" oninput="filterGroups()">'
    '</div>'
    '<div id="gcount"></div>'
    '<div class="table-wrap"><table>'
    '<thead><tr>'
    '<th>Region</th><th>Group ID</th><th>Edit count</th><th>Editor</th><th>Products now</th>'
    '<th>Brand</th><th>Category</th><th>Grouping axis</th>'
    '<th>Sizes in group</th>'
    '<th>Edit type</th><th>Group name</th><th>Product names</th><th>Product IDs</th>'
    '</tr></thead>'
    '<tbody id="gtbody">' + all_group_rows + '</tbody>'
    '</table></div>'
    '</div>'

    # ── PRODUCT MOVES ─────────────────────────────────────────────────────────
    '<div id="tab-moves" class="section">'
    '<div class="tab-header">'
    '<div class="th-eyebrow">Product-level signals</div>'
    '<div class="th-title">How products moved across groups</div>'
    f'<div class="th-sub">Three signal types &nbsp;&middot;&nbsp; {len(move_entries)} individual product moves across {len(meta)} edited groups</div>'
    '<div class="th-accent-bar"></div>'
    '</div>'

    '<div class="kpi-row">'
    '<div class="kpi kpi-green"><div class="num" style="color:var(--green)">' + str(anchor_count) + '</div><div class="label"><strong>Model group used as anchor</strong></div><div class="sub">Product was ungrouped. Editor extended the model\'s group — confirming the concept is correct and broader than the model\'s original input.</div></div>'
    '<div class="kpi kpi-blue"><div class="num" style="color:var(--blue)">' + str(preferred_count) + '</div><div class="label"><strong>Model group preferred over pre-existing</strong></div><div class="sub">Product was in an old group. Editor moved it into the model\'s new one — the model\'s grouping was more suitable.</div></div>'
    '<div class="kpi kpi-red"><div class="num" style="color:var(--dh-red)">' + str(removed_count) + '</div><div class="label"><strong>Product removed from model group</strong></div><div class="sub">Editor decided this product did not belong — a correction signal.</div></div>'
    '</div>'
    '<p class="text-muted">The first two signals are positive — the model created grouping concepts that editors agreed with and extended. The third is a correction: products grouped together that did not belong.</p>'

    '<div class="table-wrap"><table>'
    '<thead><tr>'
    '<th style="min-width:200px">Product</th>'
    '<th style="min-width:200px">Signal</th>'
    '<th>From</th>'
    '<th style="width:36px"></th>'
    '<th>To</th>'
    '<th>Interpretation</th>'
    '</tr></thead>'
    '<tbody>' + moves_rows + '</tbody>'
    '</table></div>'
    '<p class="text-footer">'
    f'{len(move_entries)} product moves across {len(meta)} groups &nbsp;&middot;&nbsp;'
    ' <span class="badge badge-green">Green</span> = model group used as anchor &nbsp;&middot;&nbsp;'
    ' <span class="badge badge-blue">Blue</span> = model group preferred over pre-existing &nbsp;&middot;&nbsp;'
    ' <span class="badge badge-red">Red</span> = correction'
    '</p>'
    '</div>'

    # ── REVIEW COVERAGE ───────────────────────────────────────────────────────
    '<div id="tab-review" class="section">'
    '<div class="tab-header">'
    '<div class="th-eyebrow">Quality measurement</div>'
    '<div class="th-title">Review coverage &amp; metric definitions</div>'
    f'<div class="th-sub">{_cov_pct}% of groups reviewed &nbsp;&middot;&nbsp; {round((_rev_total-len(meta))/_rev_total*100,1)}% acceptance rate &nbsp;&middot;&nbsp; Metric definitions for ongoing tracking</div>'
    '<div class="th-accent-bar"></div>'
    '</div>'
    # ── 3 clean hero KPIs ────────────────────────────────────────────────────
    '<div class="kpi-row" style="margin-bottom:32px">'

    '<div class="kpi" style="border-top:3px solid var(--ink-faint)">'
    f'<div class="num" style="color:var(--ink)">{_cov_pct}%</div>'
    '<div class="label">Groups reviewed</div>'
    f'<div class="sub">{_fmt(_rev_total)} of {_fmt(_total_groups)} &mdash; a very small window into the full model output</div>'
    '</div>'

    '<div class="kpi kpi-green">'
    f'<div class="num" style="color:var(--green)">{round((_rev_total - len(meta))/_rev_total*100, 1)}%</div>'
    '<div class="label">Acceptance rate</div>'
    f'<div class="sub">{_rev_total - len(meta)} of {_fmt(_rev_total)} reviewed — confirmed unchanged or editor-extended</div>'
    '</div>'

    '<div class="kpi kpi-red">'
    f'<div class="num" style="color:var(--dh-red)">{round(len(meta)/_rev_total*100,1)}%</div>'
    '<div class="label">Correction rate</div>'
    f'<div class="sub">{len(meta)} of {_fmt(_rev_total)} had products removed &mdash; of which <strong>{singleton_count} left with only 1 product</strong> (broken)</div>'
    '</div>'

    '</div>'

    # ── Coverage proportion bar ───────────────────────────────────────────────
    '<div class="prop-wrap">'
    '<div class="eyebrow">Scale of review coverage</div>'
    f'<p style="font-size:var(--text-sm);color:var(--ink-soft);margin-top:6px">Of {_fmt(_total_groups)} groups created, only {_fmt(_rev_total)} have been opened by an editor. The bar below makes this proportion visible.</p>'
    '<div class="prop-bar-outer">'
    f'<div class="prop-bar-fill" style="width:{_cov_pct}%;background:var(--ink)"></div>'
    '</div>'
    f'<div class="prop-bar-labels"><span>0</span><span>{_fmt(_rev_total)} reviewed ({_cov_pct}%)</span><span>{_fmt(_total_groups)} total</span></div>'

    # Breakdown bar — reviewed split into confirmed / extended / corrected
    '<div style="margin-top:24px">'
    f'<div class="eyebrow" style="margin-bottom:10px">Of the {_fmt(_rev_total)} reviewed — how editors responded</div>'
    '<div class="prop-breakdown">'
    f'<div class="prop-bd-seg" style="background:var(--ink-mute);flex:{len(confirmed_meta)}">{len(confirmed_meta)} confirmed</div>'
    f'<div class="prop-bd-seg" style="background:var(--green);flex:{len(_over_split_gids)}">{len(_over_split_gids)} extended</div>'
    f'<div class="prop-bd-seg" style="background:var(--amber);flex:{corrected_valid_count}">{corrected_valid_count} valid</div>'
    f'<div class="prop-bd-seg" style="background:var(--dh-red);flex:{singleton_count}">{singleton_count} broken</div>'
    '</div>'
    '<div class="prop-legend">'
    f'<div class="prop-legend-item"><div class="prop-legend-dot" style="background:var(--ink-mute)"></div>{len(confirmed_meta)} confirmed — no product change ({len(confirmed_meta)/_rev_total*100:.1f}%)</div>'
    f'<div class="prop-legend-item"><div class="prop-legend-dot" style="background:var(--green)"></div>{len(_over_split_gids)} extended — editor added products ({len(_over_split_gids)/_rev_total*100:.1f}%)</div>'
    f'<div class="prop-legend-item"><div class="prop-legend-dot" style="background:var(--amber)"></div>{corrected_valid_count} corrected &amp; valid — products removed, 2+ remain ({corrected_valid_count/_rev_total*100:.1f}%)</div>'
    f'<div class="prop-legend-item"><div class="prop-legend-dot" style="background:var(--dh-red)"></div>{singleton_count} broken — products removed, only 1 left ({singleton_count/_rev_total*100:.1f}%)</div>'
    '</div>'
    '</div>'
    '</div>'

    # ── Product scorecard ─────────────────────────────────────────────────────
    '<h2>Product-level view</h2>'
    '<p class="text-muted">Same logic applied to individual products inside the 208 reviewed groups.</p>'
    '<div class="prod-scorecard">'

    '<div class="prod-sc-cell" style="border-top:3px solid var(--green)">'
    '<div class="sc-val" style="color:var(--green)">' + str(len(_prod_confirmed) + len(_prod_extended_model)) + '</div>'
    '<div class="sc-label">Validated — no correction needed</div>'
    '<div class="sc-meta">' + str(len(_prod_confirmed)) + ' in confirmed groups &nbsp;&middot;&nbsp; ' + str(len(_prod_extended_model)) + ' model-placed in extended groups</div>'
    '</div>'

    '<div class="prod-sc-cell" style="border-top:3px solid var(--blue)">'
    '<div class="sc-val" style="color:var(--blue)">' + str(len(_prod_extended_added)) + '</div>'
    '<div class="sc-label">Editor-added into model groups</div>'
    '<div class="sc-meta">29 from ungrouped &nbsp;&middot;&nbsp; 2 moved from a pre-existing group</div>'
    '</div>'

    '<div class="prod-sc-cell" style="border-top:3px solid var(--dh-red)">'
    '<div class="sc-val" style="color:var(--dh-red)">' + str(len(_prod_removed)) + '</div>'
    '<div class="sc-label">Removed as corrections</div>'
    '<div class="sc-meta">' + f'{len(_prod_removed)/len(_prod_reviewed_total)*100:.1f}' + '% product correction rate &nbsp;&middot;&nbsp; went to another group or ungrouped</div>'
    '</div>'

    '</div>'

    # ── Metric definitions ────────────────────────────────────────────────────
    '<h2>Metric definitions</h2>'
    '<div class="dt-wrap"><table class="dt">'
    '<thead><tr><th>Metric</th><th>Definition</th><th>Value</th></tr></thead>'
    '<tbody>'
    f'<tr><td>Group review coverage</td><td>Groups with version &gt; 1 / total groups created</td><td><span class="badge badge-neutral">{_cov_pct}%</span></td></tr>'
    f'<tr><td>Group acceptance rate</td><td>Groups confirmed or extended / total reviewed</td><td><span class="badge badge-green">{round((_rev_total-len(meta))/_rev_total*100,1)}%</span></td></tr>'
    f'<tr><td>Group correction rate</td><td>Groups with products removed / total reviewed</td><td><span class="badge badge-amber">{round(len(meta)/_rev_total*100,1)}%</span></td></tr>'
    f'<tr><td>Product correction rate</td><td>Products removed / products in reviewed groups</td><td><span class="badge badge-amber">{len(_prod_removed)/len(_prod_reviewed_total)*100:.1f}%</span></td></tr>'
    f'<tr><td>Model concept extension rate</td><td>Products editor-added to model groups / total reviewed products</td><td><span class="badge badge-blue">{len(_prod_extended_added)/len(_prod_reviewed_total)*100:.1f}%</span></td></tr>'
    f'<tr><td>Groups confirmed (absolute)</td><td>Reviewed groups with no product change</td><td><span class="badge badge-neutral">{len(confirmed_meta)}</span></td></tr>'
    '</tbody></table></div>'

    '<div class="note-box" style="margin-top:16px">'
    '<strong>How to track these over time:</strong> Run a daily BigQuery job that diffs '
    'v1 vs vfinal product_ids for all groups where <code>sys.version &gt; 1</code> and '
    '<code>variant_group_id</code> was created after the model run date. '
    'Identical pid sets = confirmed &nbsp;&middot;&nbsp; only additions = extended &nbsp;&middot;&nbsp; any removals = corrected. No manual tagging required.'
    '</div>'

    '</div>'  # /review

    '</div>'  # /container
    '<script>' + JS + '</script>'
    '</body></html>'
)

out = '/Users/necmettin.tamgueney/Desktop/variant_grouping_model_analysis.html'
open(out, 'w').write(html)
print(f"Done. Written to {out} ({len(html):,} chars)")
