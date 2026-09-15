"""SQL Server source vs destination validator. Per-job settings: config.json. See README.md."""

import argparse
import csv
import datetime
import decimal
import json
import logging
import os
import re
import sys
import traceback
import uuid

if not (__name__ == "__main__" and "--test-column-filter" in sys.argv):
    import pyodbc
else:
    pyodbc = None  # self-tests only; no database driver needed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from internal_log import InternalRunLogger

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("validator")


def _json_default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    return str(obj)


def _json_dumps_safe(obj):
    return json.dumps(obj, default=_json_default)


# Fixed status labels written into ValidationLog2 (same for every job).
READY = "Ready"
MISSING_DESTINATION = "Missing Destination"
MISSING_REFERENCE = "Missing Reference"
NOT_APPLICABLE = "Not Applicable"
NOT_EVALUATED = "Not evaluated"
NOT_AVAILABLE = "N/A"

JOIN_READY = "Ready"
JOIN_MISSING_SOURCE_KEY = "Missing Source Key"
JOIN_MISSING_DESTINATION_KEY = "Missing Destination Key"
JOIN_NOT_CONFIGURED = "Not configured"

MATCH = "Match"
MISMATCH = "Mismatch"
SOURCE_BLANK = "Source Blank"
DESTINATION_BLANK = "Destination Blank"
BOTH_BLANK = "Both Blank"

# In-memory caches so we do not run the same SQL twice in one run.
DESTINATION_PRELOAD_CACHE = {}
CUSTOM_QUERY_CACHE = {}


def report_connection_error(client_id, flow_id, message):
    """Print database failures immediately while retaining normal log-table logging."""
    text = f"[client={client_id} flow={flow_id}] CONNECTION_ERROR: {message}"
    log.error(text)
    print(f"\nCONNECTION_ERROR\n{text}\n", file=sys.stderr)

# Stage constants
STAGE_BEFORE_VALIDATION = "BEFORE_VALIDATION"
STAGE_INSERT = "INSERT"
STAGE_AFTER_VALIDATION = "AFTER_VALIDATION"


class RuntimeConfig:
    # Reads config.json once and exposes options, identity rules, queries, etc.

    def __init__(self, config):
        self.config = config
        self.options = dict(config.get("options") or {})  # log_matches, batch size, nolock, ...
        self.queries = dict(config.get("queries") or {})  # SQL templates for table mode
        self.identity = dict(config.get("identity") or {})  # how to find client_id / audit_id in rows
        self.normalization_defaults = dict(config.get("normalization_defaults") or {})  # numeric / text / date
        date_cfg = config.get("date_formats")
        if date_cfg is None:
            self.date_formats = []
        elif isinstance(date_cfg, dict):
            self.date_formats = list(date_cfg.get("formats") or []) if date_cfg.get("enabled") else []
        else:
            self.date_formats = list(date_cfg or [])
        self.export_cfg = dict(config.get("export") or {})

    def option(self, key, mapping=None):
        if mapping:
            mapping_opts = mapping.get("options") or {}
            if key in mapping_opts:
                return mapping_opts[key]
        if key in self.options:
            return self.options[key]
        raise KeyError(
            f"Required option '{key}' is missing from config.json 'options' section"
        )

    def mapping_option(self, key, mapping, global_fallback_key=None):
        """Resolve a mapping-level setting with optional global options fallback."""
        if mapping and key in mapping:
            return mapping[key]
        fallback = global_fallback_key or key
        return self.option(fallback, mapping)

    def nolock_hint(self):
        return "WITH (NOLOCK)" if self.option("use_nolock") else ""

    def render_query(self, name, **params):
        if name not in self.queries:
            raise KeyError(
                f"Required query '{name}' is missing from config.json 'queries' section"
            )
        sql = self.queries[name].format(**params)
        return re.sub(r"\s+", " ", sql).strip()

    def identity_patterns(self, field):
        return list(self.identity.get(f"{field}_patterns") or [])

    def column_fallbacks(self, side, field):
        side_cfg = self.identity.get(f"{side}_column_fallbacks") or {}
        return list(side_cfg.get(field) or [])


def build_runtime_config(config):
    required_sections = ("options", "queries", "identity", "normalization_defaults")
    missing = [s for s in required_sections if s not in config]
    if missing:
        raise ValueError(
            f"config.json is missing required section(s): {', '.join(missing)}. "
            "JSON must define all runtime behavior."
        )
    return RuntimeConfig(config)


def _should_log_all_rows(runtime_cfg):
    return bool(runtime_cfg.options.get("log_all_rows", True))


def _insert_skip_reason(n_src, n_dst, fix_blanks, fix_mismatches):
    if n_src is None and n_dst is None:
        return "both_blank"
    if n_src is None and n_dst is not None:
        return "source_blank_destination_has_value"
    if n_src is not None and n_dst is not None and n_src == n_dst:
        return "already_match"
    if n_src is not None and n_dst is None and not fix_blanks:
        return "destination_blank_fix_blanks_disabled"
    if n_src is not None and n_dst is not None and n_src != n_dst and not fix_mismatches:
        return "mismatch_fix_mismatches_disabled"
    return "no_update_needed"


def _log_insert_decision(
    audit, runtime_cfg, ids, key_primary_value, src_col, dst_col, src_val, dst_val,
    action, reason, batch_number, key_context_json,
):
    internal = getattr(audit, "internal", None)
    if internal:
        internal.log_insert_row(
            audit.stage, ids, key_primary_value, src_col, dst_col,
            src_val, dst_val, action, reason, batch_number, key_context_json,
        )
    if action == "UPDATED":
        audit.log(
            client_id=ids.get("client_id"), flow_id=ids.get("flow_id"),
            audit_id=ids.get("audit_id"), client_audit_id=ids.get("client_audit_id"),
            source_column=src_col, destination_column=dst_col,
            key_value=key_primary_value, batch_number=batch_number,
            issue_type="UPDATE", value_status="UPDATED",
            source_value=src_val, destination_value=dst_val,
            exception_reason=reason,
            details=f"key_context={key_context_json}",
        )
    elif _should_log_all_rows(runtime_cfg):
        audit.log(
            client_id=ids.get("client_id"), flow_id=ids.get("flow_id"),
            audit_id=ids.get("audit_id"), client_audit_id=ids.get("client_audit_id"),
            source_column=src_col, destination_column=dst_col,
            key_value=key_primary_value, batch_number=batch_number,
            issue_type="INSERT_SKIPPED", value_status="Skipped",
            source_value=src_val, destination_value=dst_val,
            exception_reason=reason,
            details=f"key_context={key_context_json}",
        )


def _stage_context_from_mappings(mappings, catalog, run_id):
    if not mappings:
        return {"run_id": run_id}
    m = resolve_mapping_from_client(mappings[0], catalog)
    resolved = resolve_servers_and_db(m, catalog)
    return {
        "run_id": run_id,
        "source_server": resolved.get("source_server"),
        "source_database": resolved.get("source_database"),
        "destination_server": resolved.get("destination_server"),
        "destination_database": resolved.get("destination_database"),
        "source_table": m.get("source_table"),
        "destination_table": m.get("destination_table"),
        "client_id": m.get("client_id"),
        "flow_id": m.get("flow_id"),
    }

AUDIT_TABLE_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(N'{table}')
)
BEGIN
    CREATE TABLE {table} (
        log_id                BIGINT IDENTITY(1,1) PRIMARY KEY,
        run_id                UNIQUEIDENTIFIER  NOT NULL,
        run_start_time        DATETIME2         NOT NULL,
        log_time               DATETIME2         NOT NULL DEFAULT SYSDATETIME(),
        stage                 VARCHAR(50)       NULL,
        client_id             VARCHAR(100)      NULL,
        flow_id               VARCHAR(100)      NULL,
        audit_id              VARCHAR(100)      NULL,
        client_audit_id       VARCHAR(100)      NULL,
        source_server         VARCHAR(200)      NULL,
        destination_server    VARCHAR(200)      NULL,
        source_database       VARCHAR(200)      NULL,
        destination_database  VARCHAR(200)      NULL,
        source_table          VARCHAR(255)      NULL,
        destination_table     VARCHAR(255)      NULL,
        source_column         VARCHAR(255)      NULL,
        destination_column    VARCHAR(255)      NULL,
        mapping_status        VARCHAR(50)       NULL,
        join_key_status       VARCHAR(50)       NULL,
        value_status          VARCHAR(50)       NULL,
        issue_type            VARCHAR(50)       NOT NULL,
        key_value              NVARCHAR(1000)    NULL,
        source_value          NVARCHAR(MAX)     NULL,
        destination_value     NVARCHAR(MAX)     NULL,
        exception_reason      NVARCHAR(500)     NULL,
        batch_number          INT               NULL,
        details               NVARCHAR(MAX)     NULL
    );
END
"""

AUDIT_TABLE_ALTER_COLUMNS_DDL = """
IF COL_LENGTH('{table}', 'audit_id') IS NULL
BEGIN
    ALTER TABLE {table} ADD audit_id VARCHAR(100) NULL;
END

IF COL_LENGTH('{table}', 'client_audit_id') IS NULL
BEGIN
    ALTER TABLE {table} ADD client_audit_id VARCHAR(100) NULL;
END

IF COL_LENGTH('{table}', 'stage') IS NULL
BEGIN
    ALTER TABLE {table} ADD stage VARCHAR(50) NULL;
END
"""

LOG_SLOT_FIELD_TYPES = {
    "source_column": "VARCHAR(255)",
    "destination_column": "VARCHAR(255)",
    "source_value": "NVARCHAR(MAX)",
    "destination_value": "NVARCHAR(MAX)",
    "value_status": "VARCHAR(50)",
}


def log_slot_sql_columns(slot_count):
    """Numbered log columns: source_column_1, destination_column_1, source_value_1, ..."""
    cols = []
    for i in range(1, slot_count + 1):
        for field in LOG_SLOT_FIELD_TYPES:
            cols.append(f"{field}_{i}")
    return cols


def build_log_slot_alter_ddl(table, slot_count):
    if slot_count <= 0:
        return ""
    lines = []
    for i in range(1, slot_count + 1):
        for field, sql_type in LOG_SLOT_FIELD_TYPES.items():
            col = f"{field}_{i}"
            lines.append(f"""
IF COL_LENGTH('{table}', '{col}') IS NULL
BEGIN
    ALTER TABLE {table} ADD {col} {sql_type} NULL;
END""")
    return "\n".join(lines)


def flatten_column_slots(column_slots, slot_count):
    """Map a list of per-mapping slot dicts into flat source_column_1..N keys."""
    flat = {}
    for i in range(1, slot_count + 1):
        slot = column_slots[i - 1] if column_slots and i - 1 < len(column_slots) else {}
        for field in LOG_SLOT_FIELD_TYPES:
            val = slot.get(field) if slot else None
            flat[f"{field}_{i}"] = None if val is None else str(val)
    return flat

# One row per (client_id, flow_id) that is currently mid-run. Holds just
# enough to resume exactly where a killed process left off: which batch
# it was on, the last composite key value it successfully finished, and
# the running match/mismatch counters so the final summary is still
# accurate after a resume. Deleted once that mapping finishes normally.
CHECKPOINT_TABLE_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(N'{table}')
)
BEGIN
    CREATE TABLE {table} (
        checkpoint_id      BIGINT IDENTITY(1,1) PRIMARY KEY,
        client_id          VARCHAR(100)      NOT NULL,
        flow_id            VARCHAR(100)      NOT NULL,
        run_id             UNIQUEIDENTIFIER  NOT NULL,
        batch_number       INT               NOT NULL,
        last_values_json   NVARCHAR(MAX)      NOT NULL,
        counts_json        NVARCHAR(MAX)      NULL,
        updated_at         DATETIME2          NOT NULL DEFAULT SYSDATETIME()
    );
END
"""


# --------------------------------------------------------------------------
# Config / connections
# --------------------------------------------------------------------------

def load_config(path):
    # Keep config loading intentionally tiny and strict: if JSON is malformed
    # or path is wrong, we WANT a hard failure early before touching any DB.
    # This prevents partial execution with ambiguous defaults.
    with open(path, "r") as f:
        return json.load(f)


def build_connection_string(server_cfg):
    # Build one canonical ODBC string from config. The calling code controls
    # which server/database pair is requested; this helper only translates the
    # dictionary shape into a valid pyodbc connection string.
    #
    # Why this matters:
    # - Validation and logging can point to different servers/databases.
    # - We must support both Windows auth and SQL auth cleanly.
    # - Keeping this centralized avoids subtle drift between call sites.
    base = f"DRIVER={{{server_cfg['driver']}}};SERVER={server_cfg['server']};"
    if "database" in server_cfg:
        base += f"DATABASE={server_cfg['database']};"
    if server_cfg.get("auth", "sql").lower() == "windows":
        return base + "Trusted_Connection=yes;"
    return base + f"UID={server_cfg['username']};PWD={server_cfg['password']};"


class ConnectionPool:
        # Resolve and cache one pyodbc connection per (server_name, database) pair.
        # Why pooling is important in this validator:
        # - We execute many batches per mapping.
        # - Reconnecting per batch is expensive and can dominate runtime.
        # - Reusing connections keeps temp tables/session semantics predictable.
        # Safety considerations:
        # - A per-connection timeout is set so blocked queries fail fast instead of
        #   hanging forever.
        # - Unknown server aliases fail loudly with a clear config error.

    def __init__(self, servers_cfg, query_timeout_seconds):
        self.servers_cfg = servers_cfg
        self.query_timeout_seconds = query_timeout_seconds
        self._cache = {}

    def get(self, server_name, database=None):
        # Cache key includes database because a single SQL Server host can hold
        # multiple catalogs with different tables/schemas.
        key = (server_name, database)
        if key in self._cache:
            return self._cache[key]
        if server_name not in self.servers_cfg:
            raise KeyError(f"Server '{server_name}' is not defined in config['servers']")
        server_cfg = dict(self.servers_cfg[server_name])
        if database:
            server_cfg["database"] = database
        conn = pyodbc.connect(build_connection_string(server_cfg), autocommit=False)
        # Cap how long ANY query on this connection is allowed to run.
        # If the server is blocked/slow, this raises pyodbc.Error instead
        # of hanging forever -- and our callers already catch + log that.
        conn.timeout = self.query_timeout_seconds
        self._cache[key] = conn
        return conn

    def close_all(self):
        # Best-effort shutdown: do not let close-time exceptions mask upstream
        # results, especially at process end.
        for conn in self._cache.values():
            try:
                conn.close()
            except Exception:
                pass


# --------------------------------------------------------------------------
# Client catalog resolution — merges JSON clients[] with per-mapping overrides
# --------------------------------------------------------------------------

def _catalog_key(client_id, flow_id):
    return f"{str(client_id or '')}|{str(flow_id or '')}"


def build_client_catalog(config):
    """(client_id, flow_id) -> client entry; client_id alone is kept as a fallback key."""
    catalog = {}
    for c in config.get("clients", []):
        cid = str(c["client_id"])
        catalog[_catalog_key(cid, c.get("flow_id"))] = c
        catalog.setdefault(cid, c)
    return catalog


def catalog_entry(catalog, client_id, flow_id=None):
    return catalog.get(_catalog_key(client_id, flow_id)) or catalog.get(str(client_id or ""))


def resolve_servers_and_db(mapping, catalog):
    """
    Mapping-row values win; otherwise fall back to the client catalog.
    Returns dict with resolved values plus a 'catalog_missing' flag/reason.
    """
    client_id = str(mapping.get("client_id", ""))
    cat = catalog_entry(catalog, client_id, mapping.get("flow_id"))

    resolved = {
        "source_server": mapping.get("source_server"),
        "source_database": mapping.get("source_database"),
        "destination_server": mapping.get("destination_server"),
        "destination_database": mapping.get("destination_database"),
        "catalog_missing": False,
        "catalog_reason": None,
    }

    if cat:
        resolved["source_server"] = resolved["source_server"] or cat.get("source_server") or cat.get("server")
        resolved["source_database"] = resolved["source_database"] or cat.get("source_database") or cat.get("workflow_db")
        resolved["destination_server"] = resolved["destination_server"] or cat.get("destination_server")
        resolved["destination_database"] = resolved["destination_database"] or cat.get("destination_database")
    elif not (resolved["source_server"] and resolved["destination_server"]):
        resolved["catalog_missing"] = True
        resolved["catalog_reason"] = f"No client catalog entry for client_id='{client_id}' and no explicit server override given"

    return resolved


def resolve_fetch_config(mapping, catalog):
    # Two ways to load data:
    #   table        — read directly from source_table / destination_table (no custom SQL needed)
    #   custom_query — run your own SELECT in source_fetch_query / destination_fetch_query
    client_id = str(mapping.get("client_id", ""))
    cat = catalog_entry(catalog, client_id, mapping.get("flow_id")) or {}
    fetch_mode = mapping.get("fetch_mode") or cat.get("fetch_mode") or "table"  # default is table mode
    return {
        "fetch_mode": fetch_mode,
        "source_fetch_query": mapping.get("source_fetch_query") or cat.get("source_fetch_query"),
        "destination_fetch_query": mapping.get("destination_fetch_query") or cat.get("destination_fetch_query"),
    }


def resolve_mapping_from_client(mapping, catalog):
    # Fill missing mapping fields from clients[] — so a slim mappings[] row can just list columns
    client_id = str(mapping.get("client_id", ""))
    cat = catalog_entry(catalog, client_id, mapping.get("flow_id")) or {}
    resolved = dict(mapping)
    inherit_keys = (
        "flow_id", "source_table", "destination_table",
        "source_server", "source_database", "destination_server", "destination_database",
        "reference_keys", "normalization", "log_columns", "preload_columns",
        "fetch_mode", "source_fetch_query", "destination_fetch_query",
        "exclude_from_validation", "validation_pairs", "insert_pairs",
    )
    for key in inherit_keys:
        if resolved.get(key) in (None, [], "") and cat.get(key) not in (None, ""):
            resolved[key] = cat[key]
    return resolved


def _split_select_list(select_part):
    # Split "col1, col2, fn(a, b)" on commas — but skip commas inside ( ).
    parts, depth, current = [], 0, []
    for ch in select_part:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(ch)
    part = "".join(current).strip()
    if part:
        parts.append(part)
    return parts


def parse_select_column_names(sql):
    # Read column names from a SELECT query (uses AS alias when you wrote one).
    cleaned = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)  # remove /* comments */
    cleaned = re.sub(r"--[^\n]*", " ", cleaned)  # remove -- line comments
    match = re.search(r"\bSELECT\b(.*?)\bFROM\b", cleaned, flags=re.I | re.S)
    if not match:
        raise ValueError("Could not parse SELECT column list from custom fetch query")
    # Turn each SELECT item into one column name
    return [_select_expr_to_column_name(expr) for expr in _split_select_list(match.group(1))]


def _select_expr_to_column_name(expr):
    # One piece of the SELECT list, e.g. "dbo.Table.Col AS MyCol"
    expr = expr.strip()
    # If it ends with "AS SomeName", that alias is the column name we want
    alias_match = re.search(r"\bAS\s+(\[[^\]]+\]|\"[^\"]+\"|'[^']+'|\w+)\s*$", expr, flags=re.I)
    if alias_match:
        # Remove brackets or quotes around the alias
        return alias_match.group(1).strip("[]\"'")
    # There was AS but we could not read the name — ask user to fix the SQL
    if re.search(r"\bAS\s+", expr, flags=re.I):
        raise ValueError(f"Could not parse column alias from SELECT expression: {expr}")
    # No AS — use the last part after a dot: dbo.Table.Col → Col
    token = expr.split(".")[-1].strip()
    return token.strip("[]\"'")


def _column_excluded_from_validation(col_name, exclude_names, runtime_cfg):
    if not col_name:
        return True
    lower = str(col_name).lower()
    if lower in {str(n).lower() for n in exclude_names}:
        return True
    probe = {str(col_name): 1}
    for field in ("client_id", "flow_id", "audit_id", "client_audit_id", "primary_key"):
        if _pick_value_by_name_patterns(probe, runtime_cfg.identity_patterns(field)):
            return True
    return False


def _validation_exclude_names(client_cfg, runtime_cfg):
    exclude = set()
    for rk in client_cfg.get("reference_keys") or []:
        for key in ("source_column", "destination_column", "name"):
            if rk.get(key):
                exclude.add(str(rk[key]))
    for side in ("source", "destination"):
        fallbacks = runtime_cfg.identity.get(f"{side}_column_fallbacks") or {}
        for names in fallbacks.values():
            exclude.update(str(n) for n in names)
    exclude.update(str(n) for n in (client_cfg.get("exclude_from_validation") or []))
    return exclude


def build_mappings_from_client(client_cfg, runtime_cfg):
    client_id = str(client_cfg["client_id"])
    flow_id = str(client_cfg.get("flow_id", "") or "")

    # Optional manual list — use when source/dest columns do not line up by order
    pairs = client_cfg.get("validation_pairs")
    if pairs:
        return [
            {"client_id": client_id, "flow_id": flow_id, "source_column": s, "destination_column": d}
            for s, d in pairs
        ]

    src_sql = client_cfg.get("source_fetch_query")
    dst_sql = client_cfg.get("destination_fetch_query")
    if not src_sql or not dst_sql:
        return []  # need both queries to auto-build mappings

    src_cols = parse_select_column_names(src_sql)  # column names from source SELECT
    dst_cols = parse_select_column_names(dst_sql)  # column names from destination SELECT
    exclude = _validation_exclude_names(client_cfg, runtime_cfg)  # join keys, identity cols, exclude list

    # Drop join keys and excluded cols — whatever is left is what we compare
    src_val = [c for c in src_cols if not _column_excluded_from_validation(c, exclude, runtime_cfg)]
    dst_val = [c for c in dst_cols if not _column_excluded_from_validation(c, exclude, runtime_cfg)]

    if not src_val:
        raise ValueError(f"client {client_id}: no validation columns found in source_fetch_query")
    if len(src_val) != len(dst_val):
        # Source and dest must have the same number of columns, matched by SELECT order
        raise ValueError(
            f"client {client_id}: source query has {len(src_val)} validation column(s) "
            f"{src_val}, destination query has {len(dst_val)} {dst_val}. "
            "Keep the same count/order, or list extras in exclude_from_validation, "
            "or set validation_pairs explicitly."
        )

    # One mapping per column pair — slot 1, slot 2, ... in the log table
    return [
        {"client_id": client_id, "flow_id": flow_id, "source_column": s, "destination_column": d}
        for s, d in zip(src_val, dst_val)
    ]


def _norm_col(name):
    return str(name or "").strip().lower()


def filter_mappings_by_columns(mappings, catalog, any_columns=None, source_columns=None, dest_columns=None):
    """
    Filter column mappings by CLI names (case-insensitive).
    --column NAME     matches source OR destination column
    --source-column   matches source only
    --dest-column     matches destination only
    All specified groups must pass (AND across groups).
    """
    any_set = {_norm_col(c) for c in (any_columns or []) if c}
    src_set = {_norm_col(c) for c in (source_columns or []) if c}
    dst_set = {_norm_col(c) for c in (dest_columns or []) if c}
    if not any_set and not src_set and not dst_set:
        return list(mappings)

    filtered = []
    for raw in mappings:
        m = resolve_mapping_from_client(raw, catalog)
        src = _norm_col(m.get("source_column"))
        dst = _norm_col(m.get("destination_column"))
        if any_set and src not in any_set and dst not in any_set:
            continue
        if src_set and src not in src_set:
            continue
        if dst_set and dst not in dst_set:
            continue
        filtered.append(raw)

    log.info(
        f"Column filter active: {len(filtered)} of {len(mappings)} mapping(s) "
        f"(any={sorted(any_set) or '-'}, source={sorted(src_set) or '-'}, dest={sorted(dst_set) or '-'})"
    )
    return filtered


def filter_mappings_for_insert_config(mappings, catalog):
    """Apply clients[].insert_pairs when no CLI column filter (insert mode only)."""
    filtered = []
    limited_clients = set()
    for raw in mappings:
        m = resolve_mapping_from_client(raw, catalog)
        cid = str(m.get("client_id", ""))
        insert_pairs = (catalog_entry(catalog, cid, m.get("flow_id")) or {}).get("insert_pairs")
        if insert_pairs:
            limited_clients.add(cid)
            allowed = {(s, d) for s, d in insert_pairs}
            if (m.get("source_column"), m.get("destination_column")) in allowed:
                filtered.append(raw)
        else:
            filtered.append(raw)
    if limited_clients:
        log.info(
            f"Insert limited by insert_pairs for client(s) {sorted(limited_clients)}: "
            f"{len(filtered)} of {len(mappings)} column mapping(s)"
        )
    return filtered


def mappings_for_insert(all_mappings, catalog, column_filter):
    """Insert mappings: CLI filter first, else config insert_pairs, else all."""
    if column_filter.get("active"):
        return filter_mappings_by_columns(
            all_mappings, catalog,
            any_columns=column_filter.get("any_columns"),
            source_columns=column_filter.get("source_columns"),
            dest_columns=column_filter.get("dest_columns"),
        )
    return filter_mappings_for_insert_config(all_mappings, catalog)


def mappings_for_validate(all_mappings, catalog, column_filter):
    """Validate mappings: CLI filter if set, else all."""
    if column_filter.get("active"):
        return filter_mappings_by_columns(
            all_mappings, catalog,
            any_columns=column_filter.get("any_columns"),
            source_columns=column_filter.get("source_columns"),
            dest_columns=column_filter.get("dest_columns"),
        )
    return list(all_mappings)


def resolve_all_mappings(config, catalog, runtime_cfg):
    # How we know WHAT columns to compare:
    #   1) mappings[] in config.json — you list each source_column / destination_column (table mode)
    #   2) auto from SQL — only when fetch_mode=custom_query and both fetch queries exist
    explicit = config.get("mappings") or []
    if explicit:
        return explicit  # table mode: you define column pairs yourself

    mappings = []
    for client_cfg in config.get("clients", []):
        if client_cfg.get("fetch_mode") != "custom_query":
            continue  # table-mode jobs must use mappings[] — nothing to auto-build here
        if not client_cfg.get("source_fetch_query") or not client_cfg.get("destination_fetch_query"):
            continue
        mappings.extend(build_mappings_from_client(client_cfg, runtime_cfg))
    return mappings


def _row_get(row_dict, col_name):
    """Case-insensitive column lookup in a result row dict."""
    if not row_dict or not col_name:
        return None
    if col_name in row_dict:
        return row_dict[col_name]
    target = str(col_name).lower()
    for key, value in row_dict.items():
        if str(key).lower() == target:
            return value
    return None


def _resolve_result_columns(result_columns, requested_names):
    """Map configured column names to actual SQL result column names."""
    resolved = []
    for name in requested_names:
        match = next((c for c in result_columns if str(c).lower() == str(name).lower()), None)
        if match is None:
            raise ValueError(
                f"Custom fetch query must return column '{name}'. "
                f"Query returned: {result_columns}"
            )
        resolved.append(match)
    return resolved


def execute_custom_fetch_query(conn, sql, key_column_names, runtime_cfg, progress_cb=None):
    cur = conn.cursor()
    cur.execute(sql)  # runs source_fetch_query or destination_fetch_query from config
    result_columns = [c[0] for c in cur.description]  # real column names SQL Server sent back
    key_cols_resolved = _resolve_result_columns(result_columns, key_column_names)  # join key cols from reference_keys

    # Store rows by join key — row order in SQL does not matter after this
    rows = {}
    duplicate_keys = set()
    fetch_size = runtime_cfg.option("preload_fetch_size")
    progress_every = runtime_cfg.option("preload_progress_every")
    warn_on_duplicates = runtime_cfg.option("warn_on_duplicate_keys")
    loaded = 0

    while True:
        chunk = cur.fetchmany(fetch_size)  # read in chunks so big result sets do not use too much RAM
        if not chunk:
            break
        for row in chunk:
            row_dict = dict(zip(result_columns, row))  # one SQL row → {column_name: value}
            # Build join key from reference_keys, e.g. (ClientAuditId,)
            composite = tuple(_row_get(row_dict, col) for col in key_cols_resolved)
            if composite in rows and warn_on_duplicates:
                duplicate_keys.add(composite)  # same key twice — last row wins
            rows[composite] = row_dict
        loaded += len(chunk)
        if progress_cb and loaded % progress_every == 0:
            progress_cb(loaded)

    if duplicate_keys:
        log.warning(
            f"Custom fetch query returned {len(duplicate_keys)} duplicate join key(s); "
            "last row wins for each duplicate key"
        )
    return rows, key_cols_resolved  # caller does rows.get(same_key) on source and destination


# --------------------------------------------------------------------------
# FIX (see chat): cache key must depend only on what determines the result
# (side + the SQL text itself), NOT on which client is asking. Two clients
# running the exact same destination_fetch_query should hit the cache, not
# re-run an identical multi-minute full-table pull. client_id is still
# accepted as a parameter so every call site stays unchanged; it is simply
# no longer part of the key.
# --------------------------------------------------------------------------

def get_custom_query_cache(side, client_id, sql):
    return CUSTOM_QUERY_CACHE.get((side, sql))


def set_custom_query_cache(side, client_id, sql, rows):
    CUSTOM_QUERY_CACHE[(side, sql)] = rows


def reset_log_table(conn, log_table, runtime_cfg):
    """
    options.truncate_log_table_on_run
      false (default) -- every run appends, so one table holds many jobs.
                         Right when every job joins on the same reference key.
      true            -- wipe the log table (and any checkpoints) once at the
                         start of the run, then log this job into the empty table.
                         Use when the new job joins on a DIFFERENT reference key,
                         so old rows keyed differently do not sit alongside it.
    Runs once per run, before any log row is written. Never touches source or
    destination tables -- only the log table and its checkpoint table.
    """
    if not runtime_cfg.options.get("truncate_log_table_on_run", False):
        return False

    checkpoint_table = log_table + "_Checkpoint"
    cur = conn.cursor()
    for table in (log_table, checkpoint_table):
        try:
            cur.execute(f"IF OBJECT_ID(N'{table}') IS NOT NULL TRUNCATE TABLE {table}")
        except Exception:
            conn.rollback()  # no TRUNCATE permission, or the table is referenced
            cur = conn.cursor()
            cur.execute(f"IF OBJECT_ID(N'{table}') IS NOT NULL DELETE FROM {table}")
    conn.commit()
    log.warning(
        f"truncate_log_table_on_run=true -- {log_table} and {checkpoint_table} "
        "were emptied before this run"
    )
    return True


def resolve_server_name_for_log(server_ref, servers_cfg):
    """Return physical server hostname for logging (e.g. VC03-...), not alias key."""
    if not server_ref:
        return server_ref
    cfg = servers_cfg.get(server_ref)
    if isinstance(cfg, dict):
        return cfg.get("server") or server_ref
    return server_ref


# Checkpoint helpers — save/resume join keys when a long run stops mid-way.

def _serialize_key_values(values):
    out = []
    for v in values:
        if isinstance(v, datetime.datetime):
            out.append({"t": "dt", "v": v.isoformat()})
        elif isinstance(v, datetime.date):
            out.append({"t": "d", "v": v.isoformat()})
        elif v is None or isinstance(v, (int, float, str)):
            out.append({"t": "p", "v": v})  # plain -- JSON handles it natively
        else:
            # Decimal, uuid.UUID, etc. -- safe fallback, compares fine as text
            out.append({"t": "s", "v": str(v)})
    return out


def _deserialize_key_values(data):
    out = []
    for item in data:
        kind, v = item["t"], item["v"]
        if kind == "dt":
            out.append(datetime.datetime.fromisoformat(v))
        elif kind == "d":
            out.append(datetime.date.fromisoformat(v))
        else:
            out.append(v)
    return out


class AuditLogger:
    # Writes rows to ValidationLog2. Numbered slots (source_column_1..N) grow automatically.

    def __init__(self, conn, log_table, run_id, run_start_time, stage="VALIDATION", flush_every=500,
                 log_column_slots=0):
        self.conn = conn
        self.log_table = log_table
        self.checkpoint_table = log_table + "_Checkpoint"
        self.run_id = run_id
        self.run_start_time = run_start_time
        self.stage = stage
        self.flush_every = flush_every
        self.log_column_slots = max(0, int(log_column_slots or 0))
        self._slot_columns = log_slot_sql_columns(self.log_column_slots)
        self.buffer = []
        self.counts = {}
        self.default_context = {}
        self._ensure_table()
        self._ensure_checkpoint_table()

    def ensure_log_column_slots(self, slot_count):
        # Add source_column_1..N columns to the log table if they are not there yet.
        slot_count = max(0, int(slot_count or 0))
        if slot_count <= self.log_column_slots:
            return
        self.flush()
        self.log_column_slots = slot_count
        self._slot_columns = log_slot_sql_columns(slot_count)
        cur = self.conn.cursor()
        ddl = build_log_slot_alter_ddl(self.log_table, slot_count)
        if ddl:
            cur.execute(ddl)
        self.conn.commit()

    def set_stage(self, stage):
        self.stage = stage

    def set_default_context(self, **ctx):
        self.default_context = dict(ctx)

    def clear_default_context(self):
        self.default_context = {}

    def _ctx(self, key, value):
        # Explicit call-site value wins; otherwise inherit mapping-level
        # default context set at validate_mapping()/insert_mapping() start.
        return value if value is not None else self.default_context.get(key)

    def _ensure_table(self):
        cur = self.conn.cursor()
        cur.execute(AUDIT_TABLE_DDL.format(table=self.log_table))
        # Keep old log tables forward-compatible by adding new columns if missing.
        cur.execute(AUDIT_TABLE_ALTER_COLUMNS_DDL.format(table=self.log_table))
        slot_ddl = build_log_slot_alter_ddl(self.log_table, self.log_column_slots)
        if slot_ddl:
            cur.execute(slot_ddl)
        self.conn.commit()

    def _ensure_checkpoint_table(self):
        cur = self.conn.cursor()
        cur.execute(CHECKPOINT_TABLE_DDL.format(table=self.checkpoint_table))
        self.conn.commit()

    def log(self, client_id=None, flow_id=None, audit_id=None, client_audit_id=None,
            source_server=None, destination_server=None,
            source_database=None, destination_database=None, source_table=None, destination_table=None,
            source_column=None, destination_column=None, mapping_status=None, join_key_status=None,
            value_status=None, issue_type="INFO", key_value=None, source_value=None,
            destination_value=None, exception_reason=None, batch_number=None, details=None,
            column_slots=None):
        # Resolve each field against current default context so every audit row
        # remains queryable even if a call site only passes delta values.
        client_id = self._ctx("client_id", client_id)
        flow_id = self._ctx("flow_id", flow_id)
        audit_id = self._ctx("audit_id", audit_id)
        client_audit_id = self._ctx("client_audit_id", client_audit_id)
        source_server = self._ctx("source_server", source_server)
        destination_server = self._ctx("destination_server", destination_server)
        source_database = self._ctx("source_database", source_database)
        destination_database = self._ctx("destination_database", destination_database)
        source_table = self._ctx("source_table", source_table)
        destination_table = self._ctx("destination_table", destination_table)
        source_column = self._ctx("source_column", source_column)
        destination_column = self._ctx("destination_column", destination_column)

        if mapping_status is None:
            # Mapping status describes configuration readiness, not row outcome.
            # For non-configuration issues, default to Ready.
            if issue_type in ("CONFIGURATION_EXCEPTION",):
                mapping_status = MISSING_REFERENCE
            else:
                mapping_status = READY

        if join_key_status is None:
            # Join-key status defaults based on issue semantics when caller did
            # not provide an explicit value.
            if issue_type == "MISSING_IN_DESTINATION":
                join_key_status = JOIN_MISSING_DESTINATION_KEY
            elif issue_type == "MISSING_IN_SOURCE":
                join_key_status = JOIN_MISSING_SOURCE_KEY
            elif issue_type in ("INFO", "RUN_SUMMARY", "BATCH_ERROR", "ROW_ERROR", "CONNECTION_ERROR", "CONFIGURATION_EXCEPTION"):
                join_key_status = JOIN_NOT_CONFIGURED
            else:
                join_key_status = JOIN_READY

        if value_status is None:
            # Convert technical issue_type into friendly reporting values.
            if issue_type in ("MISMATCH", "SOURCE_BLANK", "DESTINATION_BLANK", "BOTH_BLANK"):
                value_status = issue_type.replace("_", " ").title()
            else:
                value_status = NOT_EVALUATED

        if key_value is None:
            key_value = NOT_AVAILABLE

        # Values are logged exactly as they came out of the row: NULL stays NULL,
        # an empty string stays empty, the text 'NA' stays 'NA'. No placeholder is
        # ever substituted -- issue_type and value_status carry the meaning instead.

        if exception_reason is None:
            exception_reason = issue_type

        if details is None:
            details = exception_reason

        slot_flat = flatten_column_slots(column_slots, self.log_column_slots) if self.log_column_slots else {}

        self.counts[issue_type] = self.counts.get(issue_type, 0) + 1
        row = (
            self.run_id, self.run_start_time, self.stage, client_id, flow_id, audit_id, client_audit_id,
            source_server, destination_server, source_database, destination_database,
            source_table, destination_table, source_column, destination_column,
            mapping_status, join_key_status, value_status, issue_type,
            str(key_value),
            None if source_value is None else str(source_value),
            None if destination_value is None else str(destination_value),
            exception_reason, batch_number, details,
        )
        if self._slot_columns:
            row = row + tuple(slot_flat.get(col) for col in self._slot_columns)
        self.buffer.append(row)
        if len(self.buffer) >= self.flush_every:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        # fast_executemany drastically improves insert throughput for large
        # row-level logging volumes.
        cur = self.conn.cursor()
        cur.fast_executemany = True
        slot_cols_sql = ", ".join(self._slot_columns)
        slot_vals_sql = (", " + ", ".join("?" * len(self._slot_columns))) if self._slot_columns else ""
        sql = f"""
            INSERT INTO {self.log_table}
            (run_id, run_start_time, stage, client_id, flow_id, audit_id, client_audit_id, source_server, destination_server,
             source_database, destination_database, source_table, destination_table,
             source_column, destination_column, mapping_status, join_key_status, value_status,
             issue_type, key_value, source_value, destination_value, exception_reason,
             batch_number, details{", " + slot_cols_sql if slot_cols_sql else ""})
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?{slot_vals_sql})
        """
        cur.executemany(sql, self.buffer)
        self.conn.commit()
        self.buffer = []

    def write_summary(self, client_id, flow_id, source_table, destination_table, extra_counts=None):
        self.flush()
        summary = dict(self.counts)
        if extra_counts:
            summary.update(extra_counts)
        self.log(
            client_id=client_id, flow_id=flow_id,
            source_table=source_table, destination_table=destination_table,
            issue_type="RUN_SUMMARY", details=json.dumps(summary),
        )
        self.flush()

    # ----------------------------------------------------------------
    # Crash-recovery checkpointing. Every method here is wrapped in its
    # own try/except and returns a safe default on failure -- a broken
    # checkpoint table should never be able to take down the actual
    # validation run, it should just mean "no resume support this time."
    # ----------------------------------------------------------------

    def load_checkpoint(self, client_id, flow_id):
        """Returns {'batch_number', 'last_values', 'counts'} if a resumable checkpoint exists, else None."""
        try:
            cur = self.conn.cursor()
            cur.execute(
                f"SELECT run_id, batch_number, last_values_json, counts_json "
                f"FROM {self.checkpoint_table} WHERE client_id = ? AND flow_id = ?",
                [client_id, flow_id],
            )
            row = cur.fetchone()
            if not row:
                return None

            checkpoint_run_id, batch_number, last_values_json, counts_json = row

            # Resume only when checkpoint state is backed by persisted audit rows.
            # If the log table was dropped/truncated but checkpoint remained,
            # resuming would incorrectly skip work. In that case, restart fresh.
            cur.execute(
                f"SELECT TOP 1 1 FROM {self.log_table} "
                f"WHERE client_id = ? AND flow_id = ? AND run_id = ?",
                [client_id, flow_id, checkpoint_run_id],
            )
            if cur.fetchone() is None:
                log.warning(
                    f"[client={client_id} flow={flow_id}] Stale checkpoint detected "
                    f"(no backing rows found in {self.log_table}); restarting from batch 1"
                )
                self.clear_checkpoint(client_id, flow_id)
                return None

            return {
                "run_id": str(checkpoint_run_id),
                "batch_number": batch_number,
                "last_values": _deserialize_key_values(json.loads(last_values_json)),
                "counts": json.loads(counts_json) if counts_json else {},
            }
        except Exception as e:
            log.warning(f"[client={client_id} flow={flow_id}] Could not read checkpoint, starting from the beginning: {e}")
            return None

    def save_checkpoint(self, client_id, flow_id, batch_number, last_values, counts):
        """Called after every successful batch so a crash loses at most one batch of progress."""
        try:
            last_values_json = json.dumps(_serialize_key_values(last_values))
            counts_json = json.dumps(counts)
            cur = self.conn.cursor()
            cur.execute(
                f"UPDATE {self.checkpoint_table} "
                f"SET run_id = ?, batch_number = ?, last_values_json = ?, counts_json = ?, updated_at = SYSDATETIME() "
                f"WHERE client_id = ? AND flow_id = ?",
                [self.run_id, batch_number, last_values_json, counts_json, client_id, flow_id],
            )
            if cur.rowcount == 0:
                cur.execute(
                    f"INSERT INTO {self.checkpoint_table} "
                    f"(client_id, flow_id, run_id, batch_number, last_values_json, counts_json) "
                    f"VALUES (?, ?, ?, ?, ?, ?)",
                    [client_id, flow_id, self.run_id, batch_number, last_values_json, counts_json],
                )
            self.conn.commit()
        except Exception as e:
            # Not fatal -- worst case, a resume later starts one batch earlier than it could have.
            log.warning(f"[client={client_id} flow={flow_id}] Could not save checkpoint at batch {batch_number}: {e}")

    def clear_checkpoint(self, client_id, flow_id):
        """Called once a mapping finishes normally, so the next run starts fresh instead of 'resuming' a done mapping."""
        try:
            cur = self.conn.cursor()
            cur.execute(
                f"DELETE FROM {self.checkpoint_table} WHERE client_id = ? AND flow_id = ?",
                [client_id, flow_id],
            )
            self.conn.commit()
        except Exception as e:
            log.warning(f"[client={client_id} flow={flow_id}] Could not clear checkpoint: {e}")


# --------------------------------------------------------------------------
# BR-01 / VR-10 / AC-02: completeness classification
# --------------------------------------------------------------------------

def classify_mapping(mapping):
    # Check config is complete before touching the database.
    # Needs: source_table, source_column, destination_table, destination_column, reference_keys.
    if mapping.get("not_applicable"):
        return NOT_APPLICABLE, [mapping.get("not_applicable_reason", "Marked not applicable in config")]

    reasons = []

    if not mapping.get("destination_table") or not mapping.get("destination_column"):
        reasons.append("destination_table/destination_column not populated")

    if not mapping.get("source_table") or not mapping.get("source_column"):
        reasons.append("source_table/source_column not populated")

    ref_keys = mapping.get("reference_keys", [])
    usable_ref_keys = [r for r in ref_keys if r.get("source_column") and r.get("destination_column")]

    if not ref_keys:
        reasons.append("no reference keys configured -- at least one is required to join source and destination rows")
    elif not usable_ref_keys:
        reasons.append("none of the configured reference keys have both a source_column and a destination_column set")

    if reasons:
        if any("destination_table/destination_column" in r for r in reasons):
            return MISSING_DESTINATION, reasons
        return MISSING_REFERENCE, reasons

    return READY, []


def classify_join_keys(mapping):
    """Per reference key: Ready / Missing Source Key / Missing Destination Key / Not configured."""
    results = []
    for ref in mapping.get("reference_keys", []):
        name = ref.get("name", "unnamed")
        src, dst = ref.get("source_column"), ref.get("destination_column")
        if not src and not dst:
            status = JOIN_NOT_CONFIGURED
        elif not src:
            status = JOIN_MISSING_SOURCE_KEY
        elif not dst:
            status = JOIN_MISSING_DESTINATION_KEY
        else:
            status = JOIN_READY
        results.append({"name": name, "source_column": src, "destination_column": dst, "status": status})
    return results


def _try_parse_date(value, date_formats, explicit_format=None):
    s = str(value).strip()
    if explicit_format:
        try:
            return datetime.datetime.strptime(s, explicit_format)
        except ValueError:
            return None
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value
    for fmt in date_formats:
        try:
            return datetime.datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def normalize_value(value, norm_cfg, runtime_cfg):
    if value is None:
        return None
    s = str(value)
    if s.strip() == "":
        return None

    norm_cfg = norm_cfg or dict(runtime_cfg.normalization_defaults)
    n_type = norm_cfg.get("type", runtime_cfg.normalization_defaults.get("type", "text"))
    if n_type == "datetime":
        n_type = "date"

    if n_type == "date":
        parsed = _try_parse_date(value, runtime_cfg.date_formats, norm_cfg.get("format"))
        if parsed is None:
            return s.strip()
        out_fmt = norm_cfg.get("output_format", "%Y-%m-%d %H:%M:%S")
        return parsed.strftime(out_fmt)

    if n_type == "numeric":
        try:
            if isinstance(value, decimal.Decimal):
                num = value
            else:
                num = decimal.Decimal(str(value).strip().replace(",", ""))
            places = norm_cfg.get("decimal_places")
            if places is not None:
                num = num.quantize(decimal.Decimal(10) ** -int(places))
            else:
                num = num.normalize()
            out = format(num, "f")
            if "." in out:
                out = out.rstrip("0").rstrip(".")
            return out
        except (decimal.InvalidOperation, ValueError, TypeError):
            return s.strip()

    out = s
    if norm_cfg.get("trim", True):
        out = out.strip()
    if norm_cfg.get("collapse_whitespace", True):
        out = re.sub(r"\s+", " ", out)
    if norm_cfg.get("case_insensitive", True):
        out = out.lower()
    return out


def compare_value(src_raw, dst_raw, norm_cfg, runtime_cfg):
    n_src = normalize_value(src_raw, norm_cfg, runtime_cfg)
    n_dst = normalize_value(dst_raw, norm_cfg, runtime_cfg)

    if n_src is None and n_dst is None:
        return BOTH_BLANK, n_src, n_dst
    if n_src is None:
        return SOURCE_BLANK, n_src, n_dst
    if n_dst is None:
        return DESTINATION_BLANK, n_src, n_dst
    if n_src != n_dst:
        return MISMATCH, n_src, n_dst
    return MATCH, n_src, n_dst


def _pick_value_by_name_patterns(key_map, patterns):
    for pattern in patterns:
        for col, value in key_map.items():
            if value is None:
                continue
            if re.match(pattern, str(col), flags=re.IGNORECASE):
                return value
    return None


def build_key_context(source_key_columns, key_values, runtime_cfg):
    key_map = {}
    for i, col in enumerate(source_key_columns):
        value = key_values[i] if i < len(key_values) else None
        key_map[str(col)] = value

    primary_value = _pick_value_by_name_patterns(
        key_map, runtime_cfg.identity_patterns("primary_key")
    )

    if primary_value is None and key_values:
        primary_value = key_values[0]

    return primary_value, key_map, _json_dumps_safe(key_map)


def _resolve_column_fallback(row, fallback_names):
    if not row or not fallback_names:
        return None
    for cand in fallback_names:
        if cand in row and row.get(cand) is not None:
            return str(row.get(cand))
    return None


def build_row_log_identity(key_map, default_client_id, default_flow_id, runtime_cfg,
                           src_row=None, dst_row=None, key_primary_value=None):
    lookup = dict(key_map or {})
    for row in (src_row, dst_row):
        if not row:
            continue
        for col, val in row.items():
            if col not in lookup:
                lookup[col] = val

    def _pick(field):
        val = _pick_value_by_name_patterns(lookup, runtime_cfg.identity_patterns(field))
        if val is not None:
            return val
        val = _resolve_column_fallback(src_row, runtime_cfg.column_fallbacks("source", field))
        if val is not None:
            return val
        return _resolve_column_fallback(dst_row, runtime_cfg.column_fallbacks("destination", field))

    row_client_id = _pick("client_id")
    row_flow_id = _pick("flow_id")
    row_audit_id = _pick("audit_id")
    row_client_audit_id = _pick("client_audit_id")

    if row_client_audit_id is None and key_primary_value not in (None, NOT_AVAILABLE):
        row_client_audit_id = str(key_primary_value)

    return (
        str(row_client_id) if row_client_id is not None else default_client_id,
        str(row_flow_id) if row_flow_id is not None else default_flow_id,
        str(row_audit_id) if row_audit_id is not None else None,
        str(row_client_audit_id) if row_client_audit_id is not None else None,
    )


def build_composite_predicate(columns, last_values):
    if last_values is None:
        return "", []
    clauses, params = [], []
    for i in range(len(columns)):
        eq_parts = [f"{columns[j]} = ?" for j in range(i)]
        gt_part = f"{columns[i]} > ?"
        clauses.append("(" + " AND ".join(eq_parts + [gt_part]) + ")")
        params.extend(last_values[:i])
        params.append(last_values[i])
    return "(" + " OR ".join(clauses) + ")", params


def fetch_key_batch(conn, table, key_columns, date_filter, active_date_col, last_values, batch_size, runtime_cfg):
    if not key_columns:
        raise ValueError("fetch_key_batch called with no key_columns -- at least one reference key is required")

    where_clauses, params = [], []

    predicate, pred_params = build_composite_predicate(key_columns, last_values)
    if predicate:
        where_clauses.append(predicate)
        params.extend(pred_params)

    if date_filter and date_filter.get("enabled") and active_date_col:
        where_clauses.append(f"{active_date_col} {date_filter['operator']} ?")
        params.append(date_filter["value"])

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    order_sql = ", ".join(key_columns)
    col_list = ", ".join(key_columns)
    sql = runtime_cfg.render_query(
        "source_key_batch",
        batch_size=batch_size,
        columns=col_list,
        table=table,
        nolock_hint=runtime_cfg.nolock_hint(),
        where=where_sql,
        order_by=order_sql,
    )

    cur = conn.cursor()
    cur.execute(sql, params)
    return [tuple(row) for row in cur.fetchall()]


class BatchKeyMatcher:
    def __init__(self, conn, key_columns, runtime_cfg):
        self.conn = conn
        self.key_columns = key_columns
        self.runtime_cfg = runtime_cfg
        self.tmp_name = f"#keys_{uuid.uuid4().hex[:8]}"
        self.tmp_cols = [f"k{i}" for i in range(len(key_columns))]
        key_col_type = runtime_cfg.option("temp_key_column_type")
        column_defs = ", ".join(f"{c} {key_col_type}" for c in self.tmp_cols)
        cur = conn.cursor()
        cur.execute(runtime_cfg.render_query(
            "temp_table_create",
            temp_table=self.tmp_name,
            column_defs=column_defs,
        ))
        # FIX (see chat): without an index on the temp table's key columns,
        # the JOIN below forces SQL Server to full-scan the DESTINATION
        # table on every single batch (no index on either side of the
        # join). That's invisible at ~90k destination rows but becomes
        # ruinously slow at 24M+ rows -- thousands of full scans instead
        # of thousands of index seeks. Index the temp table immediately
        # after creating it so the optimizer can seek instead of scan.
        index_cols = ", ".join(self.tmp_cols)
        cur.execute(
            f"CREATE CLUSTERED INDEX IX_{self.tmp_name.lstrip('#')} "
            f"ON {self.tmp_name} ({index_cols})"
        )
        self.conn.commit()

    def fetch_rows(self, table, data_columns, keys):
        if not keys:
            return {}

        cur = self.conn.cursor()
        cur.execute(self.runtime_cfg.render_query(
            "temp_table_truncate",
            temp_table=self.tmp_name,
        ))
        cur.fast_executemany = True
        placeholders = ", ".join("?" for _ in self.tmp_cols)
        insert_sql = self.runtime_cfg.render_query(
            "temp_table_insert",
            temp_table=self.tmp_name,
            columns=", ".join(self.tmp_cols),
            placeholders=placeholders,
        )
        cur.executemany(insert_sql, [tuple(str(v) for v in key) for key in keys])

        cast_for_join = self.runtime_cfg.option("cast_temp_keys_for_join")
        join_parts = []
        for i, col in enumerate(self.key_columns):
            if cast_for_join:
                join_parts.append(f"CAST(t.{col} AS NVARCHAR(500)) = tmp.{self.tmp_cols[i]}")
            else:
                join_parts.append(f"t.{col} = tmp.{self.tmp_cols[i]}")
        join_cond = " AND ".join(join_parts)
        all_cols = list(dict.fromkeys(list(self.key_columns) + data_columns))
        col_list = ", ".join(f"t.{c}" for c in all_cols)
        sql = self.runtime_cfg.render_query(
            "batch_row_fetch",
            select_columns=col_list,
            table=table,
            nolock_hint=self.runtime_cfg.nolock_hint(),
            temp_table=self.tmp_name,
            join_condition=join_cond,
        )
        cur.execute(sql)

        rows = {}
        for row in cur.fetchall():
            row_dict = dict(zip(all_cols, row))
            composite = tuple(row_dict[c] for c in self.key_columns)
            rows[composite] = row_dict
        return rows

    def close(self):
        try:
            cur = self.conn.cursor()
            cur.execute(self.runtime_cfg.render_query(
                "temp_table_drop",
                temp_table=self.tmp_name,
            ))
            self.conn.commit()
        except Exception:
            pass


class BatchUpdater:
    """
    FIX (see chat): insert used to run one UPDATE ... WHERE key = ? statement
    PER ROW, each committed individually. Without an index on the join
    column, every single one of those does a full table scan of the
    destination table -- thousands of full scans for a batch that should
    take one. This class pushes an entire batch's (key, new_value) pairs
    into one reusable #temp table, then updates them all in a single
    UPDATE ... FROM ... JOIN statement -- one scan per batch instead of
    one scan per row, mirroring the same trick BatchKeyMatcher already
    uses for reads.

    Does NOT remove the need for an index on the destination join column
    for best performance -- it reduces the number of scans, it doesn't
    make each scan itself cheap. Use both if possible.
    """

    def __init__(self, conn, ref_dest_cols, runtime_cfg):
        self.conn = conn
        self.ref_dest_cols = ref_dest_cols
        self.runtime_cfg = runtime_cfg
        self.tmp_name = f"#upd_{uuid.uuid4().hex[:8]}"
        self.key_cols = [f"k{i}" for i in range(len(ref_dest_cols))]
        key_col_type = runtime_cfg.option("temp_key_column_type")
        column_defs = ", ".join(f"{c} {key_col_type}" for c in self.key_cols)
        column_defs += ", new_value NVARCHAR(MAX)"
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {self.tmp_name} ({column_defs})")
        self.conn.commit()

    def apply(self, dst_table, dst_col, pending):
        """pending: list of (key_tuple, new_value). Runs ONE UPDATE...JOIN
        for the whole list. Returns the number of rows actually updated."""
        if not pending:
            return 0

        cur = self.conn.cursor()
        cur.execute(f"TRUNCATE TABLE {self.tmp_name}")
        cur.fast_executemany = True
        placeholders = ", ".join("?" for _ in self.key_cols) + ", ?"
        insert_sql = (
            f"INSERT INTO {self.tmp_name} ({', '.join(self.key_cols)}, new_value) "
            f"VALUES ({placeholders})"
        )
        cur.executemany(
            insert_sql,
            [tuple(str(v) for v in key) + (None if val is None else str(val),) for key, val in pending],
        )

        cast_for_join = self.runtime_cfg.option("cast_temp_keys_for_join")
        join_parts = []
        for i, col in enumerate(self.ref_dest_cols):
            if cast_for_join:
                join_parts.append(f"CAST(t.{col} AS NVARCHAR(500)) = tmp.{self.key_cols[i]}")
            else:
                join_parts.append(f"t.{col} = tmp.{self.key_cols[i]}")
        join_cond = " AND ".join(join_parts)

        update_sql = (
            f"UPDATE t SET t.{dst_col} = tmp.new_value "
            f"FROM {dst_table} t JOIN {self.tmp_name} tmp ON {join_cond}"
        )
        cur.execute(update_sql)
        rowcount = cur.rowcount
        self.conn.commit()
        return rowcount

    def close(self):
        try:
            cur = self.conn.cursor()
            cur.execute(f"DROP TABLE {self.tmp_name}")
            self.conn.commit()
        except Exception:
            pass


def fetch_all_rows_by_keys(conn, table, key_columns, data_columns, runtime_cfg, progress_cb=None):
    if not key_columns:
        return {}

    all_cols = list(dict.fromkeys(list(key_columns) + list(data_columns)))
    col_list = ", ".join(all_cols)
    sql = runtime_cfg.render_query(
        "destination_preload",
        columns=col_list,
        table=table,
        nolock_hint=runtime_cfg.nolock_hint(),
        where="",
    )

    cur = conn.cursor()
    cur.execute(sql)

    rows = {}
    duplicate_keys = set()
    fetch_size = runtime_cfg.option("preload_fetch_size")
    progress_every = runtime_cfg.option("preload_progress_every")
    warn_on_duplicates = runtime_cfg.option("warn_on_duplicate_keys")
    loaded = 0
    while True:
        chunk = cur.fetchmany(fetch_size)
        if not chunk:
            break
        for row in chunk:
            row_dict = dict(zip(all_cols, row))
            composite = tuple(row_dict[c] for c in key_columns)
            if composite in rows and warn_on_duplicates:
                duplicate_keys.add(composite)
            rows[composite] = row_dict
        loaded += len(chunk)
        if progress_cb and loaded % progress_every == 0:
            progress_cb(loaded)
    if duplicate_keys:
        log.warning(
            f"Preload found {len(duplicate_keys)} duplicate join key(s) in {table}; "
            "last row wins for each duplicate key"
        )
    return rows


def _update_column_counts(pm, value_status):
    key = {
        MATCH: "matched",
        MISMATCH: "mismatched",
        SOURCE_BLANK: "source_blank",
        DESTINATION_BLANK: "destination_blank",
        BOTH_BLANK: "both_blank",
    }[value_status]
    pm["counts"][key] += 1


def _issue_priority(issue_type):
    return {
        "MISSING_IN_DESTINATION": 6,
        "MISMATCH": 5,
        "DESTINATION_BLANK": 4,
        "SOURCE_BLANK": 3,
        "BOTH_BLANK": 2,
        "MATCH": 1,
    }.get(issue_type, 0)


VALUE_STATUS_REASONS = {
    MATCH: "values match",
    MISMATCH: "values differ",
    SOURCE_BLANK: "source is blank, destination has a value",
    DESTINATION_BLANK: "destination is blank, source has a value",
    BOTH_BLANK: "both sides blank",
}


def _build_key_reason(column_results, max_len=480):
    problems = [r for r in column_results if r["issue_type"] != "MATCH"]
    if not problems:
        return f"all {len(column_results)} column(s) match"
    parts = []
    for r in problems:
        col = r["destination_column"] or r["source_column"]
        parts.append(f"{col}: {VALUE_STATUS_REASONS.get(r['value_status'], r['value_status'])}")
    text = "; ".join(parts)
    if len(text) > max_len:
        text = text[:max_len - 3] + "..."
    return text


def _log_compare_key_combined(audit, log_matches, ids, src_table, dst_table, key_primary_value,
                               key_context_json, batch_number, src_row, dst_row, per_mapping, runtime_cfg):
    if src_row is None:
        return

    if dst_row is None:
        column_slots = [
            {
                "source_column": pm["src_col"],
                "destination_column": pm["dst_col"],
                "source_value": _row_get(src_row, pm["src_col"]),
                "destination_value": None,
                "value_status": NOT_EVALUATED,
            }
            for pm in per_mapping
        ]
        internal = getattr(audit, "internal", None)
        if internal:
            internal.log_validation_row(
                audit.stage, ids, key_primary_value, "MISSING_IN_DESTINATION", column_slots,
                src_table, dst_table, batch_number, key_context_json,
            )
        audit.log(
            **ids, source_table=src_table, destination_table=dst_table,
            source_column=column_slots[0]["source_column"],
            destination_column=column_slots[0]["destination_column"],
            key_value=key_primary_value, batch_number=batch_number,
            value_status=NOT_EVALUATED, issue_type="MISSING_IN_DESTINATION",
            join_key_status=JOIN_MISSING_DESTINATION_KEY,
            source_value=column_slots[0]["source_value"],
            destination_value=column_slots[0]["destination_value"],
            exception_reason="No destination record for this join key",
            details=key_context_json,
            column_slots=column_slots,
        )
        return

    column_results = []
    column_slots = []
    worst_issue = "MATCH"
    for pm in per_mapping:
        src_val = _row_get(src_row, pm["src_col"])
        dst_val = _row_get(dst_row, pm["dst_col"])
        value_status, _, _ = compare_value(src_val, dst_val, pm["norm_cfg"], runtime_cfg)
        _update_column_counts(pm, value_status)
        issue_type = value_status.upper().replace(" ", "_")
        column_results.append({
            "source_column": pm["src_col"],
            "destination_column": pm["dst_col"],
            "issue_type": issue_type,
            "value_status": value_status,
            "source_value": src_val,
            "destination_value": dst_val,
        })
        column_slots.append({
            "source_column": pm["src_col"],
            "destination_column": pm["dst_col"],
            "source_value": src_val,
            "destination_value": dst_val,
            "value_status": value_status,
        })
        if _issue_priority(issue_type) > _issue_priority(worst_issue):
            worst_issue = issue_type

    log_all = _should_log_all_rows(runtime_cfg)
    if worst_issue == "MATCH" and not log_matches and not log_all:
        return

    worst_result = next(r for r in column_results if r["issue_type"] == worst_issue)
    internal = getattr(audit, "internal", None)
    if internal:
        internal.log_validation_row(
            audit.stage, ids, key_primary_value, worst_issue, column_slots,
            src_table, dst_table, batch_number, key_context_json,
        )
    audit.log(
        **ids, source_table=src_table, destination_table=dst_table,
        source_column=worst_result["source_column"],
        destination_column=worst_result["destination_column"],
        key_value=key_primary_value, batch_number=batch_number,
        issue_type=worst_issue, value_status=worst_result["value_status"],
        source_value=worst_result["source_value"],
        destination_value=worst_result["destination_value"],
        exception_reason=_build_key_reason(column_results),
        details=key_context_json,
        column_slots=column_slots,
    )


def _single_column_slot(src_col, dst_col, src_val, dst_val, value_status):
    return [{
        "source_column": src_col,
        "destination_column": dst_col,
        "source_value": src_val,
        "destination_value": dst_val,
        "value_status": value_status,
    }]


def _log_compare_result(audit, runtime_cfg, log_matches, ctx, value_status, reason, batch_number):
    slot_kw = {}
    if audit.log_column_slots:
        slot_kw["column_slots"] = _single_column_slot(
            ctx["src_col"], ctx["dst_col"], ctx["src_val"], ctx["dst_val"], value_status,
        )
    log_all = _should_log_all_rows(runtime_cfg)
    if value_status == MATCH:
        ctx["counts"]["matched"] += 1
        if log_matches or log_all:
            slots = slot_kw.get("column_slots") or _single_column_slot(
                ctx["src_col"], ctx["dst_col"], ctx["src_val"], ctx["dst_val"], value_status,
            )
            internal = getattr(audit, "internal", None)
            if internal:
                internal.log_validation_row(
                    audit.stage, ctx["ids"], ctx["key_primary_value"], "MATCH", slots,
                    ctx["src_table"], ctx["dst_table"], batch_number, ctx["key_context_json"],
                )
            audit.log(
                **ctx["ids"], source_table=ctx["src_table"], destination_table=ctx["dst_table"],
                source_column=ctx["src_col"], destination_column=ctx["dst_col"],
                key_value=ctx["key_primary_value"], batch_number=batch_number,
                issue_type="MATCH", value_status=MATCH,
                source_value=ctx["src_val"], destination_value=ctx["dst_val"],
                exception_reason="Values match", details=ctx["key_context_json"],
                **slot_kw,
            )
        return
    key = {
        MISMATCH: "mismatched",
        SOURCE_BLANK: "source_blank",
        DESTINATION_BLANK: "destination_blank",
        BOTH_BLANK: "both_blank",
    }[value_status]
    ctx["counts"][key] += 1
    issue_type = value_status.upper().replace(" ", "_")
    slots = slot_kw.get("column_slots") or _single_column_slot(
        ctx["src_col"], ctx["dst_col"], ctx["src_val"], ctx["dst_val"], value_status,
    )
    internal = getattr(audit, "internal", None)
    if internal:
        internal.log_validation_row(
            audit.stage, ctx["ids"], ctx["key_primary_value"], issue_type, slots,
            ctx["src_table"], ctx["dst_table"], batch_number, ctx["key_context_json"],
        )
    audit.log(
        **ctx["ids"], source_table=ctx["src_table"], destination_table=ctx["dst_table"],
        source_column=ctx["src_col"], destination_column=ctx["dst_col"],
        key_value=ctx["key_primary_value"], batch_number=batch_number,
        issue_type=issue_type, value_status=value_status,
        source_value=ctx["src_val"], destination_value=ctx["dst_val"],
        exception_reason=reason, details=ctx["key_context_json"],
        **slot_kw,
    )


def _validate_custom_query_batch(pool, audit, mappings, catalog, runtime_cfg):
    if not mappings:
        return

    mappings = [resolve_mapping_from_client(m, catalog) for m in mappings]
    mapping0 = mappings[0]
    client_id = str(mapping0.get("client_id", ""))
    flow_id = str(mapping0.get("flow_id", ""))
    src_table = mapping0.get("source_table")
    dst_table = mapping0.get("destination_table")
    batch_size = runtime_cfg.config.get("batch_size")
    log_matches = runtime_cfg.option("log_matches", mapping0)
    log_granularity = runtime_cfg.options.get("log_granularity", "per_column")
    log_preload = runtime_cfg.options.get("log_preload_progress", False)
    fetch_cfg = resolve_fetch_config(mapping0, catalog)

    if fetch_cfg["fetch_mode"] != "custom_query":
        for m in mappings:
            validate_mapping(pool, audit, m, catalog, runtime_cfg)
        return

    resolved = resolve_servers_and_db(mapping0, catalog)
    source_server_log = resolve_server_name_for_log(resolved.get("source_server"), pool.servers_cfg)
    destination_server_log = resolve_server_name_for_log(resolved.get("destination_server"), pool.servers_cfg)

    join_keys = classify_join_keys(mapping0)
    usable_join_keys = [j for j in join_keys if j["status"] == JOIN_READY]
    ref_source_cols = [j["source_column"] for j in usable_join_keys]
    ref_dest_cols = [j["destination_column"] for j in usable_join_keys]

    per_mapping = []
    for m in mappings:
        per_mapping.append({
            "src_col": m.get("source_column"),
            "dst_col": m.get("destination_column"),
            "norm_cfg": m.get("normalization"),
            "counts": {"matched": 0, "mismatched": 0, "source_blank": 0, "destination_blank": 0, "both_blank": 0},
        })

    audit.ensure_log_column_slots(len(per_mapping))

    for m in mappings:
        audit.set_default_context(
            client_id=client_id, flow_id=flow_id,
            source_server=source_server_log, destination_server=destination_server_log,
            source_database=resolved.get("source_database"), destination_database=resolved.get("destination_database"),
            source_table=m.get("source_table"), destination_table=m.get("destination_table"),
            source_column=m.get("source_column"), destination_column=m.get("destination_column"),
        )

    try:
        src_conn = pool.get(resolved["source_server"], resolved["source_database"])
        dst_conn = pool.get(resolved["destination_server"], resolved["destination_database"])
    except Exception as e:
        audit.log(client_id=client_id, flow_id=flow_id, issue_type="CONNECTION_ERROR", details=str(e))
        report_connection_error(client_id, flow_id, str(e))
        return

    source_query = fetch_cfg["source_fetch_query"]
    destination_query = fetch_cfg["destination_fetch_query"]
    checkpoint = audit.load_checkpoint(client_id, flow_id)
    resumed = checkpoint is not None
    batch_number = checkpoint["batch_number"] if resumed else 0
    resume_after_key = tuple(checkpoint["last_values"]) if resumed and checkpoint.get("last_values") else None

    # FIX: restore prior counts on resume so RUN_SUMMARY totals include
    # rows processed before the crash, not just the post-resume portion.
    if resumed:
        prior_counts = checkpoint.get("counts") or {}
        if len(per_mapping) == 1:
            for k in per_mapping[0]["counts"]:
                per_mapping[0]["counts"][k] = prior_counts.get(k, 0)
        elif prior_counts:
            log.warning(
                f"[client={client_id} flow={flow_id}] Resuming a {len(per_mapping)}-column single-pass "
                "mapping; checkpoint only stores combined counts, per-column counts restart at 0 "
                "for this resumed run (combined RUN_SUMMARY totals will still be understated)."
            )

    def _load_side(conn, side, sql, label):
        cached = get_custom_query_cache(side, client_id, sql)
        if cached is not None:
            log.info(f"[client={client_id}] Reusing cached {label} query ({len(cached)} keys)")
            return cached
        log.info(f"[client={client_id}] Running custom {label} fetch query...")
        progress_cb = None
        if log_preload:
            def progress_cb(loaded, lbl=label):
                audit.log(client_id=client_id, flow_id=flow_id, issue_type="INFO", batch_number=0,
                          details=f"Custom {lbl} query progress: loaded {loaded} rows")
                audit.flush()
        rows, _ = execute_custom_fetch_query(
            conn, sql, ref_source_cols if side == "source" else ref_dest_cols,
            runtime_cfg, progress_cb=progress_cb,
        )
        set_custom_query_cache(side, client_id, sql, rows)
        log.info(f"[client={client_id}] Custom {label} query loaded {len(rows)} keys")
        return rows

    # FIX (see chat): destination_fetch_query used to pull the WHOLE
    # destination table (20M+ rows) unconditionally, even though we only
    # ever need the handful of ClientAuditIds that source actually
    # returned (often a few thousand). Load source first (already the
    # order below), then scope the destination query down to exactly
    # those keys via a chunked "IN (...)" filter, so destination only
    # ever scans/returns rows we can actually use.
    def _load_destination_filtered(conn, sql, source_keys):
        cache_sql = sql + f"::keyfiltered::{len(source_keys)}"
        cached = get_custom_query_cache("destination", client_id, cache_sql)
        if cached is not None:
            log.info(f"[client={client_id}] Reusing cached destination query ({len(cached)} keys)")
            return cached

        if not source_keys or len(ref_dest_cols) != 1:
            # No source keys to filter by, or a composite key we don't
            # special-case here -- fall back to the original unfiltered
            # behavior rather than silently returning nothing.
            return _load_side(conn, "destination", sql, "destination")

        dest_col = ref_dest_cols[0]
        # source_keys are 1-tuples like (12345,) since there's one ref key
        flat_values = [k[0] for k in source_keys]
        is_numeric = all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in flat_values)

        chunk_size = 2000  # keep each IN-list comfortably small
        merged_rows = {}
        log.info(
            f"[client={client_id}] Running key-filtered destination fetch for "
            f"{len(flat_values)} key(s) in chunks of {chunk_size}..."
        )
        for i in range(0, len(flat_values), chunk_size):
            chunk_vals = flat_values[i:i + chunk_size]
            if is_numeric:
                in_list = ",".join(str(v) for v in chunk_vals)
            else:
                # escape single quotes for safety since these are inlined literals
                in_list = ",".join("'" + str(v).replace("'", "''") + "'" for v in chunk_vals)
            filtered_sql = f"SELECT * FROM ({sql}) AS base WHERE base.{dest_col} IN ({in_list})"
            chunk_rows, _ = execute_custom_fetch_query(conn, filtered_sql, ref_dest_cols, runtime_cfg)
            merged_rows.update(chunk_rows)

        log.info(f"[client={client_id}] Key-filtered destination query loaded {len(merged_rows)} keys")
        set_custom_query_cache("destination", client_id, cache_sql, merged_rows)
        return merged_rows

    try:
        source_rows_cache = _load_side(src_conn, "source", source_query, "source")
        destination_rows_cache = _load_destination_filtered(
            dst_conn, destination_query, list(source_rows_cache.keys())
        )
    except Exception as e:
        audit.log(client_id=client_id, flow_id=flow_id, issue_type="CONNECTION_ERROR",
                  details=f"Custom fetch query failed: {e}")
        report_connection_error(client_id, flow_id, f"Custom fetch query failed: {e}")
        return

    all_source_keys = sorted(source_rows_cache.keys())
    all_source_keys_set = set(source_rows_cache.keys())
    if resume_after_key is not None:
        all_source_keys = [k for k in all_source_keys if k > resume_after_key]

    log.info(f"[client={client_id} flow={flow_id}] Single-pass compare: {len(all_source_keys)} keys x {len(per_mapping)} columns")

    for batch_start in range(0, len(all_source_keys), batch_size):
        batch_number += 1
        batch_keys = all_source_keys[batch_start:batch_start + batch_size]

        for key in batch_keys:
            key_primary_value, key_map, key_context_json = build_key_context(ref_source_cols, key, runtime_cfg)
            src_row = source_rows_cache.get(key)
            dst_row = destination_rows_cache.get(key)
            row_client_id, row_flow_id, row_audit_id, row_client_audit_id = build_row_log_identity(
                key_map, client_id, flow_id, runtime_cfg,
                src_row=src_row, dst_row=dst_row, key_primary_value=key_primary_value,
            )
            ids = {
                "client_id": row_client_id, "flow_id": row_flow_id,
                "audit_id": row_audit_id, "client_audit_id": row_client_audit_id,
            }

            if log_granularity == "per_key":
                _log_compare_key_combined(
                    audit, log_matches, ids, src_table, dst_table, key_primary_value,
                    key_context_json, batch_number, src_row, dst_row, per_mapping, runtime_cfg,
                )
            else:
                for pm in per_mapping:
                    ctx = {
                        "ids": ids, "src_table": src_table, "dst_table": dst_table,
                        "src_col": pm["src_col"], "dst_col": pm["dst_col"],
                        "key_primary_value": key_primary_value, "key_context_json": key_context_json,
                        "counts": pm["counts"],
                    }
                    if src_row is None:
                        continue
                    if dst_row is None:
                        audit.log(**ids, source_table=src_table, destination_table=dst_table,
                                  source_column=pm["src_col"], destination_column=pm["dst_col"],
                                  key_value=key_primary_value, batch_number=batch_number,
                                  value_status=NOT_EVALUATED, source_value=_row_get(src_row, pm["src_col"]),
                                  destination_value=None, issue_type="MISSING_IN_DESTINATION",
                                  join_key_status=JOIN_MISSING_DESTINATION_KEY,
                                  exception_reason="No destination record for this join key",
                                  details=key_context_json)
                        continue
                    src_val = _row_get(src_row, pm["src_col"])
                    dst_val = _row_get(dst_row, pm["dst_col"])
                    value_status, _, _ = compare_value(src_val, dst_val, pm["norm_cfg"], runtime_cfg)
                    reason = {
                        MISMATCH: "value mismatch", SOURCE_BLANK: "source value blank/null",
                        DESTINATION_BLANK: "destination value blank/null", BOTH_BLANK: "both source and destination blank/null",
                    }.get(value_status, "")
                    ctx["src_val"], ctx["dst_val"] = src_val, dst_val
                    _log_compare_result(audit, runtime_cfg, log_matches, ctx, value_status, reason, batch_number)

        audit.flush()
        combined = {"matched": 0, "mismatched": 0, "source_blank": 0, "destination_blank": 0, "both_blank": 0}
        for pm in per_mapping:
            for k in combined:
                combined[k] += pm["counts"][k]
        audit.save_checkpoint(client_id, flow_id, batch_number, list(batch_keys[-1]), combined)

    # FIX: orphan check (MISSING_IN_SOURCE) was missing entirely from the
    # single-pass path -- destination-only keys were never reported here,
    # even though the same check exists in _validate_mapping_custom_query
    # and in table-mode validate_mapping(). Mirror it here, gated the same
    # way (config option, skipped on a resumed run).
    check_orphans = runtime_cfg.mapping_option(
        "check_orphans_in_destination", mapping0, "check_orphans_in_destination"
    )
    if check_orphans and not resumed:
        orphan_batch = 0
        for key in sorted(destination_rows_cache.keys()):
            if key not in all_source_keys_set:
                orphan_batch += 1
                key_primary_value, key_map, key_context_json = build_key_context(
                    ref_dest_cols, key, runtime_cfg
                )
                dst_row = destination_rows_cache.get(key)
                row_client_id, row_flow_id, row_audit_id, row_client_audit_id = build_row_log_identity(
                    key_map, client_id, flow_id, runtime_cfg,
                    dst_row=dst_row, key_primary_value=key_primary_value,
                )
                audit.log(
                    client_id=row_client_id, flow_id=row_flow_id,
                    audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                    source_table=src_table, destination_table=dst_table,
                    key_value=key_primary_value, batch_number=orphan_batch,
                    issue_type="MISSING_IN_SOURCE",
                    join_key_status=JOIN_MISSING_SOURCE_KEY,
                    exception_reason="Destination record has no matching source key",
                    details=f"key_context={key_context_json}",
                )
        audit.flush()
    elif resumed:
        log.info(f"[client={client_id} flow={flow_id}] Orphan check skipped (resumed single-pass run)")

    audit.clear_checkpoint(client_id, flow_id)
    if log_granularity == "per_key":
        combined = {"matched": 0, "mismatched": 0, "source_blank": 0, "destination_blank": 0, "both_blank": 0}
        for pm in per_mapping:
            for k in combined:
                combined[k] += pm["counts"][k]
        audit.write_summary(client_id, flow_id, src_table, dst_table, extra_counts=combined)
    else:
        for pm in per_mapping:
            audit.write_summary(client_id, flow_id, src_table, dst_table, extra_counts=pm["counts"])
    log.info(f"[client={client_id} flow={flow_id}] Single-pass custom query complete ({len(per_mapping)} columns)")


def _validate_mapping_custom_query(
    pool, audit, mapping, catalog, runtime_cfg, resolved,
    source_server_log, destination_server_log,
    client_id, flow_id, src_table, dst_table, src_col, dst_col,
    fetch_cfg, src_conn, dst_conn,
    usable_join_keys, ref_source_cols, ref_dest_cols,
    norm_cfg, batch_size, log_matches,
):
    """Validate using JSON-defined source/destination fetch queries (computed ClientAuditId, etc.)."""
    source_query = fetch_cfg["source_fetch_query"]
    destination_query = fetch_cfg["destination_fetch_query"]
    join_key_status = JOIN_READY if all(j["status"] == JOIN_READY for j in usable_join_keys) else JOIN_MISSING_SOURCE_KEY

    checkpoint = audit.load_checkpoint(client_id, flow_id)
    resumed = checkpoint is not None

    audit.log(
        client_id=client_id, flow_id=flow_id,
        source_table=src_table, destination_table=dst_table,
        issue_type="INFO", batch_number=0,
        details=f"Custom-query mapping started for run_id={audit.run_id}",
    )
    audit.flush()

    if resumed:
        batch_number = checkpoint["batch_number"]
        counts = checkpoint["counts"]
        matched = counts.get("matched", 0)
        mismatched = counts.get("mismatched", 0)
        source_blank = counts.get("source_blank", 0)
        destination_blank = counts.get("destination_blank", 0)
        both_blank = counts.get("both_blank", 0)
        resume_after_key = tuple(checkpoint["last_values"]) if checkpoint.get("last_values") else None
        audit.log(
            client_id=client_id, flow_id=flow_id,
            source_table=src_table, destination_table=dst_table,
            issue_type="INFO", batch_number=batch_number,
            details=f"Resuming custom-query mapping after batch {batch_number}",
        )
    else:
        batch_number = 0
        matched = mismatched = source_blank = destination_blank = both_blank = 0
        resume_after_key = None

    def _load_side(conn, side, sql, label):
        cached = get_custom_query_cache(side, client_id, sql)
        if cached is not None:
            log.info(f"[client={client_id} flow={flow_id}] Reusing cached custom {label} query ({len(cached)} keys)")
            audit.log(
                client_id=client_id, flow_id=flow_id,
                issue_type="INFO", batch_number=0,
                details=f"Reused cached custom {label} query with {len(cached)} keys",
            )
            audit.flush()
            return cached

        log.info(f"[client={client_id} flow={flow_id}] Running custom {label} fetch query...")
        audit.log(
            client_id=client_id, flow_id=flow_id,
            issue_type="INFO", batch_number=0,
            details=f"Executing custom {label} fetch query",
        )
        audit.flush()

        def _progress(loaded):
            audit.log(
                client_id=client_id, flow_id=flow_id,
                issue_type="INFO", batch_number=0,
                details=f"Custom {label} query progress: loaded {loaded} rows",
            )
            audit.flush()

        rows, _ = execute_custom_fetch_query(
            conn, sql, ref_source_cols if side == "source" else ref_dest_cols,
            runtime_cfg, progress_cb=_progress,
        )
        set_custom_query_cache(side, client_id, sql, rows)
        log.info(f"[client={client_id} flow={flow_id}] Custom {label} query loaded {len(rows)} keys")
        audit.log(
            client_id=client_id, flow_id=flow_id,
            issue_type="INFO", batch_number=0,
            details=f"Custom {label} query complete: {len(rows)} keys",
        )
        audit.flush()
        return rows

    try:
        source_rows_cache = _load_side(src_conn, "source", source_query, "source")
        destination_rows_cache = _load_side(dst_conn, "destination", destination_query, "destination")
    except Exception as e:
        audit.log(
            client_id=client_id, flow_id=flow_id,
            source_table=src_table, destination_table=dst_table,
            issue_type="CONNECTION_ERROR",
            details=f"Custom fetch query failed: {e}",
        )
        log.error(f"[client={client_id} flow={flow_id}] Custom fetch query failed: {e}")
        return

    all_source_keys = sorted(source_rows_cache.keys())
    if resume_after_key is not None:
        all_source_keys = [k for k in all_source_keys if k > resume_after_key]

    all_source_keys_set = set(source_rows_cache.keys())

    for batch_start in range(0, len(all_source_keys), batch_size):
        batch_number += 1
        batch_keys = all_source_keys[batch_start:batch_start + batch_size]
        if not batch_keys:
            break

        for key in batch_keys:
            try:
                key_primary_value, key_map, key_context_json = build_key_context(
                    ref_source_cols, key, runtime_cfg
                )
                src_row = source_rows_cache.get(key)
                dst_row = destination_rows_cache.get(key)
                row_client_id, row_flow_id, row_audit_id, row_client_audit_id = build_row_log_identity(
                    key_map, client_id, flow_id, runtime_cfg,
                    src_row=src_row, dst_row=dst_row, key_primary_value=key_primary_value,
                )

                if src_row is None:
                    audit.log(
                        client_id=row_client_id, flow_id=row_flow_id,
                        audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                        source_table=src_table, destination_table=dst_table,
                        key_value=key_primary_value, batch_number=batch_number,
                        issue_type="ROW_ERROR",
                        details=f"Key missing from custom source query result; key_context={key_context_json}",
                    )
                    continue

                dst_row = destination_rows_cache.get(key)
                if dst_row is None:
                    audit.log(
                        client_id=row_client_id, flow_id=row_flow_id,
                        audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                        source_table=src_table, destination_table=dst_table,
                        source_column=src_col, destination_column=dst_col,
                        key_value=key_primary_value, batch_number=batch_number,
                        value_status=NOT_EVALUATED,
                        source_value=_row_get(src_row, src_col),
                        destination_value=None,
                        issue_type="MISSING_IN_DESTINATION",
                        join_key_status=JOIN_MISSING_DESTINATION_KEY,
                        exception_reason="No destination record for this join key",
                        details=f"key_context={key_context_json}",
                    )
                    continue

                value_status, _, _ = compare_value(
                    _row_get(src_row, src_col), _row_get(dst_row, dst_col), norm_cfg, runtime_cfg
                )
                if value_status == MATCH:
                    matched += 1
                    if log_matches:
                        audit.log(
                            client_id=row_client_id, flow_id=row_flow_id,
                            audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                            source_table=src_table, destination_table=dst_table,
                            source_column=src_col, destination_column=dst_col,
                            key_value=key_primary_value, batch_number=batch_number,
                            issue_type="MATCH", value_status=MATCH,
                            source_value=_row_get(src_row, src_col),
                            destination_value=_row_get(dst_row, dst_col),
                            exception_reason="Values match",
                            details=f"key_context={key_context_json}",
                        )
                    continue
                elif value_status == MISMATCH:
                    mismatched += 1
                    reason = "value mismatch"
                elif value_status == SOURCE_BLANK:
                    source_blank += 1
                    reason = "source value blank/null"
                elif value_status == DESTINATION_BLANK:
                    destination_blank += 1
                    reason = "destination value blank/null"
                else:
                    both_blank += 1
                    reason = "both source and destination blank/null"

                audit.log(
                    client_id=row_client_id, flow_id=row_flow_id,
                    audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                    source_table=src_table, destination_table=dst_table,
                    source_column=src_col, destination_column=dst_col,
                    key_value=key_primary_value, batch_number=batch_number,
                    issue_type=value_status.upper().replace(" ", "_"),
                    value_status=value_status,
                    source_value=_row_get(src_row, src_col),
                    destination_value=_row_get(dst_row, dst_col),
                    exception_reason=reason,
                    details=f"key_context={key_context_json}",
                )
            except Exception as e:
                audit.log(
                    client_id=client_id, flow_id=flow_id,
                    source_table=src_table, destination_table=dst_table,
                    key_value=key, batch_number=batch_number,
                    issue_type="ROW_ERROR",
                    details=f"Unexpected error comparing custom-query row: {e}",
                )

        audit.flush()
        audit.save_checkpoint(
            client_id, flow_id, batch_number, list(batch_keys[-1]),
            {
                "matched": matched, "mismatched": mismatched,
                "source_blank": source_blank, "destination_blank": destination_blank,
                "both_blank": both_blank,
            },
        )
        log.info(f"[client={client_id} flow={flow_id}] Custom-query batch {batch_number}: compared {len(batch_keys)} keys")

    check_orphans = runtime_cfg.mapping_option(
        "check_orphans_in_destination", mapping, "check_orphans_in_destination"
    )
    if check_orphans and not resumed:
        orphan_batch = 0
        for key in sorted(destination_rows_cache.keys()):
            if key not in all_source_keys_set:
                orphan_batch += 1
                key_primary_value, key_map, key_context_json = build_key_context(
                    ref_dest_cols, key, runtime_cfg
                )
                dst_row = destination_rows_cache.get(key)
                row_client_id, row_flow_id, row_audit_id, row_client_audit_id = build_row_log_identity(
                    key_map, client_id, flow_id, runtime_cfg,
                    dst_row=dst_row, key_primary_value=key_primary_value,
                )
                audit.log(
                    client_id=row_client_id, flow_id=row_flow_id,
                    audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                    source_table=src_table, destination_table=dst_table,
                    key_value=key_primary_value, batch_number=orphan_batch,
                    issue_type="MISSING_IN_SOURCE",
                    join_key_status=JOIN_MISSING_SOURCE_KEY,
                    exception_reason="Destination record has no matching source key",
                    details=f"key_context={key_context_json}",
                )
        audit.flush()
    elif resumed:
        log.info(f"[client={client_id} flow={flow_id}] Orphan check skipped (resumed custom-query run)")

    audit.clear_checkpoint(client_id, flow_id)
    audit.write_summary(
        client_id, flow_id, src_table, dst_table,
        extra_counts={
            "matched": matched, "mismatched": mismatched,
            "source_blank": source_blank, "destination_blank": destination_blank,
            "both_blank": both_blank,
        },
    )
    log.info(
        f"[client={client_id} flow={flow_id}] Custom-query finished. "
        f"matched={matched} mismatched={mismatched} source_blank={source_blank} "
        f"destination_blank={destination_blank} both_blank={both_blank}"
    )


def validate_mapping(pool, audit, mapping, catalog, runtime_cfg):
    mapping = resolve_mapping_from_client(mapping, catalog)
    client_id = str(mapping.get("client_id", ""))
    flow_id = str(mapping.get("flow_id", ""))
    src_table = mapping.get("source_table")
    dst_table = mapping.get("destination_table")
    src_col = mapping.get("source_column")
    dst_col = mapping.get("destination_column")
    batch_size = runtime_cfg.config.get("batch_size")
    date_filter_cfg = runtime_cfg.config.get("date_filter")
    log_matches = runtime_cfg.option("log_matches", mapping)
    if batch_size is None:
        raise ValueError("config.json must define top-level 'batch_size'")

    resolved = resolve_servers_and_db(mapping, catalog)
    source_server_log = resolve_server_name_for_log(resolved.get("source_server"), pool.servers_cfg)
    destination_server_log = resolve_server_name_for_log(resolved.get("destination_server"), pool.servers_cfg)
    audit.set_default_context(
        client_id=client_id,
        flow_id=flow_id,
        source_server=source_server_log,
        destination_server=destination_server_log,
        source_database=resolved.get("source_database"),
        destination_database=resolved.get("destination_database"),
        source_table=src_table,
        destination_table=dst_table,
        source_column=src_col,
        destination_column=dst_col,
    )

    mapping_status, reasons = classify_mapping(mapping)
    join_keys = classify_join_keys(mapping)
    join_key_status = JOIN_READY if all(j["status"] == JOIN_READY for j in join_keys) else JOIN_MISSING_SOURCE_KEY

    try:
        if mapping_status != READY:
            audit.log(
                client_id=client_id, flow_id=flow_id,
                source_server=source_server_log, destination_server=destination_server_log,
                source_database=resolved["source_database"], destination_database=resolved["destination_database"],
                source_table=src_table, destination_table=dst_table,
                source_column=src_col, destination_column=dst_col,
                mapping_status=mapping_status, join_key_status=join_key_status,
                issue_type="CONFIGURATION_EXCEPTION",
                exception_reason="; ".join(reasons),
                details=json.dumps({"reference_keys": join_keys}),
            )
            log.info(f"[client={client_id} flow={flow_id}] Skipped (status={mapping_status}): {reasons}")
            return

        if resolved["catalog_missing"]:
            audit.log(
                client_id=client_id, flow_id=flow_id, source_table=src_table, destination_table=dst_table,
                mapping_status=mapping_status, issue_type="CONFIGURATION_EXCEPTION",
                exception_reason="Missing catalog",
                details=resolved["catalog_reason"],
            )
            log.error(f"[client={client_id} flow={flow_id}] {resolved['catalog_reason']}")
            return

        try:
            src_conn = pool.get(resolved["source_server"], resolved["source_database"])
            dst_conn = pool.get(resolved["destination_server"], resolved["destination_database"])
        except Exception as e:
            audit.log(
                client_id=client_id, flow_id=flow_id, source_table=src_table, destination_table=dst_table,
                source_server=source_server_log, destination_server=destination_server_log,
                source_database=resolved["source_database"], destination_database=resolved["destination_database"],
                issue_type="CONNECTION_ERROR", exception_reason="Pass / Fail: Fail (VR-07)",
                details=f"{e}",
            )
            log.error(f"[client={client_id} flow={flow_id}] Connection failed: {e}")
            return

        log.info(f"[client={client_id} flow={flow_id}] {src_table}.{src_col} -> {dst_table}.{dst_col}")

        usable_join_keys = [j for j in join_keys if j["status"] == JOIN_READY]
        ref_source_cols = [j["source_column"] for j in usable_join_keys]
        ref_dest_cols = [j["destination_column"] for j in usable_join_keys]
        norm_cfg = mapping.get("normalization")
        fetch_cfg = resolve_fetch_config(mapping, catalog)

        if fetch_cfg["fetch_mode"] == "custom_query":
            if not fetch_cfg["source_fetch_query"] or not fetch_cfg["destination_fetch_query"]:
                audit.log(
                    client_id=client_id, flow_id=flow_id,
                    source_table=src_table, destination_table=dst_table,
                    issue_type="CONFIGURATION_EXCEPTION",
                    exception_reason="fetch_mode=custom_query requires source_fetch_query and destination_fetch_query",
                )
                return
            if runtime_cfg.option("single_pass_custom_query"):
                return
            _validate_mapping_custom_query(
                pool, audit, mapping, catalog, runtime_cfg, resolved,
                source_server_log, destination_server_log,
                client_id, flow_id, src_table, dst_table, src_col, dst_col,
                fetch_cfg, src_conn, dst_conn,
                usable_join_keys, ref_source_cols, ref_dest_cols,
                norm_cfg, batch_size, log_matches,
            )
            return

        extra_log_cols_src = [c["source_column"] for c in mapping.get("log_columns", []) if c.get("source_column")]
        extra_log_cols_dst = [c["destination_column"] for c in mapping.get("log_columns", []) if c.get("destination_column")]
        src_data_cols = list(dict.fromkeys(ref_source_cols + [src_col] + extra_log_cols_src))
        dst_data_cols = list(dict.fromkeys(ref_dest_cols + [dst_col] + extra_log_cols_dst))
        preload_columns = [c for c in mapping.get("preload_columns", []) if c]
        if preload_columns:
            dst_data_cols = list(dict.fromkeys(dst_data_cols + preload_columns))

        checkpoint = audit.load_checkpoint(client_id, flow_id)
        resumed = checkpoint is not None

        audit.log(
            client_id=client_id, flow_id=flow_id,
            source_table=src_table, destination_table=dst_table,
            issue_type="INFO", batch_number=0,
            details=f"Mapping execution started for run_id={audit.run_id}",
        )
        audit.flush()

        if resumed:
            last_values = checkpoint["last_values"]
            batch_number = checkpoint["batch_number"]
            counts = checkpoint["counts"]
            matched = counts.get("matched", 0)
            mismatched = counts.get("mismatched", 0)
            source_blank = counts.get("source_blank", 0)
            destination_blank = counts.get("destination_blank", 0)
            both_blank = counts.get("both_blank", 0)
            audit.log(
                client_id=client_id, flow_id=flow_id, source_table=src_table, destination_table=dst_table,
                issue_type="INFO", batch_number=batch_number,
                details=(
                    f"Resuming from checkpoint after batch {batch_number} "
                    f"(checkpoint_run_id={checkpoint.get('run_id')})"
                ),
            )
            log.info(f"[client={client_id} flow={flow_id}] Resuming from checkpoint after batch {batch_number}")
        else:
            last_values = None
            batch_number = 0
            matched, mismatched, source_blank, destination_blank, both_blank = 0, 0, 0, 0, 0

        all_source_keys = set()

        src_matcher = BatchKeyMatcher(src_conn, ref_source_cols, runtime_cfg)
        use_destination_preload = runtime_cfg.mapping_option(
            "preload_destination_rows", mapping, "preload_destination_rows"
        )
        dst_matcher = None
        destination_rows_cache = {}

        if use_destination_preload:
            try:
                log.info(f"[client={client_id} flow={flow_id}] Preloading destination rows from {dst_table}...")

                preload_cache_key = (
                    resolved.get("destination_server"),
                    resolved.get("destination_database"),
                    dst_table,
                    tuple(ref_dest_cols),
                    tuple(sorted(dst_data_cols)),
                )

                if preload_cache_key in DESTINATION_PRELOAD_CACHE:
                    destination_rows_cache = DESTINATION_PRELOAD_CACHE[preload_cache_key]
                    log.info(
                        f"[client={client_id} flow={flow_id}] Reusing cached destination preload: "
                        f"{len(destination_rows_cache)} destination keys"
                    )
                    audit.log(
                        client_id=client_id,
                        flow_id=flow_id,
                        source_table=src_table,
                        destination_table=dst_table,
                        issue_type="INFO",
                        batch_number=0,
                        details=f"Reused destination preload cache with {len(destination_rows_cache)} keys",
                    )
                    audit.flush()
                else:
                    def _preload_progress(loaded):
                        audit.log(
                            client_id=client_id,
                            flow_id=flow_id,
                            source_table=src_table,
                            destination_table=dst_table,
                            issue_type="INFO",
                            batch_number=0,
                            details=f"Preload progress: loaded {loaded} destination rows",
                        )
                        audit.flush()

                    destination_rows_cache = fetch_all_rows_by_keys(
                        dst_conn,
                        dst_table,
                        ref_dest_cols,
                        dst_data_cols,
                        runtime_cfg,
                        progress_cb=_preload_progress,
                    )
                    DESTINATION_PRELOAD_CACHE[preload_cache_key] = destination_rows_cache
                    log.info(
                        f"[client={client_id} flow={flow_id}] Preload complete: {len(destination_rows_cache)} destination keys cached"
                    )
                    audit.log(
                        client_id=client_id,
                        flow_id=flow_id,
                        source_table=src_table,
                        destination_table=dst_table,
                        issue_type="INFO",
                        batch_number=0,
                        details=f"Preload complete: {len(destination_rows_cache)} destination keys cached",
                    )
                    audit.flush()
            except Exception as e:
                use_destination_preload = False
                log.warning(
                    f"[client={client_id} flow={flow_id}] Destination preload failed; falling back to batch JOINs: {e}"
                )

        if not use_destination_preload:
            dst_matcher = BatchKeyMatcher(dst_conn, ref_dest_cols, runtime_cfg)

        try:
            while True:
                batch_number += 1
                try:
                    active_date_col = (date_filter_cfg or {}).get("source_column") if date_filter_cfg else None
                    batch_keys = fetch_key_batch(
                        src_conn, src_table, ref_source_cols, date_filter_cfg,
                        active_date_col, last_values, batch_size, runtime_cfg,
                    )
                except Exception as e:
                    audit.log(client_id=client_id, flow_id=flow_id, source_table=src_table, destination_table=dst_table,
                               issue_type="BATCH_ERROR", batch_number=batch_number,
                               details=f"Failed to fetch source key batch: {e}")
                    log.error(f"[client={client_id} flow={flow_id}] Batch {batch_number}: key fetch failed: {e}")
                    break

                if not batch_keys:
                    break
                last_values = list(batch_keys[-1])
                all_source_keys.update(batch_keys)

                try:
                    src_rows = src_matcher.fetch_rows(src_table, src_data_cols, batch_keys)
                    if use_destination_preload:
                        dst_rows = {k: destination_rows_cache.get(k) for k in batch_keys}
                    else:
                        dst_rows = dst_matcher.fetch_rows(dst_table, dst_data_cols, batch_keys)
                except Exception as e:
                    audit.log(client_id=client_id, flow_id=flow_id, source_table=src_table, destination_table=dst_table,
                               issue_type="BATCH_ERROR", batch_number=batch_number,
                               details=f"Failed to fetch row data for batch: {e}")
                    log.error(f"[client={client_id} flow={flow_id}] Batch {batch_number}: row fetch failed: {e}")
                    continue

                for key in batch_keys:
                    try:
                        key_primary_value, key_map, key_context_json = build_key_context(
                            ref_source_cols, key, runtime_cfg
                        )
                        row_client_id, row_flow_id, row_audit_id, row_client_audit_id = build_row_log_identity(
                            key_map, client_id, flow_id, runtime_cfg
                        )
                        src_row = src_rows.get(key)
                        if src_row is not None and extra_log_cols_src:
                            for col in extra_log_cols_src:
                                if col in src_row and col not in key_map:
                                    key_map[col] = src_row[col]
                            row_client_id, row_flow_id, row_audit_id, row_client_audit_id = build_row_log_identity(
                                key_map, client_id, flow_id, runtime_cfg,
                                src_row=src_row, key_primary_value=key_primary_value,
                            )
                            key_context_json = _json_dumps_safe(key_map)
                        if src_row is None:
                            audit.log(client_id=row_client_id, flow_id=row_flow_id,
                                       audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                                       source_table=src_table,
                                       destination_table=dst_table, key_value=key_primary_value, batch_number=batch_number,
                                       issue_type="ROW_ERROR",
                                       details=f"Key returned by pagination but row missing on re-fetch; key_context={key_context_json}")
                            continue

                        dst_row = dst_rows.get(key)
                        if dst_row is None:
                            miss_slots = _single_column_slot(
                                src_col, dst_col, src_row.get(src_col), None, NOT_EVALUATED,
                            )
                            miss_ids = {
                                "client_id": row_client_id, "flow_id": row_flow_id,
                                "audit_id": row_audit_id, "client_audit_id": row_client_audit_id,
                            }
                            internal = getattr(audit, "internal", None)
                            if internal:
                                internal.log_validation_row(
                                    audit.stage, miss_ids, key_primary_value, "MISSING_IN_DESTINATION",
                                    miss_slots, src_table, dst_table, batch_number, key_context_json,
                                )
                            audit.log(client_id=row_client_id, flow_id=row_flow_id,
                                       audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                                       source_table=src_table,
                                       destination_table=dst_table, source_column=src_col, destination_column=dst_col,
                                       key_value=key_primary_value, batch_number=batch_number,
                                       value_status=NOT_EVALUATED,
                                       source_value=src_row.get(src_col),
                                       destination_value=None,
                                       issue_type="MISSING_IN_DESTINATION", join_key_status=JOIN_MISSING_DESTINATION_KEY,
                                       exception_reason="No destination record for this join key",
                                       details=f"key_context={key_context_json}")
                            continue

                        if extra_log_cols_dst:
                            for col in extra_log_cols_dst:
                                if col in dst_row and col not in key_map:
                                    key_map[col] = dst_row[col]
                            row_client_id, row_flow_id, row_audit_id, row_client_audit_id = build_row_log_identity(
                                key_map, client_id, flow_id, runtime_cfg,
                                src_row=src_row, dst_row=dst_row, key_primary_value=key_primary_value,
                            )
                            key_context_json = _json_dumps_safe(key_map)

                        value_status, n_src, n_dst = compare_value(
                            src_row.get(src_col), dst_row.get(dst_col), norm_cfg, runtime_cfg
                        )
                        row_ids = {
                            "client_id": row_client_id, "flow_id": row_flow_id,
                            "audit_id": row_audit_id, "client_audit_id": row_client_audit_id,
                        }
                        log_all = _should_log_all_rows(runtime_cfg)
                        if value_status == MATCH:
                            matched += 1
                            if log_matches or log_all:
                                slots = _single_column_slot(
                                    src_col, dst_col, src_row.get(src_col), dst_row.get(dst_col), value_status,
                                )
                                internal = getattr(audit, "internal", None)
                                if internal:
                                    internal.log_validation_row(
                                        audit.stage, row_ids, key_primary_value, "MATCH",
                                        slots, src_table, dst_table, batch_number, key_context_json,
                                    )
                                audit.log(
                                    client_id=row_client_id, flow_id=row_flow_id,
                                    audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                                    source_table=src_table, destination_table=dst_table,
                                    source_column=src_col, destination_column=dst_col,
                                    key_value=key_primary_value, batch_number=batch_number,
                                    issue_type="MATCH", value_status=MATCH,
                                    source_value=src_row.get(src_col),
                                    destination_value=dst_row.get(dst_col),
                                    exception_reason="Values match",
                                    details=f"key_context={key_context_json}",
                                )
                            continue
                        elif value_status == MISMATCH:
                            mismatched += 1
                            reason = "value mismatch"
                        elif value_status == SOURCE_BLANK:
                            source_blank += 1
                            reason = "source value blank/null"
                        elif value_status == DESTINATION_BLANK:
                            destination_blank += 1
                            reason = "destination value blank/null"
                        else:
                            both_blank += 1
                            reason = "both source and destination blank/null"

                        issue_type = value_status.upper().replace(" ", "_")
                        slots = _single_column_slot(
                            src_col, dst_col, src_row.get(src_col), dst_row.get(dst_col), value_status,
                        )
                        internal = getattr(audit, "internal", None)
                        if internal:
                            internal.log_validation_row(
                                audit.stage, row_ids, key_primary_value, issue_type,
                                slots, src_table, dst_table, batch_number, key_context_json,
                            )
                        audit.log(
                            client_id=row_client_id, flow_id=row_flow_id,
                            audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                            source_table=src_table, destination_table=dst_table,
                            source_column=src_col, destination_column=dst_col, key_value=key_primary_value, batch_number=batch_number,
                            issue_type=value_status.upper().replace(" ", "_"), value_status=value_status,
                            source_value=src_row.get(src_col), destination_value=dst_row.get(dst_col),
                            exception_reason=reason,
                            details=f"key_context={key_context_json}",
                        )
                    except Exception as e:
                        audit.log(client_id=row_client_id if 'row_client_id' in locals() else client_id,
                                   flow_id=row_flow_id if 'row_flow_id' in locals() else flow_id,
                                   audit_id=row_audit_id if 'row_audit_id' in locals() else None,
                                   client_audit_id=row_client_audit_id if 'row_client_audit_id' in locals() else None,
                                   source_table=src_table, destination_table=dst_table,
                                   key_value=key_primary_value if 'key_primary_value' in locals() else key,
                                   batch_number=batch_number, issue_type="ROW_ERROR",
                                   details=f"Unexpected error comparing row: {e}")
                        continue

                audit.flush()
                audit.save_checkpoint(
                    client_id, flow_id, batch_number, last_values,
                    {"matched": matched, "mismatched": mismatched, "source_blank": source_blank,
                     "destination_blank": destination_blank, "both_blank": both_blank},
                )
                log.info(f"[client={client_id} flow={flow_id}] Batch {batch_number}: compared {len(batch_keys)} keys")

            check_orphans = runtime_cfg.mapping_option(
                "check_orphans_in_destination", mapping, "check_orphans_in_destination"
            )
            if check_orphans and not resumed:
                _check_orphans(
                    dst_conn, audit, mapping, client_id, flow_id, ref_dest_cols,
                    ref_source_cols, all_source_keys, date_filter_cfg, batch_size, runtime_cfg,
                )
            elif resumed:
                log.info(f"[client={client_id} flow={flow_id}] Orphan check skipped (this was a resumed run)")
        finally:
            src_matcher.close()
            if dst_matcher is not None:
                dst_matcher.close()

        audit.clear_checkpoint(client_id, flow_id)

        audit.write_summary(
            client_id, flow_id, src_table, dst_table,
            extra_counts={
                "matched": matched, "mismatched": mismatched,
                "source_blank": source_blank, "destination_blank": destination_blank,
                "both_blank": both_blank,
            },
        )
        log.info(f"[client={client_id} flow={flow_id}] Finished. matched={matched} mismatched={mismatched} "
                 f"source_blank={source_blank} destination_blank={destination_blank} both_blank={both_blank}")
    finally:
        audit.clear_default_context()


def _check_orphans(dst_conn, audit, mapping, client_id, flow_id, dst_key_columns,
                    src_key_columns, source_keys, date_filter_cfg, batch_size, runtime_cfg):
    dst_table = mapping.get("destination_table")
    src_table = mapping.get("source_table")

    last_values = None
    batch_number = 0
    while True:
        batch_number += 1
        try:
            active_date_col = (date_filter_cfg or {}).get("destination_column") if date_filter_cfg else None
            batch_keys = fetch_key_batch(
                dst_conn, dst_table, dst_key_columns, date_filter_cfg,
                active_date_col, last_values, batch_size, runtime_cfg,
            )
        except Exception as e:
            audit.log(client_id=client_id, flow_id=flow_id, source_table=src_table, destination_table=dst_table,
                       issue_type="BATCH_ERROR", batch_number=batch_number,
                       details=f"Orphan check: failed to fetch destination key batch: {e}")
            break

        if not batch_keys:
            break
        last_values = list(batch_keys[-1])

        for key in batch_keys:
            if key not in source_keys:
                key_primary_value, key_map, key_context_json = build_key_context(
                    dst_key_columns, key, runtime_cfg
                )
                row_client_id, row_flow_id, row_audit_id, row_client_audit_id = build_row_log_identity(
                    key_map, client_id, flow_id, runtime_cfg
                )
                audit.log(client_id=row_client_id, flow_id=row_flow_id,
                           audit_id=row_audit_id, client_audit_id=row_client_audit_id,
                           source_table=src_table, destination_table=dst_table,
                           key_value=key_primary_value, batch_number=batch_number, issue_type="MISSING_IN_SOURCE",
                           join_key_status=JOIN_MISSING_SOURCE_KEY,
                           exception_reason="Destination record has no matching source key",
                           details=f"key_context={key_context_json}")
        audit.flush()


def export_exception_report(conn, log_table, run_id, out_path, runtime_cfg):
    cur = conn.cursor()
    exclude_types = runtime_cfg.export_cfg.get("exclude_issue_types") or DEFAULT_EXPORT_EXCLUDE_ISSUE_TYPES
    exclude_sql = ", ".join(f"'{t}'" for t in exclude_types) if exclude_types else "''"
    sql = runtime_cfg.render_query(
        "exception_report",
        log_table=log_table,
        exclude_issue_types=exclude_sql,
    )
    cur.execute(sql, [run_id])
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description]

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(list(row))

    log.info(f"Exception report written to {out_path} ({len(rows)} rows)")


def _insert_update_decision(n_src, n_dst, fix_blanks, fix_mismatches):
    if fix_blanks and n_dst is None and n_src is not None:
        return True, "Replaced NULL with source value"
    if fix_mismatches and n_dst is not None and n_src is not None and n_src != n_dst:
        return True, "Replaced mismatched value"
    return False, None


def _execute_destination_update(dst_conn, dst_table, dst_col, ref_dest_cols, key, src_val, runtime_cfg):
    where_parts = [f"{ref_dest_cols[i]} = ?" for i in range(len(ref_dest_cols))]
    where_clause = " AND ".join(where_parts)
    update_sql = runtime_cfg.render_query(
        "destination_update",
        table=dst_table,
        column=dst_col,
        where=where_clause,
    )
    cur = dst_conn.cursor()
    cur.execute(update_sql, [src_val] + list(key))
    dst_conn.commit()


def _load_custom_query_side(pool, audit, mapping, catalog, runtime_cfg, side, ref_cols, label):
    """Run source/destination fetch query and return {join_key: row_dict}."""
    mapping = resolve_mapping_from_client(mapping, catalog)
    client_id = str(mapping.get("client_id", ""))
    flow_id = str(mapping.get("flow_id", ""))
    fetch_cfg = resolve_fetch_config(mapping, catalog)
    resolved = resolve_servers_and_db(mapping, catalog)
    sql = fetch_cfg["source_fetch_query"] if side == "source" else fetch_cfg["destination_fetch_query"]

    cached = get_custom_query_cache(side, client_id, sql)
    if cached is not None:
        log.info(f"[client={client_id}] Reusing cached {label} query for insert ({len(cached)} keys)")
        return cached, resolved

    conn = pool.get(
        resolved["source_server"] if side == "source" else resolved["destination_server"],
        resolved["source_database"] if side == "source" else resolved["destination_database"],
    )
    log.info(f"[client={client_id}] Running custom {label} fetch query for insert...")
    rows, _ = execute_custom_fetch_query(conn, sql, ref_cols, runtime_cfg)
    set_custom_query_cache(side, client_id, sql, rows)
    log.info(f"[client={client_id}] Custom {label} insert query loaded {len(rows)} keys")
    return rows, resolved


def _insert_custom_query_batch(pool, audit, mappings, catalog, runtime_cfg):
    """Insert/update using custom fetch queries — all columns in one pass per client."""
    if not mappings:
        return

    mappings = [resolve_mapping_from_client(m, catalog) for m in mappings]
    mapping0 = mappings[0]
    client_id = str(mapping0.get("client_id", ""))
    flow_id = str(mapping0.get("flow_id", ""))
    src_table = mapping0.get("source_table")
    dst_table = mapping0.get("destination_table")
    batch_size = runtime_cfg.config.get("batch_size")
    fix_blanks = runtime_cfg.option("fix_blanks", mapping0)
    fix_mismatches = runtime_cfg.option("fix_mismatches", mapping0)

    join_keys = classify_join_keys(mapping0)
    usable_join_keys = [j for j in join_keys if j["status"] == JOIN_READY]
    ref_source_cols = [j["source_column"] for j in usable_join_keys]
    ref_dest_cols = [j["destination_column"] for j in usable_join_keys]

    per_mapping = []
    for m in mappings:
        per_mapping.append({
            "src_col": m.get("source_column"),
            "dst_col": m.get("destination_column"),
            "norm_cfg": m.get("normalization"),
        })
        audit.set_default_context(
            client_id=client_id, flow_id=flow_id,
            source_table=m.get("source_table"), destination_table=m.get("destination_table"),
            source_column=m.get("source_column"), destination_column=m.get("destination_column"),
        )

    try:
        source_rows_cache, resolved = _load_custom_query_side(
            pool, audit, mapping0, catalog, runtime_cfg, "source", ref_source_cols, "source",
        )
        destination_rows_cache, _ = _load_custom_query_side(
            pool, audit, mapping0, catalog, runtime_cfg, "destination", ref_dest_cols, "destination",
        )
        dst_conn = pool.get(resolved["destination_server"], resolved["destination_database"])
    except Exception as e:
        audit.log(client_id=client_id, flow_id=flow_id, issue_type="CONNECTION_ERROR",
                  details=f"Custom fetch query failed during insert: {e}")
        report_connection_error(client_id, flow_id, f"Custom fetch query failed during insert: {e}")
        return

    all_source_keys = sorted(source_rows_cache.keys())
    updated_count, skipped_count = 0, 0
    batch_number = 0

    # FIX: batched writes instead of one UPDATE + one commit per row (see
    # BatchUpdater docstring). One updater is reused for the whole client's
    # run and closed at the end, same lifecycle as BatchKeyMatcher.
    updater = BatchUpdater(dst_conn, ref_dest_cols, runtime_cfg)

    log.info(
        f"[client={client_id} flow={flow_id}] Custom-query insert: "
        f"{len(all_source_keys)} keys x {len(per_mapping)} column(s)"
    )

    try:
        for batch_start in range(0, len(all_source_keys), batch_size):
            batch_number += 1
            batch_keys = all_source_keys[batch_start:batch_start + batch_size]

            # One pending list per validated column -- different columns can
            # need different subsets of rows updated within the same batch.
            pending_by_col = {id(pm): [] for pm in per_mapping}
            # Row context saved so we can log AFTER the batched UPDATE
            # actually runs, instead of before -- if the UPDATE fails we
            # want a batch-level error, not thousands of false "UPDATED" logs.
            log_ctx_by_col = {id(pm): [] for pm in per_mapping}

            for key in batch_keys:
                key_primary_value, key_map, key_context_json = build_key_context(
                    ref_source_cols, key, runtime_cfg
                )
                internal = getattr(audit, "internal", None)
                if internal and internal.enabled:
                    tr = internal._trackers.get(audit.stage)
                    if tr:
                        tr.record_insert_key_seen()

                src_row = source_rows_cache.get(key)
                dst_row = destination_rows_cache.get(key)
                row_client_id, row_flow_id, row_audit_id, row_client_audit_id = build_row_log_identity(
                    key_map, client_id, flow_id, runtime_cfg,
                    src_row=src_row, dst_row=dst_row, key_primary_value=key_primary_value,
                )

                ids = {
                    "client_id": row_client_id, "flow_id": row_flow_id,
                    "audit_id": row_audit_id, "client_audit_id": row_client_audit_id,
                }

                if src_row is None or dst_row is None:
                    for pm in per_mapping:
                        _log_insert_decision(
                            audit, runtime_cfg, ids, key_primary_value,
                            pm["src_col"], pm["dst_col"],
                            _row_get(src_row, pm["src_col"]) if src_row else None,
                            _row_get(dst_row, pm["dst_col"]) if dst_row else None,
                            "SKIPPED", "missing_source_or_destination_row",
                            batch_number, key_context_json,
                        )
                        skipped_count += 1
                    continue

                for pm in per_mapping:
                    src_val = _row_get(src_row, pm["src_col"])
                    dst_val = _row_get(dst_row, pm["dst_col"])
                    n_src = normalize_value(src_val, pm["norm_cfg"], runtime_cfg)
                    n_dst = normalize_value(dst_val, pm["norm_cfg"], runtime_cfg)
                    should_update, update_reason = _insert_update_decision(
                        n_src, n_dst, fix_blanks, fix_mismatches,
                    )
                    if should_update:
                        pending_by_col[id(pm)].append((key, src_val))
                        log_ctx_by_col[id(pm)].append(
                            (ids, key_primary_value, src_val, dst_val, update_reason, key_context_json)
                        )
                    else:
                        skipped_count += 1
                        skip_reason = _insert_skip_reason(n_src, n_dst, fix_blanks, fix_mismatches)
                        _log_insert_decision(
                            audit, runtime_cfg, ids, key_primary_value,
                            pm["src_col"], pm["dst_col"], src_val, dst_val,
                            "SKIPPED", skip_reason, batch_number, key_context_json,
                        )

            # One batched UPDATE per column for this whole batch of keys,
            # instead of one UPDATE per row.
            for pm in per_mapping:
                pending = pending_by_col[id(pm)]
                if not pending:
                    continue
                try:
                    updater.apply(dst_table, pm["dst_col"], pending)
                    updated_count += len(pending)
                    for ids, key_primary_value, src_val, dst_val, update_reason, key_context_json in log_ctx_by_col[id(pm)]:
                        _log_insert_decision(
                            audit, runtime_cfg, ids, key_primary_value,
                            pm["src_col"], pm["dst_col"], src_val, dst_val,
                            "UPDATED", update_reason, batch_number, key_context_json,
                        )
                except Exception as e:
                    audit.log(
                        client_id=client_id, flow_id=flow_id, issue_type="BATCH_ERROR",
                        batch_number=batch_number,
                        source_column=pm["src_col"], destination_column=pm["dst_col"],
                        details=f"Batched update failed for {len(pending)} row(s): {e}",
                    )

            audit.flush()
            log.info(f"[client={client_id} flow={flow_id}] Insert batch {batch_number}: processed {len(batch_keys)} keys")
    finally:
        updater.close()

    log.info(
        f"[client={client_id} flow={flow_id}] Custom-query INSERT complete. "
        f"updated={updated_count} skipped={skipped_count}"
    )
    audit.clear_default_context()


def _insert_mapping_custom_query(pool, audit, mapping, catalog, runtime_cfg):
    """Insert/update one column using custom fetch queries (when single_pass is off)."""
    _insert_custom_query_batch(pool, audit, [mapping], catalog, runtime_cfg)


def _run_insert_mappings(pool, audit, mappings, catalog, runtime_cfg):
    """Run insert for all mappings — custom_query (batched) or table mode (per mapping)."""
    if not mappings:
        log.warning("Insert skipped: no column mappings to run")
        return
    groups, other = _group_custom_query_mappings(mappings, catalog, runtime_cfg)
    for (cid, fid), group in groups.items():
        try:
            _insert_custom_query_batch(pool, audit, group, catalog, runtime_cfg)
        except Exception as e:
            audit.log(client_id=cid, flow_id=fid, issue_type="RUN_ERROR", details=str(e))
            audit.flush()
            log.error(f"[client={cid} flow={fid}] Custom-query insert failed: {e}")
    for mapping in other:
        try:
            insert_mapping(pool, audit, mapping, catalog, runtime_cfg)
        except Exception as e:
            audit.log(
                client_id=mapping.get("client_id"), flow_id=mapping.get("flow_id"),
                issue_type="RUN_ERROR", details=str(e),
            )
            audit.flush()
            log.error(f"[client={mapping.get('client_id')}] Insert failed: {e}")


def insert_mapping(pool, audit, mapping, catalog, runtime_cfg):
    mapping = resolve_mapping_from_client(mapping, catalog)
    client_id = str(mapping.get("client_id", ""))
    flow_id = str(mapping.get("flow_id", ""))
    src_table = mapping.get("source_table")
    dst_table = mapping.get("destination_table")
    src_col = mapping.get("source_column")
    dst_col = mapping.get("destination_column")
    batch_size = runtime_cfg.config.get("batch_size")
    date_filter_cfg = runtime_cfg.config.get("date_filter")
    fix_blanks = runtime_cfg.option("fix_blanks", mapping)
    fix_mismatches = runtime_cfg.option("fix_mismatches", mapping)
    if batch_size is None:
        raise ValueError("config.json must define top-level 'batch_size'")

    fetch_cfg = resolve_fetch_config(mapping, catalog)
    if fetch_cfg["fetch_mode"] == "custom_query":
        if not fetch_cfg["source_fetch_query"] or not fetch_cfg["destination_fetch_query"]:
            audit.log(
                client_id=client_id, flow_id=flow_id,
                source_table=src_table, destination_table=dst_table,
                issue_type="CONFIGURATION_EXCEPTION",
                exception_reason="fetch_mode=custom_query requires source_fetch_query and destination_fetch_query",
            )
            return
        if runtime_cfg.option("single_pass_custom_query"):
            return
        _insert_mapping_custom_query(pool, audit, mapping, catalog, runtime_cfg)
        return

    resolved = resolve_servers_and_db(mapping, catalog)
    source_server_log = resolve_server_name_for_log(resolved.get("source_server"), pool.servers_cfg)
    destination_server_log = resolve_server_name_for_log(resolved.get("destination_server"), pool.servers_cfg)
    audit.set_default_context(
        client_id=client_id, flow_id=flow_id,
        source_server=source_server_log, destination_server=destination_server_log,
        source_database=resolved.get("source_database"), destination_database=resolved.get("destination_database"),
        source_table=src_table, destination_table=dst_table,
        source_column=src_col, destination_column=dst_col,
    )

    mapping_status, reasons = classify_mapping(mapping)
    join_keys = classify_join_keys(mapping)

    try:
        if mapping_status != READY:
            audit.log(client_id=client_id, flow_id=flow_id, source_table=src_table, destination_table=dst_table,
                      issue_type="CONFIGURATION_EXCEPTION", exception_reason="; ".join(reasons))
            return

        if resolved["catalog_missing"]:
            audit.log(client_id=client_id, flow_id=flow_id, source_table=src_table, destination_table=dst_table,
                      issue_type="CONFIGURATION_EXCEPTION", details=resolved["catalog_reason"])
            return

        try:
            src_conn = pool.get(resolved["source_server"], resolved["source_database"])
            dst_conn = pool.get(resolved["destination_server"], resolved["destination_database"])
        except Exception as e:
            audit.log(client_id=client_id, flow_id=flow_id, issue_type="CONNECTION_ERROR", details=str(e))
            return

        log.info(f"[client={client_id} flow={flow_id}] INSERT: {src_table}.{src_col} -> {dst_table}.{dst_col}")

        usable_join_keys = [j for j in join_keys if j["status"] == JOIN_READY]
        ref_source_cols = [j["source_column"] for j in usable_join_keys]
        ref_dest_cols = [j["destination_column"] for j in usable_join_keys]
        src_data_cols = list(dict.fromkeys(ref_source_cols + [src_col]))
        dst_data_cols = list(dict.fromkeys(ref_dest_cols + [dst_col]))

        src_matcher = BatchKeyMatcher(src_conn, ref_source_cols, runtime_cfg)
        dst_matcher = BatchKeyMatcher(dst_conn, ref_dest_cols, runtime_cfg)
        updated_count, skipped_count = 0, 0
        last_values = None
        batch_number = 0

        try:
            while True:
                batch_number += 1
                try:
                    active_date_col = (date_filter_cfg or {}).get("source_column") if date_filter_cfg else None
                    batch_keys = fetch_key_batch(
                        src_conn, src_table, ref_source_cols, date_filter_cfg,
                        active_date_col, last_values, batch_size, runtime_cfg,
                    )
                except Exception as e:
                    audit.log(client_id=client_id, flow_id=flow_id, issue_type="BATCH_ERROR",
                              batch_number=batch_number, details=f"Key fetch failed: {e}")
                    break

                if not batch_keys:
                    break
                last_values = list(batch_keys[-1])

                try:
                    src_rows = src_matcher.fetch_rows(src_table, src_data_cols, batch_keys)
                    dst_rows = dst_matcher.fetch_rows(dst_table, dst_data_cols, batch_keys)
                except Exception as e:
                    audit.log(client_id=client_id, flow_id=flow_id, issue_type="BATCH_ERROR",
                              batch_number=batch_number, details=f"Row fetch failed: {e}")
                    continue

                for key in batch_keys:
                    try:
                        key_primary_value, key_map, key_context_json = build_key_context(
                            ref_source_cols, key, runtime_cfg
                        )
                        internal = getattr(audit, "internal", None)
                        if internal and internal.enabled:
                            tr = internal._trackers.get(audit.stage)
                            if tr:
                                tr.record_insert_key_seen()

                        src_row = src_rows.get(key)
                        dst_row = dst_rows.get(key)
                        row_client_id, row_flow_id, row_audit_id, row_client_audit_id = build_row_log_identity(
                            key_map, client_id, flow_id, runtime_cfg,
                            src_row=src_row, dst_row=dst_row, key_primary_value=key_primary_value,
                        )

                        row_ids = {
                            "client_id": row_client_id, "flow_id": row_flow_id,
                            "audit_id": row_audit_id, "client_audit_id": row_client_audit_id,
                        }

                        if src_row is None or dst_row is None:
                            _log_insert_decision(
                                audit, runtime_cfg, row_ids, key_primary_value,
                                src_col, dst_col,
                                src_row.get(src_col) if src_row else None,
                                dst_row.get(dst_col) if dst_row else None,
                                "SKIPPED", "missing_source_or_destination_row",
                                batch_number, key_context_json,
                            )
                            skipped_count += 1
                            continue

                        src_val = src_row.get(src_col)
                        dst_val = dst_row.get(dst_col)
                        norm_cfg = mapping.get("normalization")
                        n_src = normalize_value(src_val, norm_cfg, runtime_cfg)
                        n_dst = normalize_value(dst_val, norm_cfg, runtime_cfg)

                        should_update, update_reason = _insert_update_decision(
                            n_src, n_dst, fix_blanks, fix_mismatches,
                        )

                        if should_update:
                            _execute_destination_update(
                                dst_conn, dst_table, dst_col, ref_dest_cols, key, src_val, runtime_cfg,
                            )
                            updated_count += 1
                            _log_insert_decision(
                                audit, runtime_cfg, row_ids, key_primary_value,
                                src_col, dst_col, src_val, dst_val,
                                "UPDATED", update_reason, batch_number, key_context_json,
                            )
                        else:
                            skipped_count += 1
                            skip_reason = _insert_skip_reason(n_src, n_dst, fix_blanks, fix_mismatches)
                            _log_insert_decision(
                                audit, runtime_cfg, row_ids, key_primary_value,
                                src_col, dst_col, src_val, dst_val,
                                "SKIPPED", skip_reason, batch_number, key_context_json,
                            )
                    except Exception as e:
                        audit.log(client_id=client_id, flow_id=flow_id, issue_type="ROW_ERROR",
                                  batch_number=batch_number, details=f"Update error: {e}")

                audit.flush()
                log.info(f"[client={client_id} flow={flow_id}] Batch {batch_number}: processed {len(batch_keys)} keys")
        finally:
            src_matcher.close()
            dst_matcher.close()

        log.info(f"[client={client_id} flow={flow_id}] INSERT complete. updated={updated_count} skipped={skipped_count}")
    finally:
        audit.clear_default_context()


def parse_db_object(name):
    parts = name.replace("[", "").replace("]", "").split(".")
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return "dbo", parts[-1]


def discover_log_slot_columns(conn, log_table):
    """Return source_column_1..N groups present on the log table (auto, no config)."""
    schema, table = parse_db_object(log_table)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.name
        FROM sys.columns c
        INNER JOIN sys.tables t ON c.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ?
        ORDER BY c.column_id
        """,
        schema,
        table,
    )
    names = {r[0] for r in cur.fetchall()}
    slot_nums = sorted(
        int(m.group(1))
        for name in names
        for m in [re.match(r"source_column_(\d+)$", name, re.IGNORECASE)]
        if m
    )
    cols = []
    for i in slot_nums:
        for prefix in ("source_column", "destination_column", "source_value", "destination_value", "value_status"):
            col = f"{prefix}_{i}"
            if col in names:
                cols.append(col)
    return cols


def build_export_column_list(base_columns, slot_columns):
    """Insert auto-discovered slot columns after destination_value when exporting."""
    if not slot_columns:
        return list(base_columns)
    anchor = "destination_value"
    if anchor in base_columns:
        idx = base_columns.index(anchor) + 1
        return list(base_columns[:idx]) + list(slot_columns) + list(base_columns[idx:])
    return list(base_columns) + list(slot_columns)


DEFAULT_EXPORT_COLUMNS = [
    "run_id", "run_start_time", "log_time", "stage", "client_id", "flow_id",
    "audit_id", "client_audit_id", "source_server", "destination_server",
    "source_database", "destination_database", "source_table", "destination_table",
    "source_column", "destination_column", "mapping_status", "join_key_status",
    "value_status", "issue_type", "key_value", "source_value", "destination_value",
    "exception_reason", "batch_number", "details",
]

DEFAULT_EXPORT_EXCLUDE_ISSUE_TYPES = ["INFO", "RUN_SUMMARY"]

DEFAULT_EXPORT_SHEETS = {
    "validation": "validation_log",
    "insert": "insert_log",
    "revalidation": "revalidation_log",
}

DEFAULT_EXPORT_EMPTY_MESSAGE = "No issues found - all records matched"
DEFAULT_EXPORT_OUTPUT_FILE = "validation_report.xlsx"


def _max_mapping_slots(mappings, catalog, runtime_cfg):
    """How many numbered log column slots this run needs (max mappings per client group)."""
    groups, other = _group_custom_query_mappings(mappings, catalog, runtime_cfg)
    max_slots = 1 if mappings else 0
    for group in groups.values():
        max_slots = max(max_slots, len(group))
    if other:
        max_slots = max(max_slots, 1)
    return max_slots


def _group_custom_query_mappings(mappings, catalog, runtime_cfg):
    groups, other = {}, []
    if not runtime_cfg.option("single_pass_custom_query"):
        return {}, mappings
    for raw in mappings:
        m = resolve_mapping_from_client(raw, catalog)
        fc = resolve_fetch_config(m, catalog)
        if fc["fetch_mode"] == "custom_query" and fc.get("source_fetch_query") and fc.get("destination_fetch_query"):
            groups.setdefault((str(m.get("client_id", "")), str(m.get("flow_id", "") or "")), []).append(raw)
        else:
            other.append(raw)
    return groups, other


def _run_validate_mappings(pool, audit, mappings, catalog, runtime_cfg):
    max_slots = _max_mapping_slots(mappings, catalog, runtime_cfg)
    if max_slots:
        audit.ensure_log_column_slots(max_slots)
    groups, other = _group_custom_query_mappings(mappings, catalog, runtime_cfg)
    for (cid, fid), group in groups.items():
        try:
            _validate_custom_query_batch(pool, audit, group, catalog, runtime_cfg)
        except Exception as e:
            audit.log(client_id=cid, flow_id=fid, issue_type="RUN_ERROR", details=str(e))
            audit.flush()
            log.error(f"[client={cid} flow={fid}] Single-pass validation failed: {e}")
    for mapping in other:
        try:
            validate_mapping(pool, audit, mapping, catalog, runtime_cfg)
        except Exception as e:
            audit.log(client_id=mapping.get("client_id"), flow_id=mapping.get("flow_id"),
                      issue_type="RUN_ERROR", details=str(e))
            audit.flush()
            log.error(f"[client={mapping.get('client_id')}] Mapping failed: {e}")


def _column_filter_test_cols(mappings, catalog):
    out = []
    for m in mappings:
        r = resolve_mapping_from_client(m, catalog)
        out.append((r["source_column"], r["destination_column"]))
    return out


def _column_filter_test_catalog():
    return build_client_catalog({"clients": [{"client_id": "80"}, {"client_id": "99"}]})


def run_column_filter_self_tests():
    """Built-in self-tests for --column / --source-column / --dest-column filtering."""
    sample = [
        {"client_id": "80", "source_column": "Dispute_1_Estimated_Overpayment_Amount", "destination_column": "Overpayment"},
        {"client_id": "80", "source_column": "DISPUTE_2_ESTIMATED_OVERPAYMENT_AMOUNT", "destination_column": "OverpaidLevel2Appeal"},
        {"client_id": "80", "source_column": "INITIAL_ESTIMATED_OVERPAYMENT_AMOUNT", "destination_column": "Estimated_Overpayment"},
    ]
    custom = [
        {"client_id": "99", "source_column": "overpayment", "destination_column": "paymentover"},
        {"client_id": "99", "source_column": "fee_amount", "destination_column": "total_fee"},
    ]
    catalog = _column_filter_test_catalog()
    cols = _column_filter_test_cols

    tests = []

    def check(name, condition):
        if not condition:
            raise AssertionError(name)
        tests.append(name)

    m = filter_mappings_by_columns(sample, catalog, any_columns=["Overpayment"])
    check("match_destination_name", cols(m, catalog) == [
        ("Dispute_1_Estimated_Overpayment_Amount", "Overpayment")])

    m = filter_mappings_by_columns(sample, catalog, any_columns=["INITIAL_ESTIMATED_OVERPAYMENT_AMOUNT"])
    check("match_source_name", cols(m, catalog) == [
        ("INITIAL_ESTIMATED_OVERPAYMENT_AMOUNT", "Estimated_Overpayment")])

    m = filter_mappings_by_columns(custom, catalog, any_columns=["paymentover"])
    check("match_dest_paymentover", cols(m, catalog) == [("overpayment", "paymentover")])

    m = filter_mappings_by_columns(custom, catalog, any_columns=["overpayment"])
    check("match_source_overpayment", cols(m, catalog) == [("overpayment", "paymentover")])

    m = filter_mappings_by_columns(custom, catalog, source_columns=["overpayment"], dest_columns=["paymentover"])
    check("source_and_dest_pair", cols(m, catalog) == [("overpayment", "paymentover")])

    m = filter_mappings_by_columns(custom, catalog, source_columns=["overpayment"], dest_columns=["total_fee"])
    check("source_dest_mismatch_empty", cols(m, catalog) == [])

    m = filter_mappings_by_columns(custom, catalog, any_columns=["PAYMENTOVER"])
    check("case_insensitive", len(m) == 1)

    cf = {"active": True, "any_columns": ["paymentover"], "source_columns": None, "dest_columns": None}
    v = mappings_for_validate(custom, catalog, cf)
    i = mappings_for_insert(custom, catalog, cf)
    check("validate_insert_same_filter", cols(v, catalog) == cols(i, catalog))

    for name in tests:
        print(f"PASS: {name}")
    print(f"\nAll {len(tests)} column-filter self-tests passed.")
    return 0


def _resolve_cli_column_filters(args):
    any_cols = list(getattr(args, "column", None) or [])
    any_cols.extend(getattr(args, "insert_column", None) or [])
    src_cols = list(getattr(args, "source_column", None) or [])
    dst_cols = list(getattr(args, "dest_column", None) or [])
    if getattr(args, "overpayment", False):
        any_cols.append("Overpayment")
    if getattr(args, "overpaid_level2", False):
        any_cols.append("OverpaidLevel2Appeal")
    if getattr(args, "estimated_overpayment", False):
        any_cols.append("Estimated_Overpayment")
    active = bool(any_cols or src_cols or dst_cols)
    return {
        "active": active,
        "any_columns": any_cols or None,
        "source_columns": src_cols or None,
        "dest_columns": dst_cols or None,
    }


def apply_client_defaults(config):
    """Merge top-level client_defaults into every clients[] entry and fill
    {placeholder} templates using each client's own resolved fields. Runs
    once, right after load_config(), before anything else reads config['clients']."""
    defaults = config.get("client_defaults") or {}
    if not defaults:
        return
    for client in config.get("clients", []):
        merged = dict(defaults)
        merged.update(client)  # the client's own explicit values always win
        fmt_ctx = {k: v for k, v in merged.items() if isinstance(v, (str, int, float))}
        for key in ("source_table", "source_fetch_query", "destination_fetch_query"):
            val = merged.get(key)
            if isinstance(val, str):
                try:
                    merged[key] = val.format(**fmt_ctx)
                except KeyError:
                    pass  # leave as-is if a placeholder has no matching field
        client.clear()
        client.update(merged)


def main():
    parser = argparse.ArgumentParser(description="Validate source vs destination field mappings.")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    parser.add_argument("--mode", choices=["validate", "insert", "workflow"], default="validate",
                        help="Mode: validate, insert, or workflow (runs all 3 stages)")
    parser.add_argument("--stage", choices=["VALIDATION", "INSERT", "REVALIDATION"], default="VALIDATION",
                        help="Stage marker for logs (VALIDATION, INSERT, REVALIDATION)")
    parser.add_argument("--run-id", dest="run_id", help="Use existing run_id")
    parser.add_argument("--report", help="Optional path to write a CSV exception report for this run")
    parser.add_argument(
        "--column", "-c", action="append", dest="column", metavar="NAME",
        help="Run only columns matching this source OR destination name (repeatable).",
    )
    parser.add_argument(
        "--source-column", action="append", dest="source_column", metavar="SRC_COL",
        help="Run only this source column name (repeatable). Pair with --dest-column to pin one mapping.",
    )
    parser.add_argument(
        "--dest-column", "--destination-column", action="append", dest="dest_column",
        metavar="DEST_COL", help="Run only this destination column name (repeatable).",
    )
    parser.add_argument(
        "--insert-column", action="append", dest="insert_column", metavar="NAME",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--overpayment", action="store_true",
        help="Shortcut: only the Overpayment column (source or destination name).",
    )
    parser.add_argument(
        "--overpaid-level2", action="store_true", dest="overpaid_level2",
        help="Shortcut: only OverpaidLevel2Appeal.",
    )
    parser.add_argument(
        "--estimated-overpayment", action="store_true", dest="estimated_overpayment",
        help="Shortcut: only Estimated_Overpayment.",
    )
    parser.add_argument(
        "--test-column-filter", action="store_true",
        help="Run built-in column-filter self-tests (no database) and exit.",
    )
    args = parser.parse_args()
    if args.test_column_filter:
        run_column_filter_self_tests()
        sys.exit(0)
    column_filter = _resolve_cli_column_filters(args)

    config = load_config(args.config)
    apply_client_defaults(config)
    runtime_cfg = build_runtime_config(config)
    run_id = args.run_id if args.run_id else str(uuid.uuid4())
    run_start_time = datetime.datetime.now()
    log_table = config["log_table"]
    query_timeout_seconds = config.get("query_timeout_seconds")
    if query_timeout_seconds is None:
        raise ValueError("config.json must define top-level 'query_timeout_seconds'")
    log_flush_every = runtime_cfg.option("log_flush_every")

    pool = ConnectionPool(config.get("servers", {}), query_timeout_seconds=query_timeout_seconds)
    catalog = build_client_catalog(config)
    all_mappings = resolve_all_mappings(config, catalog, runtime_cfg)
    if not all_mappings:
        log.error("No mappings found. Add mappings[] for table mode, or custom_query fetch SQL in clients[].")
        sys.exit(1)
    log.info(f"Resolved {len(all_mappings)} validation mapping(s) for this run")

    validate_mappings = mappings_for_validate(all_mappings, catalog, column_filter)
    insert_mappings = mappings_for_insert(all_mappings, catalog, column_filter)
    if column_filter.get("active") and not validate_mappings:
        log.error("Column filter matched no mappings. Check --column / --source-column / --dest-column names.")
        sys.exit(1)
    if args.mode == "insert" and not insert_mappings:
        log.error("Insert column filter matched no mappings.")
        sys.exit(1)

    if args.mode == "workflow":
        mappings = validate_mappings
    elif args.mode == "insert":
        mappings = insert_mappings
    else:
        mappings = validate_mappings

    log.info(f"Running {args.mode} mode with {len(mappings)} column mapping(s)")

    internal_logger = InternalRunLogger.from_config(config, run_id, run_start_time, _SCRIPT_DIR)
    stage_ctx = _stage_context_from_mappings(mappings, catalog, run_id)
    if internal_logger.enabled:
        internal_logger.set_run_context(**stage_ctx)
        log.info(f"Internal log file: {internal_logger.log_file_str}")

    log_server, log_db = config.get("log_server"), config.get("log_database")
    try:
        if log_server not in config.get("servers", {}):
            raise KeyError(f"Log server '{log_server}' is not defined in config['servers']")
        log_server_cfg = dict(config["servers"][log_server])
        if log_db:
            log_server_cfg["database"] = log_db
        log_conn = pyodbc.connect(build_connection_string(log_server_cfg), autocommit=False)
        log_conn.timeout = query_timeout_seconds
    except Exception as e:
        log.error(f"Could not connect to log server '{log_server}': {e}")
        sys.exit(1)

    try:
        reset_log_table(log_conn, log_table, runtime_cfg)
    except Exception as e:
        log.error(f"Could not reset log table '{log_table}': {e}")
        sys.exit(1)

    if args.mode == "workflow":
        log.info(f"WORKFLOW started. run_id={run_id}")

        def _audit_for_stage(stage_name):
            a = AuditLogger(log_conn, log_table, run_id, run_start_time, stage=stage_name,
                            flush_every=log_flush_every)
            a.internal = internal_logger
            return a

        log.info("--- STAGE 1: VALIDATION ---")
        internal_logger.begin_stage("VALIDATION", **stage_ctx)
        audit = _audit_for_stage("VALIDATION")
        _run_validate_mappings(pool, audit, validate_mappings, catalog, runtime_cfg)
        audit.flush()
        summary = internal_logger.end_stage("VALIDATION")
        if summary:
            log.info(f"Validation summary: {summary[0].get('total_keys_scanned')} keys in {summary[0].get('duration_seconds')}s")

        log.info("--- STAGE 2: INSERT ---")
        internal_logger.begin_stage("INSERT", **stage_ctx)
        audit = _audit_for_stage("INSERT")
        _run_insert_mappings(pool, audit, insert_mappings, catalog, runtime_cfg)
        audit.flush()
        summary = internal_logger.end_stage("INSERT")
        if summary:
            s = summary[0]
            log.info(
                f"Insert summary: scanned={s.get('total_column_checks')} "
                f"updated={s.get('insert_updated_total')} skipped={s.get('insert_skipped_total')}"
            )

        # FIX: INSERT just changed destination rows on the actual server, but
        # CUSTOM_QUERY_CACHE / DESTINATION_PRELOAD_CACHE are in-memory and know
        # nothing about that. Without clearing them, REVALIDATION would reuse
        # the destination snapshot fetched back in the VALIDATION stage --
        # i.e. it would validate against pre-insert data and never show the
        # updates actually took effect. Clear both before revalidating.
        CUSTOM_QUERY_CACHE.clear()
        DESTINATION_PRELOAD_CACHE.clear()
        log.info("Cleared in-memory query caches before revalidation so it reads post-insert data")

        log.info("--- STAGE 3: REVALIDATION ---")
        internal_logger.begin_stage("REVALIDATION", **stage_ctx)
        audit = _audit_for_stage("REVALIDATION")
        _run_validate_mappings(pool, audit, validate_mappings, catalog, runtime_cfg)
        audit.flush()
        summary = internal_logger.end_stage("REVALIDATION")
        if summary:
            log.info(f"Revalidation summary: {summary[0].get('total_keys_scanned')} keys in {summary[0].get('duration_seconds')}s")

        internal_logger.close(status="SUCCESS")
        log.info(f"WORKFLOW complete. run_id={run_id}")
    else:
        stage = args.stage
        if args.mode == "insert" and args.stage == "VALIDATION":
            stage = "INSERT"

        log.info(f"Run started. run_id={run_id} stage={stage}")
        internal_logger.begin_stage(stage, **stage_ctx)
        audit = AuditLogger(log_conn, log_table, run_id, run_start_time, stage=stage,
                            flush_every=log_flush_every)
        audit.internal = internal_logger

        if args.mode == "insert":
            _run_insert_mappings(pool, audit, insert_mappings, catalog, runtime_cfg)
        else:
            _run_validate_mappings(pool, audit, validate_mappings, catalog, runtime_cfg)

        audit.flush()
        summary = internal_logger.end_stage(stage)
        if summary:
            log.info(f"Stage {stage} summary written to {internal_logger.log_file_str}")
        internal_logger.close(status="SUCCESS")
        log.info(f"Run complete. run_id={run_id}")

    if args.report:
        try:
            export_exception_report(log_conn, log_table, run_id, args.report, runtime_cfg)
        except Exception as e:
            log.error(f"Failed to export exception report: {e}")

    pool.close_all()
    print(f"RUN_ID={run_id}")


if __name__ == "__main__":
    main()
