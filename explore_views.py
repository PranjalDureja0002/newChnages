"""
Oracle View Explorer — Run locally to compare 2 spend views.

Usage:
    python explore_views.py

Set your connection details below OR via environment variables:
    ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD

Output: prints a structured comparison report to console + saves to explore_results.txt
"""

import os
import json

# ── NEW Connection Config (Direct + Indirect Spend views) ─────────────────
HOST = os.getenv("ORACLE_HOST", "YOUR_HOST_HERE")
PORT = int(os.getenv("ORACLE_PORT", "1521"))
SERVICE = os.getenv("ORACLE_SERVICE", "YOUR_SERVICE_HERE")
USER = os.getenv("ORACLE_USER", "YOUR_USER_HERE")
PASSWORD = os.getenv("ORACLE_PASSWORD", "YOUR_PASSWORD_HERE")

# ── OLD Connection Config (VW_SPEND_REPORT_VIEW — different port/service) ─
OLD_HOST = os.getenv("ORACLE_OLD_HOST", HOST)          # same host usually
OLD_PORT = int(os.getenv("ORACLE_OLD_PORT", "1521"))   # << REPLACE with old port
OLD_SERVICE = os.getenv("ORACLE_OLD_SERVICE", "YOUR_OLD_SERVICE_HERE")  # << REPLACE
OLD_USER = os.getenv("ORACLE_OLD_USER", USER)
OLD_PASSWORD = os.getenv("ORACLE_OLD_PASSWORD", PASSWORD)
ENABLE_OLD_VIEW_COMPARISON = True  # Set to False to skip old view comparison

# ── View Names ─────────────────────────────────────────────────────────────
SCHEMA = "PISVIEW"                   # Oracle schema that owns the new views
VIEW_1 = "VW_DIRECT_SPEND_ALL"      # Direct Spend view (unqualified for metadata)
VIEW_2 = "VW_INDIRECT_SPEND_ALL"    # Indirect Spend view
OLD_VIEW = "VW_SPEND_REPORT_VIEW"   # Old view (on old instance, no schema prefix needed)

# Fully qualified names for data queries (FROM clauses)
FQ_VIEW_1 = f"{SCHEMA}.{VIEW_1}"
FQ_VIEW_2 = f"{SCHEMA}.{VIEW_2}"

# Date filter (same as your pipeline's mandatory filter)
DATE_FILTER = "INVOICE_DATE >= DATE '2024-04-01'"


def connect(host=HOST, port=PORT, service=SERVICE, user=USER, password=PASSWORD):
    import oracledb
    dsn = oracledb.makedsn(host, port, service_name=service)
    return oracledb.connect(user=user, password=password, dsn=dsn)


def connect_old():
    """Connect to the OLD Oracle instance (different port/service) for VW_SPEND_REPORT_VIEW."""
    return connect(host=OLD_HOST, port=OLD_PORT, service=OLD_SERVICE,
                   user=OLD_USER, password=OLD_PASSWORD)


def query(conn, sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall() if cols else []
        return cols, rows
    except Exception as e:
        return ["ERROR"], [(str(e),)]
    finally:
        cur.close()


def query_columns_fallback(conn, fq_view):
    """Get column names by querying the view directly (fallback if all_tab_columns returns nothing)."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {fq_view} WHERE 1=0")
        cols = [d[0] for d in cur.description]
        rows = [(c, "", "", "", i+1) for i, c in enumerate(cols)]
        return ["COLUMN_NAME", "DATA_TYPE", "DATA_LENGTH", "NULLABLE", "COLUMN_ID"], rows
    except Exception as e:
        return ["ERROR"], [(str(e),)]
    finally:
        cur.close()


def print_table(title, cols, rows, file=None):
    out = f"\n{'='*80}\n{title}\n{'='*80}\n"
    if not rows:
        out += "(no results)\n"
    else:
        # Compute widths
        widths = [len(c) for c in cols]
        str_rows = []
        for row in rows:
            sr = [str(v) if v is not None else "NULL" for v in row]
            str_rows.append(sr)
            for i, v in enumerate(sr):
                if i < len(widths):
                    widths[i] = max(widths[i], len(v))

        header = " | ".join(c.ljust(widths[i]) for i, c in enumerate(cols))
        out += header + "\n"
        out += "-+-".join("-" * w for w in widths) + "\n"
        for sr in str_rows[:100]:  # cap at 100 rows
            out += " | ".join(sr[i].ljust(widths[i]) if i < len(sr) else "" for i in range(len(cols))) + "\n"
        if len(rows) > 100:
            out += f"... ({len(rows) - 100} more rows)\n"

    print(out)
    if file:
        file.write(out)


def main():
    print(f"Connecting to {HOST}:{PORT}/{SERVICE} as {USER}...")
    conn = connect()
    print("Connected!\n")

    report_path = os.path.join(os.path.dirname(__file__), "explore_results.txt")
    f = open(report_path, "w", encoding="utf-8")

    # Step 0: Find spend-related views in the PISVIEW schema
    print_table(
        f"STEP 0: All views in {SCHEMA} schema with SPEND/DIRECT/INDIRECT in name",
        *query(conn, f"""
            SELECT view_name FROM all_views
            WHERE owner = '{SCHEMA}'
              AND (UPPER(view_name) LIKE '%SPEND%'
                OR UPPER(view_name) LIKE '%DIRECT%'
                OR UPPER(view_name) LIKE '%INDIRECT%')
            ORDER BY view_name
        """), file=f
    )

    # Step 1: Schema comparison (using ALL_TAB_COLUMNS, with fallback)
    for vname, fq, label in [(VIEW_1, FQ_VIEW_1, "View 1 (Direct)"), (VIEW_2, FQ_VIEW_2, "View 2 (Indirect)")]:
        cols, rows = query(conn, f"""
            SELECT column_name, data_type, data_length, nullable, column_id
            FROM all_tab_columns
            WHERE owner = '{SCHEMA}' AND table_name = '{vname}'
            ORDER BY column_id
        """)
        if not rows or (len(rows) == 1 and cols == ["ERROR"]):
            print(f"  all_tab_columns returned nothing for {vname}, using fallback...")
            cols, rows = query_columns_fallback(conn, fq)
        print_table(f"STEP 1: Columns in {label} ({fq})", cols, rows, file=f)

    # Common columns (via fallback-safe approach)
    # Get column lists from both views
    v1_cols_r = query(conn, f"""
        SELECT column_name FROM all_tab_columns
        WHERE owner = '{SCHEMA}' AND table_name = '{VIEW_1}' ORDER BY column_id
    """)
    v2_cols_r = query(conn, f"""
        SELECT column_name FROM all_tab_columns
        WHERE owner = '{SCHEMA}' AND table_name = '{VIEW_2}' ORDER BY column_id
    """)
    # Fallback if all_tab_columns is empty
    if not v1_cols_r[1] or (len(v1_cols_r[1]) == 1 and v1_cols_r[0] == ["ERROR"]):
        _, v1_fb = query_columns_fallback(conn, FQ_VIEW_1)
        v1_col_names = [r[0] for r in v1_fb]
    else:
        v1_col_names = [r[0] for r in v1_cols_r[1]]
    if not v2_cols_r[1] or (len(v2_cols_r[1]) == 1 and v2_cols_r[0] == ["ERROR"]):
        _, v2_fb = query_columns_fallback(conn, FQ_VIEW_2)
        v2_col_names = [r[0] for r in v2_fb]
    else:
        v2_col_names = [r[0] for r in v2_cols_r[1]]

    v1_set = set(v1_col_names)
    v2_set = set(v2_col_names)
    common = sorted(v1_set & v2_set)
    only_v1 = sorted(v1_set - v2_set)
    only_v2 = sorted(v2_set - v1_set)

    print_table(
        f"COMMON COLUMNS ({len(common)} columns in both views)",
        ["COLUMN_NAME"], [(c,) for c in common], file=f
    )
    print_table(
        f"COLUMNS ONLY IN {VIEW_1} ({len(only_v1)} columns)",
        ["COLUMN_NAME"], [(c,) for c in only_v1], file=f
    )
    print_table(
        f"COLUMNS ONLY IN {VIEW_2} ({len(only_v2)} columns)",
        ["COLUMN_NAME"], [(c,) for c in only_v2], file=f
    )

    # Step 2: Row counts (using fully qualified view names)
    for fq_vname, label in [(FQ_VIEW_1, VIEW_1), (FQ_VIEW_2, VIEW_2)]:
        print_table(
            f"ROW COUNT: {fq_vname}",
            *query(conn, f"SELECT COUNT(*) AS total_rows FROM {fq_vname}"), file=f
        )
        print_table(
            f"ROW COUNT (filtered): {fq_vname} WHERE {DATE_FILTER}",
            *query(conn, f"SELECT COUNT(*) AS filtered_rows FROM {fq_vname} WHERE {DATE_FILTER}"), file=f
        )

    # Step 3: Sample data
    for fq_vname, label in [(FQ_VIEW_1, "Direct"), (FQ_VIEW_2, "Indirect")]:
        print_table(
            f"SAMPLE DATA: {fq_vname} (first 3 rows)",
            *query(conn, f"SELECT * FROM {fq_vname} FETCH FIRST 3 ROWS ONLY"), file=f
        )

    # Step 4: Key column profiling
    for fq_vname, label in [(FQ_VIEW_1, "Direct"), (FQ_VIEW_2, "Indirect")]:
        # Amount stats
        print_table(
            f"AMOUNT STATS: {fq_vname}",
            *query(conn, f"""
                SELECT COUNT(*) AS rows, COUNT(AMOUNT) AS non_null,
                       ROUND(MIN(AMOUNT),2) AS min_amt, ROUND(MAX(AMOUNT),2) AS max_amt,
                       ROUND(SUM(AMOUNT),2) AS sum_amt
                FROM {fq_vname} WHERE {DATE_FILTER}
            """), file=f
        )

        # Region distribution
        print_table(
            f"REGION distribution: {fq_vname}",
            *query(conn, f"""
                SELECT REGION, COUNT(*) AS cnt,
                       ROUND(SUM(AMOUNT / EXCH_RATE), 2) AS spend_eur
                FROM {fq_vname} WHERE {DATE_FILTER}
                GROUP BY REGION ORDER BY spend_eur DESC
            """), file=f
        )

        # Country distribution
        print_table(
            f"COUNTRY distribution: {fq_vname}",
            *query(conn, f"""
                SELECT COUNTRY, COUNT(*) AS cnt FROM {fq_vname}
                WHERE {DATE_FILTER} GROUP BY COUNTRY ORDER BY cnt DESC
            """), file=f
        )

        # Currency
        print_table(
            f"CURRENCY distribution: {fq_vname}",
            *query(conn, f"""
                SELECT EXCH_CURRENCY, COUNT(*) AS cnt FROM {fq_vname}
                WHERE {DATE_FILTER} GROUP BY EXCH_CURRENCY ORDER BY cnt DESC
            """), file=f
        )

        # Date range
        print_table(
            f"DATE RANGE: {fq_vname}",
            *query(conn, f"SELECT MIN(INVOICE_DATE) AS min_date, MAX(INVOICE_DATE) AS max_date FROM {fq_vname}"),
            file=f
        )

    # Step 5: Overlap analysis
    for dim, col in [("SUPPLIER", "SUPPLIER_NO"), ("PLANT", "PLANT_NO"), ("COMMODITY", "COMMODITY_DESCRIPTION")]:
        print_table(
            f"{dim} OVERLAP",
            *query(conn, f"""
                SELECT
                    (SELECT COUNT(DISTINCT {col}) FROM {FQ_VIEW_1} WHERE {DATE_FILTER}) AS direct_count,
                    (SELECT COUNT(DISTINCT {col}) FROM {FQ_VIEW_2} WHERE {DATE_FILTER}) AS indirect_count,
                    (SELECT COUNT(*) FROM (
                        SELECT DISTINCT {col} FROM {FQ_VIEW_1} WHERE {DATE_FILTER}
                        INTERSECT
                        SELECT DISTINCT {col} FROM {FQ_VIEW_2} WHERE {DATE_FILTER}
                    )) AS common_count
                FROM DUAL
            """), file=f
        )

    # Step 6: UNION ALL feasibility test
    print_table(
        "UNION ALL TEST: Combined spend by region",
        *query(conn, f"""
            SELECT REGION,
                   SUM(DIRECT_SPEND) AS DIRECT_SPEND_EUR,
                   SUM(INDIRECT_SPEND) AS INDIRECT_SPEND_EUR,
                   SUM(DIRECT_SPEND) + SUM(INDIRECT_SPEND) AS TOTAL_SPEND_EUR
            FROM (
                SELECT REGION, ROUND(AMOUNT / EXCH_RATE, 2) AS DIRECT_SPEND, 0 AS INDIRECT_SPEND
                FROM {FQ_VIEW_1} WHERE {DATE_FILTER}
                UNION ALL
                SELECT REGION, 0 AS DIRECT_SPEND, ROUND(AMOUNT / EXCH_RATE, 2) AS INDIRECT_SPEND
                FROM {FQ_VIEW_2} WHERE {DATE_FILTER}
            ) combined
            GROUP BY REGION
            ORDER BY TOTAL_SPEND_EUR DESC
        """), file=f
    )

    conn.close()

    # ── Optional: Compare with OLD view on separate Oracle instance ──────
    if ENABLE_OLD_VIEW_COMPARISON:
        print(f"\nConnecting to OLD instance ({OLD_HOST}:{OLD_PORT}/{OLD_SERVICE}) for {OLD_VIEW}...")
        try:
            old_conn = connect_old()
            print("Connected to old instance!\n")

            # Old view schema (old instance uses user_tab_columns — view is in user's own schema)
            print_table(
                f"OLD VIEW: Columns in {OLD_VIEW} (old instance)",
                *query(old_conn, f"""
                    SELECT column_name, data_type, data_length, nullable, column_id
                    FROM user_tab_columns
                    WHERE table_name = '{OLD_VIEW}'
                    ORDER BY column_id
                """), file=f
            )

            # Old view row count
            print_table(
                f"OLD VIEW: Row count {OLD_VIEW}",
                *query(old_conn, f"SELECT COUNT(*) AS total_rows FROM {OLD_VIEW}"), file=f
            )
            print_table(
                f"OLD VIEW: Row count (filtered) {OLD_VIEW}",
                *query(old_conn, f"SELECT COUNT(*) AS filtered_rows FROM {OLD_VIEW} WHERE {DATE_FILTER}"), file=f
            )

            # Columns in old view — fetch for cross-comparison
            # Try user_tab_columns first, fallback to querying the view directly
            _, old_cols = query(old_conn, f"""
                SELECT column_name FROM user_tab_columns
                WHERE table_name = '{OLD_VIEW}' ORDER BY column_id
            """)
            if not old_cols or (len(old_cols) == 1 and old_cols[0] == ("ERROR",)):
                _, old_cols = query_columns_fallback(old_conn, OLD_VIEW)
            old_col_set = {r[0] for r in old_cols}

            # Reuse v1_set / v2_set from Step 1 (already computed above)
            v1_col_set = v1_set
            v2_col_set = v2_set
            new_all_cols = v1_col_set | v2_col_set

            # Columns in old view but NOT in either new view
            old_only = sorted(old_col_set - new_all_cols)
            if old_only:
                title = f"COLUMNS IN OLD VIEW ({OLD_VIEW}) BUT NOT IN EITHER NEW VIEW"
                out = f"\n{'='*80}\n{title}\n{'='*80}\n"
                for c in old_only:
                    out += f"  {c}\n"
                print(out)
                f.write(out)

            # Columns in new views but NOT in old view
            new_only = sorted(new_all_cols - old_col_set)
            if new_only:
                title = f"COLUMNS IN NEW VIEWS BUT NOT IN OLD VIEW ({OLD_VIEW})"
                out = f"\n{'='*80}\n{title}\n{'='*80}\n"
                for c in new_only:
                    in_v1 = "V1" if c in v1_col_set else "  "
                    in_v2 = "V2" if c in v2_col_set else "  "
                    out += f"  [{in_v1}] [{in_v2}] {c}\n"
                print(out)
                f.write(out)

            # Full column mapping: old view -> new views
            all_cols = sorted(old_col_set | new_all_cols)
            title = "FULL COLUMN MAPPING: Old View vs New Views"
            out = f"\n{'='*80}\n{title}\n{'='*80}\n"
            out += f"{'COLUMN_NAME':<40} {'OLD':^5} {'V1':^5} {'V2':^5}\n"
            out += f"{'-'*40} {'-'*5} {'-'*5} {'-'*5}\n"
            for c in all_cols:
                old_flag = "  Y  " if c in old_col_set else "  -  "
                v1_flag = "  Y  " if c in v1_col_set else "  -  "
                v2_flag = "  Y  " if c in v2_col_set else "  -  "
                out += f"{c:<40} {old_flag} {v1_flag} {v2_flag}\n"
            print(out)
            f.write(out)

            # Spend comparison: old view total vs sum of new views
            _, old_spend = query(old_conn, f"""
                SELECT ROUND(SUM(AMOUNT / EXCH_RATE), 2) AS total_spend_eur
                FROM {OLD_VIEW} WHERE {DATE_FILTER}
            """)
            old_conn.close()

            new_conn2 = connect()
            _, v1_spend = query(new_conn2, f"""
                SELECT ROUND(SUM(AMOUNT / EXCH_RATE), 2) AS total_spend_eur
                FROM {FQ_VIEW_1} WHERE {DATE_FILTER}
            """)
            _, v2_spend = query(new_conn2, f"""
                SELECT ROUND(SUM(AMOUNT / EXCH_RATE), 2) AS total_spend_eur
                FROM {FQ_VIEW_2} WHERE {DATE_FILTER}
            """)
            new_conn2.close()

            title = "SPEND COMPARISON: Old View vs New Views Combined"
            out = f"\n{'='*80}\n{title}\n{'='*80}\n"
            old_total = old_spend[0][0] if old_spend else "N/A"
            v1_total = v1_spend[0][0] if v1_spend else "N/A"
            v2_total = v2_spend[0][0] if v2_spend else "N/A"
            try:
                combined = float(v1_total) + float(v2_total)
                diff = float(old_total) - combined
                out += f"  Old view ({OLD_VIEW}):       {old_total:>20} EUR\n"
                out += f"  New view 1 ({VIEW_1}):  {v1_total:>20} EUR\n"
                out += f"  New view 2 ({VIEW_2}):  {v2_total:>20} EUR\n"
                out += f"  New combined:                 {combined:>20.2f} EUR\n"
                out += f"  Difference (old - combined):  {diff:>20.2f} EUR\n"
                if abs(diff) < 1:
                    out += "  >> MATCH: Old view total equals sum of new views\n"
                else:
                    out += f"  >> MISMATCH: {abs(diff):,.2f} EUR difference — investigate\n"
            except (TypeError, ValueError):
                out += f"  Old: {old_total}, V1: {v1_total}, V2: {v2_total} (could not compute)\n"
            print(out)
            f.write(out)

        except Exception as e:
            msg = f"\nSkipping old view comparison — could not connect: {e}\n"
            print(msg)
            f.write(msg)

    f.close()
    print(f"\n\nFull report saved to: {report_path}")


if __name__ == "__main__":
    main()
