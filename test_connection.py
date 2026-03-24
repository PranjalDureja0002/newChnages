"""
Quick diagnostic script — test new Oracle connection and find the views.
"""

import os
import oracledb

HOST = os.getenv("ORACLE_HOST", "YOUR_HOST_HERE")
PORT = int(os.getenv("ORACLE_PORT", "1521"))
SERVICE = os.getenv("ORACLE_SERVICE", "YOUR_SERVICE_HERE")
USER = os.getenv("ORACLE_USER", "YOUR_USER_HERE")
PASSWORD = os.getenv("ORACLE_PASSWORD", "YOUR_PASSWORD_HERE")

dsn = oracledb.makedsn(HOST, PORT, service_name=SERVICE)
conn = oracledb.connect(user=USER, password=PASSWORD, dsn=dsn)
cur = conn.cursor()

print(f"Connected as: {USER} @ {HOST}:{PORT}/{SERVICE}")
print()

# 1. Who am I?
cur.execute("SELECT USER FROM DUAL")
print(f"Current user: {cur.fetchone()[0]}")

cur.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL")
print(f"Current schema: {cur.fetchone()[0]}")
print()

# 2. What schemas have SPEND/DIRECT/INDIRECT views?
print("=" * 60)
print("Searching ALL schemas for SPEND/DIRECT/INDIRECT views...")
print("=" * 60)
cur.execute("""
    SELECT owner, view_name
    FROM all_views
    WHERE UPPER(view_name) LIKE '%SPEND%'
       OR UPPER(view_name) LIKE '%DIRECT%'
       OR UPPER(view_name) LIKE '%INDIRECT%'
    ORDER BY owner, view_name
""")
rows = cur.fetchall()
if rows:
    for owner, vname in rows:
        print(f"  {owner}.{vname}")
else:
    print("  (none found)")
print()

# 3. Check if PISVIEW schema views are accessible
print("=" * 60)
print("Checking PISVIEW schema specifically...")
print("=" * 60)
cur.execute("""
    SELECT view_name FROM all_views
    WHERE owner = 'PISVIEW'
    ORDER BY view_name
""")
rows = cur.fetchall()
if rows:
    for (vname,) in rows:
        print(f"  PISVIEW.{vname}")
else:
    print("  No views found in PISVIEW schema (no access or schema doesn't exist)")
print()

# 4. Check all tables/views accessible to us (any schema) with SPEND in name
print("=" * 60)
print("All accessible tables/views with SPEND in name...")
print("=" * 60)
cur.execute("""
    SELECT owner, table_name, 'TABLE' AS obj_type FROM all_tables
    WHERE UPPER(table_name) LIKE '%SPEND%'
    UNION ALL
    SELECT owner, view_name, 'VIEW' FROM all_views
    WHERE UPPER(view_name) LIKE '%SPEND%'
    ORDER BY 1, 2
""")
rows = cur.fetchall()
if rows:
    for owner, name, otype in rows:
        print(f"  {otype:5s}  {owner}.{name}")
else:
    print("  (none found)")
print()

# 5. Try user's own schema views
print("=" * 60)
print("Views in YOUR schema (user_views)...")
print("=" * 60)
cur.execute("SELECT view_name FROM user_views ORDER BY view_name")
rows = cur.fetchall()
if rows:
    for (vname,) in rows:
        print(f"  {vname}")
else:
    print("  (none)")
print()

# 6. Try user's own tables
print("=" * 60)
print("Tables in YOUR schema (user_tables)...")
print("=" * 60)
cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
rows = cur.fetchall()
if rows:
    for (tname,) in rows[:30]:
        print(f"  {tname}")
    if len(rows) > 30:
        print(f"  ... and {len(rows) - 30} more")
else:
    print("  (none)")
print()

# 7. Direct test — try querying the views
print("=" * 60)
print("Direct query tests...")
print("=" * 60)
for view in [
    "PISVIEW.VW_DIRECT_SPEND_ALL",
    "PISVIEW.VW_INDIRECT_SPEND_ALL",
    "VW_DIRECT_SPEND_ALL",
    "VW_INDIRECT_SPEND_ALL",
]:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {view}")
        cnt = cur.fetchone()[0]
        print(f"  {view} => {cnt:,} rows  ✓")
    except Exception as e:
        print(f"  {view} => ERROR: {e}")
print()

# 8. Check grants/privileges
print("=" * 60)
print("Your privileges on PISVIEW objects...")
print("=" * 60)
cur.execute("""
    SELECT table_name, privilege
    FROM all_tab_privs
    WHERE grantee = USER AND table_schema = 'PISVIEW'
    ORDER BY table_name, privilege
""")
rows = cur.fetchall()
if rows:
    for tname, priv in rows:
        print(f"  {tname}: {priv}")
else:
    print("  (no direct grants found — might have access via role)")

cur.close()
conn.close()
print("\nDone.")
