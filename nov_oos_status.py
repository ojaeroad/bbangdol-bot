"""Read-only November OOS diagnostics. No schema creation or DB mutation."""
from __future__ import annotations
import os
from datetime import datetime, timezone
import psycopg
from performance_oos_nov_collector import OOS_START, OOS_END_EXCLUSIVE, NOV_ENABLED


def main():
    print('NOV_ENABLED:', NOV_ENABLED)
    print('NOV_START_UTC:', OOS_START.isoformat() if NOV_ENABLED else 'NOT_CONFIGURED')
    print('NOV_END_EXCLUSIVE_UTC:', OOS_END_EXCLUSIVE.isoformat())
    if not NOV_ENABLED:
        print('STOP: set TAJEOMON_NOV_OOS_START_UTC before deploying')
        return
    url=os.getenv('PERFORMANCE_DATABASE_URL') or os.getenv('DATABASE_URL')
    if not url:
        print('DB URL missing in runtime environment')
        return
    with psycopg.connect(url, connect_timeout=5) as conn:
        for table in ('performance_oos_nov_feed_health','performance_oos_nov_shadow_occurrences','performance_oos_nov_shadow_events'):
            exists=conn.execute('SELECT to_regclass(%s)',(table,)).fetchone()[0]
            if not exists:
                print(table, 'NOT_CREATED_YET')
                continue
            # Only allow the hardcoded trusted identifiers above.
            row=conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()
            print(table, 'rows=',row[0])
        registry=conn.execute("SELECT to_regclass('performance_oos_freeze_registry')").fetchone()[0]
        if registry:
            row=conn.execute("SELECT freeze_key,oos_start,oos_end_exclusive FROM performance_oos_freeze_registry WHERE freeze_key=%s",('TAJEOMON_V152_OOS_202611',)).fetchone()
            print('NOV_FREEZE:',row or 'NOT_CREATED_YET')
    print('CHECK_UTC:',datetime.now(timezone.utc).isoformat())
if __name__=='__main__':main()
