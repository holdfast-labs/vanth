"""Demo: a backup that reports what it is doing but no progress."""
import time

from _events import duration_scale, emit

scale = duration_scale()
tables = ["users", "orders", "events", "invoices", "audit_log", "sessions", "products"]
copied = 0.0
for k in range(70):
    time.sleep(3 * scale)
    copied += 0.37
    emit("checkpoint", f"copying /var/lib/postgres/{tables[k % len(tables)]} ({copied:.1f} GB)")
print("backup complete")
