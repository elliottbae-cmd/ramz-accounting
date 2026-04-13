"""Create a test token for the GM portal."""
import uuid
import sys
sys.path.insert(0, ".")
from labor.supabase_db import get_supabase

sb = get_supabase()
token = str(uuid.uuid4())
sb.table("rev_band_submissions").upsert({
    "location_id": "112-0001",
    "week_start": "2026-04-02",
    "selected_band": "",
    "status": "pending_gm",
    "token": token,
}).execute()
print(f"Test URL: https://ramz-gm-select.streamlit.app/?token={token}")
