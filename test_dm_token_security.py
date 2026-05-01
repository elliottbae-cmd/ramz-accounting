"""
End-to-end sanity test for the DM token / race guard fixes.

Exercises rev_band_submissions against the live Supabase using a far-future
week_start (year 2099) so it can't collide with any production row. Cleans
up the test row at the end regardless of outcome.

Credential resolution (in order):
  1. SUPABASE_URL + SUPABASE_KEY env vars  (CI / GitHub Actions)
  2. .streamlit/secrets.toml               (local dev)

Run:    python test_dm_token_security.py
Expect: all checks pass, row deleted at the end. Exit code 0 on success.
"""
import os
import sys
import uuid
import pathlib
from datetime import date
from supabase import create_client


def _load_credentials():
    """Resolve Supabase URL+key from env vars first, then local secrets.toml."""
    env_url = os.environ.get("SUPABASE_URL", "").strip()
    env_key = os.environ.get("SUPABASE_KEY", "").strip()
    if env_url and env_key:
        return env_url, env_key

    secrets_path = pathlib.Path(__file__).resolve().parent / ".streamlit" / "secrets.toml"
    if secrets_path.exists():
        try:
            import toml
            secrets = toml.load(str(secrets_path))
            return secrets["supabase"]["url"], secrets["supabase"]["key"]
        except Exception as e:
            print(f"ERROR: failed to load secrets from {secrets_path}: {e}")
            sys.exit(2)

    print("ERROR: no Supabase credentials available.")
    print("  Set SUPABASE_URL and SUPABASE_KEY env vars, or provide "
          ".streamlit/secrets.toml")
    sys.exit(2)


_url, _key = _load_credentials()
sb         = create_client(_url, _key)

# ── Pick a real store + far-future week so we can't collide with prod ─────
TEST_STORE_ID = "112-0001"
TEST_WEEK     = date(2099, 12, 31)

# Track what we created so cleanup works even if a check fails midway
_test_row_id = None


def _ok(msg):
    print(f"  [OK]   {msg}")


def _fail(msg):
    print(f"  [FAIL] {msg}")
    raise AssertionError(msg)


def _cleanup():
    """Always-runs cleanup. Idempotent."""
    try:
        sb.table("rev_band_submissions").delete().eq(
            "location_id", TEST_STORE_ID
        ).eq("week_start", str(TEST_WEEK)).execute()
        print("\nCleanup: test row deleted.")
    except Exception as e:
        print(f"\n⚠ Cleanup failed: {e}")


def main():
    global _test_row_id

    # Defensive: clear any leftover from a previous failed run
    sb.table("rev_band_submissions").delete().eq(
        "location_id", TEST_STORE_ID
    ).eq("week_start", str(TEST_WEEK)).execute()

    print("=" * 60)
    print("DM TOKEN / RACE GUARD SANITY TEST")
    print("=" * 60)
    print(f"Store: {TEST_STORE_ID}    Week: {TEST_WEEK}\n")

    # ── 1. Create a fresh submission row ────────────────────────────────
    print("1. Creating test submission row...")
    gm_token = str(uuid.uuid4())
    dm_token = str(uuid.uuid4())
    ins = sb.table("rev_band_submissions").insert({
        "location_id": TEST_STORE_ID,
        "week_start":  str(TEST_WEEK),
        "token":       gm_token,
        "dm_token":    dm_token,
        "status":      "pending_gm",
    }).execute()
    if not ins.data:
        _fail("insert returned no row")
    _test_row_id = ins.data[0]["id"]
    _ok(f"row created (id={_test_row_id})")

    # Re-read to confirm dm_token persisted
    row = sb.table("rev_band_submissions").select("token,dm_token,status").eq(
        "id", _test_row_id).execute().data[0]
    if row["token"] == row["dm_token"]:
        _fail("token == dm_token — they MUST be distinct")
    _ok("token and dm_token are distinct UUIDs")
    if row["status"] != "pending_gm":
        _fail(f"unexpected initial status: {row['status']}")
    _ok("initial status is pending_gm")

    # ── 2. GM lookup by token works ─────────────────────────────────────
    print("\n2. GM lookup by token...")
    r = sb.table("rev_band_submissions").select("id").eq("token", gm_token).execute()
    if len(r.data or []) != 1 or r.data[0]["id"] != _test_row_id:
        _fail("GM token lookup failed to find the row")
    _ok("GM token resolves to the correct row")

    # ── 3. PRIVILEGE ESCALATION CHECK — GM token must NOT match dm_token ─
    print("\n3. Privilege-escalation guard: GM token via dm_token lookup...")
    r = sb.table("rev_band_submissions").select("id").eq("dm_token", gm_token).execute()
    if r.data:
        _fail(f"GM token matched a dm_token row — escalation possible! "
              f"Found {len(r.data)} row(s)")
    _ok("GM token does NOT match any dm_token row (escalation blocked)")

    # ── 4. DM lookup by dm_token works ──────────────────────────────────
    print("\n4. DM lookup by dm_token...")
    r = sb.table("rev_band_submissions").select("id").eq("dm_token", dm_token).execute()
    if len(r.data or []) != 1 or r.data[0]["id"] != _test_row_id:
        _fail("DM token lookup failed to find the row")
    _ok("DM token resolves to the correct row")

    # ── 5. GM submit race guard (status: pending_gm → pending_dm) ───────
    print("\n5. GM submit race guard...")
    upd = sb.table("rev_band_submissions").update({
        "selected_band": "TEST_BAND",
        "status":        "pending_dm",
    }).eq("token", gm_token).eq("status", "pending_gm").execute()
    if not upd.data:
        _fail("first GM submit returned no rows — guard fired incorrectly")
    _ok("first GM submit applied (status now pending_dm)")

    # Replay the same update — should NOT apply (status is now pending_dm)
    upd = sb.table("rev_band_submissions").update({
        "selected_band": "REPLAY_ATTEMPT",
        "status":        "pending_dm",
    }).eq("token", gm_token).eq("status", "pending_gm").execute()
    if upd.data:
        _fail("replayed GM submit applied — race guard NOT working")
    _ok("replayed GM submit blocked by race guard")

    # Confirm selected_band is the original, not REPLAY_ATTEMPT
    row = sb.table("rev_band_submissions").select("selected_band,status").eq(
        "id", _test_row_id).execute().data[0]
    if row["selected_band"] != "TEST_BAND":
        _fail(f"selected_band was clobbered: {row['selected_band']}")
    _ok("selected_band preserved (no clobber)")

    # ── 6. DM approve race guard (status: pending_dm → pending_admin) ───
    print("\n6. DM approve race guard...")
    upd = sb.table("rev_band_submissions").update({
        "status":         "pending_admin",
        "dm_approved_by": "test_runner",
    }).eq("id", _test_row_id).eq("status", "pending_dm").execute()
    if not upd.data:
        _fail("first DM approve returned no rows — guard fired incorrectly")
    _ok("first DM approve applied (status now pending_admin)")

    # Replay — should NOT apply
    upd = sb.table("rev_band_submissions").update({
        "status":         "pending_admin",
        "dm_approved_by": "REPLAY_ATTEMPT",
    }).eq("id", _test_row_id).eq("status", "pending_dm").execute()
    if upd.data:
        _fail("replayed DM approve applied — race guard NOT working")
    _ok("replayed DM approve blocked by race guard")

    # Confirm dm_approved_by is the original, not REPLAY_ATTEMPT
    row = sb.table("rev_band_submissions").select("dm_approved_by").eq(
        "id", _test_row_id).execute().data[0]
    if row["dm_approved_by"] != "test_runner":
        _fail(f"dm_approved_by was clobbered: {row['dm_approved_by']}")
    _ok("dm_approved_by preserved (no clobber)")

    print("\n" + "=" * 60)
    print("ALL CHECKS PASSED")
    print("=" * 60)


if __name__ == "__main__":
    rc = 0
    try:
        main()
    except AssertionError as e:
        print(f"\nTEST FAILED: {e}")
        rc = 1
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e}")
        rc = 2
    finally:
        _cleanup()
    sys.exit(rc)
