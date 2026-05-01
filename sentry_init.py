"""
Shared Sentry initialization for the Ram-Z accounting repo.

Call ``init_sentry("component-name")`` near the top of every entry point
(Streamlit app, GitHub Actions script, etc.). Resolution order for the DSN:

    1. SENTRY_DSN environment variable          (CI / GitHub Actions)
    2. Streamlit secrets [sentry] → dsn          (Streamlit Cloud)

If neither is set, or if ``sentry-sdk`` is not installed, ``init_sentry``
silently does nothing — so dev runs and pre-DSN deploys keep working.
"""
from __future__ import annotations

import os


def init_sentry(component: str) -> bool:
    """Initialize Sentry error reporting. Returns True if active."""
    dsn = (os.environ.get("SENTRY_DSN") or "").strip()

    # Streamlit fallback — only attempted if streamlit is importable AND
    # we're inside a script that has loaded secrets.
    if not dsn:
        try:
            import streamlit as st  # type: ignore
            dsn = (st.secrets.get("sentry", {}).get("dsn") or "").strip()
        except Exception:
            dsn = ""

    if not dsn:
        return False

    try:
        import sentry_sdk  # type: ignore
    except ImportError:
        return False  # sentry-sdk not installed — silent no-op

    try:
        sentry_sdk.init(
            dsn=dsn,
            environment=os.environ.get("SENTRY_ENV", "production"),
            release=(os.environ.get("GITHUB_SHA") or "")[:8] or None,
            traces_sample_rate=0.0,   # errors only — no perf tracing overhead
            send_default_pii=False,
        )
        sentry_sdk.set_tag("component", component)
        return True
    except Exception:
        return False  # never let Sentry break the host process
