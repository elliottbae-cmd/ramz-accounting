"""
Ram-Z Email Service
-------------------
SendGrid-powered email delivery for the GM/DM revenue band workflow.

Handles:
  - Sending branded HTML emails to GMs and DMs
  - Reminder escalation (8am, noon, 5pm with CC escalation)
  - Email logging to Supabase
"""

import os
from datetime import datetime

import streamlit as st

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import (
        Mail, Email, To, HtmlContent,
        Cc, Subject,
    )
    SENDGRID_AVAILABLE = True
except ImportError:
    SENDGRID_AVAILABLE = False


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------
def _get_sendgrid_key():
    """Get SendGrid API key from Streamlit secrets or env."""
    try:
        return st.secrets["sendgrid"]["api_key"]
    except Exception:
        return os.environ.get("SENDGRID_API_KEY", "")


def _get_from_email():
    """Get verified sender email from Streamlit secrets or env."""
    try:
        return st.secrets["sendgrid"]["from_email"]
    except Exception:
        return os.environ.get("SENDGRID_FROM_EMAIL", "")


# ---------------------------------------------------------------------------
# Core send function
# ---------------------------------------------------------------------------
def send_email(to_email, subject, html_body, cc_emails=None, from_email=None):
    """
    Send an HTML email via SendGrid.

    Args:
        to_email: Recipient email address (str)
        subject: Email subject line (str)
        html_body: HTML content of the email (str)
        cc_emails: Optional list of CC email addresses
        from_email: Override sender (defaults to secrets config)

    Returns:
        dict with 'success' (bool), 'status_code' (int), 'message' (str)
    """
    if not SENDGRID_AVAILABLE:
        return {"success": False, "status_code": 0,
                "message": "sendgrid package not installed"}

    api_key = _get_sendgrid_key()
    sender = from_email or _get_from_email()

    if not api_key:
        return {"success": False, "status_code": 0,
                "message": "SendGrid API key not configured"}
    if not sender:
        return {"success": False, "status_code": 0,
                "message": "Sender email not configured"}

    message = Mail(
        from_email=Email(sender, "Ram-Z Restaurant Group"),
        to_emails=To(to_email),
        subject=Subject(subject),
        html_content=HtmlContent(html_body),
    )

    # Add CC recipients if provided
    if cc_emails:
        for cc in cc_emails:
            message.add_cc(Cc(cc))

    try:
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        return {
            "success": response.status_code in (200, 201, 202),
            "status_code": response.status_code,
            "message": "Email sent successfully",
        }
    except Exception as e:
        return {
            "success": False,
            "status_code": 0,
            "message": str(e),
        }


# ---------------------------------------------------------------------------
# Ram-Z branded email template
# ---------------------------------------------------------------------------
def _ramz_email_template(body_content, store_name=""):
    """Wrap body content in Ram-Z branded HTML email template."""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="margin:0; padding:0; background-color:#f5f5f5; font-family:Arial,Helvetica,sans-serif;">
        <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f5f5f5; padding:20px 0;">
            <tr>
                <td align="center">
                    <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border-radius:8px; overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,0.1);">
                        <!-- Header -->
                        <tr>
                            <td style="background-color:#2B3A4E; padding:24px 32px; text-align:center;">
                                <h1 style="margin:0; color:#C49A5C; font-size:28px; font-weight:bold; letter-spacing:1px;">
                                    RAM-Z
                                </h1>
                                <p style="margin:4px 0 0; color:#ffffff; font-size:12px; letter-spacing:2px;">
                                    RESTAURANT GROUP
                                </p>
                            </td>
                        </tr>
                        <!-- Store banner -->
                        {"" if not store_name else f'''
                        <tr>
                            <td style="background-color:#C49A5C; padding:12px 32px; text-align:center;">
                                <p style="margin:0; color:#ffffff; font-size:16px; font-weight:bold;">
                                    {store_name}
                                </p>
                            </td>
                        </tr>
                        '''}
                        <!-- Body -->
                        <tr>
                            <td style="padding:32px;">
                                {body_content}
                            </td>
                        </tr>
                        <!-- Footer -->
                        <tr>
                            <td style="background-color:#f8f8f8; padding:16px 32px; text-align:center; border-top:1px solid #eeeeee;">
                                <p style="margin:0; color:#999999; font-size:11px;">
                                    Ram-Z Restaurant Group | Confidential
                                </p>
                                <p style="margin:4px 0 0; color:#cccccc; font-size:10px;">
                                    This is an automated message. Please do not reply directly.
                                </p>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """


# ---------------------------------------------------------------------------
# GM Revenue Band Selection Email
# ---------------------------------------------------------------------------
def build_gm_selection_email(
    store_name,
    gm_name,
    week_label,
    submission_url,
    py_sales=None,
    prev_week_1_sales=None,
    prev_week_2_sales=None,
    avg_recent_sales=None,
    avg_sos=None,
    last_week_sos_rank=None,
    sos_total_stores=None,
    avg_negative_reviews=None,
    last_week_votg_rank=None,
    votg_total_stores=None,
):
    """
    Build the HTML email body for GM revenue band selection.

    Returns subject (str) and html_body (str).
    """
    # Format currency values
    def fmt_currency(val):
        if val is None:
            return "N/A"
        return f"${val:,.0f}"

    def fmt_number(val, decimals=1):
        if val is None:
            return "N/A"
        return f"{val:.{decimals}f}"

    subject = f"Action Required: Select Revenue Band for {store_name} — {week_label}"

    body = f"""
        <p style="color:#333333; font-size:16px; line-height:1.6;">
            Hi {gm_name or 'Team'},
        </p>
        <p style="color:#333333; font-size:15px; line-height:1.6;">
            Please select the <strong>revenue band</strong> for <strong>{store_name}</strong>
            for the upcoming week (<strong>{week_label}</strong>).
        </p>

        <!-- Data Summary Table -->
        <table width="100%" cellpadding="8" cellspacing="0" style="margin:20px 0; border-collapse:collapse;">
            <tr style="background-color:#2B3A4E;">
                <td colspan="2" style="color:#ffffff; font-size:14px; font-weight:bold; padding:10px 12px; border-radius:4px 4px 0 0;">
                    Store Performance Snapshot
                </td>
            </tr>
            <tr style="background-color:#f9f9f9;">
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#666; font-size:13px;">
                    Prior Year — Same Week Sales
                </td>
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#333; font-size:13px; font-weight:bold; text-align:right;">
                    {fmt_currency(py_sales)}
                </td>
            </tr>
            <tr>
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#666; font-size:13px;">
                    Last Week Sales
                </td>
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#333; font-size:13px; font-weight:bold; text-align:right;">
                    {fmt_currency(prev_week_1_sales)}
                </td>
            </tr>
            <tr style="background-color:#f9f9f9;">
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#666; font-size:13px;">
                    Two Weeks Ago Sales
                </td>
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#333; font-size:13px; font-weight:bold; text-align:right;">
                    {fmt_currency(prev_week_2_sales)}
                </td>
            </tr>
            <tr>
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#666; font-size:13px;">
                    Avg Sales (Last 2 Weeks)
                </td>
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#333; font-size:13px; font-weight:bold; text-align:right;">
                    {fmt_currency(avg_recent_sales)}
                </td>
            </tr>
            <tr style="background-color:#f9f9f9;">
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#666; font-size:13px;">
                    Avg Speed of Service (Last 4 Weeks)
                </td>
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#333; font-size:13px; font-weight:bold; text-align:right;">
                    {fmt_number(avg_sos / 60 if avg_sos is not None else None)} min
                </td>
            </tr>
            <tr>
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#666; font-size:13px;">
                    Speed of Service Rank (Last Week)
                </td>
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#333; font-size:13px; font-weight:bold; text-align:right;">
                    {f"{int(last_week_sos_rank)} of {int(sos_total_stores)}" if last_week_sos_rank is not None and sos_total_stores is not None else (fmt_number(last_week_sos_rank, 0) if last_week_sos_rank is not None else "N/A")}
                </td>
            </tr>
            <tr style="background-color:#f9f9f9;">
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#666; font-size:13px;">
                    Avg Negative Reviews (Last 4 Weeks)
                </td>
                <td style="padding:8px 12px; border-bottom:1px solid #eee; color:#333; font-size:13px; font-weight:bold; text-align:right;">
                    {fmt_number(avg_negative_reviews, 0) if avg_negative_reviews is not None else "N/A"}
                </td>
            </tr>
            <tr>
                <td style="padding:8px 12px; color:#666; font-size:13px;">
                    VOTG Rank (Last Week)
                </td>
                <td style="padding:8px 12px; color:#333; font-size:13px; font-weight:bold; text-align:right;">
                    {f"{int(last_week_votg_rank)} of {int(votg_total_stores)}" if last_week_votg_rank is not None and votg_total_stores is not None else (fmt_number(last_week_votg_rank, 0) if last_week_votg_rank is not None else "N/A")}
                </td>
            </tr>
        </table>

        <!-- CTA Button -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin:24px 0;">
            <tr>
                <td align="center">
                    <a href="{submission_url}"
                       style="display:inline-block; background-color:#C49A5C; color:#ffffff;
                              font-size:16px; font-weight:bold; padding:14px 40px;
                              text-decoration:none; border-radius:6px;
                              letter-spacing:0.5px;">
                        Select Revenue Band
                    </a>
                </td>
            </tr>
        </table>

        <p style="color:#999999; font-size:12px; text-align:center; margin-top:16px;">
            Please complete your selection before the deadline. If you have questions,
            contact your District Manager.
        </p>
    """

    html_body = _ramz_email_template(body, store_name)
    return subject, html_body


# ---------------------------------------------------------------------------
# Reminder email builder
# ---------------------------------------------------------------------------
def build_reminder_email(
    store_name,
    gm_name,
    week_label,
    submission_url,
    reminder_number,
    recipient_type="gm",
):
    """
    Build a reminder email. Escalates urgency with each reminder.

    reminder_number: 1 = morning, 2 = noon (DM cc), 3 = 5pm (DM + CEO cc)
    """
    urgency_labels = {
        1: "Reminder",
        2: "Second Reminder — Action Needed",
        3: "Final Notice — Immediate Action Required",
    }

    urgency = urgency_labels.get(reminder_number, "Reminder")

    if recipient_type == "dm":
        subject = f"{urgency}: Approve Revenue Band for {store_name} — {week_label}"
        action_text = "Please review and approve the revenue band selection."
        button_text = "Review & Approve"
    else:
        subject = f"{urgency}: Select Revenue Band for {store_name} — {week_label}"
        action_text = "You have not yet selected a revenue band for your store."
        button_text = "Select Revenue Band Now"

    urgency_color = "#333333"
    if reminder_number == 2:
        urgency_color = "#E67E22"
    elif reminder_number >= 3:
        urgency_color = "#E74C3C"

    body = f"""
        <p style="color:{urgency_color}; font-size:18px; font-weight:bold; margin-bottom:8px;">
            {urgency}
        </p>
        <p style="color:#333333; font-size:15px; line-height:1.6;">
            Hi {gm_name or 'Team'},
        </p>
        <p style="color:#333333; font-size:15px; line-height:1.6;">
            {action_text} The week of <strong>{week_label}</strong> for
            <strong>{store_name}</strong> is still pending.
        </p>

        {"<p style='color:#E74C3C; font-size:14px; font-weight:bold;'>This is your final notice. Failure to respond will be logged.</p>" if reminder_number >= 3 else ""}

        <table width="100%" cellpadding="0" cellspacing="0" style="margin:24px 0;">
            <tr>
                <td align="center">
                    <a href="{submission_url}"
                       style="display:inline-block; background-color:{'#E74C3C' if reminder_number >= 3 else '#C49A5C'};
                              color:#ffffff; font-size:16px; font-weight:bold;
                              padding:14px 40px; text-decoration:none; border-radius:6px;">
                        {button_text}
                    </a>
                </td>
            </tr>
        </table>
    """

    html_body = _ramz_email_template(body, store_name)
    return subject, html_body


# ---------------------------------------------------------------------------
# DM Approval Email
# ---------------------------------------------------------------------------
def build_dm_approval_email(
    store_name,
    gm_name,
    dm_name,
    week_label,
    selected_band,
    approval_url,
):
    """Build email notifying DM that a GM has submitted their band selection."""

    subject = f"Approval Needed: {store_name} selected {selected_band} — {week_label}"

    body = f"""
        <p style="color:#333333; font-size:16px; line-height:1.6;">
            Hi {dm_name or 'DM'},
        </p>
        <p style="color:#333333; font-size:15px; line-height:1.6;">
            <strong>{gm_name or 'The GM'}</strong> at <strong>{store_name}</strong>
            has submitted their revenue band selection for <strong>{week_label}</strong>.
        </p>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin:20px 0; border-collapse:collapse;">
            <tr style="background-color:#2B3A4E;">
                <td colspan="2" style="color:#ffffff; font-size:14px; font-weight:bold; padding:10px 12px; border-radius:4px 4px 0 0;">
                    Submission Details
                </td>
            </tr>
            <tr style="background-color:#f9f9f9;">
                <td style="padding:10px 12px; border-bottom:1px solid #eee; color:#666;">Store</td>
                <td style="padding:10px 12px; border-bottom:1px solid #eee; color:#333; font-weight:bold; text-align:right;">
                    {store_name}
                </td>
            </tr>
            <tr>
                <td style="padding:10px 12px; border-bottom:1px solid #eee; color:#666;">Selected Revenue Band</td>
                <td style="padding:10px 12px; border-bottom:1px solid #eee; color:#333; font-weight:bold; text-align:right;">
                    {selected_band}
                </td>
            </tr>
            <tr style="background-color:#f9f9f9;">
                <td style="padding:10px 12px; color:#666;">Week</td>
                <td style="padding:10px 12px; color:#333; font-weight:bold; text-align:right;">
                    {week_label}
                </td>
            </tr>
        </table>

        <table width="100%" cellpadding="0" cellspacing="0" style="margin:24px 0;">
            <tr>
                <td align="center">
                    <a href="{approval_url}"
                       style="display:inline-block; background-color:#27AE60; color:#ffffff;
                              font-size:16px; font-weight:bold; padding:14px 40px;
                              text-decoration:none; border-radius:6px;">
                        Review & Approve
                    </a>
                </td>
            </tr>
        </table>
    """

    html_body = _ramz_email_template(body, store_name)
    return subject, html_body


# ---------------------------------------------------------------------------
# Test function (run from command line)
# ---------------------------------------------------------------------------
def send_test_email(to_email):
    """Send a branded test email to verify SendGrid setup."""
    subject, html_body = build_gm_selection_email(
        store_name="Stillwater (OK)",
        gm_name="Test GM",
        week_label="Thu 4/2 - Wed 4/8",
        submission_url="https://example.com/select?token=test123",
        py_sales=35000,
        prev_week_1_sales=37500,
        prev_week_2_sales=36200,
        avg_recent_sales=36850,
        avg_sos=185.3,
        last_week_sos_rank=125,
        sos_total_stores=532,
        avg_negative_reviews=12,
        last_week_votg_rank=486,
        votg_total_stores=532,
    )

    result = send_email(to_email, subject, html_body)
    return result


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python email_service.py <recipient_email>")
        print("  Set SENDGRID_API_KEY and SENDGRID_FROM_EMAIL env vars first.")
        sys.exit(1)

    to = sys.argv[1]
    print(f"Sending test email to {to}...")
    result = send_test_email(to)
    print(f"Result: {result}")
