"""
Shared constants for the Ram-Z labor forecasting engine.
Import from here instead of duplicating across files.
"""
from datetime import date

# Holiday calendar — used by forecasting, scenario engine, and feature engineering
# Update yearly: add next year's Thanksgiving & Christmas dates
HOLIDAY_CALENDAR = {
    # Closed days
    date(2024, 11, 28): 'thanksgiving', date(2025, 11, 27): 'thanksgiving',
    date(2026, 11, 26): 'thanksgiving',
    date(2023, 12, 25): 'christmas',   date(2024, 12, 25): 'christmas',
    date(2025, 12, 25): 'christmas',   date(2026, 12, 25): 'christmas',
    # Lower traffic
    date(2023, 12, 24): 'christmas_eve', date(2024, 12, 24): 'christmas_eve',
    date(2025, 12, 24): 'christmas_eve', date(2026, 12, 24): 'christmas_eve',
    date(2023, 11, 22): 'day_before_thanksgiving',
    date(2024, 11, 27): 'day_before_thanksgiving',
    date(2025, 11, 26): 'day_before_thanksgiving',
    date(2026, 11, 25): 'day_before_thanksgiving',
}

# State abbreviation by location_id
STATE_MAP = {
    '112-0001': 'OK', '112-0002': 'OK', '112-0003': 'OK', '112-0004': 'OK',
    '112-0005': 'OK', '112-0031': 'OK', '112-0035': 'OK', '112-0038': 'OK',
    '112-0006': 'TX', '112-0007': 'TX', '112-0008': 'TX', '112-0009': 'TX',
    '112-0010': 'TX', '112-0011': 'TX', '112-0032': 'TX', '112-0033': 'TX',
    '112-0034': 'TX', '112-0036': 'TX', '112-0037': 'TX',
    '112-0012': 'OH', '112-0013': 'OH', '112-0014': 'OH', '112-0015': 'OH',
    '112-0016': 'OH', '112-0017': 'OH', '112-0018': 'OH', '112-0019': 'OH',
    '112-0020': 'OH', '112-0021': 'OH', '112-0022': 'OH', '112-0023': 'OH',
    '112-0024': 'OH', '112-0025': 'OH', '112-0026': 'OH', '112-0028': 'OH',
    '112-0029': 'OH', '112-0030': 'OH',
    '112-0027': 'KY',
}
