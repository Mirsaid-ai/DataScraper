"""
Shared constants for Ukraine RDD research scrapers.
Unit: raion-month | Regions: Sumy, Chernihiv, Kharkiv | Period: Jan 2021–Dec 2024
"""

from pathlib import Path

# ── Target regions (Ukrainian names used on Ukrainian websites) ──────────────
REGIONS = ["Сумська", "Чернігівська", "Харківська"]

REGIONS_EN = {
    "Сумська": "Sumy",
    "Чернігівська": "Chernihiv",
    "Харківська": "Kharkiv",
}

# ── Date range ────────────────────────────────────────────────────────────────
DATE_START = "2021-01-01"
DATE_END   = "2024-12-31"

# ── Directory paths ───────────────────────────────────────────────────────────
ROOT_DIR  = Path(__file__).resolve().parent.parent   # econ590/
DATA_DIR  = ROOT_DIR / "data"

RAW_DIR   = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
FINAL_DIR = DATA_DIR / "final" / "panel"

RAW_ACLED        = RAW_DIR / "acled"
RAW_WORK_UA      = RAW_DIR / "work_ua"
RAW_ROBOTA_UA    = RAW_DIR / "robota_ua"
RAW_OPENDATABOT  = RAW_DIR / "opendatabot"
RAW_HDX          = RAW_DIR / "hdx_displacement"

CLEAN_ACLED       = CLEAN_DIR / "acled"
CLEAN_WORK_UA     = CLEAN_DIR / "work_ua"
CLEAN_ROBOTA_UA   = CLEAN_DIR / "robota_ua"
CLEAN_OPENDATABOT = CLEAN_DIR / "opendatabot"

# ── Polite scraping ───────────────────────────────────────────────────────────
DELAY_SECONDS = 2

# ── ACLED API ─────────────────────────────────────────────────────────────────
ACLED_API_URL   = "https://acleddata.com/api/acled/read"
ACLED_AUTH_URL  = "https://acleddata.com/oauth/token"
ACLED_PAGE_SIZE = 500          # API maximum rows per call
ACLED_FIELDS = [
    "event_id_cnty",
    "event_date",
    "event_type",
    "sub_event_type",
    "admin1",
    "admin2",
    "location",
    "latitude",
    "longitude",
    "fatalities",
]

# Map ACLED admin1 names (English) to our target oblasts
ACLED_OBLAST_NAMES = ["Sumy", "Chernihiv", "Kharkiv"]

# ── Work.ua region city IDs ───────────────────────────────────────────────────
# city_id used in work.ua URLs: /jobs-<city_id>/
# 2 = Харків, 7 = Суми, 10 = Чернігів  (oblast-level filters)
WORK_UA_REGION_IDS = {
    "Харківська": {"city_id": 2,  "slug": "kharkiv"},
    "Сумська":    {"city_id": 7,  "slug": "sumy"},
    "Чернігівська": {"city_id": 10, "slug": "chernihiv"},
}

# ── Robota.ua region slugs ────────────────────────────────────────────────────
ROBOTA_UA_REGION_SLUGS = {
    "Харківська":   "kharkivska-oblast",
    "Сумська":      "sumska-oblast",
    "Чернігівська": "chernihivska-oblast",
}

# ── HDX / CKAN API ────────────────────────────────────────────────────────────
HDX_API_BASE = "https://data.humdata.org/api/3"
HDX_SEARCH_TERMS = [
    "IOM DTM Ukraine displacement",
    "UNHCR Ukraine displacement",
]
