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

# ── Jooble ────────────────────────────────────────────────────────────────────
# REST API: POST https://jooble.org/api/{api_key}
# Free key registration: https://jooble.org/api/about
JOOBLE_API_BASE = "https://jooble.org/api"

# Ukrainian oblast names as understood by the Jooble location parameter
JOOBLE_OBLAST_LOCATIONS = {
    "Харківська": "Харківська область",
    "Сумська":    "Сумська область",
    "Чернігівська": "Чернігівська область",
}

# Wayback Machine CDX endpoint
WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
# Known Jooble all-Ukraine page archived in Wayback Machine
JOOBLE_SEARCH_URL = "https://ua.jooble.org/SearchResult"

CLEAN_JOOBLE = CLEAN_DIR / "jooble"

# ── HDX / CKAN API ────────────────────────────────────────────────────────────
HDX_API_BASE = "https://data.humdata.org/api/3"
HDX_SEARCH_TERMS = [
    "IOM DTM Ukraine displacement",
    "UNHCR Ukraine displacement",
]

# ── LUN.ua Statistics ─────────────────────────────────────────────────────────
# Real-estate statistics at https://lun.ua/stat/
# City IDs discovered via network inspection of the stat sub-site.
LUN_STAT_API  = "https://lun.ua/stat/api/data"
LUN_STAT_BASE = "https://lun.ua/stat"

# cityId maps to the major city (not the whole oblast);
# the city is the primary data unit on lun.ua stat pages.
LUN_CITY_IDS = {
    "Харківська":   120,   # Kharkiv
    "Сумська":      118,   # Sumy
    "Чернігівська": 125,   # Chernihiv
}

LUN_CITY_SLUGS = {
    "Харківська":   "kharkiv",
    "Сумська":      "sumy",
    "Чернігівська": "chernihiv",
}

# contractTypeId values (from API reverse-engineering):
#   1 = primary market (new construction) sale prices
#   2 = rental prices (UAH/month per apartment)
LUN_CONTRACT_SALE   = 1
LUN_CONTRACT_RENTAL = 2

CLEAN_LUN = CLEAN_DIR / "lun_ua"
RAW_LUN   = RAW_DIR   / "lun_ua"
