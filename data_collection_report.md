# Data Collection Report — Ukraine RDD/DID Study (econ590)

**Project:** Economic impacts of the 2022 Russian occupation in Sumy, Chernihiv, and Kharkiv oblasts
**Panel target:** Oblast × month, Jan 2021–Dec 2024 (3 oblasts × 48 months = 144 rows)
**Date:** March 2026

---

## 1. ACLED — Armed Conflict Location & Event Data

**What we wanted:**
Conflict event counts, fatalities, and shelling events by raion and month for Sumy, Chernihiv, and Kharkiv oblasts, 2021–2024. This was the primary source for the RDD treatment-intensity variable.

**What we got:**
Nothing yet — columns `conflict_events`, `fatalities`, `shelling_events` are stubbed as `NaN` in the panel.

**What happened / problems:**
ACLED requires account registration and API key activation. The account was registered but the API key had not been activated at the time of data collection. An attempt to query the API endpoint (`https://api.acleddata.com/acled/read`) returned HTTP 403 (Forbidden). The scraper (`01_acled.py`) is complete and handles pagination (ACLED returns max 500 rows per call), but cannot run until the API key is activated.

**Status:** Pending — user will supply API key. Once provided, run `python scrapers/01_acled.py` and then re-run `python scrapers/06_build_panel.py` to populate the stub columns.

---

## 2. Work.ua — Job Posting Counts

**What we wanted:**
Monthly job posting counts for each of the three target oblasts, Jan 2021–Dec 2024, to use as a labor-demand proxy in the DID specification.

**What we got:**
Current-snapshot counts only (no historical time series).
- Kharkiv: extracted via `/jobs-kharkiv/` URL pattern
- Sumy and Chernihiv: similar snapshots

**What happened / problems:**
Work.ua is a Single Page Application (React/Next.js). Date-filtered search URLs (e.g., `?period=month&date=2022-03`) return a ~4 KB HTML shell with no rendered job counts — all data loads dynamically via JavaScript after page load. The site does not expose a public API for historical counts.

The scraper (`02_work_ua.py`) was adapted to extract the current total vacancy count per oblast (using the regex pattern `"Зараз у нас N вакансій"` from the page). This gives a real-time snapshot but cannot reconstruct the 2021–2024 backfill needed for the panel.

**Conclusion:** Work.ua data is stored as a snapshot monitor (`data/clean/work_ua/work_ua_region_month.csv`). It is **not** merged into the main panel. For historical labor statistics, the recommended alternative is official Ukrstat/DSZ data.

---

## 3. Robota.ua — Job Posting Counts

**What we wanted:**
Same as Work.ua: monthly job posting counts by oblast, 2021–2024.

**What we got:**
Current-snapshot counts by oblast (one row per oblast per scraper run).
- Kharkiv (Харківська): 5,577 active vacancies
- Sumy (Сумська): 1,711 active vacancies
- Chernihiv (Чернігівська): 1,843 active vacancies
*(as of 2026-03-21)*

**What happened / problems:**
Two approaches failed before finding a working solution:

1. **HTML scraping of search pages:** Robota.ua is an Angular SPA. GET requests to job search URLs (e.g., `/zapros/vacancy/{oblast-slug}`) returned a 4 KB shell — no rendered content, no job counts.

2. **REST API (`api.robota.ua/vacancy/search`):** A POST request with `regionId` in the JSON body (region IDs 19, 21, 25 for the target oblasts) returned the same national total (~103,450 vacancies) for every region ID tested, indicating the server-side filter was not applied.

3. **City dictionary API (working solution):** `GET api.robota.ua/dictionary/city` returns a JSON array of all cities, each containing a `vacancyCount` and `centerId` (the oblast/region identifier). Summing `vacancyCount` by `centerId` for the three target oblasts yields accurate regional vacancy totals.

Like Work.ua, Robota.ua provides no historical archive — only currently active listings. The scraper is designed as an ongoing monitor that appends one snapshot row per oblast per run.

**Conclusion:** Stored as snapshot monitor (`data/clean/robota_ua/robota_ua_region_month.csv`). Not merged into the main panel.

---

## 4. OpenDataBot — Business Registrations (Abandoned)

**What we wanted:**
Monthly counts of new company (UO) and FOP (sole proprietor) registrations by oblast, 2021–2024.

**What we got:**
Nothing — data extraction failed entirely.

**What happened / problems:**
OpenDataBot's analytics pages (e.g., `opendata.com.ua/analytics`) are rendered by a Nuxt.js SPA. Server-rendered HTML contains placeholder text and stale fallback data rather than actual statistics. During testing, the scraped "year" values parsed as registration counts (e.g., returning `2026` as a count instead of the true number), confirming the data was fake/placeholder content.

No public JSON API was found. The scraper (`04_opendatabot.py`) was abandoned.

**Replacement:** Direct EDR bulk download from `data.gov.ua` (see §5 below).

---

## 5. EDR / data.gov.ua — Business Registrations (National)

**What we wanted:**
Monthly new-registration counts for UO (legal entities) and FOP (sole proprietors) **by oblast**, 2021–2024, to measure business formation as an economic outcome.

**What we got:**
National monthly counts only — no oblast breakdown.
- 48-month panel (Jan 2021–Dec 2024), one row per month
- `new_companies` (UO) and `new_fops` (FOP)
- Clear war signal: March 2022 company registrations = 503 (−88% vs. February 2022's 4,095)

**What happened / problems:**
Ukraine's EDR bulk export is available on `data.gov.ua` as two large ZIP files of XML:
- `uo.zip` — all legal entities (~386 MB, ~1.99M records)
- `fop.zip` — all sole proprietors (~534 MB, ~6.87M records)

The original plan was to filter records by the ADDRESS field to isolate registrations in the three target oblasts. After downloading and inspecting the XML schema (`UO_schema.xsd`) and scanning 50,000+ records, it was confirmed that **the ADDRESS element is absent from the bulk export** — it was removed from the public dataset, likely for privacy reasons. The XML SUBJECT elements contain only: NAME, EDRPOU, RECORD, REGISTRATION, STAN (status).

Several fallback approaches were tried to obtain oblast-level filtering:
- **Registration number prefixes** (codes like 1480x = Kharkiv): Unreliable for Sumy; prefix mapping not authoritative.
- **CKAN/data.gov.ua search** for aggregated regional datasets: No relevant datasets found.
- **MinJust statistics portal**: HTTP 403 (blocked).
- **Ukrstat**: Frames-based site with broken encoding; no machine-readable API.
- **State Tax Service (opendata.tax.gov.ua)**: DNS failure — domain unreachable.

**Conclusion:** National totals are the best available from this source. Used as a national control variable / time fixed-effect proxy in the panel (same value broadcast to all 3 oblast rows per month). Stored at `data/clean/opendatabot/edr_national_month.csv`.

**Technical note on download:** The initial download attempt failed at ~8 MB due to a server-side `BrokenPipeError` (connection dropped mid-transfer). Resumable download logic was added using HTTP `Range` headers with 10 retry attempts and 5-second delays between attempts.

---

## 6. HDX / IOM DTM — IDP Displacement Data

**What we wanted:**
Monthly internally displaced persons (IDP) counts by oblast, covering the post-invasion period, to measure population displacement as an outcome variable.

**What we got:**
Complete data — 135 rows covering all three oblasts from February 2022 through January 2026.
Schema: `oblast | month | idp_present` (cumulative persons present)

**What happened / problems:**
No significant problems. The IOM Displacement Tracking Matrix (DTM) publishes structured data on the Humanitarian Data Exchange (HDX) platform via a CKAN API. The data was downloaded programmatically using the HDX CKAN API (`data.humdata.org/api/3/`), parsing the dataset resource list to find the correct CSV file.

Minor issue: HDX uses oblast names in the "-ska" suffix form (e.g., "Kharkivska", "Sumska", "Chernihivska"), while the panel uses short English names ("Kharkiv", "Sumy", "Chernihiv"). This was resolved with an explicit name-mapping dictionary (`HDX_TO_EN`) in the panel builder.

Pre-invasion months (Jan 2021–Jan 2022) have no IDP records; these are filled with `0` in the panel.

**Conclusion:** Successfully merged into the panel as `idp_present`.

---

## Summary Table

| Source | Goal | Outcome | In Panel? |
|--------|------|---------|-----------|
| ACLED | Conflict events/fatalities by raion-month | Pending (API key blocked) | Stub (NaN) |
| Work.ua | Job postings by oblast-month (historical) | Current snapshot only (SPA) | No |
| Robota.ua | Job postings by oblast-month (historical) | Current snapshot only (SPA) | No |
| OpenDataBot | Business registrations by oblast-month | Failed (Nuxt.js SPA, fake data) | No |
| EDR / data.gov.ua | Business registrations by oblast-month | National totals only (no ADDRESS field) | Yes (national control) |
| HDX / IOM DTM | IDP displacement by oblast-month | Complete, Feb 2022–Jan 2026 | Yes |

---

## Methodology: How the Data Was Scraped

### General principles
- Raw files are **never overwritten**; each download is timestamped.
- A 2-second delay (`DELAY_SECONDS = 2`) is observed between HTTP requests to avoid server overload.
- All scripts log failures and print row-count summaries on completion.
- Each scraper can be run in `--test` / `--peek` mode for dry-run validation before saving.

### ACLED (`scrapers/01_acled.py`)
The ACLED API (`https://api.acleddata.com/acled/read`) requires a registered email and API key passed as query parameters. The script reads credentials from a `.env` file. Requests are made with `country=Ukraine`, `admin1` filtered to the three target oblasts, and a date range of 2021-01-01 to 2024-12-31. Since the API returns a maximum of 500 rows per call, the script paginates using the `page` parameter until an empty result is returned. Raw CSV is saved to `data/raw/acled/` with a datestamp; a clean aggregate at raion-month resolution is written to `data/clean/acled/acled_raion_month.csv`.

### Work.ua (`scrapers/02_work_ua.py`)
The scraper sends a GET request to `https://www.work.ua/jobs-{city_slug}/` for each target oblast. Because the site is a React SPA that does not server-render job counts, the script extracts the vacancy count from the raw HTML using the regex `r"Зараз у нас (\d[\d\s]*) вакансій"`. Results are appended to `data/clean/work_ua/work_ua_region_month.csv` as a snapshot row (deduplicated on `snapshot_date + region`).

### Robota.ua (`scrapers/03_robota_ua.py`)
A single GET request is made to `https://api.robota.ua/dictionary/city`, which returns a JSON array of all cities with `vacancyCount` and `centerId` fields. The script sums `vacancyCount` for all cities belonging to `centerId` values 19 (Суми), 21 (Харків), and 25 (Чернігів). Results are appended to `data/clean/robota_ua/robota_ua_region_month.csv` as a snapshot row.

### EDR bulk export (`scrapers/04_edr.py`)
Two ZIP files are downloaded from `data.gov.ua` using the CKAN resource API: `uo.zip` (legal entities) and `fop.zip` (sole proprietors). Downloads use `stream=True` with HTTP `Range` headers for resumability (up to 10 retry attempts). Each ZIP contains a single large XML file. Records are extracted using `xml.etree.ElementTree.iterparse` in streaming mode to avoid loading multi-gigabyte files into memory. For each SUBJECT element, the registration date is parsed from the `REGISTRATION` field (format: `"DD.MM.YYYY; DD.MM.YYYY; <reg_number>"`), and records are counted by month. The result is a 48-row national monthly panel saved to `data/clean/opendatabot/edr_national_month.csv`.

### HDX / IOM DTM (`scrapers/05_hdx_displacement.py`)
The script queries the HDX CKAN API (`https://data.humdata.org/api/3/action/package_show`) to retrieve the IOM DTM Ukraine displacement dataset. It parses the resource list to find the relevant CSV file URL and downloads it directly. The raw CSV is saved to `data/raw/hdx_displacement/`; a cleaned version filtered to the three target oblasts is written to `data/clean/hdx_displacement/idp_oblast_month.csv`.

### Panel assembly (`scrapers/06_build_panel.py`)
The final panel is assembled in four steps:
1. **Spine:** A balanced grid of 3 oblasts × 48 months (Jan 2021–Dec 2024) is constructed using `pandas.period_range`.
2. **Treatment dummies:** `post_invasion = 1` if month ≥ 2022-02; `occupation_phase = 1` for Sumy/Chernihiv in Feb–Apr 2022 and for Kharkiv in Feb–Sep 2022 (reflecting the partial occupation/battle phase).
3. **Left-join merges:** IDP data (by oblast + month) and EDR national data (by month, broadcast to all 3 oblasts) are merged onto the spine. Pre-invasion IDP values are filled with 0.
4. **ACLED stubs:** `conflict_events`, `fatalities`, `shelling_events` are added as `NaN` columns pending API activation.

Output: `data/final/panel/panel_oblast_month.csv` (144 rows × 12 columns) and `data/final/panel/panel_metadata.txt`.
