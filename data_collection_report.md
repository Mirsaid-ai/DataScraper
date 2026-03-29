# Data Collection Report — Ukraine RDD/DID Study (econ590)

**Project:** Economic impacts of the 2022 Russian occupation in Sumy, Chernihiv, and Kharkiv oblasts
**Panel target:** Oblast × month, Jan 2021–Dec 2024 (3 oblasts × 48 months = 144 rows)
**Date:** March 2026 (updated 28 March 2026)

---

## 1. ACLED — Armed Conflict Location & Event Data

**What we wanted:**
Conflict event counts, fatalities, and shelling events by raion and month for Sumy, Chernihiv, and Kharkiv oblasts, 2021–2024. This was the primary source for the RDD treatment-intensity variable.

**What we got:**
Complete data — two ACLED bulk export files were supplied directly, bypassing the API entirely:

- `ukraine_full_data_up_to-2026-02-27.xlsx` — full Ukraine conflict event log, 268,006 rows, covering 2020-01-01 to 2026-02-27
- `Ukraine_Infrastructure_Tags_2026-03-04.xlsx` — subset of events tagged by infrastructure type damaged, 12,888 rows, covering 2022-01-12 to 2026-02-27

For the three target oblasts (Kharkiv, Sumy, Chernihiv), 2021–2024:
- **42,190 event rows** across 17 raions (Kharkiv: 7, Sumy: 5, Chernihiv: 5)
- **17,656 total fatalities** recorded
- Event type breakdown: Explosions/Remote violence (35,756), Battles (4,609), Strategic developments (1,375), Violence against civilians (289)
- Sub-event breakdown: Shelling/artillery/missile attack (25,805), Air/drone strike (9,544), Armed clash (4,430)

The infrastructure tags file adds a `TAGS_INFRASTRUCTURE` column categorising damage to: Residential, Energy, Education, Health. For target oblasts it contains 3,800 tagged events, enabling additional outcome variables (e.g. `energy_attacks`, `education_attacks` per raion-month).

**What happened / problems:**
The ACLED API (endpoint `https://api.acleddata.com/acled/read`) was blocked at HTTP 403 during initial collection due to an unactivated API key. This is now moot — the bulk export files provide richer data (all fields, including `TAGS` and `TAGS_INFRASTRUCTURE`) than the API would have returned.

The existing API scraper (`scrapers/01_acled.py`) is no longer needed for initial panel construction. A replacement script should read the xlsx files directly, aggregate to raion × month, and write the same clean CSV format consumed by `06_build_panel.py`.

**Status:** Data in hand. Next step: write `scrapers/01_acled_xlsx.py` to replace the API scraper, then re-run `python scrapers/06_build_panel.py` to populate `conflict_events`, `fatalities`, `shelling_events`, `battle_events`, and optionally `energy_attacks` / `education_attacks` / `health_attacks`.

---

## 2. Jooble (ua.jooble.org) — Job Posting Counts

**What we wanted:**
Monthly job posting counts for each of the three target oblasts, Jan 2021–Dec 2024.

**What we got:**
Nine national (all-Ukraine) vacancy count data points recovered via the Wayback Machine — **not oblast-level**.  No oblast-specific Jooble pages are archived in the Wayback Machine.

| Snapshot date | Ukraine-wide active vacancies | Source |
|---|---|---|
| 2021-05-16 | 98,056 | Wayback HTML (`вакансій` text) |
| 2022-04-01 | 47,390 | Wayback HTML (`вакансій` text) |
| 2022-05-30 | 45,121 | Wayback HTML (`вакансія` text) |
| 2023-03-16 | 35,099 | Wayback HTML (`вакансій` text) |
| 2023-09-06 | 42,900 | Wayback JS state (`activeJobsCount`) |
| 2024-02-14 | 34,989 | Wayback HTML (`вакансій` text) |
| 2024-04-29 | 42,900 | Wayback JS state (`activeJobsCount`) |
| 2024-05-29 | 42,900 | Wayback JS state (`activeJobsCount`) |
| 2024-11-13 | 34,039 | Wayback HTML (`вакансій` text) |

The war signal is clear: 98,056 → 47,390 (−52%) between May 2021 and April 2022.  By 2023–2024 the national total had settled at roughly 35,000–43,000 — well below the pre-war peak but showing partial recovery.

**What happened / problems:**

1. **Jooble is server-side rendered (unlike work.ua / robota.ua)** — the HTML delivered to a plain HTTP client already contains the rendered job count.  This means Wayback Machine snapshots are usable without a headless browser.

2. **No historical API with date filters** — Jooble's official REST API (`POST https://jooble.org/api/{key}`) returns currently active listings only.  Parameters: `keywords`, `location`, `radius`, `page`.  No `dateFrom`/`dateTo`.  A free API key can be obtained at `https://jooble.org/api/about`.

3. **Zero oblast-specific Wayback snapshots** — CDX searches for URLs containing Харків, Суми, Чернігів (Ukrainian) and for `ua.jooble.org/SearchResult?location=...` found no archived oblast-level pages in 2021–2024.

4. **HTML format changed across years** — three different count formats had to be detected:
   - 2021–2022: `"98 056 вакансій"` in visible page text (no-break space as thousands separator)
   - 2022 (May): `"45 121 вакансія"` — grammatical singular form for numbers ending in 1
   - 2023–2024: `"activeJobsCount":42900` embedded in a `<script>` state JSON block

5. **Wayback Machine rate-limiting** — sequential Wayback fetches trigger `Connection refused` after ~5–6 requests per session.  Running the scraper twice (with a short pause) picks up all 9 snapshots across two sessions.

**Conclusion:** The 9 national data points are saved at `data/clean/jooble/jooble_oblast_snapshot.csv`.  They are suitable for use as a **national labor-market trend control variable** (analogous to the EDR national registrations), but cannot substitute for oblast-level monthly panel data.  For ongoing monitoring, the scraper's `--mode api` with a Jooble API key will append one current snapshot row per oblast per run.

---

## 3. Work.ua — Job Posting Counts

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

## 7. Fulldata_City_Region_Monthly — Multi-Indicator Panel

**File:** `possibleSets/Fulldata_City_Region_Monthly_Nov052025.csv`
**What it is:** 138,383-row long-format panel covering all Ukrainian oblasts, 2014–2025, with 8 columns: `City`, `Date`, `Y`, `M`, `Indicator`, `Value`, `Source`, `Region`. Contains ~170 distinct indicators across real-estate, labor market, prices, and ACLED conflict event counts.

**Assessment for the three target oblasts (Kharkivska, Sumska, Chernihivska), 2021–2024:**

**✅ Fully usable — Primary Market housing prices (`Primary Market: ave_prim_buy, usd/m2`):** Complete 48/48 months for all three oblasts. Clean new outcome variable not previously available in the project.

**⚠️ Partially usable — Job Salary & Resume Salary (30 sectors, UAH and USD):** Post-war data only — Sumy and Chernihiv start 2022-07 (18 months missing), Kharkiv starts 2022-09 (20 months missing). The entire pre-invasion baseline (Jan 2021–Jan 2022) is absent, which prevents using these as DID outcome variables without imputation. Usable for post-invasion descriptives or event-study plots only.

**⚠️ Partial — Secondary Market prices (rent + sale, 1/2/3-room):** Starts 2023-05 only; 28 of 48 months missing per oblast. No pre-war data.

**❌ Redundant / inferior — ACLED conflict variables (`event_type_*`, `sub_event_type_*`, `disorder_type_*`):** Cover only 2022-07 to 2024-12, missing the 2021 baseline and the critical Feb–Jun 2022 invasion onset. Already superseded by the full ACLED bulk xlsx (42,190 rows, starts 2021).

**❌ Unusable for cross-oblast — Price Level (food, fuel):** Present only for Sumska; Kharkiv and Chernihiv have no price-level rows.

**Conclusion:** The only indicator that fully covers the 48-month panel for all three oblasts is primary market housing prices. This will be extracted and added as a new outcome variable (`prim_price_usd_m2`). The salary series are noted as supplementary post-war data. Everything else is either already covered by better sources or too incomplete to use.

---

## 8. LUN.ua — Real Estate Prices (Primary Market, Rental)

**What we wanted:**
Monthly primary-market housing prices and rental prices for Kharkiv, Sumy, and Chernihiv, Jan 2021–Dec 2024, to serve as economic outcome variables in the DID/RDD panel.

**What we got:**

| Dataset | Period | Oblasts | Variables | Rows |
|---------|--------|---------|-----------|------|
| Primary market by housing class | Jan 2021–Dec 2024 | Kharkiv, Sumy, Chernihiv | avg price UAH/m² and USD/m² by class (economy/comfort/business/premium) + overall average | 720 |
| Flat price history by room count | May 2023–Dec 2024 | Kharkiv, Sumy, Chernihiv | median total price and price/m² in UAH and USD for 1/2/3-bedroom flats | 297 |
| Affordability ratio | May 2023–Dec 2024 | Kharkiv, Sumy, Chernihiv | years of rental needed to buy a flat (by room count) | 180 |

**Key finding:** The primary-market data goes back to January 2021, providing a complete 48-month panel (47/48 months non-zero; May 2022 = 0 listings for Kharkiv due to total market halt during the early invasion). The war signal is very clear:

| Kharkiv primary market | USD/m² | Active listings |
|------------------------|--------|-----------------|
| Jan 2022 (pre-invasion) | $950 | 70 |
| Feb 2022 (invasion month) | $970 | 74 |
| May 2022 (heaviest fighting) | $0 | 0 |
| Jun 2022 | $630 (−35%) | 11 |
| Dec 2022 | $550 (−43%) | 19 |
| Dec 2024 | ~$460 | ~30 |

**What happened / problems:**

1. **Direct API discovery:** The LUN statistics site (`lun.ua/stat/`) exposes an undocumented but publicly accessible JSON API at `lun.ua/stat/api/data/`. The API was discovered by inspecting network requests in the browser. No authentication is required.

2. **City IDs (from network inspection):** Kharkiv = 120, Sumy = 118, Chernihiv = 125. These refer to the primary city in each oblast (not the whole oblast).

3. **API endpoints used:**
   - `GET /price?cityId={id}` → primary-market average and class breakdown, **monthly from Jan 2021**
   - `GET /flat-price-history?cityId={id}&contractTypeId=1` → primary market by room count (1/2/3-bed), from May 2023
   - `GET /flat-price-history?cityId={id}&contractTypeId=2` → **rental** prices per month by room count, from May 2023
   - `GET /sale-in-rent-history?cityId={id}` → affordability ratio (years of rent to buy), from May 2023

4. **contractTypeId semantics** (reverse-engineered from data values):
   - `contractTypeId=1` = primary market (new construction) sale prices (~$650–$970/m² for Kharkiv)
   - `contractTypeId=2` = **rental** prices per month (~4,000 UAH/month for 1-bed in Kharkiv)
   - There is no time-series endpoint for secondary (resale) market sale prices — only a current-snapshot district breakdown (`/districts?cityId=xxx`)

5. **Wayback Machine:** The `lun.ua/stat/` sub-site launched in May 2023 and has never been archived in the Wayback Machine. The scraper includes `--wayback` flag for future use but returns no data currently.

6. **Firecrawl:** Supported via `--firecrawl-key FC_KEY` flag. Because the stat pages are Remix (React) SPAs, Firecrawl renders them and extracts visible price text from the markdown. This is a supplementary method — the direct API is more complete.

7. **Secondary market gap:** No historical time-series for secondary (resale) sale prices exists in the LUN API. The `Fulldata_City_Region_Monthly` dataset has secondary market prices from May 2023 only, confirming this as a structural data gap.

**Status:** Complete. Three CSV files in `data/clean/lun_ua/`:
- `lun_primary_class_month.csv` — **primary outcome variable** `prim_price_usd_m2` by class, full 47/48 months × 3 oblasts, ready for panel merge
- `lun_flat_price_history.csv` — room-count breakdown, 2023-05 to 2024-12
- `lun_affordability_month.csv` — affordability ratio supplement

**Next step:** Merge `lun_primary_class_month.csv` into the panel via `06_build_panel.py`, adding `prim_price_usd_m2_avg` (all-class) as a key outcome variable. This supersedes the `prim_price_usd_m2` column planned from the Fulldata dataset (same data, direct source, full pre-war baseline).

### Technical notes

#### LUN.ua page structure
```
https://lun.ua/stat/primary/{slug}   ← primary market stats
https://lun.ua/stat/sale/{slug}       ← secondary market stats (current only)
https://lun.ua/stat/rent/{slug}       ← rental stats
```

#### LUN.ua stat API base: `https://lun.ua/stat/api/data/`

| Endpoint | contractTypeId | Data |
|----------|---------------|------|
| `/price?cityId=X` | — | Primary market by class + overall avg, monthly from 2021-01 |
| `/flat-price-history?cityId=X&contractTypeId=1` | 1=primary sale | Price by room count, from 2023-05 |
| `/flat-price-history?cityId=X&contractTypeId=2` | 2=rental | Rental price/month by room count, from 2023-05 |
| `/sale-in-rent-history?cityId=X` | — | Years-of-rent-to-buy ratio, from 2023-05 |

---

## Summary Table

| Source | Goal | Outcome | In Panel? |
|--------|------|---------|-----------|
| ACLED (bulk xlsx) | Conflict events/fatalities by raion-month | **Complete** — 42,190 rows, 2021–2024, 3 oblasts | Pending processing |
| ACLED Infrastructure Tags (xlsx) | Infrastructure damage type by raion-month | **Complete** — 3,800 rows, 2022–2026, 3 oblasts | Pending processing |
| Jooble (Wayback) | Job postings by oblast-month (historical) | **9 national data points** (2021–2024); no oblast split | Yes (national control) |
| Work.ua | Job postings by oblast-month (historical) | Current snapshot only (SPA) | No |
| Robota.ua | Job postings by oblast-month (historical) | Current snapshot only (SPA) | No |
| OpenDataBot | Business registrations by oblast-month | Failed (Nuxt.js SPA, fake data) | No |
| EDR / data.gov.ua | Business registrations by oblast-month | National totals only (no ADDRESS field) | Yes (national control) |
| HDX / IOM DTM | IDP displacement by oblast-month | Complete, Feb 2022–Jan 2026 | Yes |
| Fulldata_City_Region_Monthly (possibleSets) | Multi-indicator panel | Primary housing prices: **complete 48 months, 3 oblasts**; salaries: post-2022 only; conflict counts: inferior to ACLED xlsx | Partial — `prim_price_usd_m2` pending extraction |
| LUN.ua stat API | Primary market prices by class, rental prices | **Complete** — 47/48 months × 3 oblasts from Jan 2021; rental + room-count breakdown from May 2023 | `prim_price_usd_m2_avg` pending panel merge |

---

## Methodology: How the Data Was Scraped

### General principles
- Raw files are **never overwritten**; each download is timestamped.
- A 2-second delay (`DELAY_SECONDS = 2`) is observed between HTTP requests to avoid server overload.
- All scripts log failures and print row-count summaries on completion.
- Each scraper can be run in `--test` / `--peek` mode for dry-run validation before saving.

### ACLED (`scrapers/01_acled_xlsx.py` — replaces `01_acled.py`)
The API-based scraper (`01_acled.py`) is superseded by a direct xlsx reader. The replacement script reads `ukraine_full_data_up_to-2026-02-27.xlsx` and `Ukraine_Infrastructure_Tags_2026-03-04.xlsx` from the project root. It filters to `ADMIN1 ∈ {Kharkiv, Sumy, Chernihiv}` and `YEAR ∈ [2021, 2024]`, then aggregates to raion × month with the following variables:
- `conflict_events` — total event count (all event types)
- `fatalities` — sum of the `FATALITIES` field
- `shelling_events` — count where `SUB_EVENT_TYPE == "Shelling/artillery/missile attack"`
- `battle_events` — count where `EVENT_TYPE == "Battles"`
- `civilian_violence_events` — count where `EVENT_TYPE == "Violence against civilians"`
- `energy_attacks`, `education_attacks`, `health_attacks` — counts from the infrastructure tags file where `TAGS_INFRASTRUCTURE` contains the respective keyword

Raw xlsx files are treated as immutable inputs (not re-downloaded). Clean aggregate is written to `data/clean/acled/acled_raion_month.csv` in the same schema expected by `06_build_panel.py`.

### Jooble (`scrapers/02_jooble.py`)
Two modes are available:

**Wayback Machine mode (`--mode wayback`):** Queries the Wayback Machine CDX API for monthly-collapsed snapshots of `ua.jooble.org/SearchResult` between 2021 and 2024.  For each snapshot found, fetches the archived HTML using `urllib.request` (not `requests` — the plain `urllib` default headers avoid Wayback Machine's content-negotiation quirks) and extracts the Ukraine-wide vacancy count using three patterns, tried in order:
1. `(\d[\d\s\u00a0\xa0]*)\s*вакансі[яій]` — matches the count in visible page text across all grammatical forms of the Ukrainian word for "vacancy"
2. `"activeJobsCount":\s*(\d+)` — matches the JSON field added to script-embedded state in 2023+
3. `"jobsCount":\s*(\d+)` — fallback for intermediate page formats

Retries the CDX query up to 4 times with exponential backoff on timeout.  Results are deduplicated on `snapshot_date + region_raw + mode` before writing to `data/clean/jooble/jooble_oblast_snapshot.csv`.

**API mode (`--mode api --key YOUR_KEY`):** POSTs to `https://jooble.org/api/{key}` with `{"keywords": "", "location": "Харківська область", "page": "1"}` for each of the three target oblasts; records `totalCount` from the response.  Designed as an ongoing forward-looking snapshot monitor.  Key obtainable free at `https://jooble.org/api/about`.

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
