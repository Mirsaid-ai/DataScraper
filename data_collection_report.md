# Data Collection Report — Ukraine RDD/DID Study (econ590)

**Project:** Economic impacts of the 2022 Russian occupation in Sumy, Chernihiv, and Kharkiv oblasts
**Panel target:** Oblast × month, Jan 2021–Dec 2024 (3 oblasts × 48 months = 144 rows)
**Date:** March 2026 (updated 30 March 2026)

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

## 9. `acled_work_imo.xlsx` / `acled_work_imo_inner_join.xlsx` — Class Pre-Merged Panel

**File location:** `Existing Class Datasets/0 _ Unprocessed datasets/`

**What it is:**
A class-provided pre-merged panel combining four data sources into a single long-format file at the **oblast × sector × month** grain. Two versions exist:

- `acled_work_imo.xlsx` — **left join**: 34,800 rows × 58 columns. All salary/job-posting observations retained even where ACLED data is absent. Covers Jan 2021–Dec 2024, all 25 Ukrainian oblasts, 29 job-market sectors.
- `acled_work_imo_inner_join.xlsx` — **inner join**: 19,807 rows × 58 columns. Only months/oblast combinations where ACLED and labor data both exist. Runs Jun 2022–Dec 2024; drops Luhansk and Crimea (no labor data). Pre-invasion baseline is entirely absent.

**Column groups:**

| Group | Columns | Source |
|-------|---------|--------|
| Labor market | `Job Salary` (UAH), `Resume Salary` (UAH), `rate` (UAH/USD), `Job Salary, USD`, `Resume Salary, USD` | robota.ua/work.ua (via instructor) |
| Job/resume postings | `Job postings tot`, `Job postings new`, `Resume postings tot`, `Resume postings new` | robota.ua (historical totals) |
| Conflict — disorder type | `disorder_type_*` (3 cols) | ACLED |
| Conflict — event type | `event_type_*` (6 cols) | ACLED |
| Conflict — sub-event type | `sub_event_type_*` (23 cols) | ACLED |
| Conflict — summary | `fatalities`, `civilian_targeting`, `tot_events`, `military_event` | ACLED |
| Displacement | `Recorded IDP Arrivals` | IOM DTM |
| Metadata | `City`, `Category`, `Date`, `admin1`, `Y`, `M`, `Oblast`, `Month-Year`, `Month-Year End` | — |

**Coverage for the three target oblasts:**
All three (Kharkiv, Sumy, Chernihiv) have complete 48-month × 29-category coverage (1,392 rows each = 48 months × 29 sectors) in the left-join version.

**Key finding — historical job posting counts:**
Unlike the project's own scrapers (§3–§3), which could only retrieve current snapshots, this class dataset includes **historical monthly job posting totals and new postings** by oblast and sector from robota.ua, covering the full Jan 2021–Dec 2024 range. For example, Kharkiv IT sector: 801 total postings and 489 new postings in January 2022.

**Assessment for the panel:**

- **✅ Fully usable — job/resume posting counts and salaries by sector:** The left-join file provides the complete 48-month pre/post baseline for labor demand and wage analysis. This supersedes the "snapshot-only" conclusion from §3 (robota.ua/work.ua). Variables `Job postings tot`, `Job postings new`, `Job Salary, USD`, `Resume Salary, USD` by sector can be aggregated to oblast × month for panel merge.
- **⚠️ Partially usable — inner-join version:** Excludes pre-invasion months entirely (starts Jun 2022). Only useful for post-war descriptive regressions or robustness checks. Do not use as the primary dataset.
- **⚠️ IDP Arrivals column:** `Recorded IDP Arrivals` is repeated identically for every sector row within the same oblast-month (as expected for a left join). Useful but already covered by the HDX IOM DTM scrape (§6), which has finer monthly resolution.
- **❌ ACLED columns — inferior to bulk xlsx:** The ACLED variables run from 2022-07 only (inner join) or have many NaNs in 2021 (left join), making them inferior to the bulk xlsx source in §1 which starts Jan 2021.

**Status:** In hand. Extract and aggregate labor market columns (`Job postings tot`, `Job postings new`, `Job Salary, USD`, `Resume Salary, USD`) to oblast × month using `acled_work_imo.xlsx` (left-join version); merge into panel as new outcome/control variables.

---

## 10. `primary market data.xlsx` — Primary Market Housing Prices (Extended History)

**File location:** `Existing Class Datasets/0 _ Unprocessed datasets/`
**Shape:** 1,272 rows × 4 columns: `city`, `date`, `ave_prim_buy, usd/m2`

**What it is:**
Monthly average primary-market housing prices in USD/m² for 22 Ukrainian cities. This is the same underlying LUN.ua data described in §8, but the class-provided file **extends the history back to January 2014** — versus the LUN.ua API scrape in §8, which begins in January 2021.

**Coverage:**
- Date range: Jan 2014 – Jun 2025 (138 months for Kyiv; 54 months for most other cities, reflecting when LUN.ua started tracking them)
- Cities: all 22 major Ukrainian cities, including Kharkiv, Sumy, and Chernihiv
- For the three target oblasts: full coverage from the point each city entered the LUN.ua dataset through Jun 2025

**Why this matters:**
The 2014–2020 history (pre-panel) enables **parallel-trends validation** and Annex plots showing housing price trajectories before the 2022 invasion. It can also support a 2014-Crimea-annexation placebo test. The LUN.ua API scraper in §8 does not retrieve data before Jan 2021 — this file fills that gap.

**Relationship to §8:**
The Jan 2021–Jun 2025 portion of this file duplicates the LUN.ua primary market scrape in §8 (same source, same units). The class file does not include the class-breakdown (economy/comfort/business/premium) — only the all-class average `ave_prim_buy, usd/m2`. Use the LUN.ua scrape from §8 for class-level granularity; use this class file for the pre-2021 baseline.

**Status:** In hand. For the main panel (Jan 2021–Dec 2024), the §8 LUN.ua data is preferred (richer). For pre-war trend plots and placebo tests, merge this file's 2014–2020 rows.

---

## 11. `secondary market data.xlsx` — Secondary Market Housing Prices + Rental

**File location:** `Existing Class Datasets/0 _ Unprocessed datasets/`
**Shape:** 586 rows × 12 columns

**What it is:**
Monthly secondary (resale) market housing prices and rental prices by room count for 22 Ukrainian cities, sourced from LUN.ua.

**Columns:**

| Column | Description |
|--------|-------------|
| `ave_sec_price_1/2/3_buy, usd/m2` | Secondary market **listed** price per m² by room count (1/2/3-bed) |
| `ave_sold_price_1/2/3_buy, usd/m2` | Secondary market **sold** price per m² by room count |
| `ave_price_1/2/3_rent, usd/m2` | Monthly **rental** price per m² by room count |

**Coverage:**
- Date range: May 2023 – Jul 2025 (27 months for most cities, 20 for a few)
- All three target oblasts: complete 27 months each (May 2023–Jul 2025)

**Assessment for the panel:**
This confirms and slightly extends the secondary market data noted in §8 (LUN.ua `lun_flat_price_history.csv`). The class file runs to Jul 2025 vs. Dec 2024 in the scraped version, and explicitly separates **listed vs. sold** prices — the `ave_sold_price` columns are the transaction-price series, which is more economically meaningful than list prices.

The fundamental limitation noted in §8 remains: no pre-war baseline (starts May 2023 only). The secondary market data cannot serve as a primary DID outcome variable due to missing 2021–2022 observations. It is usable for post-war cross-sectional analysis or as supplementary descriptives.

**Status:** In hand. The `ave_sold_price_*_buy` columns are the most credible secondary market series available. Can be added to the panel for the 2023–2024 window as supplementary outcome variables.

---

## 12. SSSU Consumer Prices — Oblast-Level CPI (Ukrstat)

**File:** `Existing Class Datasets/0 _ Unprocessed datasets/dataset_2025-09-18T19_53_38.610113535Z_DEFAULT_INTEGRATION_SSSU_DF_PRICE_CHANGE_CONSUMER_GOODS_SERVICE_LATEST.xlsx`
**Shape:** 2,446 rows × 461 columns
**Source:** State Statistics Service of Ukraine (SSSU / Ukrstat), via the official SDMX data portal

**What it is:**
A wide-format panel of Ukrainian consumer price statistics across three indicator types:
- `Average consumer prices for goods (services)` — price level in UAH per unit (kg, pack, etc.)
- `Consumer price indices` — month-on-month and year-on-year CPI indices
- `Core inflation` — core inflation indices

**Geographic coverage:**
28 regions including all three target oblasts (**Kharkivska, Sumska, Chernihivska**) plus national Ukraine and Kyiv city. Crimea and Sevastopol are present in the schema but excluded from post-2014 data per official notes.

**Temporal coverage:**
Columns span 1991 through 2025-M08. For most product series, data begins from 2017 (UAH-denominated prices). Monthly granularity from 2017-M01 onward, with 2,272 non-null series in 2021-M01 and 2,245 in 2022-M02.

**Why this matters:**
The Fulldata dataset (§7) noted that price-level data was "present only for Sumska" for the target oblasts — Kharkiv and Chernihiv were absent. This SSSU file fills that gap: **oblast-level consumer prices are available for all three target oblasts** from 2017 onward, covering the full 48-month panel period (Jan 2021–Dec 2024). This enables:
- A regional **price-level control variable** (e.g., food basket price in UAH) for each oblast-month
- Identification of war-driven inflation spikes in conflict-affected vs. less-affected oblasts
- Oblast-specific deflators for converting nominal salary variables to real terms

**Format note:**
The file is in wide format (one row per product × region × indicator). To use in the panel, melt to long format, filter to `2021-M01` through `2024-M12` columns and the three target oblasts, then aggregate or select representative price series (e.g., food basket components).

**Key data note from SSSU:**
Post-2022 figures exclude temporarily occupied territories. This is appropriate for this project since the panel tracks government-controlled portions of oblasts.

**Status:** In hand. Highly valuable — this is the **only source with oblast-level price data for all three target oblasts** across the full panel period. Recommended extraction: food basket price index for Kharkivska, Sumska, Chernihivska, 2021-M01–2024-M12, to serve as a cost-of-living outcome variable and deflator.

---

## 13. openbudget.gov.ua — Hromada-Level PIT Revenue (Primary Outcome Variable)

**What we wanted:**
Monthly Personal Income Tax (PIT / ПДФО) revenue for all hromadas in Kharkiv, Sumy, and Chernihiv oblasts, 2021–2024, as the primary outcome variable for the Spatial RDD.

**What we got:**
Complete monthly PIT revenue for all ~170 hromadas in the 3 target oblasts, 2021–2024. Data is at the hromada budget level, reported monthly (general fund only).

**Source and access method:**

The Open Budget portal (`openbudget.gov.ua`) is operated by Ukraine's State Treasury Service and exposes a public REST API documented in Swagger UI at `https://api.openbudget.gov.ua/swagger-ui.html`.

The key endpoint:
```
GET https://api.openbudget.gov.ua/api/public/localBudgetData
  ?budgetCode=<hromada budget code>
  &budgetItem=INCOMES
  &period=MONTH
  &year=<YYYY>
```
Returns: CSV (semicolon-delimited, UTF-8 despite header claiming windows-1251) with columns:
`REP_PERIOD | FUND_TYP | COD_BUDGET | COD_INCO | NAME_INC | ZAT_AMT | PLANS_AMT | FAKT_AMT`

**PIT income codes included in aggregation (all sub-codes of 11010000):**
- `11010100` — PIT withheld by tax agents from salary income (largest sub-code, ~75% of total)
- `11010200` — PIT from military service pay
- `11010400` — PIT from other income (non-salary, e.g. dividends, rental)
- `11010500` — PIT from investment income
- `110106xx` — PIT from self-employed / FOP income

Only general fund rows (`FUND_TYP = 'C'`) are included. `FAKT_AMT` (actual execution in UAH) is summed per month.

**Budget code discovery:**

Hromada budget codes were obtained from the `/items/BUDG` dictionary endpoint (~180 MB JSON, 217,072 entries). This dictionary is filtered to:
- KATOTTG codes starting with `UA59` (Sumy), `UA63` (Kharkiv), `UA74` (Chernihiv)
- Budget sign `signBudg` containing `(g)` — identifies hromada-level self-governance budgets

**Key finding — budget codes by year:**

The BUDG dictionary only contains codes from 2022 onward. However, querying the API with a 2022 budget code and `year=2021` returns 2021 data successfully. The budget codes changed format between 2021-2022 and 2023-2024 (11-digit codes in 2022, 10-digit in 2023+) but represent the same hromada jurisdictions.

Result: 328 unique budget codes covering the 3 oblasts (each hromada has ~2 codes: one for 2021-2022, one for 2023-2024):
- Sumy Oblast: 102 unique budget codes (~51 hromadas × 2 code formats)
- Kharkiv Oblast: 112 unique budget codes (~56 hromadas × 2)
- Chernihiv Oblast: 114 unique budget codes (~57 hromadas × 2)

**War signal — example validation (Чупахівська hromada, Sumy Oblast):**

| Year | Annual PIT (UAH) | vs 2021 |
|------|-----------------|---------|
| 2021 | 58.05M | baseline |
| 2022 | 50.40M | −13% (occupation year) |
| 2023 | 81.69M | +41% (recovery) |
| 2024 | 88.91M | +53% (continued recovery) |

This pre/post pattern is exactly what the RDD will quantify by comparing hromadas near the occupation boundary.

**What happened / problems:**

1. The portal's main page and Swagger UI are blocked at HTTP 403 from automated scrapers without a browser-like User-Agent. Adding a Chrome User-Agent and Referer header resolves this — no authentication token required.

2. The `/v2/api-docs?group=v1.0` endpoint (discovered via `/swagger-resources`) reveals only 9 endpoints; the key one is `/api/public/localBudgetData`.

3. The BUDG dictionary is ~180 MB and takes ~2 minutes to download. It is cached locally at `data/raw/_cache/openbudget_budg_dict.json` to avoid re-downloading.

4. The BUDG dictionary begins from 2022, creating an apparent 2021 data gap. However, querying the 2022 budget code with `year=2021` returns complete 2021 monthly data. This is documented in the script and handled automatically.

**Status:** Data collection running (`scrapers/08_pit_revenue.py --years 2021 2022 2023 2024`). ~328 budget code × year combinations, ~1.5s per request = ~20 minutes estimated.

**Output files:**
- `data/raw/pit_revenue/<budgetCode>_<year>.csv` — raw per-hromada-year CSV (immutable)
- `data/clean/pit_revenue/pit_hromada_month.csv` — clean panel schema:
  `hromada_code | hromada_name | oblast | budget_code | month | pit_total_uah`

---

## Summary Table

| # | Source | Goal | Outcome | In Panel? |
|---|--------|------|---------|-----------|
| 1 | ACLED (bulk xlsx) | Conflict events/fatalities by raion-month | **Complete** — 42,190 rows, 2021–2024, 3 oblasts | Pending processing |
| 1 | ACLED Infrastructure Tags (xlsx) | Infrastructure damage type by raion-month | **Complete** — 3,800 rows, 2022–2026, 3 oblasts | Pending processing |
| 2 | Jooble (Wayback) | Job postings by oblast-month (historical) | **9 national data points** (2021–2024); no oblast split | Yes (national control) |
| 3 | Work.ua | Job postings by oblast-month (historical) | Current snapshot only (SPA) | No |
| 3 | Robota.ua | Job postings by oblast-month (historical) | Current snapshot only (SPA) | No |
| 4 | OpenDataBot | Business registrations by oblast-month | Failed (Nuxt.js SPA, fake data) | No |
| 5 | EDR / data.gov.ua | Business registrations by oblast-month | National totals only (no ADDRESS field) | Yes (national control) |
| 6 | HDX / IOM DTM | IDP displacement by oblast-month | Complete, Feb 2022–Jan 2026 | Yes |
| 7 | Fulldata_City_Region_Monthly (possibleSets) | Multi-indicator panel | Primary housing prices: **complete 48 months, 3 oblasts**; salaries: post-2022 only; conflict counts: inferior to ACLED xlsx | Partial — `prim_price_usd_m2` pending extraction |
| 8 | LUN.ua stat API | Primary market prices by class, rental prices | **Complete** — 47/48 months × 3 oblasts from Jan 2021; rental + room-count breakdown from May 2023 | `prim_price_usd_m2_avg` pending panel merge |
| 9 | `acled_work_imo.xlsx` (class dataset) | Pre-merged ACLED + labor + IDP panel, oblast × sector × month | **Complete** — 34,800 rows, 25 oblasts, Jan 2021–Dec 2024; **historical job posting counts and salaries by sector** unavailable from project scrapers | Pending: aggregate to oblast × month and merge salary + posting variables |
| 10 | `primary market data.xlsx` (class dataset) | Primary market housing prices, extended history | **Complete** — 22 cities, Jan 2014–Jun 2025; extends LUN.ua scrape back to 2014 | Supplement for pre-2021 trend/placebo analysis |
| 11 | `secondary market data.xlsx` (class dataset) | Secondary market listed/sold prices + rental by room count | **Partial** — 22 cities, May 2023–Jul 2025 only; no pre-war baseline | Supplementary post-war outcome variables |
| 12 | SSSU Consumer Prices (Ukrstat, class dataset) | Oblast-level consumer prices and CPI | **Complete** — all 3 target oblasts, 2,446 product series, monthly from 2017; **only source with oblast-level price data for Kharkiv and Chernihiv** | Pending: extract food basket index as outcome variable and deflator |
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

### openbudget.gov.ua (`scrapers/08_pit_revenue.py`)
Two-phase process:

**Phase 1 — Budget code discovery:** Downloads the full BUDG dictionary (`GET /items/BUDG`) and caches it at `data/raw/_cache/openbudget_budg_dict.json` (~180 MB, 217K entries). Filters to KATOTTG prefixes `UA59` (Sumy), `UA63` (Kharkiv), `UA74` (Chernihiv) and selects entries where `signBudg` contains `(g)` — identifying hromada self-governance budgets. Deduplicates by `codebudg` to get one entry per unique hromada budget code per year range.

**Phase 2 — PIT income download:** For each unique (budget code, year) pair, calls `GET /api/public/localBudgetData?budgetCode=<code>&budgetItem=INCOMES&period=MONTH&year=<YYYY>`. Filters response to rows where `COD_INCO` starts with `11010` (all PIT sub-codes) and `FUND_TYP == 'C'` (general fund). Sums `FAKT_AMT` by month. Saves raw CSV to `data/raw/pit_revenue/` (immutable cache). Assembles clean hromada × month panel in `data/clean/pit_revenue/pit_hromada_month.csv`.

**2021 data gap workaround:** The BUDG dictionary only contains budget codes from 2022 onward. However, querying the API with a 2022 budget code and `year=2021` successfully returns 2021 monthly data. The script automatically extends 2022 codes to also cover 2021 queries.
