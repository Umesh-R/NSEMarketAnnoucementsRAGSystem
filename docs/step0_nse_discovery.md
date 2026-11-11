# Step 0: NSE endpoint discovery (probed 2026-11-04)

Probe script: `scripts/probe_nse.py` (throwaway). All findings below were observed on 2026-11-04 with BHARTIARTL and RELIANCE, at no more than 1 request per second. NSE changes these endpoints without notice, so re-run the probe before relying on anything here.

## Endpoints

The live corporate-filings pages (`/companies-listing/corporate-filings-*`) are jQuery pages that call the REST paths directly. These "legacy" paths are the **primary** path, not a fallback.

| doc_type | Endpoint | Params | Attachment field(s) |
|---|---|---|---|
| announcement | `GET /api/corporate-announcements` | `index=equities&symbol=X&from_date=DD-MM-YYYY&to_date=DD-MM-YYYY` | `attchmntFile` |
| xbrl (announcement XBRL) | same endpoint | same + `reqXbrl=true` | `attchmntFile` (`.xml`, disjoint from the PDF set) |
| board_meeting | `GET /api/corporate-board-meetings` | `index=equities&symbol=X&from_date&to_date` | `attachment` (XML), `ixbrl` (HTML) |
| corp_action | `GET /api/corporates-corporateActions` | `index=equities&symbol=X&from_date&to_date` | **none** (metadata only) |
| financial_result (≤ Feb 2025) | `GET /api/corporates-financial-results` | `index=equities&symbol=X&period=Quarterly[&from_date&to_date]` | `xbrl` |
| financial_result (2025+, Integrated Filing) | `GET /api/integrated-filing-results` | `index=equities&symbol=X&type=Integrated Filing- Financials` | `xbrl`, `ixbrl`, `pdf_attach`; paginated (`size`, `page`, `totalCount`) |
| Nifty 50 list | `GET https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv` | none | CSV: Company Name, Industry, Symbol, Series, ISIN Code |

Dead or changed:
- `/api/corporates-announcements` (plural): 404.
- `/api/equity-stockIndices?index=NIFTY 50`: 404 with `+` or `%20` encoding.
- Quote page `/get-quote/equity/<SYM>/`: 404. The real URL is `/get-quote/equity/<SYM>/<Company-Name-Slug>`. The legacy `/get-quotes/equity?symbol=<SYM>` redirects to it.

Dispatcher (Next.js app): `/api/NextApi/apiClient/GetQuoteApi?functionName=...`. The spec's `/api/GetQuoteApi` path is wrong. Sibling handlers: `/api/NextApi/apiClient?functionName=`, `apiStatic`, `cmsHandler`, `dynamicApi`.
- `functionName=getCorporateAnnouncement&symbol=X&marketApiType=equities[&noOfRecords=N]` returns the same row schema as the legacy endpoint. Without `marketApiType` it returns a 404 from the backend route `/CorporateService/announcements/undefined`. Date params were not tested. Use it as the fallback.
- `functionName=getIndexData&type=All` (on `apiClient`) lists indices but not their constituents (`constituents: null`).
- No dispatcher functions were found for board meetings, corporate actions or results in the quote-page bundles.

## Announcement row fields

`an_dt` (`04-Nov-2026 17:03:04`), `attFileSize`, `attchmntFile`, `attchmntText`, `bflag`, `csvName`, `desc` (category), `difference`, `dt` (`DDMMYYYYHHMMSS`, always 14 chars), `exchdisstime`, `fileSize`, `hasXbrl`, `old_new`, `orgid`, `seq_id`, `smIndustry`, `sm_isin`, `sm_name`, `sort_date` (`YYYY-MM-DD HH:MM:SS`), `symbol`.

## Date ranges

- The UI's 1D/1W/1M/3M/6M/1Y buttons just compute `from_date`/`to_date` in the browser.
- The format must be `DD-MM-YYYY`. ISO dates don't raise an error: they return `{"data":[],"msg":"no data found"}`, a dict rather than the usual list. An empty result can also be a plain `[]`, so a parser must accept both shapes.
- `to_date` is inclusive.
- The UI refuses announcement ranges longer than 30 days without a company selected. The server does not enforce this.

## Row cap: none found

- BHARTIARTL with no dates: 2,608 rows (2004 to 2026).
- RELIANCE with no dates: 3,373 rows. RELIANCE from 2023-10-01 to 2026-11-04: 798 rows in one response.
- All equities over 3 months with no symbol: 55,570 rows (39 MB) in one response.
- Monthly and quarterly windows match the corresponding slice of a larger query exactly.
- The `[0 … 99]` shown in the UI is client-side pagination.
- **However, the no-date "full history" query is less complete than windowed queries.** It omitted 2 RELIANCE rows from Feb 2024 and returned one row with its time truncated to `00:00:00`. Harvest with explicit date windows.

## Data-quality quirks that affect the manifest

- `attchmntFile == "-"` means a text-only announcement with no file. BHARTIARTL has 391 of these (2 since 2023-10). Under `source_url UNIQUE` they would all collapse into one row.
- One URL can serve several announcements. Example: `BHARTIARTL_14042021234810_D3STXdisclosurefinal.pdf` appears as both "Demerger" and "Amalgamation/Merger", with different `seq_id`s.
- A URL's content can change: `BHARTIARTL_press.zip` was posted in Aug 2015 and again in Nov 2015 (different `seq_id`s).
- The API sometimes returns exact duplicate rows.
- `seq_id` is null for older rows (none since 2023-10).
- Extensions seen: pdf (75%), zip (25%), a few xlsx; xml/html for XBRL.

## HTTP behaviour

- The minimal header set is a browser `User-Agent` alone. Cookie priming, Referer, Accept and Accept-Language were all unnecessary on the probe date. Priming is still cheap insurance.
- Without a browser UA, `www.nseindia.com` resets the HTTP/2 stream (`RemoteProtocolError`), and `nsearchives` hangs until it times out. There was no clean 403 in either case. Treat both as "blocked".
- Attachments (`nsearchives.nseindia.com`) need no Referer and no cookies. They support `Accept-Ranges: bytes` (`206` on a Range request), and return a weak ETag `W/"<size>-<mtime_ms>"` and `Last-Modified`.
- A missing attachment returns a `404` with `text/html`. Always check status and content-type before storing.
- HTTP/1.1 and HTTP/2 both work.
