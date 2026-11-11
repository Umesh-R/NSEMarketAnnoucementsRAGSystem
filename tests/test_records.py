from __future__ import annotations

from datetime import datetime

from nse_ingest.records import (
    clean_url, file_ext, parse_compact_dt, parse_nse_dt, to_records,
)


def ann(**over: object) -> dict:
    row = {
        "an_dt": "04-Nov-2026 17:03:04", "attchmntFile": "https://nsearchives.nseindia.com/corporate/A_1.pdf",
        "attchmntText": "informed the Exchange", "desc": "Updates", "dt": "04112026170304",
        "seq_id": "106844705", "symbol": "BHARTIARTL", "smIndustry": "Telecom",
    }
    row.update(over)
    return row


def test_parse_compact_dt() -> None:
    assert parse_compact_dt("04112026170304") == datetime(2026, 11, 4, 17, 3, 4)
    assert parse_compact_dt("31122023235959") == datetime(2023, 12, 31, 23, 59, 59)
    for bad in (None, "", "-", "0411202617030", "abcdefghijklmn"):
        assert parse_compact_dt(bad) is None


def test_parse_nse_dt_formats() -> None:
    assert parse_nse_dt("04-Nov-2026 17:03:04") == datetime(2026, 11, 4, 17, 3, 4)
    assert parse_nse_dt("06-Feb-2025 23:15") == datetime(2025, 2, 6, 23, 15)
    assert parse_nse_dt("04-Aug-2026") == datetime(2026, 8, 4)
    assert parse_nse_dt("-") is None and parse_nse_dt(None) is None


def test_dash_sentinel_means_no_attachment() -> None:
    assert clean_url("-") is None and clean_url("") is None and clean_url(None) is None
    (rec,) = to_records("announcement", [ann(attchmntFile="-")], "BHARTIARTL")
    assert rec.files == ()


def test_url_is_taken_verbatim() -> None:
    weird = "https://nsearchives.nseindia.com/corporate/mjain_14062019171735_Clarification Telemedia_229.pdf"
    (rec,) = to_records("announcement", [ann(attchmntFile=weird)], "BHARTIARTL")
    assert rec.files[0].url == weird


def test_announcement_fields() -> None:
    (rec,) = to_records("xbrl", [ann()], "IGNORED")
    assert rec.filing_key == "xbrl:106844705"
    assert rec.symbol == "BHARTIARTL" and rec.category == "Updates"
    assert rec.broadcast_dt == datetime(2026, 11, 4, 17, 3, 4)


def test_null_seq_id_gets_stable_content_key() -> None:
    a = to_records("announcement", [ann(seq_id=None)], "X")[0]
    b = to_records("announcement", [ann(seq_id=None, smIndustry="changed volatile field")], "X")[0]
    c = to_records("announcement", [ann(seq_id=None, desc="Other")], "X")[0]
    assert a.filing_key == b.filing_key != c.filing_key
    assert a.filing_key.startswith("announcement:h:")


def test_board_meeting_has_two_files() -> None:
    row = {"bm_symbol": "BHARTIARTL", "bm_date": "04-Aug-2026", "bm_purpose": "Board Meeting Intimation",
           "bm_desc": "to consider results", "bm_timestamp": "24-Jul-2026 23:04:32",
           "attachment": "https://nsearchives.nseindia.com/corporate/xbrl/a.xml",
           "ixbrl": "https://nsearchives.nseindia.com/corporate/ixbrl/a_WEB.html"}
    (rec,) = to_records("board_meeting", [row], "BHARTIARTL")
    assert [f.role for f in rec.files] == ["attachment", "ixbrl"]
    assert rec.broadcast_dt == datetime(2026, 7, 24, 23, 4, 32)


def test_financial_result_both_shapes() -> None:
    legacy = {"seqNumber": "1193121", "symbol": "X", "broadCastDate": "06-Feb-2025 23:15:57",
              "xbrl": "https://nsearchives.nseindia.com/corporate/xbrl/I.xml", "relatingTo": "Third Quarter"}
    integ = {"seq_Id": "182434", "symbol": "X", "broadcast_Date": "04-Aug-2026 20:21:35",
             "type": "Integrated Filing- Financials", "pdf_attach": None, "xbrl": "https://h/x.xml",
             "ixbrl": "https://h/x.html", "qe_Date": "30-JUN-2026"}
    a, b = to_records("financial_result", [legacy, integ], "X")
    assert a.filing_key == "financial_result:fr:1193121" and len(a.files) == 1
    assert b.filing_key == "financial_result:if:182434" and len(b.files) == 2


def test_corp_action_has_no_files() -> None:
    row = {"symbol": "BHARTIARTL", "subject": "Dividend - Rs 24 Per Share", "exDate": "24-Jul-2026",
           "recDate": "24-Jul-2026", "caBroadcastDate": None}
    (rec,) = to_records("corp_action", [row], "BHARTIARTL")
    assert rec.files == () and rec.broadcast_dt == datetime(2026, 7, 24)


def test_file_ext() -> None:
    assert file_ext("https://h/corporate/A.PDF") == "pdf"
    assert file_ext("https://h/corporate/BHARTIARTL_press.zip") == "zip"
    assert file_ext("https://h/download?id=1", "application/pdf; charset=x") == "pdf"
    assert file_ext("https://h/noext") == "bin"
