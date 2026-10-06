# -*- coding: utf-8 -*-
"""
Multi-Account Order Panel
==========================
A GUI wrapper around the original multi-account order-blaster script.

New capture-based workflow
---------------------------
Per account you no longer type price/qty/side/symbol into the UI. Instead:
  1. Send the order once from the browser.
  2. DevTools -> Network -> right-click the request -> Copy -> "Copy as
     cURL (bash)".
  3. Paste that raw curl command straight into the account's "هدر" box —
     parse_captured_curl() below reads it directly (URL, headers, cookies,
     body), no external curl-to-python converter needed. Pasting
     curl-to-python converter output (the older workflow) still works too;
     parse_captured_input() auto-detects which format it's looking at.

That pasted text already contains the URL, headers, cookies, and the JSON
body — with price/side/quantity/symbol baked in — exactly as the browser
sent it.

- Accounts (name, broker, pasted request) are stored in accounts.json next
  to this file — never hardcoded in source.
- A single start/end time (HH:MM:SS, second precision) at the top of the
  panel is shared by every account. From that start time to that end time,
  each account fires its own requests back-to-back every interval_ms
  milliseconds, on its own thread, without waiting for the previous
  request's response.
- Start spins up one thread per enabled account; Stop signals all threads
  to stop after their current request.
- When more than one account is enabled at once, the shared "live log"
  panel only prints each account's start/stop lines (not every single
  request) — with several accounts firing concurrently, responses don't
  come back in send order anyway, so per-request live logging is just
  overhead that slows the sending down. Full per-request detail for each
  account is still recorded and viewable via its own "مشاهده لاگ" window.

Run:  python panel_edited_v2.py
Requires: requests  (pip install requests)

این نسخه کاملاً تک‌فایلی است — همه چیز (شامل تنظیمات هر کارگزاری) همین
یک فایل است تا مشکل «فایل کنارش پیدا نشد» دیگر پیش نیاید.
"""

import ast
import json
import os
import queue
import re
import shlex
import threading
import time
import uuid
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

import requests

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "accounts.json")

# ============================================================================
# Embedded logo (نورآی / nouray.ir) — base64 PNG, ~30KB, so the whole panel
# stays a single self-contained .py file with no external image asset to
# lose or forget to ship alongside it.
# ============================================================================
LOGO_PNG_BASE64 = (
    "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"
    "enqQo0KFaQngkaWyZCXAnHpe2Pg7O5wRftuuLMOAD5Aux8Ychl1Ocq/MmMAitZxGJM5mq/2ATn3Bk8AfL1IbNzkw/cLydQWY5koT"
    "w7X4Ll1gWtlQAutKctc1wTubdAWm3cPwWcelOJY+ob93wEVjhq3YB6oNEOhj3RP6G5pJD4GfGmF0uL93cNAuXEgeqXNw3I0DafVn"
    "n7C7Gb/1JcZueye4K8vUnQHHiR1Djwh+Gk2iLByA0wHDN3hmxJ3Y3xs+KnTB9hYMnOZS9Qexvu+Mg33N9B2UtCH6sp+Lx3nKqZJY"
    "O1gG/8UMMh57qCx/tX8g7d6ulqpyLxrew8NP3fpPfGZz1rZ1weIZfDlgJcrZ9CEaDRsjkx+UshmXNoYDeshqoVrpNezxTiltwdm2"
    "M/PZ8NeLO3n8+8KHLx+ka4yu27jl6oavWrQqpk+dubwariUnZ69Ulw/29I7M4gmwei7P3kP6gKW5d8CdgJI9m/14UtxmRMZEHP+I"
    "UoQYKu7v74HBb4DnmBTrdLrdIOjTxGI0erIW/DB0T/hleGPJWVuhnOlbhU+NHBE5nc6cDB/izGByJ/b3h4+hPhhSQLWrrOdeyGxy"
    "gk03fXPwQXUro9c3WauS87bst1AhByFQwafsQ6FzKZyt5p09bj42yOXbdrBZrBdF9rujATCHxdArM4XWpe2nXSO0+eT2aTpzSZ8M"
    "vZMET41X6oNyZr9Le58yLFnwynLR47dpL7KfED5eqRhk+twlcair78wp3Cx79ucQ8fDq73OWSzVY6Sc06rBYseFTFxANonJPiD39"
    "gMg+2KoewLx8EAZlS0lllJNRRKPg80PcZYk8CcNJg5fguQFCPS11b7jT8JMojTGT7ILhnzSoihanZD0FfLpSoREFwcihkln3cKZX"
    "LRd3WgzSUm4f4DdcWcJA8NXx2J2pj1sp3XSWHPIPiyZMinPsTcUupj/BeR5V4nk5UIMVWowgNI5/5CgK1qHmnVRBICbuC/0uo+iz"
    "DbeOV/DD492h687ohoFekHc0/HvDp9Qm/XwNp9onIVuim75iyO0SWMUj6bddZ4fhY4MR+ytfmj76pZVy93bRmaAu8kqdTGy9N9I1"
    "Fb2Whr4mSY2crlXXNgbshjjCbWEw2AfaCrv2IpE7CvgbrBRZpHb5xcqfm2sIojgftpvAvkMJJzCM4M5e5wvgw5aLpi/A68832S9d"
    "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"
    "e3loAAAAAElFTkSuQmCC"
)


# ============================================================================
# Broker adapters (previously a separate broker_adapters.py — merged into
# this single file so nothing can go missing).
# ============================================================================

DEFAULT_ERROR_CODE_FIELDS = (
    "errorCode", "ErrorCode", "errCode", "ErrCode",
    "resultCode", "ResultCode", "error_code", "err_code",
)

_REQUEST_METHODS = ("get", "post", "put", "patch", "delete", "head", "options")


class CapturedRequestError(ValueError):
    """Raised when the pasted browser-capture code can't be understood."""


def _literal(node):
    return ast.literal_eval(node)


def _resolve(node, variables):
    """Resolve an AST node to a Python value: either a literal, or a lookup
    into `variables` if it's just a bare name referencing an earlier
    assignment (the normal shape of curl-to-python converter output)."""
    if isinstance(node, ast.Name):
        if node.id in variables:
            return variables[node.id]
        raise CapturedRequestError(f"متغیر «{node.id}» در متن هدر تعریف نشده است.")
    return _literal(node)


_IMPORT_LINE_RE = re.compile(r"^\s*(import\s+\S+|from\s+\S+\s+import\s+.+)\s*$")


def _strip_import_lines(text: str) -> str:
    """Drop any 'import x' / 'from x import y' lines from pasted curl-to-python
    output. These lines are never needed (this module never executes the
    pasted code — it only reads specific assignments/calls via ast), so the
    user shouldn't have to delete them by hand every time a converter adds
    'import requests' at the top."""
    return "\n".join(line for line in text.splitlines() if not _IMPORT_LINE_RE.match(line))


# ----------------------------------------------------------------------------
# Raw "curl" command parsing (no curlconverter.com round-trip needed).
#
# Chrome/Edge's "Copy as cURL (bash)" wraps header/cookie/body values that
# contain quotes, unicode, or other special characters in bash's ANSI-C
# quoting: $'...'. Python's shlex doesn't understand that syntax at all (it
# would leave the literal $'...' in the token, backslash-escapes and all),
# so it's decoded by hand below before handing the text to shlex. Firefox's
# copy-as-cURL mostly just uses plain single quotes, which shlex already
# handles correctly on its own.
# ----------------------------------------------------------------------------

_ANSI_C_SIMPLE_ESCAPES = {
    "n": "\n", "t": "\t", "r": "\r", "a": "\a", "b": "\b", "f": "\f", "v": "\v",
    "\\": "\\", "'": "'", '"': '"', "e": "\x1b",
}


def _decode_ansi_c_quote(inner: str) -> str:
    """Decode the contents of a bash $'...' literal into a plain Python str."""
    out = []
    i = 0
    n = len(inner)
    while i < n:
        c = inner[i]
        if c == "\\" and i + 1 < n:
            nxt = inner[i + 1]
            if nxt in _ANSI_C_SIMPLE_ESCAPES:
                out.append(_ANSI_C_SIMPLE_ESCAPES[nxt])
                i += 2
                continue
            if nxt == "x" and i + 3 < n:
                try:
                    out.append(chr(int(inner[i + 2:i + 4], 16)))
                    i += 4
                    continue
                except ValueError:
                    pass
            if nxt == "u" and i + 5 < n:
                try:
                    out.append(chr(int(inner[i + 2:i + 6], 16)))
                    i += 6
                    continue
                except ValueError:
                    pass
            if nxt == "U" and i + 9 < n:
                try:
                    out.append(chr(int(inner[i + 2:i + 10], 16)))
                    i += 10
                    continue
                except ValueError:
                    pass
        out.append(c)
        i += 1
    return "".join(out)


_ANSI_C_QUOTE_RE = re.compile(r"\$'((?:[^'\\]|\\.)*)'", re.DOTALL)


def _preprocess_curl_text(text: str) -> str:
    """Join backslash-newline line continuations (curl commands copied from a
    multi-line terminal view) and rewrite every $'...' ANSI-C literal into an
    equivalent plain single-quoted literal that shlex can tokenize."""
    text = re.sub(r"\\\r?\n", " ", text)

    def _sub(m):
        decoded = _decode_ansi_c_quote(m.group(1))
        return "'" + decoded.replace("'", "'\\''") + "'"

    return _ANSI_C_QUOTE_RE.sub(_sub, text)


_CURL_FLAGS_WITH_VALUE = {
    "-H": "header", "--header": "header",
    "-b": "cookie", "--cookie": "cookie",
    "-X": "method", "--request": "method",
    "-d": "data", "--data": "data", "--data-raw": "data",
    "--data-binary": "data", "--data-ascii": "data", "--data-urlencode": "data",
    "-A": "user_agent", "--user-agent": "user_agent",
    "-e": "referer", "--referer": "referer",
    "--url": "url",
}
# Flags that take a value we don't need — consumed and discarded, so their
# value is never mistaken for the URL.
_CURL_IGNORED_FLAGS_WITH_VALUE = {
    "-u", "--user", "--proxy", "--connect-timeout", "--max-time", "--resolve",
    "--limit-rate", "--cert", "--key", "--cacert", "-o", "--output", "--range",
}
_CURL_IGNORED_FLAGS_NO_VALUE = {
    "--compressed", "-k", "--insecure", "-s", "--silent", "-v", "--verbose",
    "-i", "--include", "-L", "--location", "-G", "--get", "-4", "-6",
}


def parse_captured_curl(raw_text: str) -> dict:
    """Parse a raw 'curl ...' command — exactly what a browser's DevTools
    'Copy as cURL (bash)' produces — straight into the same dict shape
    parse_captured_request() returns. No curlconverter.com step needed."""
    text = _strip_import_lines(raw_text).strip()
    if not text:
        raise CapturedRequestError("متن curl خالی است.")
    text = _preprocess_curl_text(text)

    try:
        tokens = shlex.split(text, posix=True)
    except ValueError as e:
        raise CapturedRequestError(f"متن curl قابل خواندن نیست (نقل‌قول‌ها به‌هم نمی‌خورد): {e}")

    if not tokens or tokens[0].lower().rstrip(".exe") not in ("curl",):
        raise CapturedRequestError("متن باید با دستور curl شروع شود.")
    tokens = tokens[1:]

    url = None
    method = None
    headers = {}
    cookies = {}
    data_parts = []

    i = 0
    while i < len(tokens):
        tok = tokens[i]
        kind = _CURL_FLAGS_WITH_VALUE.get(tok)
        if kind:
            if i + 1 >= len(tokens):
                raise CapturedRequestError(f"بعد از «{tok}» مقداری پیدا نشد.")
            val = tokens[i + 1]
            i += 2
            if kind == "header":
                if ":" in val:
                    k, v = val.split(":", 1)
                    headers[k.strip()] = v.strip()
            elif kind == "cookie":
                for part in val.split(";"):
                    part = part.strip()
                    if "=" in part:
                        ck, cv = part.split("=", 1)
                        cookies[ck.strip()] = cv.strip()
            elif kind == "method":
                method = val.lower()
            elif kind == "data":
                data_parts.append(val)
            elif kind == "user_agent":
                headers.setdefault("User-Agent", val)
            elif kind == "referer":
                headers.setdefault("Referer", val)
            elif kind == "url":
                url = val
            continue

        if tok in _CURL_IGNORED_FLAGS_WITH_VALUE:
            i += 2
            continue
        if tok in _CURL_IGNORED_FLAGS_NO_VALUE:
            i += 1
            continue
        if tok.startswith("-") and tok != "-":
            # unknown flag — skip just the flag itself, never swallow the
            # next token, so we don't accidentally eat the real URL
            i += 1
            continue

        if url is None:
            url = tok
        i += 1

    if not url:
        raise CapturedRequestError("آدرس (URL) در متن curl پیدا نشد.")

    # A 'Cookie' header (rather than -b) sometimes carries the cookies too —
    # fold it in the same way.
    for hk in list(headers.keys()):
        if hk.lower() == "cookie":
            for part in headers.pop(hk).split(";"):
                part = part.strip()
                if "=" in part:
                    ck, cv = part.split("=", 1)
                    cookies.setdefault(ck.strip(), cv.strip())

    body_text = "&".join(data_parts) if data_parts else None
    if method is None:
        method = "post" if body_text is not None else "get"

    json_body = None
    data = None
    if body_text is not None:
        stripped = body_text.strip()
        content_type = next((v for k, v in headers.items() if k.lower() == "content-type"), "")
        looks_like_json = stripped[:1] in ("{", "[")
        if looks_like_json and ("json" in content_type.lower() or not content_type):
            try:
                json_body = json.loads(stripped)
            except (ValueError, TypeError):
                data = body_text
        else:
            data = body_text

    return {
        "method": method,
        "url": url,
        "headers": headers,
        "cookies": cookies,
        "params": {},
        "json_body": json_body,
        "data": data,
    }


def parse_captured_input(raw_text: str) -> dict:
    """Entry point used by every broker: auto-detects whether the pasted text
    is a raw 'curl ...' command (straight from DevTools -> Copy as cURL) or
    curl-to-python converter output, and parses it accordingly. This means
    curlconverter.com is optional now, not a required step."""
    text = (raw_text or "").strip()
    if not text:
        raise CapturedRequestError("هدر خالی است.")
    probe = re.sub(r"\\\r?\n", " ", text).lstrip()
    if re.match(r"^curl(\.exe)?(\s|$)", probe, flags=re.IGNORECASE):
        return parse_captured_curl(text)
    return parse_captured_request(text)


def parse_captured_request(raw_text: str) -> dict:
    """Parse a full curl->python converted request into its parts.

    Returns a dict with keys: method, url, headers, cookies, params,
    json_body (dict or None), data (str/bytes/dict or None).
    Raises CapturedRequestError with a Persian message on anything it can't
    make sense of, so the caller can show it directly to the user.
    """
    text = (raw_text or "").strip()
    if not text:
        raise CapturedRequestError("هدر خالی است.")
    text = _strip_import_lines(text)

    try:
        tree = ast.parse(text)
    except SyntaxError as e:
        raise CapturedRequestError(f"کد پایتون قابل خواندن نیست (خطای نحوی): {e}")

    variables = {}
    call_node = None

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            try:
                variables[name] = _literal(node.value)
            except (ValueError, SyntaxError):
                # not a plain literal (e.g. json.dumps(...)) — skip, handled
                # separately below if needed
                pass
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "requests"
            and node.func.attr in _REQUEST_METHODS + ("request",)
        ):
            call_node = node  # keep the last one, in case of multiple

    if call_node is None:
        raise CapturedRequestError(
            "خط requests.get/post(...) در متن پیدا نشد. لطفاً کل کد تبدیل‌شده "
            "از bash به پایتون را (نه فقط بخش هدرها) اینجا قرار دهید."
        )

    attr = call_node.func.attr
    args = call_node.args
    if attr == "request":
        if len(args) < 2:
            raise CapturedRequestError("فراخوانی requests.request باید متد و آدرس را داشته باشد.")
        method = str(_resolve(args[0], variables)).lower()
        url = _resolve(args[1], variables)
    else:
        method = attr
        if not args:
            raise CapturedRequestError("آدرس درخواست (URL) پیدا نشد.")
        url = _resolve(args[0], variables)

    if not isinstance(url, str) or not url.startswith("http"):
        raise CapturedRequestError("آدرس درخواست (URL) معتبر نیست.")

    kwargs = {}
    for kw in call_node.keywords:
        if kw.arg is None:
            continue
        try:
            kwargs[kw.arg] = _resolve(kw.value, variables)
        except CapturedRequestError:
            raise
        except (ValueError, SyntaxError):
            pass

    headers = kwargs.get("headers") or {}
    cookies = kwargs.get("cookies") or {}
    params = kwargs.get("params") or {}
    json_body = kwargs.get("json")
    data = kwargs.get("data")

    if not isinstance(headers, dict):
        headers = {}
    if not isinstance(cookies, dict):
        cookies = {}

    return {
        "method": method,
        "url": url,
        "headers": {str(k): str(v) for k, v in headers.items()},
        "cookies": {str(k): str(v) for k, v in cookies.items()},
        "params": params if isinstance(params, dict) else {},
        "json_body": json_body,
        "data": data,
    }


def extract_error_code(text, extra_fields=()):
    """Best-effort extraction of a broker's error/result code from a JSON
    response body. `extra_fields` lets a specific broker's field names be
    tried first."""
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return ""
    if not isinstance(data, dict):
        return ""
    for key in tuple(extra_fields) + DEFAULT_ERROR_CODE_FIELDS:
        if key in data and data[key] not in (None, ""):
            return str(data[key])
    for key, value in data.items():
        lk = key.lower()
        if "error" in lk and "code" in lk and value not in (None, ""):
            return str(value)
    return ""


# broker_key -> broker config. All four currently share the same generic
# parser/error-code logic; give a broker its own `parse`/`error_code_fields`
# here later if its capture format or error reporting turns out to differ.
BROKERS = {
    "saman": {
        "label": "سامان",
        "parse": parse_captured_input,
        "error_code_fields": (),
    },
    "pasargad": {
        "label": "پاسارگاد",
        "parse": parse_captured_input,
        "error_code_fields": (),
    },
    "karizma": {
        "label": "کاریزما",
        "parse": parse_captured_input,
        "error_code_fields": (),
    },
    "exir": {
        "label": "اکسیر",
        "parse": parse_captured_input,
        "error_code_fields": (),
    },
}

DEFAULT_ORDER = {
    "interval_ms": 300,
    "stop_on_text": "",
}

DEFAULT_START_TIME = "08:44:57.500"
DEFAULT_END_TIME = "08:45:00.500"

DEFAULT_TEST_COUNT = 5
DEFAULT_TEST_INTERVAL_MS = 340


class InvalidTimeError(ValueError):
    """Raised when a start/end time string isn't a valid HH:MM:SS[.mmm]."""


_HMS_RE = re.compile(r"^(\d{1,2}):(\d{1,2}):(\d{1,2})(?:[.,](\d{1,6}))?$")


def parse_hms_today(text: str) -> datetime:
    """Parse an 'HH:MM:SS' or 'HH:MM:SS.mmm' string into a datetime for
    *today*, with optional millisecond/microsecond precision.
    Raises InvalidTimeError with a Persian message on bad input."""
    text = (text or "").strip()
    m = _HMS_RE.match(text)
    if not m:
        raise InvalidTimeError(
            f"زمان «{text}» باید به شکل ساعت:دقیقه:ثانیه یا ساعت:دقیقه:ثانیه.میلی‌ثانیه باشد "
            f"(مثلاً 08:44:57 یا 08:44:57.500)."
        )
    h, mi, s, frac = m.groups()
    h, mi, s = int(h), int(mi), int(s)
    microsecond = int((frac + "000000")[:6]) if frac else 0
    if not (0 <= h <= 23 and 0 <= mi <= 59 and 0 <= s <= 59):
        raise InvalidTimeError(f"زمان «{text}» خارج از محدوده‌ی مجاز است.")
    now = datetime.now()
    return now.replace(hour=h, minute=mi, second=s, microsecond=microsecond)

HEADER_PLACEHOLDER_HINT = (
    "# روش ۱ (ساده‌تر، پیشنهادی): متن کامل خروجی «Copy as cURL (bash)» را\n"
    "# مستقیماً همین‌جا جای‌گذاری کنید — از curl شروع می‌شود، مثلاً:\n"
    "#   curl 'https://example.com/order' -H 'Cookie: ...' -H 'Content-Type: application/json' --data-raw '{\"qty\":5}'\n"
    "#\n"
    "# روش ۲ (در صورت نیاز): کد پایتونِ تبدیل‌شده با curlconverter.com، شامل:\n"
    "#   cookies = {...}\n"
    "#   headers = {...}\n"
    "#   json_data = {...}  (یا data = ...)\n"
    "#   response = requests.post('...', cookies=cookies, headers=headers, json=json_data)\n"
)

GUIDE_TEXT = """راهنمای گرفتن هدر درخواست از مرورگر
====================================

۱. کارگزاری را در مرورگر باز کن.
۲. قیمت، تعداد و نماد را در فرم سفارش تنظیم کن.
۳. دکمه‌ی «خرید» را بزن.
۴. روی صفحه راست‌کلیک کن و «بازرسی» / Inspect را انتخاب کن، سپس به تب
   Network (شبکه) برو.
۵. همان درخواستِ سفارش را در لیست تب Network پیدا کن — اگر پیدا نشد، یک بار
   دیگر «خرید» را بزن تا در لیست ظاهر شود.
۶. روی همان درخواست، سمت راست کلیک کن و گزینه‌ی
   Copy → "Copy as cURL (bash)" را انتخاب کن.
۷. همان متنِ curl کپی‌شده را عیناً (بدون هیچ تبدیلی) در قسمت «هدر» همان
   حساب، داخل این برنامه جای‌گذاری کن. برنامه خودش دستور curl را می‌خواند —
   دیگر نیازی به مراجعه به curlconverter.com نیست.

نکته ۱: اگر برنامه نتوانست متنِ curl را بخواند (پیام خطا در همان لحظه‌ی
ذخیره نشان داده می‌شود)، همچنان می‌توانی همان متن را در curlconverter.com
به پایتون تبدیل کنی و کد پایتونِ خروجی را به‌جایش جای‌گذاری کنی — این روش
قدیمی هم کماکان پشتیبانی می‌شود، فقط دیگر مرحله‌ی اجباری نیست.

نکته ۲: لازم نیست خط‌های import requests یا import‌های دیگر را دستی پاک
کنی؛ برنامه خودش هنگام خواندن هدر، هر خط import را نادیده می‌گیرد و حذف
می‌کند.

راه‌اندازی کلیک خودکار در خودِ مرورگر (برای کارگزاری‌های تحت سامانه‌ی اکسیر)
=============================================================================
اگر ترجیح می‌دهی به‌جای ارسال درخواست از این پنل، خودِ دکمه‌ی خرید در صفحه‌ی
کارگزاری در زمان مشخص به‌صورت خودکار کلیک شود، برای بروکرهای تحت سامانه‌ی
اکسیر می‌توانی کد زیر (یا کدی مشابه آن) را در تب Console ابزار Inspect
مرورگر وارد کنی تا در بازه‌ی زمانی مشخص‌شده، خودکار شروع به کلیک کند:

    const start = "02:44:56";
    const end   = "02:45:04";
    const interval = 320;

    function toSeconds(t) {
        const [h, m, s] = t.split(":").map(Number);
        return h * 3600 + m * 60 + s;
    }

    const startSec = toSeconds(start);
    const endSec = toSeconds(end);

    const timer = setInterval(() => {
        const now = new Date();
        const nowSec =
            now.getHours() * 3600 +
            now.getMinutes() * 60 +
            now.getSeconds();

        if (nowSec >= startSec && nowSec <= endSec) {
            document.querySelector('.buy-btn')?.click();
            console.log("CLICK", now.toLocaleTimeString());
        }

        if (nowSec > endSec) {
            clearInterval(timer);
            console.log("تمام شد");
        }
    }, interval);

پیش از اجرا، مقدار ".buy-btn" را با سلکتور واقعی دکمه‌ی خرید در صفحه‌ی همان
کارگزاری جایگزین کن (از همان تب Inspect، روی دکمه راست‌کلیک کن و
"Inspect" را بزن تا کلاس/سلکتور دقیق دکمه دیده شود). همین روش با کمی
تغییر در نام کلاس دکمه، برای سایر کارگزاری‌ها هم قابل پیاده‌سازی است.

توجه: این روش (کلیک در کنسول مرورگر) و روش این پنل (ارسال مستقیم درخواست از
پایتون) دو راه مستقل‌اند؛ لازم نیست هر دو را هم‌زمان اجرا کنی — هرکدام که
برایت راحت‌تر و پایدارتر است را انتخاب کن.
"""


def load_accounts():
    if not os.path.exists(CONFIG_PATH):
        return []
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            accounts = json.load(f)
    except Exception:
        return []
    # backfill a stable id for accounts saved before this field existed, and
    # migrate the old repeat-count + interval-in-seconds fields (from before
    # the shared start/end schedule) into a single interval_ms field.
    changed = False
    for acc in accounts:
        if not acc.get("id"):
            acc["id"] = str(uuid.uuid4())
            changed = True
        order = acc.setdefault("order", {})
        if "interval_ms" not in order:
            old_interval_sec = order.pop("interval", None)
            order["interval_ms"] = float(old_interval_sec) * 1000 if old_interval_sec else DEFAULT_ORDER["interval_ms"]
            changed = True
        if "repeat" in order:
            order.pop("repeat", None)
            changed = True
    if changed:
        save_accounts(accounts)
    return accounts


def save_accounts(accounts):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(accounts, f, ensure_ascii=False, indent=2)


class AccountDialog(tk.Toplevel):
    """Add/Edit dialog for a single account."""

    def __init__(self, parent, account=None):
        super().__init__(parent)
        self.title("ویرایش حساب" if account else "افزودن حساب")
        self.resizable(False, False)
        self.result = None
        self.account = account or {
            "id": None,
            "name": "",
            "broker": next(iter(BROKERS)),
            "enabled": True,
            "header": "",
            "order": dict(DEFAULT_ORDER),
        }

        broker_keys = list(BROKERS.keys())
        self.broker_labels = {k: BROKERS[k]["label"] for k in broker_keys}
        self.label_to_key = {v: k for k, v in self.broker_labels.items()}

        self.broker_var = tk.StringVar(value=self.broker_labels.get(self.account["broker"], self.broker_labels[broker_keys[0]]))
        self.name_var = tk.StringVar(value=self.account["name"])
        self.enabled_var = tk.BooleanVar(value=self.account.get("enabled", True))

        row = 0
        ttk.Label(self, text="نام کاربری:").grid(row=row, column=0, sticky="e", padx=6, pady=4)
        ttk.Entry(self, textvariable=self.name_var, width=30).grid(row=row, column=1, columnspan=2, sticky="w", padx=6, pady=4)
        row += 1

        ttk.Label(self, text="نام کارگزاری:").grid(row=row, column=0, sticky="e", padx=6, pady=4)
        broker_box = ttk.Combobox(self, textvariable=self.broker_var, state="readonly",
                                   values=[self.broker_labels[k] for k in broker_keys], width=18)
        broker_box.grid(row=row, column=1, sticky="w", padx=6, pady=4)
        ttk.Checkbutton(self, text="فعال", variable=self.enabled_var).grid(row=row, column=2, sticky="w")
        row += 1

        ttk.Label(self, text="هدر (متن خام curl کپی‌شده از مرورگر، یا کد پایتونِ تبدیل‌شده):").grid(
            row=row, column=0, columnspan=3, sticky="w", padx=6, pady=(8, 0))
        row += 1

        header_frame = ttk.Frame(self)
        header_frame.grid(row=row, column=0, columnspan=3, sticky="we", padx=6, pady=(0, 4))
        self.header_text = tk.Text(header_frame, width=90, height=14, wrap="none", font=("Consolas", 10))
        self.header_text.pack(side="left", fill="both", expand=True)
        header_scroll = ttk.Scrollbar(header_frame, command=self.header_text.yview)
        header_scroll.pack(side="right", fill="y")
        self.header_text.configure(yscrollcommand=header_scroll.set)
        existing_header = self.account.get("header", "")
        self.header_text.insert("1.0", existing_header or HEADER_PLACEHOLDER_HINT)
        row += 1

        self.order_frame = ttk.LabelFrame(self, text="تنظیمات ارسال")
        self.order_frame.grid(row=row, column=0, columnspan=3, sticky="we", padx=6, pady=6)
        row += 1

        btns = ttk.Frame(self)
        btns.grid(row=row, column=0, columnspan=3, pady=8)
        ttk.Button(btns, text="ذخیره", command=self._on_save).pack(side="left", padx=4)
        ttk.Button(btns, text="انصراف", command=self.destroy).pack(side="left", padx=4)

        self.order_entries = {}
        self._build_order_fields()

        self.grab_set()

    def _build_order_fields(self):
        for w in self.order_frame.winfo_children():
            w.destroy()
        self.order_entries.clear()

        existing_order = self.account.get("order", {})
        order_fields = ["interval_ms", "stop_on_text"]
        labels = {
            "interval_ms": "وقفه بین ارسال‌ها (میلی‌ثانیه):",
            "stop_on_text": "کد پاسخ توقف (اختیاری):",
        }
        for i, field in enumerate(order_fields):
            ttk.Label(self.order_frame, text=labels[field]).grid(row=i, column=0, sticky="e", padx=4, pady=2)
            default = existing_order.get(field, DEFAULT_ORDER.get(field, ""))
            if field == "stop_on_text":
                var = tk.StringVar(value=default or "")
                ttk.Entry(self.order_frame, textvariable=var, width=50).grid(row=i, column=1, sticky="w", padx=4, pady=2)
                ttk.Label(self.order_frame, text="اگر این متن/کد در پاسخ کارگزاری دیده شود، ارسال برای همین حساب متوقف می‌شود",
                          foreground="#666").grid(row=i, column=2, sticky="w", padx=4)
            else:
                var = tk.StringVar(value=str(default) if default != "" else "")
                ttk.Entry(self.order_frame, textvariable=var, width=20).grid(row=i, column=1, sticky="w", padx=4, pady=2)
            self.order_entries[field] = var

    def _on_save(self):
        name = self.name_var.get().strip()
        if not name:
            messagebox.showerror("خطا", "نام کاربری را وارد کنید.")
            return

        broker_label = self.broker_var.get()
        broker_key = self.label_to_key.get(broker_label)
        if not broker_key:
            messagebox.showerror("خطا", "کارگزاری را انتخاب کنید.")
            return

        header_text = self.header_text.get("1.0", "end").strip()
        if not header_text or header_text == HEADER_PLACEHOLDER_HINT.strip():
            messagebox.showerror("خطا", "هدر (متن curl یا کد پایتونِ تبدیل‌شده) را وارد کنید.")
            return

        # Validate parse-ability now, so mistakes are caught at save time
        # rather than only when Start is pressed.
        try:
            BROKERS[broker_key]["parse"](header_text)
        except CapturedRequestError as e:
            if not messagebox.askyesno(
                "هشدار",
                f"هدر قابل تجزیه نیست:\n{e}\n\nبا این حال ذخیره شود؟"
            ):
                return

        order = {}
        try:
            for field, var in self.order_entries.items():
                val = var.get().strip()
                if field == "interval_ms":
                    order[field] = float(val) if val else DEFAULT_ORDER["interval_ms"]
                else:
                    order[field] = val
        except ValueError:
            messagebox.showerror("خطا", "مقدار وقفه (میلی‌ثانیه) را درست وارد کنید.")
            return

        self.result = {
            "id": self.account.get("id") or str(uuid.uuid4()),
            "name": name,
            "broker": broker_key,
            "enabled": self.enabled_var.get(),
            "header": header_text,
            "order": order,
        }
        self.destroy()


MAX_RESPONSE_CHARS = 3000  # cap stored response text so the log window stays responsive


class OrderWorker(threading.Thread):
    """Runs the repeat-loop for a single account, mirroring the original per-user() functions."""

    def __init__(self, account, log_queue, stop_event, records, start_ts, end_ts, quiet,
                 max_requests=None, interval_override_ms=None):
        super().__init__(daemon=True)
        self.account = account
        self.log_queue = log_queue
        self.stop_event = stop_event
        self.records = records  # shared list this worker appends to (one per account)
        self.start_ts = start_ts  # epoch seconds — shared across all accounts
        self.end_ts = end_ts      # epoch seconds — shared across all accounts
        # max_requests: اگر مقدار داشته باشد، دقیقاً همین تعداد درخواست ارسال
        # می‌شود و سپس متوقف می‌شود (صرف‌نظر از end_ts) — برای دکمه‌ی «تست/دیباگ»
        # که تعداد دقیق مهم است، نه یک بازه‌ی زمانی.
        self.max_requests = max_requests
        # interval_override_ms: اگر مقدار داشته باشد، به‌جای order.interval_ms
        # همین مقدار به‌عنوان فاصله بین ارسال‌ها استفاده می‌شود — برای همان دکمه‌ی
        # تست، که فاصله‌اش را کاربر جدا از تنظیمات هر حساب وارد می‌کند.
        self.interval_override_ms = interval_override_ms
        # وقتی بیش از یک حساب هم‌زمان فعال است، لاگ کردن هر تک‌درخواست در
        # «گزارش زنده» فایده‌ای ندارد (پاسخ‌ها به ترتیب نمی‌آیند) و فقط با
        # رقابت بر سر GIL/به‌روزرسانی UI سرعت ارسال را کم می‌کند؛ در آن حالت
        # فقط خط شروع و خلاصه‌ی پایانی هر حساب لاگ می‌شود، نه هر درخواست.
        self.quiet = quiet

    def log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        self.log_queue.put(f"[{ts}] {self.account['name']}: {msg}")

    def run(self):
        broker_key = self.account["broker"]
        spec = BROKERS.get(broker_key)
        if not spec:
            self.log(f"کارگزاری ناشناخته: {broker_key}")
            return

        order = self.account.get("order", {})
        if self.interval_override_ms is not None:
            interval_ms = float(self.interval_override_ms) or 300.0
        else:
            interval_ms = float(order.get("interval_ms", DEFAULT_ORDER["interval_ms"])) or 300.0
        interval = interval_ms / 1000.0
        stop_on_text = (order.get("stop_on_text") or "").strip()
        error_code_fields = spec.get("error_code_fields", ())

        try:
            parsed = spec["parse"](self.account.get("header", ""))
        except CapturedRequestError as e:
            self.log(f"خطا در خواندن هدر: {e}")
            return

        method = parsed["method"]
        url = parsed["url"]
        headers = parsed["headers"]
        cookies = parsed["cookies"]
        params = parsed["params"]
        json_body = parsed["json_body"]
        data = parsed["data"]

        start_str = datetime.fromtimestamp(self.start_ts).strftime("%H:%M:%S.%f")[:-3]
        end_str = datetime.fromtimestamp(self.end_ts).strftime("%H:%M:%S.%f")[:-3]
        if self.max_requests is not None:
            approx_count = self.max_requests
            self.log(f"شروع تست — {approx_count} درخواست، هر {interval_ms:.0f}ms (بدون انتظار برای پاسخ درخواست قبلی)")
        else:
            approx_count = max(1, int((self.end_ts - self.start_ts) * 1000 / interval_ms) + 1) if self.end_ts > self.start_ts else 1
            self.log(f"شروع — بازه {start_str} تا {end_str}، هر {interval_ms:.0f}ms (تقریباً {approx_count} ارسال، بدون انتظار برای پاسخ درخواست قبلی)")

        # نکته‌ی مهم زمان‌بندی: هر ارسال باید دقیقاً در لحظه‌ی start + n*interval
        # رخ دهد، نه interval ثانیه بعد از پایانِ پاسخ درخواست قبلی. اگر ارسال n
        # را در همین ترد و به‌صورت synchronous بفرستیم و منتظر پاسخش بمانیم،
        # کندی/تأخیر پاسخ کارگزاری مستقیماً زمان شروع ارسال n+1 را عقب می‌اندازد
        # (این همان چیزی بود که فاصله‌ی واقعی را از ۳۰۰ به ۵۰۰ میلی‌ثانیه می‌رساند).
        # برای رفع این مشکل، هر ارسال در یک ترد جداگانه انجام می‌شود: حلقه‌ی
        # زمان‌بندی فقط تا لحظه‌ی مقرر صبر می‌کند، ارسال را «شلیک» می‌کند و بدون
        # صبر برای پاسخ، بلافاصله برای ارسال بعدی زمان‌بندی می‌کند.
        early_stop = threading.Event()
        records_lock = threading.Lock()
        sent_counter = {"n": 0}
        send_threads = []

        def send_one(n):
            if early_stop.is_set() or self.stop_event.is_set():
                return
            sent_at = datetime.now()
            record = {
                "n": n + 1,
                "sent_at": sent_at.strftime("%H:%M:%S.%f")[:-3],
                "received_at": "",
                "status_code": "",
                "error_code": "",
                "response_text": "",
            }
            try:
                resp = requests.request(
                    method, url,
                    headers=headers, cookies=cookies, params=params,
                    json=json_body, data=data if json_body is None else None,
                    timeout=5,
                )
                received_at = datetime.now()
                text = resp.text or ""
                error_code = extract_error_code(text, error_code_fields)
                record["received_at"] = received_at.strftime("%H:%M:%S.%f")[:-3]
                record["status_code"] = resp.status_code
                record["error_code"] = error_code
                record["response_text"] = text[:MAX_RESPONSE_CHARS]
                with records_lock:
                    self.records.append(record)
                    sent_counter["n"] += 1
                if not self.quiet:
                    self.log(f"#{n+1} -> HTTP {resp.status_code}" + (f" | errorCode={error_code}" if error_code else ""))

                if stop_on_text and stop_on_text in text:
                    early_stop.set()
                    self.log(f"کد پاسخ توقف در پاسخ #{n+1} پیدا شد — ارسال‌های باقی‌ماندهٔ این حساب لغو شد.")
            except requests.RequestException as e:
                record["received_at"] = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                record["response_text"] = f"خطای شبکه: {e}"
                with records_lock:
                    self.records.append(record)
                    sent_counter["n"] += 1
                if not self.quiet:
                    self.log(f"#{n+1} خطای شبکه: {e}")
            except Exception as e:
                record["received_at"] = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                record["response_text"] = f"خطا: {e}"
                with records_lock:
                    self.records.append(record)
                    sent_counter["n"] += 1
                if not self.quiet:
                    self.log(f"#{n+1} خطا: {e}")

        # زمان‌بندی بر اساس ساعت مطلقِ مشترکِ همه‌ی حساب‌ها (start_ts..end_ts) است،
        # نه یک تایمر نسبی که از لحظه‌ی فشردن «شروع» حساب شود. اگر start_ts هنوز
        # نرسیده باشد، این ترد همین‌جا منتظر می‌ماند؛ اگر already گذشته باشد
        # (مثلاً برنامه با کمی تأخیر اجرا شد)، بلافاصله از «الان» شروع می‌کند.
        n = 0
        while True:
            if self.stop_event.is_set():
                self.log("متوقف شد توسط کاربر.")
                break
            if early_stop.is_set():
                break
            if self.max_requests is not None and n >= self.max_requests:
                break

            scheduled_at = self.start_ts + n * interval
            if scheduled_at > self.end_ts:
                break

            wait = scheduled_at - time.time()
            if wait > 0:
                if self.stop_event.wait(wait):
                    self.log("متوقف شد توسط کاربر.")
                    break
            if early_stop.is_set():
                break

            t = threading.Thread(target=send_one, args=(n,), daemon=True)
            send_threads.append(t)
            t.start()
            n += 1

        # صبر برای پایان یافتن ارسال‌هایی که هنوز در حال دریافت پاسخ‌اند، فقط
        # برای گزارش نهایی — این صبر روی زمان‌بندی ارسال‌ها تأثیری ندارد چون همه
        # قبلاً شلیک شده‌اند.
        for t in send_threads:
            t.join(timeout=10)

        elapsed = time.time() - self.start_ts
        self.log(f"پایان — {sent_counter['n']} درخواست ارسال شد در {elapsed:.2f}s")


class LogWindow(tk.Toplevel):
    """Shows every request sent for one account: send time, receive time, HTTP status,
    broker error code, and the full response body (in a large, word-wrapped panel so
    long JSON responses are fully readable, not just their first few words)."""

    def __init__(self, parent, account_name, records):
        super().__init__(parent)
        self.title(f"لاگ درخواست‌ها — {account_name}")
        self.geometry("1100x650")
        self.records = records
        self._last_count = 0

        # --- top: compact summary table (one row per request) ---
        table_frame = ttk.Frame(self)
        table_frame.pack(fill="x", padx=6, pady=(6, 3))

        columns = ("n", "sent_at", "received_at", "status_code", "error_code")
        headers = ["#", "زمان ارسال", "زمان دریافت پاسخ", "کد پاسخ HTTP", "کد خطا (errorCode)"]
        widths = (40, 110, 130, 100, 160)
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=8)
        for c, h, w in zip(columns, headers, widths):
            self.tree.heading(c, text=h)
            self.tree.column(c, width=w, anchor="center")
        self.tree.pack(fill="x", side="left", expand=True)
        scroll = ttk.Scrollbar(table_frame, command=self.tree.yview)
        scroll.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        # --- bottom: full response text of the selected row, large + word-wrapped ---
        text_frame = ttk.LabelFrame(self, text="متن کامل پاسخ (درخواست انتخاب‌شده در جدول بالا)")
        text_frame.pack(fill="both", expand=True, padx=6, pady=(3, 6))
        self.response_text = tk.Text(text_frame, wrap="word", state="disabled", font=("Consolas", 11))
        self.response_text.pack(fill="both", expand=True, side="left")
        text_scroll = ttk.Scrollbar(text_frame, command=self.response_text.yview)
        text_scroll.pack(side="right", fill="y")
        self.response_text.configure(yscrollcommand=text_scroll.set)

        self._poll()

    def _set_response_text(self, content):
        self.response_text.configure(state="normal")
        self.response_text.delete("1.0", "end")
        self.response_text.insert("1.0", content)
        self.response_text.configure(state="disabled")

    def _on_select(self, event=None):
        sel = self.tree.selection()
        if not sel:
            return
        idx = int(sel[0])
        if idx >= len(self.records):
            return
        self._set_response_text(self.records[idx].get("response_text", ""))

    def _poll(self):
        if not self.winfo_exists():
            return
        current_count = len(self.records)
        if current_count < self._last_count:
            # records were cleared (a new run started) — redraw from scratch
            self.tree.delete(*self.tree.get_children())
            self._last_count = 0
            self._set_response_text("")
            current_count = len(self.records)
        if current_count > self._last_count:
            prev_selection = self.tree.selection()
            prev_last_id = str(self._last_count - 1) if self._last_count > 0 else None
            following_tail = (not prev_selection) or (prev_selection[0] == prev_last_id)

            for i in range(self._last_count, current_count):
                r = self.records[i]
                self.tree.insert("", "end", iid=str(i), values=(
                    r.get("n", ""),
                    r.get("sent_at", ""),
                    r.get("received_at", ""),
                    r.get("status_code", ""),
                    r.get("error_code", ""),
                ))
            self._last_count = current_count

            if following_tail:
                new_last_id = str(current_count - 1)
                self.tree.selection_set(new_last_id)
                self.tree.see(new_last_id)
                self._set_response_text(self.records[current_count - 1].get("response_text", ""))
        self.after(300, self._poll)


class PanelApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("پنل چند حسابی ارسال سفارش — نورآی")
        self.geometry("900x600")

        # لوگوی جاسازی‌شده (base64) — یک نسخه‌ی کامل برای آیکن پنجره، و یک
        # نسخه‌ی کوچک‌شده (subsample) برای نوار عنوان بالای پنل.
        self.logo_full = tk.PhotoImage(data=LOGO_PNG_BASE64)
        self.logo_thumb = self.logo_full.subsample(4, 4)
        try:
            self.iconphoto(True, self.logo_full)
        except tk.TclError:
            pass  # روی بعضی سیستم‌عامل‌ها/نسخه‌های Tcl آیکن پنجره پشتیبانی نمی‌شود؛ صرفاً تزیینی است

        self.accounts = load_accounts()
        self.log_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.workers = []
        self.logs_by_account = {acc["id"]: [] for acc in self.accounts}
        self.open_log_windows = {}  # account id -> LogWindow, so we don't open duplicates

        self._build_ui()
        self._refresh_table()
        self.after(150, self._drain_log)

    def _build_ui(self):
        header = ttk.Frame(self)
        header.pack(fill="x", padx=6, pady=(6, 0))
        ttk.Label(header, image=self.logo_thumb).pack(side="left", padx=(0, 10))
        title_box = ttk.Frame(header)
        title_box.pack(side="left", fill="y")
        ttk.Label(title_box, text="نورآی", font=("Tahoma", 15, "bold")).pack(anchor="w")
        ttk.Label(title_box, text="پنل چند حسابی ارسال سفارش", foreground="#666").pack(anchor="w")
        ttk.Separator(self, orient="horizontal").pack(fill="x", padx=6, pady=(6, 0))

        schedule_frame = ttk.LabelFrame(self, text="زمان‌بندی مشترک همه‌ی حساب‌ها")
        schedule_frame.pack(fill="x", padx=6, pady=(6, 0))

        self.start_time_var = tk.StringVar(value=DEFAULT_START_TIME)
        self.end_time_var = tk.StringVar(value=DEFAULT_END_TIME)

        ttk.Label(schedule_frame, text="زمان شروع (HH:MM:SS.mmm):").pack(side="left", padx=(6, 2), pady=6)
        ttk.Entry(schedule_frame, textvariable=self.start_time_var, width=14).pack(side="left", padx=(0, 12))
        ttk.Label(schedule_frame, text="زمان پایان (HH:MM:SS.mmm):").pack(side="left", padx=(0, 2))
        ttk.Entry(schedule_frame, textvariable=self.end_time_var, width=14).pack(side="left", padx=(0, 6))
        ttk.Label(
            schedule_frame,
            text="از لحظه‌ی شروع تا پایان، هر حساب طبق «وقفه»ی خودش پشت‌سرهم درخواست می‌فرستد.",
            foreground="#666",
        ).pack(side="left", padx=(6, 6))

        test_frame = ttk.LabelFrame(self, text="تست / دیباگ ارسال")
        test_frame.pack(fill="x", padx=6, pady=(6, 0))

        self.test_count_var = tk.StringVar(value=str(DEFAULT_TEST_COUNT))
        self.test_interval_var = tk.StringVar(value=str(DEFAULT_TEST_INTERVAL_MS))

        ttk.Label(test_frame, text="تعداد درخواست:").pack(side="left", padx=(6, 2), pady=6)
        ttk.Entry(test_frame, textvariable=self.test_count_var, width=6).pack(side="left", padx=(0, 12))
        ttk.Label(test_frame, text="فاصله بین درخواست‌ها (ms):").pack(side="left", padx=(0, 2))
        ttk.Entry(test_frame, textvariable=self.test_interval_var, width=8).pack(side="left", padx=(0, 12))
        ttk.Button(test_frame, text="🧪 ارسال تست به حساب‌های فعال", command=self._start_test).pack(side="left", padx=6)
        ttk.Label(
            test_frame,
            text="بلافاصله (بدون توجه به زمان‌بندی بالا) روی همه‌ی حساب‌های فعال اجرا می‌شود — برای تست اتصال/هدر.",
            foreground="#666",
        ).pack(side="left", padx=(6, 6))

        toolbar = ttk.Frame(self)
        toolbar.pack(fill="x", padx=6, pady=6)

        ttk.Button(toolbar, text="افزودن حساب", command=self._add_account).pack(side="left", padx=2)
        ttk.Button(toolbar, text="ویرایش", command=self._edit_account).pack(side="left", padx=2)
        ttk.Button(toolbar, text="حذف", command=self._delete_account).pack(side="left", padx=2)
        ttk.Separator(toolbar, orient="vertical").pack(side="left", fill="y", padx=6)
        ttk.Button(toolbar, text="📖 راهنما", command=self._show_guide).pack(side="left", padx=2)
        ttk.Separator(toolbar, orient="vertical").pack(side="left", fill="y", padx=6)
        self.start_btn = ttk.Button(toolbar, text="▶ شروع همه فعال‌ها", command=self._start_all)
        self.start_btn.pack(side="left", padx=2)
        self.stop_btn = ttk.Button(toolbar, text="⏹ توقف", command=self._stop_all, state="disabled")
        self.stop_btn.pack(side="left", padx=2)

        columns = ("name", "enabled", "broker", "interval", "log")
        headers = ["نام کاربری", "فعال/غیرفعال", "نام کارگزاری", "وقفه (ms)", "لاگ"]
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=10)
        for c, h in zip(columns, headers):
            self.tree.heading(c, text=h)
            self.tree.column(c, width=110 if c != "log" else 100, anchor="center")
        self.tree.pack(fill="x", padx=6, pady=(0, 6))
        self.tree.bind("<Double-1>", self._on_tree_double_click)
        self.tree.bind("<ButtonPress-1>", self._on_tree_press)
        self.tree.bind("<B1-Motion>", self._on_tree_motion)
        self.tree.bind("<ButtonRelease-1>", self._on_tree_release)
        self._drag_iid = None
        self._drag_start_y = None
        self._drag_moved = False

        ttk.Label(
            self,
            text="نکته: روی ستون «فعال/غیرفعال» کلیک کنید تا آن حساب فعال/غیرفعال شود. "
                 "برای تغییر ترتیب حساب‌ها، ردیف را با ماوس بکشید (درگ‌اندروپ).",
            foreground="#666",
        ).pack(fill="x", padx=6, pady=(0, 4))

        log_frame = ttk.LabelFrame(self, text="گزارش زنده")
        log_frame.pack(fill="both", expand=True, padx=6, pady=6)
        self.log_text = tk.Text(log_frame, state="disabled", wrap="none")
        self.log_text.pack(fill="both", expand=True, side="left")
        scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scroll.set)

    def _refresh_table(self):
        self.tree.delete(*self.tree.get_children())
        for idx, acc in enumerate(self.accounts):
            order = acc.get("order", {})
            self.tree.insert("", "end", iid=str(idx), values=(
                acc.get("name", ""),
                "✔" if acc.get("enabled") else "—",
                BROKERS.get(acc.get("broker"), {}).get("label", acc.get("broker")),
                order.get("interval_ms", ""),
                "📄 مشاهده لاگ",
            ))

    def _on_tree_double_click(self, event):
        region = self.tree.identify("region", event.x, event.y)
        row = self.tree.identify_row(event.y)
        if region != "cell" or not row:
            return
        col = self.tree.identify_column(event.x)  # e.g. '#10'
        col_index = int(col.replace("#", "")) - 1
        columns = self.tree["columns"]
        idx = int(row)
        if 0 <= col_index < len(columns) and columns[col_index] == "log":
            self._open_log_window(idx)
        elif 0 <= col_index < len(columns) and columns[col_index] == "enabled":
            pass  # تغییر وضعیت فعال/غیرفعال با یک کلیک ساده انجام می‌شود، نه دابل‌کلیک
        else:
            self._edit_account()

    def _on_tree_press(self, event):
        region = self.tree.identify("region", event.x, event.y)
        row = self.tree.identify_row(event.y) if region == "cell" else None
        self._drag_iid = row
        self._drag_start_y = event.y
        self._drag_moved = False

    def _on_tree_motion(self, event):
        if self._drag_iid is None:
            return
        if self._drag_start_y is not None and abs(event.y - self._drag_start_y) > 4:
            self._drag_moved = True
        if not self._drag_moved:
            return
        target_row = self.tree.identify_row(event.y)
        if target_row and target_row != self._drag_iid:
            self.tree.move(self._drag_iid, "", self.tree.index(target_row))

    def _on_tree_release(self, event):
        if self._drag_iid is None:
            return
        dragged_iid = self._drag_iid
        moved = self._drag_moved
        self._drag_iid = None
        self._drag_start_y = None
        self._drag_moved = False

        if moved:
            # ترتیب بصری جدول تغییر کرده — همان ترتیب را در self.accounts هم
            # اعمال و ذخیره می‌کنیم (هر iid همچنان همان حساب همیشگی‌اش را
            # نشان می‌دهد، فقط جایگاهش در لیست عوض شده).
            try:
                new_order = [self.accounts[int(iid)] for iid in self.tree.get_children("")]
            except (ValueError, IndexError):
                return
            self.accounts = new_order
            save_accounts(self.accounts)
            self._refresh_table()
            return

        # یک کلیک ساده (بدون درگ): اگر روی ستون «فعال/غیرفعال» بود، تغییرش بده.
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        col = self.tree.identify_column(event.x)
        try:
            col_index = int(col.replace("#", "")) - 1
        except ValueError:
            return
        columns = self.tree["columns"]
        if 0 <= col_index < len(columns) and columns[col_index] == "enabled":
            try:
                idx = int(dragged_iid)
            except ValueError:
                return
            self._toggle_enabled(idx)

    def _toggle_enabled(self, idx):
        if not (0 <= idx < len(self.accounts)):
            return
        self.accounts[idx]["enabled"] = not self.accounts[idx].get("enabled", True)
        save_accounts(self.accounts)
        self._refresh_table()
        try:
            self.tree.selection_set(str(idx))
        except tk.TclError:
            pass

    def _open_log_window(self, idx):
        if idx is None or not (0 <= idx < len(self.accounts)):
            return
        acc = self.accounts[idx]
        acc_id = acc["id"]
        records = self.logs_by_account.setdefault(acc_id, [])

        existing = self.open_log_windows.get(acc_id)
        if existing is not None and existing.winfo_exists():
            existing.lift()
            return

        win = LogWindow(self, acc.get("name", ""), records)
        self.open_log_windows[acc_id] = win

    def _selected_index(self):
        sel = self.tree.selection()
        if not sel:
            return None
        return int(sel[0])

    def _show_guide(self):
        win = tk.Toplevel(self)
        win.title("راهنما")
        win.geometry("760x620")

        frame = ttk.Frame(win)
        frame.pack(fill="both", expand=True, padx=6, pady=6)
        text = tk.Text(frame, wrap="word", font=("Consolas", 10))
        text.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(frame, command=text.yview)
        scroll.pack(side="right", fill="y")
        text.configure(yscrollcommand=scroll.set)
        text.insert("1.0", GUIDE_TEXT)
        text.configure(state="disabled")  # read-only but still selectable/copyable

        ttk.Button(win, text="بستن", command=win.destroy).pack(pady=(0, 8))

    def _add_account(self):
        dlg = AccountDialog(self)
        self.wait_window(dlg)
        if dlg.result:
            self.accounts.append(dlg.result)
            save_accounts(self.accounts)
            self._refresh_table()

    def _edit_account(self):
        idx = self._selected_index()
        if idx is None:
            messagebox.showinfo("راهنما", "یک حساب را از جدول انتخاب کنید.")
            return
        dlg = AccountDialog(self, account=self.accounts[idx])
        self.wait_window(dlg)
        if dlg.result:
            self.accounts[idx] = dlg.result
            save_accounts(self.accounts)
            self._refresh_table()

    def _delete_account(self):
        idx = self._selected_index()
        if idx is None:
            return
        if messagebox.askyesno("حذف", "این حساب حذف شود؟"):
            acc_id = self.accounts[idx].get("id")
            del self.accounts[idx]
            save_accounts(self.accounts)
            self.logs_by_account.pop(acc_id, None)
            win = self.open_log_windows.pop(acc_id, None)
            if win is not None and win.winfo_exists():
                win.destroy()
            self._refresh_table()

    def _start_all(self):
        if self.workers and any(w.is_alive() for w in self.workers):
            messagebox.showerror("خطا", "یک اجرا (شروع یا تست) در حال انجام است؛ صبر کنید تمام شود یا آن را متوقف کنید.")
            return

        enabled = [a for a in self.accounts if a.get("enabled")]
        if not enabled:
            messagebox.showinfo("راهنما", "هیچ حساب فعالی وجود ندارد.")
            return

        try:
            start_dt = parse_hms_today(self.start_time_var.get())
            end_dt = parse_hms_today(self.end_time_var.get())
        except InvalidTimeError as e:
            messagebox.showerror("خطا", str(e))
            return
        if end_dt <= start_dt:
            messagebox.showerror("خطا", "زمان پایان باید بعد از زمان شروع باشد.")
            return
        start_ts = start_dt.timestamp()
        end_ts = end_dt.timestamp()

        # هر دو زمان (شروع و پایان) برای «امروز» ساخته می‌شوند. اگر کاربر فیلدهای
        # زمان را به‌روز نکرده باشد (مثلاً مقدار پیش‌فرض نمونه هنوز در فیلد مانده)
        # و الان از کل این بازه گذشته باشد، اجازه‌ی اجرا نمی‌دهیم — چون نتیجه‌اش
        # دقیقاً شبیه دکمه‌ی «تست» (ارسال فوری و بدون صبر) است، نه یک ارسال
        # زمان‌بندی‌شده‌ی واقعی. اگر فقط زمان شروع گذشته ولی پایان هنوز نرسیده،
        # این می‌تواند عمدی باشد (مثلاً اجرای دیرهنگام برنامه)، پس فقط هشدار
        # می‌دهیم و با تأیید صریح کاربر ادامه می‌دهیم.
        now_ts = time.time()
        if end_ts <= now_ts:
            messagebox.showerror(
                "خطا",
                "بازه‌ی زمانی وارد‌شده (شروع و پایان) کاملاً گذشته است.\n"
                "ساعت‌های «زمان شروع» و «زمان پایان» را به‌روز کنید، یا برای ارسال "
                "فوری از بخش «تست / دیباگ ارسال» استفاده کنید.",
            )
            return
        if start_ts <= now_ts:
            if not messagebox.askyesno(
                "هشدار",
                f"زمان شروع ({start_dt.strftime('%H:%M:%S')}) از الان گذشته است.\n"
                "اگر ادامه دهید، ارسال همین الان شروع می‌شود (نه در زمان مدنظرتان) "
                "و تا زمان پایان ادامه پیدا می‌کند.\n\nمطمئنید می‌خواهید ادامه دهید؟",
            ):
                return

        # وقتی بیش از یک حساب هم‌زمان فعال است، لاگ کردن تک‌تک درخواست‌ها در
        # گزارش زنده فایده‌ای ندارد (ترتیب پاسخ‌ها به‌هم می‌ریزد) و فقط سرعت
        # ارسال را کم می‌کند — پس آن حالت را «ساکت» می‌کنیم.
        quiet = len(enabled) > 1

        self.stop_event.clear()
        self.workers = []
        for acc in enabled:
            records = self.logs_by_account.setdefault(acc["id"], [])
            records.clear()  # fresh log each run; open log windows will pick up new entries via polling
            w = OrderWorker(acc, self.log_queue, self.stop_event, records, start_ts, end_ts, quiet)
            self.workers.append(w)
        for w in self.workers:
            w.start()
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self._append_log(
            f"--- شروع اجرا برای {len(enabled)} حساب — بازه {start_dt.strftime('%H:%M:%S')} "
            f"تا {end_dt.strftime('%H:%M:%S')}"
            + (" (گزارش زنده هر درخواست غیرفعال شد چون چند حساب هم‌زمان فعال است) ---" if quiet else " ---")
        )

    def _start_test(self):
        if self.workers and any(w.is_alive() for w in self.workers):
            messagebox.showerror("خطا", "یک اجرا (شروع یا تست) در حال انجام است؛ صبر کنید تمام شود یا آن را متوقف کنید.")
            return

        enabled = [a for a in self.accounts if a.get("enabled")]
        if not enabled:
            messagebox.showinfo("راهنما", "هیچ حساب فعالی وجود ندارد.")
            return

        try:
            count = int(self.test_count_var.get().strip())
            interval_ms = float(self.test_interval_var.get().strip())
            if count <= 0 or interval_ms <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("خطا", "تعداد درخواست باید عدد صحیح مثبت و فاصله باید عدد مثبت (میلی‌ثانیه) باشد.")
            return

        start_ts = time.time()
        # پنجره‌ی پایان صرفاً یک حاشیه‌ی امن است؛ توقف واقعی با شمارنده‌ی
        # count کنترل می‌شود (نگاه کنید به max_requests در OrderWorker).
        end_ts = start_ts + count * (interval_ms / 1000.0) + 5

        self.stop_event.clear()
        self.workers = []
        for acc in enabled:
            records = self.logs_by_account.setdefault(acc["id"], [])
            records.clear()  # لاگ تازه برای هر اجرای تست
            w = OrderWorker(
                acc, self.log_queue, self.stop_event, records, start_ts, end_ts,
                quiet=False, max_requests=count, interval_override_ms=interval_ms,
            )
            self.workers.append(w)
        for w in self.workers:
            w.start()
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self._append_log(
            f"--- تست شروع شد: {count} درخواست با فاصله {interval_ms:.0f}ms برای {len(enabled)} حساب فعال ---"
        )

    def _stop_all(self):
        self.stop_event.set()
        self._append_log("--- درخواست توقف ارسال شد ---")
        self.stop_btn.config(state="disabled")
        self.start_btn.config(state="normal")

    def _drain_log(self):
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self._append_log(msg)
        except queue.Empty:
            pass
        if self.workers and all(not w.is_alive() for w in self.workers):
            self.start_btn.config(state="normal")
            self.stop_btn.config(state="disabled")
            self.workers = []
        self.after(150, self._drain_log)

    def _append_log(self, msg):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")


if __name__ == "__main__":
    app = PanelApp()
    app.mainloop()
