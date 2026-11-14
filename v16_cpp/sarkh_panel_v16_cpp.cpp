// ============================================================================
// sarkh_engine — موتور ارسال «بدون رابط» (headless) برای sarkh panel v15، به C++
//
//  • همان accounts.json پنل پایتون را می‌خواند (curl کامل + وقفه/تریگر/تعداد و ...)
//  • همان منطق: زمان‌بندی تریگری، اتصال گرم (هر شلیک یک اتصال TLS از‌قبل‌آماده)،
//    زنجیره‌ی تیکت نهایت‌نگر، دیباگ بای والیوم، توقف بعد از ثبت، خلاصه‌ی نتیجه
//  • فرق اصلی: زمان‌بندی با انتظار فعال (spin) روی CLOCK_REALTIME، چند ریسمان ارسال
//    قابل pin روی هسته، و ثبت «تأخیر ارسال» (late_us) برای هر درخواست
//
// ساخت:  make          اجرا:  ./sarkh_engine run --accounts accounts.json --base 08:44:50
// ============================================================================
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>
#include <zlib.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "json.hpp"

using ojson = nlohmann::ordered_json;
typedef int64_t i64;
using KV = std::vector<std::pair<std::string, std::string>>;

static const char* ENGINE_VERSION = "sarkh_engine (C++ port of sarkh panel v15)";

// ---------------------------------------------------------------------------
// ثابت‌ها (همان مقدارهای پنل پایتون)
// ---------------------------------------------------------------------------
static const double MIN_INTERVAL_MS = 300.0;
static const int WARM_MIN_LEAD_MS = 300;
static const int WARM_AUTO_DEFAULT_MS = 600;
static const int WARM_MAX_LEAD_MS = 3000;
static const double WARM_FIRST_LEAD_S = 3.0;
static const int WARM_STAGGER_MAX_MS = 60;
static const double TICKET_POLL_SECONDS = 0.3;
static const double TICKET_LEAD_SECONDS = 3.0;
static const double TICKET_WATCH_INTERVAL = 0.08;
static const double TICKET_EXTRA_WAIT = 0.25;
static const size_t MAX_RESPONSE_CHARS = 20000;
static const double IO_TIMEOUT_S = 5.0;

// ---------------------------------------------------------------------------
// ابزارهای کوچک
// ---------------------------------------------------------------------------
static inline i64 now_ns() {
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (i64)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}
static inline void cpu_relax() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#endif
}
static std::string lower(std::string s) {
    for (auto& c : s) c = (char)tolower((unsigned char)c);
    return s;
}
static std::string trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && isspace((unsigned char)s[a])) a++;
    while (b > a && isspace((unsigned char)s[b - 1])) b--;
    return s.substr(a, b - a);
}
static bool starts_with(const std::string& s, const std::string& p) { return s.compare(0, p.size(), p) == 0; }
static bool contains(const std::string& s, const std::string& p) { return s.find(p) != std::string::npos; }

static void kv_set(KV& kv, const std::string& k, const std::string& v) {
    for (auto& p : kv)
        if (p.first == k) { p.second = v; return; }
    kv.emplace_back(k, v);
}
static bool kv_has_key(const KV& kv, const std::string& k) {
    for (auto& p : kv) if (p.first == k) return true;
    return false;
}
static bool kv_has_ci(const KV& kv, const std::string& lk) {
    for (auto& p : kv) if (lower(p.first) == lk) return true;
    return false;
}
static std::string kv_get_ci(const KV& kv, const std::string& lk) {
    for (auto& p : kv) if (lower(p.first) == lk) return p.second;
    return "";
}

static void append_utf8(std::string& out, uint32_t cp) {
    if (cp < 0x80) out += (char)cp;
    else if (cp < 0x800) { out += (char)(0xC0 | (cp >> 6)); out += (char)(0x80 | (cp & 0x3F)); }
    else if (cp < 0x10000) { out += (char)(0xE0 | (cp >> 12)); out += (char)(0x80 | ((cp >> 6) & 0x3F)); out += (char)(0x80 | (cp & 0x3F)); }
    else { out += (char)(0xF0 | (cp >> 18)); out += (char)(0x80 | ((cp >> 12) & 0x3F)); out += (char)(0x80 | ((cp >> 6) & 0x3F)); out += (char)(0x80 | (cp & 0x3F)); }
}
// حداکثر n «نویسه» (نه بایت) از ابتدای رشته‌ی UTF-8
static std::string utf8_prefix(const std::string& s, size_t n) {
    size_t i = 0, chars = 0;
    while (i < s.size() && chars < n) {
        unsigned char c = (unsigned char)s[i];
        size_t len = c < 0x80 ? 1 : (c >> 5) == 6 ? 2 : (c >> 4) == 14 ? 3 : (c >> 3) == 30 ? 4 : 1;
        i += len; chars++;
    }
    return s.substr(0, std::min(i, s.size()));
}
static std::string fmt_clock(i64 ns, bool micro = false) {
    time_t sec = (time_t)(ns / 1000000000LL);
    i64 rem = ns % 1000000000LL;
    struct tm t;
    localtime_r(&sec, &t);
    char buf[48];
    if (micro) snprintf(buf, sizeof buf, "%02d:%02d:%02d.%06d", t.tm_hour, t.tm_min, t.tm_sec, (int)(rem / 1000));
    else snprintf(buf, sizeof buf, "%02d:%02d:%02d.%03d", t.tm_hour, t.tm_min, t.tm_sec, (int)(rem / 1000000));
    return buf;
}
static std::string fmt_stamp(i64 ns) {
    time_t sec = (time_t)(ns / 1000000000LL);
    struct tm t;
    localtime_r(&sec, &t);
    char buf[48];
    strftime(buf, sizeof buf, "%Y%m%d_%H%M%S", &t);
    return buf;
}
static std::string num_str(double v) {
    char buf[64];
    if (v == std::floor(v) && std::fabs(v) < 1e15) snprintf(buf, sizeof buf, "%lld", (long long)v);
    else snprintf(buf, sizeof buf, "%g", v);
    return buf;
}

// «HH:MM:SS[.mmm]» برای امروزِ ساعت محلی
static bool parse_hms_today(const std::string& text, i64& out_ns, std::string& err) {
    static const std::regex re(R"(^(\d{1,2}):(\d{1,2}):(\d{1,2})(?:[.,](\d{1,6}))?$)");
    std::smatch m;
    std::string t = trim(text);
    if (!std::regex_match(t, m, re)) { err = "زمان «" + t + "» باید HH:MM:SS یا HH:MM:SS.mmm باشد"; return false; }
    int h = std::stoi(m[1]), mi = std::stoi(m[2]), s = std::stoi(m[3]);
    long micro = 0;
    if (m[4].matched) { std::string f = m[4].str() + "000000"; micro = std::stol(f.substr(0, 6)); }
    if (h > 23 || mi > 59 || s > 59) { err = "زمان «" + t + "» خارج از محدوده است"; return false; }
    time_t now = time(nullptr);
    struct tm tmv;
    localtime_r(&now, &tmv);
    tmv.tm_hour = h; tmv.tm_min = mi; tmv.tm_sec = s;
    time_t base = mktime(&tmv);
    out_ns = (i64)base * 1000000000LL + (i64)micro * 1000LL;
    return true;
}

// ---------------------------------------------------------------------------
// پارس curl  (معادل parse_captured_curl پایتون: $'..' و shlex و پرچم‌ها)
// ---------------------------------------------------------------------------
struct ParsedReq {
    std::string method;      // حروف کوچک مثل پایتون
    std::string url;
    KV headers, cookies;
    bool has_json = false;
    ojson json_body;
    bool has_data = false;
    std::string data;
};

static std::string decode_ansi_c(const std::string& in) {
    std::string out;
    size_t i = 0, n = in.size();
    while (i < n) {
        char c = in[i];
        if (c == '\\' && i + 1 < n) {
            char nx = in[i + 1];
            switch (nx) {
                case 'n': out += '\n'; i += 2; continue;
                case 't': out += '\t'; i += 2; continue;
                case 'r': out += '\r'; i += 2; continue;
                case 'a': out += '\a'; i += 2; continue;
                case 'b': out += '\b'; i += 2; continue;
                case 'f': out += '\f'; i += 2; continue;
                case 'v': out += '\v'; i += 2; continue;
                case '\\': out += '\\'; i += 2; continue;
                case '\'': out += '\''; i += 2; continue;
                case '"': out += '"'; i += 2; continue;
                case 'e': out += '\x1b'; i += 2; continue;
                default: break;
            }
            auto hex = [&](size_t from, size_t len, uint32_t& v) {
                if (from + len > n) return false;
                v = 0;
                for (size_t j = 0; j < len; j++) {
                    char h = in[from + j];
                    int d = isdigit((unsigned char)h) ? h - '0' : (h >= 'a' && h <= 'f') ? h - 'a' + 10 : (h >= 'A' && h <= 'F') ? h - 'A' + 10 : -1;
                    if (d < 0) return false;
                    v = v * 16 + d;
                }
                return true;
            };
            uint32_t v;
            if (nx == 'x' && hex(i + 2, 2, v)) { out += (char)v; i += 4; continue; }   // بایت خام مثل bash
            if (nx == 'u' && hex(i + 2, 4, v)) { append_utf8(out, v); i += 6; continue; }
            if (nx == 'U' && hex(i + 2, 8, v)) { append_utf8(out, v); i += 10; continue; }
        }
        out += c;
        i++;
    }
    return out;
}

static std::string preprocess_curl_text(const std::string& text) {
    // برگرداندن «\<newline>» به فاصله
    std::string t;
    for (size_t i = 0; i < text.size(); i++) {
        if (text[i] == '\\' && i + 1 < text.size() && (text[i + 1] == '\n' || (text[i + 1] == '\r' && i + 2 < text.size() && text[i + 2] == '\n'))) {
            t += ' ';
            i += (text[i + 1] == '\r') ? 2 : 1;
            continue;
        }
        t += text[i];
    }
    // $'...' → '...' (با رمزگشایی)
    std::string out;
    size_t i = 0;
    while (i < t.size()) {
        if (t[i] == '$' && i + 1 < t.size() && t[i + 1] == '\'') {
            size_t j = i + 2;
            bool closed = false;
            while (j < t.size()) {
                if (t[j] == '\\' && j + 1 < t.size()) { j += 2; continue; }
                if (t[j] == '\'') { closed = true; break; }
                j++;
            }
            if (closed) {
                std::string dec = decode_ansi_c(t.substr(i + 2, j - (i + 2)));
                out += '\'';
                for (char ch : dec) { if (ch == '\'') out += "'\\''"; else out += ch; }
                out += '\'';
                i = j + 1;
                continue;
            }
        }
        out += t[i++];
    }
    return out;
}

// معادل shlex.split(posix=True)
static bool shlex_split(const std::string& s, std::vector<std::string>& out, std::string& err) {
    size_t i = 0, n = s.size();
    std::string cur;
    bool have = false;
    while (i < n) {
        char c = s[i];
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            if (have) { out.push_back(cur); cur.clear(); have = false; }
            i++;
        } else if (c == '\\') {
            if (i + 1 >= n) { err = "No escaped character"; return false; }
            cur += s[i + 1]; have = true; i += 2;
        } else if (c == '\'') {
            size_t j = s.find('\'', i + 1);
            if (j == std::string::npos) { err = "No closing quotation"; return false; }
            cur += s.substr(i + 1, j - i - 1); have = true; i = j + 1;
        } else if (c == '"') {
            size_t j = i + 1;
            bool closed = false;
            while (j < n) {
                if (s[j] == '\\' && j + 1 < n && (s[j + 1] == '"' || s[j + 1] == '\\')) { cur += s[j + 1]; j += 2; continue; }
                if (s[j] == '"') { closed = true; break; }
                cur += s[j++];
            }
            if (!closed) { err = "No closing quotation"; return false; }
            have = true; i = j + 1;
        } else {
            cur += c; have = true; i++;
        }
    }
    if (have) out.push_back(cur);
    return true;
}

static bool parse_captured_curl(const std::string& raw, ParsedReq& pr, std::string& err) {
    std::string text = trim(raw);
    if (text.empty()) { err = "متن curl خالی است."; return false; }
    // خطوط import (خروجی مبدل پایتون) نادیده گرفته می‌شوند ولی فقط curl پشتیبانی می‌شود
    text = preprocess_curl_text(text);
    std::vector<std::string> tokens;
    std::string e2;
    if (!shlex_split(text, tokens, e2)) { err = "متن curl قابل خواندن نیست: " + e2; return false; }
    if (tokens.empty()) { err = "متن باید با دستور curl شروع شود."; return false; }
    std::string t0 = lower(tokens[0]);
    if (t0 != "curl" && t0 != "curl.exe") { err = "متن باید با دستور curl شروع شود (کد پایتونِ تبدیل‌شده در موتور C++ پشتیبانی نمی‌شود)."; return false; }
    tokens.erase(tokens.begin());

    static const std::map<std::string, std::string> with_value = {
        {"-H", "header"}, {"--header", "header"}, {"-b", "cookie"}, {"--cookie", "cookie"},
        {"-X", "method"}, {"--request", "method"}, {"-d", "data"}, {"--data", "data"}, {"--data-raw", "data"},
        {"--data-binary", "data"}, {"--data-ascii", "data"}, {"--data-urlencode", "data"},
        {"-A", "ua"}, {"--user-agent", "ua"}, {"-e", "referer"}, {"--referer", "referer"}, {"--url", "url"}};
    static const std::set<std::string> ign_val = {"-u", "--user", "--proxy", "--connect-timeout", "--max-time", "--resolve",
                                                  "--limit-rate", "--cert", "--key", "--cacert", "-o", "--output", "--range"};
    static const std::set<std::string> ign_novalue = {"--compressed", "-k", "--insecure", "-s", "--silent", "-v", "--verbose",
                                                      "-i", "--include", "-L", "--location", "-G", "--get", "-4", "-6"};
    std::string url, method;
    bool have_url = false, have_method = false;
    std::vector<std::string> data_parts;
    size_t i = 0;
    while (i < tokens.size()) {
        const std::string& tok = tokens[i];
        auto it = with_value.find(tok);
        if (it != with_value.end()) {
            if (i + 1 >= tokens.size()) { err = "بعد از «" + tok + "» مقداری پیدا نشد."; return false; }
            const std::string val = tokens[i + 1];
            i += 2;
            const std::string& kind = it->second;
            if (kind == "header") {
                size_t p = val.find(':');
                if (p != std::string::npos) kv_set(pr.headers, trim(val.substr(0, p)), trim(val.substr(p + 1)));
            } else if (kind == "cookie") {
                std::stringstream ss(val);
                std::string part;
                while (std::getline(ss, part, ';')) {
                    part = trim(part);
                    size_t p = part.find('=');
                    if (p != std::string::npos) kv_set(pr.cookies, trim(part.substr(0, p)), trim(part.substr(p + 1)));
                }
            } else if (kind == "method") { method = lower(val); have_method = true; }
            else if (kind == "data") data_parts.push_back(val);
            else if (kind == "ua") { if (!kv_has_key(pr.headers, "User-Agent")) pr.headers.emplace_back("User-Agent", val); }
            else if (kind == "referer") { if (!kv_has_key(pr.headers, "Referer")) pr.headers.emplace_back("Referer", val); }
            else if (kind == "url") { url = val; have_url = true; }
            continue;
        }
        if (ign_val.count(tok)) { i += 2; continue; }
        if (ign_novalue.count(tok)) { i += 1; continue; }
        if (!tok.empty() && tok[0] == '-' && tok != "-") { i += 1; continue; }
        if (!have_url) { url = tok; have_url = true; }
        i += 1;
    }
    if (!have_url || url.empty()) { err = "آدرس (URL) در متن curl پیدا نشد."; return false; }
    // هدر Cookie → cookies (setdefault)
    for (size_t h = 0; h < pr.headers.size();) {
        if (lower(pr.headers[h].first) == "cookie") {
            std::stringstream ss(pr.headers[h].second);
            std::string part;
            while (std::getline(ss, part, ';')) {
                part = trim(part);
                size_t p = part.find('=');
                if (p != std::string::npos) {
                    std::string ck = trim(part.substr(0, p));
                    if (!kv_has_key(pr.cookies, ck)) pr.cookies.emplace_back(ck, trim(part.substr(p + 1)));
                }
            }
            pr.headers.erase(pr.headers.begin() + h);
        } else h++;
    }
    bool have_body = !data_parts.empty();
    std::string body_text;
    for (size_t d = 0; d < data_parts.size(); d++) { if (d) body_text += "&"; body_text += data_parts[d]; }
    pr.method = have_method ? method : (have_body ? "post" : "get");
    pr.url = url;
    if (have_body) {
        std::string stripped = trim(body_text);
        std::string ctype = lower(kv_get_ci(pr.headers, "content-type"));
        bool looks_json = !stripped.empty() && (stripped[0] == '{' || stripped[0] == '[');
        if (looks_json && (contains(ctype, "json") || ctype.empty())) {
            ojson j = ojson::parse(stripped, nullptr, false);
            if (!j.is_discarded()) { pr.has_json = true; pr.json_body = std::move(j); }
            else { pr.has_data = true; pr.data = body_text; }
        } else { pr.has_data = true; pr.data = body_text; }
    }
    return true;
}

// ---------------------------------------------------------------------------
// json.dumps(..., allow_nan=False) پایتون: جداکننده‌های «, » و «: »، ensure_ascii=True
// ---------------------------------------------------------------------------
static void py_escape_string(const std::string& s, std::string& out) {
    out += '"';
    size_t i = 0, n = s.size();
    char buf[16];
    auto emit_u = [&](uint32_t u) { snprintf(buf, sizeof buf, "\\u%04x", u); out += buf; };
    while (i < n) {
        unsigned char c = (unsigned char)s[i];
        uint32_t cp = c;
        size_t len = 1;
        if (c >= 0xF0 && i + 3 < n + 0) { len = 4; cp = ((c & 7) << 18) | (((unsigned char)s[i + 1] & 0x3F) << 12) | (((unsigned char)s[i + 2] & 0x3F) << 6) | ((unsigned char)s[i + 3] & 0x3F); }
        else if (c >= 0xE0 && i + 2 < n + 0) { len = 3; cp = ((c & 15) << 12) | (((unsigned char)s[i + 1] & 0x3F) << 6) | ((unsigned char)s[i + 2] & 0x3F); }
        else if (c >= 0xC0 && i + 1 < n + 0) { len = 2; cp = ((c & 31) << 6) | ((unsigned char)s[i + 1] & 0x3F); }
        i += len;
        if (cp == '"') out += "\\\"";
        else if (cp == '\\') out += "\\\\";
        else if (cp == '\n') out += "\\n";
        else if (cp == '\r') out += "\\r";
        else if (cp == '\t') out += "\\t";
        else if (cp == '\b') out += "\\b";
        else if (cp == '\f') out += "\\f";
        else if (cp >= 0x20 && cp <= 0x7e) out += (char)cp;
        else if (cp >= 0x10000) { cp -= 0x10000; emit_u(0xD800 | (cp >> 10)); emit_u(0xDC00 | (cp & 0x3FF)); }
        else emit_u(cp);
    }
    out += '"';
}
static void py_dumps(const ojson& j, std::string& out) {
    switch (j.type()) {
        case ojson::value_t::null: out += "null"; break;
        case ojson::value_t::boolean: out += j.get<bool>() ? "true" : "false"; break;
        case ojson::value_t::string: py_escape_string(j.get_ref<const std::string&>(), out); break;
        case ojson::value_t::array: {
            out += '[';
            bool first = true;
            for (auto& e : j) { if (!first) out += ", "; first = false; py_dumps(e, out); }
            out += ']';
            break;
        }
        case ojson::value_t::object: {
            out += '{';
            bool first = true;
            for (auto it = j.begin(); it != j.end(); ++it) {
                if (!first) out += ", ";
                first = false;
                py_escape_string(it.key(), out);
                out += ": ";
                py_dumps(it.value(), out);
            }
            out += '}';
            break;
        }
        default: out += j.dump(); break;   // عددها
    }
}
static std::string py_dumps_str(const ojson& j) { std::string s; py_dumps(j, s); return s; }

// ---------------------------------------------------------------------------
// خلاصه‌ی پاسخ (parse_response / extract_error_code / summary / is_success)
// ---------------------------------------------------------------------------
static const char* ERROR_CODE_FIELDS[] = {"errorCode", "ErrorCode", "errCode", "ErrCode", "resultCode", "ResultCode", "error_code", "err_code"};
static const char* MSG_KEYS[] = {"detail", "message", "msg", "errorMessage", "error_description", "description", "title"};

static std::string py_str(const ojson& v) {     // str(value) پایتون، تقریبی
    if (v.is_string()) return v.get<std::string>();
    if (v.is_boolean()) return v.get<bool>() ? "True" : "False";
    if (v.is_null()) return "None";
    return v.dump();
}
static bool json_nonempty(const ojson& v) { return !(v.is_null() || (v.is_string() && v.get<std::string>().empty())); }

static std::string extract_error_code(const ojson& data) {
    if (!data.is_object()) return "";
    for (const char* k : ERROR_CODE_FIELDS) {
        auto it = data.find(k);
        if (it != data.end() && json_nonempty(*it)) return py_str(*it);
    }
    for (auto it = data.begin(); it != data.end(); ++it) {
        std::string lk = lower(it.key());
        if (contains(lk, "error") && contains(lk, "code") && json_nonempty(it.value())) return py_str(it.value());
    }
    return "";
}
static std::string first_text(const ojson& d) {
    for (const char* k : MSG_KEYS) {
        auto it = d.find(k);
        if (it != d.end() && it->is_string()) { std::string t = trim(it->get<std::string>()); if (!t.empty()) return t; }
    }
    return "";
}
struct RespInfo { ojson data; bool has_json = false; std::string message, code; };
static RespInfo parse_response(const std::string& text) {
    RespInfo r;
    ojson j = ojson::parse(text, nullptr, false);
    if (!j.is_discarded()) { r.data = std::move(j); r.has_json = true; }
    if (r.has_json && r.data.is_object()) {
        r.message = first_text(r.data);
        if (r.message.empty()) {
            for (const char* ik : {"error", "errors", "data"}) {
                auto it = r.data.find(ik);
                if (it == r.data.end()) continue;
                if (it->is_object()) r.message = first_text(*it);
                else if (it->is_string()) r.message = trim(it->get<std::string>());
                if (!r.message.empty()) break;
            }
        }
    } else if (!r.has_json && !text.empty()) {
        std::string norm;
        bool prev_space = true;
        for (char c : text) {
            bool sp = isspace((unsigned char)c);
            if (sp) { if (!prev_space) norm += ' '; prev_space = true; }
            else { norm += c; prev_space = false; }
        }
        while (!norm.empty() && norm.back() == ' ') norm.pop_back();
        r.message = utf8_prefix(norm, 150);
    }
    static const std::regex code_re(
        R"(^\s*(?:کد\s*خطا|error\s*code|code)\s*(?:[:=]|：)?\s*(\d{1,6})\s*(?:[-:]|–|—)?\s*)", std::regex::icase);
    std::smatch m;
    if (!r.message.empty() && std::regex_search(r.message, m, code_re)) {
        r.code = m[1].str();
        std::string rest = trim(r.message.substr(m.length(0)));
        if (!rest.empty()) r.message = rest;
    }
    return r;
}
static const ojson* find_order_ticket(const ojson& data) {
    if (!data.is_object()) return nullptr;
    auto d = data.find("data");
    if (d != data.end() && d->is_object()) {
        auto t = d->find("orderTicket");
        if (t != d->end() && t->is_object()) return &*t;
    }
    auto t = data.find("orderTicket");
    if (t != data.end() && t->is_object()) return &*t;
    return nullptr;
}
static ojson extract_order_ticket(const std::string& text) {      // null اگر نبود
    ojson j = ojson::parse(text, nullptr, false);
    if (j.is_discarded()) return ojson(nullptr);
    const ojson* t = find_order_ticket(j);
    return t ? *t : ojson(nullptr);
}
static bool status_ok(int st) { return st >= 200 && st < 300; }

struct Summary { std::string shown, message; bool success = false; };
static Summary summarize_generic(int status, const std::string& text) {
    RespInfo r = parse_response(text);
    std::string code = extract_error_code(r.data);
    if (code.empty()) code = r.code;
    Summary s;
    s.shown = code; s.message = r.message;
    s.success = status_ok(status) && code.empty();
    return s;
}
static Summary summarize_nahayatnegar(int status, const std::string& text) {
    RespInfo r = parse_response(text);
    std::string code = extract_error_code(r.data);
    if (code.empty()) code = r.code;
    Summary s;
    s.message = r.message;
    bool ok = status_ok(status);
    bool has_ticket = r.has_json && find_order_ticket(r.data) != nullptr;
    s.success = ok && has_ticket;
    if (s.success) s.shown = "✔ ثبت شد";
    else if (!code.empty()) s.shown = code;
    else s.shown = (ok ? "؟ HTTP " : "HTTP ") + std::to_string(status);
    return s;
}

// ---------------------------------------------------------------------------
// دیباگ بای والیوم
// ---------------------------------------------------------------------------
struct Num { bool ok = false; bool is_float = false; bool is_str = false; double d = 0; long long i = 0; };
static Num as_number(const ojson& v) {
    Num n;
    if (v.is_boolean()) return n;
    if (v.is_number_integer()) { n.ok = true; n.i = v.get<long long>(); n.d = (double)n.i; return n; }
    if (v.is_number_float()) { n.ok = true; n.is_float = true; n.d = v.get<double>(); return n; }
    if (v.is_string()) {
        std::string t = trim(v.get_ref<const std::string&>());
        if (!t.empty() && std::all_of(t.begin(), t.end(), [](char c) { return isdigit((unsigned char)c); })) {
            n.ok = true; n.is_str = true; n.i = std::stoll(t); n.d = (double)n.i;
        }
    }
    return n;
}
static ojson with_number(const ojson& old, double num) {
    Num k = as_number(old);
    if (k.is_float) return ojson(num);
    if (k.is_str) return ojson(std::to_string((long long)std::llround(num)));
    return ojson((long long)std::llround(num));
}
typedef std::vector<std::string> PathKeys;   // کلید دیکشنری یا شاخص لیست به‌صورت رشته با پیشوند
static void find_volume_paths(const ojson& obj, const std::set<std::string>& wanted, std::vector<std::vector<ojson>>& out, std::vector<ojson> path) {
    if (obj.is_object()) {
        for (auto it = obj.begin(); it != obj.end(); ++it) {
            if (wanted.count(lower(it.key())) && as_number(it.value()).ok) { auto p = path; p.push_back(it.key()); out.push_back(p); }
            else if (it.value().is_object() || it.value().is_array()) { auto p = path; p.push_back(it.key()); find_volume_paths(it.value(), wanted, out, p); }
        }
    } else if (obj.is_array()) {
        for (size_t i = 0; i < obj.size(); i++)
            if (obj[i].is_object() || obj[i].is_array()) { auto p = path; p.push_back((long long)i); find_volume_paths(obj[i], wanted, out, p); }
    }
}
static ojson* at_path(ojson& root, const std::vector<ojson>& path, size_t upto) {
    ojson* cur = &root;
    for (size_t i = 0; i < upto; i++) {
        if (path[i].is_string()) cur = &(*cur)[path[i].get<std::string>()];
        else cur = &(*cur)[path[i].get<size_t>()];
    }
    return cur;
}
struct VolumeVariants { std::vector<ojson> bodies; std::vector<std::string> form_bodies; std::vector<long long> vols; long long base = 0; };

static bool build_volume_variants(const ojson& body, const std::set<std::string>& wanted, const std::string& total_key, int n_total,
                                  VolumeVariants& out, std::string& reason) {
    std::vector<std::vector<ojson>> paths;
    find_volume_paths(body, wanted, paths, {});
    if (paths.empty()) { reason = "هیچ فیلد حجمی در بدنه پیدا نشد (نام فیلد را در «volume_fields» بنویس)"; return false; }
    ojson tmp = body;
    Num b = as_number(*at_path(tmp, paths[0], paths[0].size()));
    long long base = (long long)b.d;
    if (base < n_total) { reason = "حجم پایه (" + std::to_string(base) + ") کمتر از تعداد درخواست‌هاست (" + std::to_string(n_total) + ")"; return false; }
    out.base = base;
    for (int k = 1; k <= n_total; k++) {
        int d = n_total - k;
        ojson v = body;
        for (auto& p : paths) {
            ojson* slot = at_path(v, p, p.size());
            Num old = as_number(*slot);
            *slot = with_number(*slot, old.d - d);
        }
        if (!total_key.empty() && v.is_object()) {
            auto it = v.find(total_key);
            if (it != v.end() && as_number(*it).ok) {
                double tot = as_number(*it).d;
                double scaled = std::nearbyint(tot * (double)(base - d) / (double)base);   // round-half-even مثل پایتون
                *it = with_number(*it, scaled);
            }
        }
        out.bodies.push_back(std::move(v));
        out.vols.push_back(base - d);
    }
    return true;
}
static bool build_volume_variants_form(const std::string& data, const std::set<std::string>& wanted, int n_total,
                                       VolumeVariants& out, std::string& reason) {
    std::vector<std::string> segs;
    size_t pos = 0;
    while (true) {
        size_t amp = data.find('&', pos);
        segs.push_back(data.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos));
        if (amp == std::string::npos) break;
        pos = amp + 1;
    }
    struct Hit { size_t idx; std::string key; long long val; };
    std::vector<Hit> hits;
    for (size_t i = 0; i < segs.size(); i++) {
        size_t eq = segs[i].find('=');
        if (eq == std::string::npos || eq == 0 || segs[i].find('=', eq + 1) != std::string::npos) continue;
        std::string key = segs[i].substr(0, eq), val = segs[i].substr(eq + 1);
        if (val.empty() || !std::all_of(val.begin(), val.end(), [](char c) { return isdigit((unsigned char)c); })) continue;
        if (!wanted.count(lower(key))) continue;
        hits.push_back({i, key, std::stoll(val)});
    }
    if (hits.empty()) { reason = "هیچ فیلد حجمی در بدنه‌ی فرم پیدا نشد"; return false; }
    long long base = hits[0].val;
    if (base < n_total) { reason = "حجم پایه (" + std::to_string(base) + ") کمتر از تعداد درخواست‌هاست (" + std::to_string(n_total) + ")"; return false; }
    out.base = base;
    for (int k = 1; k <= n_total; k++) {
        int d = n_total - k;
        std::vector<std::string> s2 = segs;
        for (auto& h : hits) s2[h.idx] = h.key + "=" + std::to_string(h.val - d);
        std::string joined;
        for (size_t i = 0; i < s2.size(); i++) { if (i) joined += "&"; joined += s2[i]; }
        out.form_bodies.push_back(joined);
        out.vols.push_back(base - d);
    }
    return true;
}

// ---------------------------------------------------------------------------
// URL و میزبان
// ---------------------------------------------------------------------------
struct Url {
    bool ok = false, https = false;
    std::string host, target, host_header;
    int port = 80;
};
static bool parse_url(const std::string& url, Url& u) {
    size_t p = url.find("://");
    if (p == std::string::npos) return false;
    std::string scheme = lower(url.substr(0, p));
    if (scheme != "http" && scheme != "https") return false;
    u.https = scheme == "https";
    size_t a = p + 3;
    size_t e = url.find_first_of("/?#", a);
    std::string auth = url.substr(a, e == std::string::npos ? std::string::npos : e - a);
    std::string rest = e == std::string::npos ? "" : url.substr(e);
    size_t at = auth.rfind('@');
    if (at != std::string::npos) auth = auth.substr(at + 1);
    u.port = u.https ? 443 : 80;
    if (!auth.empty() && auth[0] == '[') {
        size_t rb = auth.find(']');
        if (rb == std::string::npos) return false;
        u.host = auth.substr(1, rb - 1);
        if (rb + 1 < auth.size() && auth[rb + 1] == ':') u.port = atoi(auth.c_str() + rb + 2);
    } else {
        size_t c = auth.find(':');
        if (c != std::string::npos) { u.host = auth.substr(0, c); u.port = atoi(auth.c_str() + c + 1); }
        else u.host = auth;
    }
    if (u.host.empty() || u.port <= 0) return false;
    size_t hash = rest.find('#');
    if (hash != std::string::npos) rest = rest.substr(0, hash);
    size_t q = rest.find('?');
    std::string path = q == std::string::npos ? rest : rest.substr(0, q);
    std::string query = q == std::string::npos ? "" : rest.substr(q + 1);
    if (path.empty()) path = "/";
    u.target = path + (query.empty() ? "" : "?" + query);
    std::string shown = contains(u.host, ":") ? "[" + u.host + "]" : u.host;
    u.host_header = (u.port == (u.https ? 443 : 80)) ? shown : shown + ":" + std::to_string(u.port);
    u.ok = true;
    return true;
}

struct HostInfo {
    Url u;
    sockaddr_storage addr{};
    socklen_t alen = 0;
    int family = 0;
    bool resolved = false;
    std::string ip;
    bool is_ip_literal = false;
};
static void resolve_host(HostInfo& h) {
    addrinfo hints{}, *res = nullptr;
    hints.ai_socktype = SOCK_STREAM;
    char port[16];
    snprintf(port, sizeof port, "%d", h.u.port);
    unsigned char tmp[16];
    h.is_ip_literal = inet_pton(AF_INET, h.u.host.c_str(), tmp) == 1 || inet_pton(AF_INET6, h.u.host.c_str(), tmp) == 1;
    if (getaddrinfo(h.u.host.c_str(), port, &hints, &res) != 0 || !res) return;
    addrinfo* pick = res;
    for (addrinfo* r = res; r; r = r->ai_next)
        if (r->ai_family == AF_INET) { pick = r; break; }
    memcpy(&h.addr, pick->ai_addr, pick->ai_addrlen);
    h.alen = pick->ai_addrlen;
    h.family = pick->ai_family;
    char ipb[64] = {0};
    if (pick->ai_family == AF_INET) inet_ntop(AF_INET, &((sockaddr_in*)pick->ai_addr)->sin_addr, ipb, sizeof ipb);
    else inet_ntop(AF_INET6, &((sockaddr_in6*)pick->ai_addr)->sin6_addr, ipb, sizeof ipb);
    h.ip = ipb;
    h.resolved = true;
    freeaddrinfo(res);
}

// ---------------------------------------------------------------------------
// TLS و اتصال
// ---------------------------------------------------------------------------
static SSL_CTX* g_ctx = nullptr;
static bool g_insecure = false;

static void init_tls(bool insecure, const std::string& cafile) {
    g_insecure = insecure;
    g_ctx = SSL_CTX_new(TLS_client_method());
    SSL_CTX_set_min_proto_version(g_ctx, TLS1_2_VERSION);
    SSL_CTX_set_mode(g_ctx, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER | SSL_MODE_ENABLE_PARTIAL_WRITE);
    if (insecure) SSL_CTX_set_verify(g_ctx, SSL_VERIFY_NONE, nullptr);
    else {
        if (!cafile.empty()) SSL_CTX_load_verify_locations(g_ctx, cafile.c_str(), nullptr);
        else SSL_CTX_set_default_verify_paths(g_ctx);
        SSL_CTX_set_verify(g_ctx, SSL_VERIFY_PEER, nullptr);
    }
}

struct Conn {
    int fd = -1;
    SSL* ssl = nullptr;
};
static void conn_close(Conn& c) {
    if (c.ssl) { SSL_free(c.ssl); c.ssl = nullptr; }
    if (c.fd >= 0) { close(c.fd); c.fd = -1; }
}
static void set_nonblock(int fd) { fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK); }
static void set_timeouts(int fd, double s) {
    timeval tv;
    tv.tv_sec = (time_t)s;
    tv.tv_usec = (suseconds_t)((s - (double)tv.tv_sec) * 1e6);
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
}
static std::string ssl_err_string() {
    unsigned long e = ERR_get_error();
    if (!e) return "";
    char buf[256];
    ERR_error_string_n(e, buf, sizeof buf);
    return buf;
}

// TCP + (TLS)؛ سوکت در حالت «مسدودکننده با timeout» برمی‌گردد
static bool open_conn(const HostInfo& h, Conn& c, double* tcp_ms, double* tls_ms, std::string& err) {
    HostInfo hh = h;
    if (!hh.resolved) { resolve_host(hh); if (!hh.resolved) { err = "DNS حل نشد: " + h.u.host; return false; } }
    int fd = socket(hh.family, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) { err = std::string("socket: ") + strerror(errno); return false; }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    set_timeouts(fd, IO_TIMEOUT_S);
    i64 t0 = now_ns();
    if (connect(fd, (sockaddr*)&hh.addr, hh.alen) != 0) { err = std::string("connect: ") + strerror(errno); close(fd); return false; }
    i64 t1 = now_ns();
    c.fd = fd;
    if (tcp_ms) *tcp_ms = (t1 - t0) / 1e6;
    if (hh.u.https) {
        SSL* ssl = SSL_new(g_ctx);
        SSL_set_fd(ssl, fd);
        if (!hh.is_ip_literal) {
            SSL_set_tlsext_host_name(ssl, hh.u.host.c_str());
            if (!g_insecure) SSL_set1_host(ssl, hh.u.host.c_str());
        } else if (!g_insecure) {
            X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), hh.u.host.c_str());
        }
        ERR_clear_error();
        if (SSL_connect(ssl) != 1) {
            err = "TLS: " + ssl_err_string();
            long vr = SSL_get_verify_result(ssl);
            if (vr != X509_V_OK) err += std::string(" (") + X509_verify_cert_error_string(vr) + ")";
            SSL_free(ssl);
            close(fd);
            c.fd = -1;
            return false;
        }
        c.ssl = ssl;
        if (tls_ms) *tls_ms = (now_ns() - t1) / 1e6;
    } else if (tls_ms) *tls_ms = 0;
    return true;
}

enum { IO_OK = 0, IO_WANT_READ = -1, IO_WANT_WRITE = -2, IO_EOF = -3, IO_ERR = -4 };
static ssize_t conn_read(Conn& c, char* buf, size_t n, int& st) {
    st = IO_OK;
    if (c.ssl) {
        ERR_clear_error();
        int r = SSL_read(c.ssl, buf, (int)n);
        if (r > 0) return r;
        int e = SSL_get_error(c.ssl, r);
        if (e == SSL_ERROR_WANT_READ) st = IO_WANT_READ;
        else if (e == SSL_ERROR_WANT_WRITE) st = IO_WANT_WRITE;
        else if (e == SSL_ERROR_ZERO_RETURN) st = IO_EOF;
        else if (e == SSL_ERROR_SYSCALL && errno == 0) st = IO_EOF;
        else if (e == SSL_ERROR_SYSCALL && (errno == EAGAIN || errno == EWOULDBLOCK)) st = IO_WANT_READ;
        else st = IO_ERR;
        return -1;
    }
    ssize_t r = read(c.fd, buf, n);
    if (r > 0) return r;
    if (r == 0) { st = IO_EOF; return -1; }
    st = (errno == EAGAIN || errno == EWOULDBLOCK) ? IO_WANT_READ : IO_ERR;
    return -1;
}
static bool conn_write_all(Conn& c, const char* p, size_t n, std::string& err) {
    size_t off = 0;
    i64 deadline = now_ns() + 3000000000LL;
    while (off < n) {
        int wait_ev = 0;
        if (c.ssl) {
            ERR_clear_error();
            int r = SSL_write(c.ssl, p + off, (int)(n - off));
            if (r > 0) { off += (size_t)r; continue; }
            int e = SSL_get_error(c.ssl, r);
            if (e == SSL_ERROR_WANT_WRITE) wait_ev = POLLOUT;
            else if (e == SSL_ERROR_WANT_READ) wait_ev = POLLIN;
            else { err = "TLS write: " + ssl_err_string(); return false; }
        } else {
            ssize_t r = send(c.fd, p + off, n - off, MSG_NOSIGNAL);
            if (r > 0) { off += (size_t)r; continue; }
            if (errno == EAGAIN || errno == EWOULDBLOCK) wait_ev = POLLOUT;
            else if (errno == EINTR) continue;
            else { err = std::string("write: ") + strerror(errno); return false; }
        }
        i64 left = deadline - now_ns();
        if (left <= 0) { err = "write timeout"; return false; }
        pollfd pf{c.fd, (short)wait_ev, 0};
        poll(&pf, 1, (int)std::max<i64>(1, left / 1000000));
    }
    return true;
}
// آیا اتصال بیکار هنوز زنده است؟ (بدون ارسال هیچ داده‌ای)
static bool conn_alive(Conn& c) {
    if (c.fd < 0) return false;
    char b;
    if (c.ssl) {
        ERR_clear_error();
        int r = SSL_read(c.ssl, &b, 1);
        if (r > 0) return false;
        int e = SSL_get_error(c.ssl, r);
        return e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE;
    }
    ssize_t r = recv(c.fd, &b, 1, MSG_PEEK | MSG_DONTWAIT);
    if (r == 0) return false;
    if (r < 0) return errno == EAGAIN || errno == EWOULDBLOCK;
    return false;
}

// ---------------------------------------------------------------------------
// پارسر افزایشی‌ی پاسخ HTTP/1.1
// ---------------------------------------------------------------------------
struct RespParser {
    enum State { HEADERS, BODY_CL, CHUNK_SIZE, CHUNK_DATA, CHUNK_CRLF, TRAILERS, BODY_CLOSE, DONE, FAILED };
    State st = HEADERS;
    std::string buf, body, err;
    size_t pos = 0;
    int status = 0;
    KV hdrs;
    long long remain = 0;
    bool keep_alive = true;
    bool done() const { return st == DONE; }
    bool failed() const { return st == FAILED; }
    void feed(const char* p, size_t n) { buf.append(p, n); advance(); }
    void on_eof() {
        if (st == BODY_CLOSE) st = DONE;
        else if (st != DONE && st != FAILED) { st = FAILED; err = "اتصال پیش از کامل‌شدن پاسخ بسته شد"; }
    }
    void advance() {
        while (true) {
            switch (st) {
                case HEADERS: {
                    size_t e = buf.find("\r\n\r\n", pos);
                    if (e == std::string::npos) { compact(); return; }
                    std::string head = buf.substr(pos, e - pos);
                    pos = e + 4;
                    size_t le = head.find("\r\n");
                    std::string line = head.substr(0, le);
                    size_t sp = line.find(' ');
                    status = sp == std::string::npos ? 0 : atoi(line.c_str() + sp + 1);
                    if (status < 100) { st = FAILED; err = "پاسخ HTTP نامعتبر: " + line.substr(0, 60); return; }
                    hdrs.clear();
                    size_t cur = le == std::string::npos ? head.size() : le + 2;
                    while (cur < head.size()) {
                        size_t nl = head.find("\r\n", cur);
                        std::string l = head.substr(cur, nl == std::string::npos ? std::string::npos : nl - cur);
                        size_t c = l.find(':');
                        if (c != std::string::npos) hdrs.emplace_back(lower(trim(l.substr(0, c))), trim(l.substr(c + 1)));
                        if (nl == std::string::npos) break;
                        cur = nl + 2;
                    }
                    if (status >= 100 && status < 200 && status != 101) break;      // 1xx: پاسخ بعدی
                    std::string te = lower(kv_get_ci(hdrs, "transfer-encoding"));
                    std::string cl = kv_get_ci(hdrs, "content-length");
                    keep_alive = lower(kv_get_ci(hdrs, "connection")) != "close";
                    if (status == 204 || status == 304 || status == 101) st = DONE;
                    else if (contains(te, "chunked")) st = CHUNK_SIZE;
                    else if (!cl.empty()) { remain = atoll(cl.c_str()); st = remain > 0 ? BODY_CL : DONE; }
                    else { st = BODY_CLOSE; keep_alive = false; }
                    break;
                }
                case BODY_CL: {
                    size_t take = (size_t)std::min<long long>((long long)(buf.size() - pos), remain);
                    body.append(buf, pos, take);
                    pos += take; remain -= (long long)take;
                    if (remain == 0) st = DONE; else { compact(); return; }
                    break;
                }
                case CHUNK_SIZE: {
                    size_t e = buf.find("\r\n", pos);
                    if (e == std::string::npos) { compact(); return; }
                    std::string ln = buf.substr(pos, e - pos);
                    size_t sc = ln.find(';');
                    if (sc != std::string::npos) ln = ln.substr(0, sc);
                    ln = trim(ln);
                    char* endp = nullptr;
                    long long sz = strtoll(ln.c_str(), &endp, 16);
                    if (ln.empty() || *endp != '\0' || sz < 0) { st = FAILED; err = "اندازه‌ی chunk نامعتبر"; return; }
                    pos = e + 2;
                    if (sz == 0) st = TRAILERS; else { remain = sz; st = CHUNK_DATA; }
                    break;
                }
                case CHUNK_DATA: {
                    size_t take = (size_t)std::min<long long>((long long)(buf.size() - pos), remain);
                    body.append(buf, pos, take);
                    pos += take; remain -= (long long)take;
                    if (remain == 0) st = CHUNK_CRLF; else { compact(); return; }
                    break;
                }
                case CHUNK_CRLF:
                    if (buf.size() - pos < 2) { compact(); return; }
                    pos += 2; st = CHUNK_SIZE;
                    break;
                case TRAILERS: {
                    size_t e = buf.find("\r\n", pos);
                    if (e == std::string::npos) { compact(); return; }
                    bool empty = e == pos;
                    pos = e + 2;
                    if (empty) st = DONE;
                    break;
                }
                case BODY_CLOSE:
                    body.append(buf, pos, std::string::npos);
                    pos = buf.size();
                    compact();
                    return;
                default:
                    return;
            }
        }
    }
    void compact() { if (pos > 0) { buf.erase(0, pos); pos = 0; } }
};

static std::string inflate_body(const std::string& raw, int wbits, bool& ok) {
    z_stream zs{};
    ok = false;
    if (inflateInit2(&zs, wbits) != Z_OK) return "";
    std::string out;
    zs.next_in = (Bytef*)raw.data();
    zs.avail_in = (uInt)raw.size();
    char chunk[16384];
    int rc;
    do {
        zs.next_out = (Bytef*)chunk;
        zs.avail_out = sizeof chunk;
        rc = inflate(&zs, Z_NO_FLUSH);
        if (rc != Z_OK && rc != Z_STREAM_END) { inflateEnd(&zs); return ""; }
        out.append(chunk, sizeof chunk - zs.avail_out);
        if (out.size() > (size_t)16 << 20) break;
    } while (rc != Z_STREAM_END);
    inflateEnd(&zs);
    ok = true;
    return out;
}
static std::string decode_body(const RespParser& rp) {
    std::string enc = lower(kv_get_ci(rp.hdrs, "content-encoding"));
    const std::string& raw = rp.body;
    if (raw.empty() || enc.empty() || enc == "identity") return raw;
    bool ok = false;
    std::string out;
    if (enc == "gzip" || enc == "x-gzip") out = inflate_body(raw, 16 + MAX_WBITS, ok);
    else if (enc == "deflate") { out = inflate_body(raw, MAX_WBITS, ok); if (!ok) out = inflate_body(raw, -MAX_WBITS, ok); }
    else return "[پاسخ با فشرده‌سازی " + enc + " خوانده نشد]";
    if (!ok) return "[خطا در باز کردن پاسخ فشرده (" + enc + ")]";
    return out;
}

// ---------------------------------------------------------------------------
// ساخت بایت‌های درخواست (معادل build_request_parts + http.client)
// ---------------------------------------------------------------------------
static std::string build_wire(const std::string& method_lc, const std::string& target, const KV& headers, const KV& cookies,
                              const std::string* json_body_dumped, const std::string* data_body, const std::string& host_header) {
    std::string method = method_lc;
    for (auto& c : method) c = (char)toupper((unsigned char)c);
    KV hdrs;
    for (auto& p : headers) {
        std::string lk = lower(p.first);
        if (lk == "host" || lk == "content-length" || lk == "accept-encoding" || lk == "transfer-encoding") continue;
        kv_set(hdrs, p.first, p.second);
    }
    std::string body;
    bool has_body = false;
    if (json_body_dumped) {
        body = *json_body_dumped; has_body = true;
        if (!kv_has_ci(hdrs, "content-type")) hdrs.emplace_back("Content-Type", "application/json");
    } else if (data_body) { body = *data_body; has_body = true; }
    if (!cookies.empty() && !kv_has_ci(hdrs, "cookie")) {
        std::string c;
        for (size_t i = 0; i < cookies.size(); i++) { if (i) c += "; "; c += cookies[i].first + "=" + cookies[i].second; }
        hdrs.emplace_back("Cookie", c);
    }
    hdrs.emplace_back("Host", host_header);
    hdrs.emplace_back("Accept-Encoding", "gzip, deflate");
    std::string w;
    w.reserve(512 + body.size());
    w += method + " " + target + " HTTP/1.1\r\n";
    if (has_body) w += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    else if (method == "POST" || method == "PUT" || method == "PATCH") w += "Content-Length: 0\r\n";
    for (auto& p : hdrs) w += p.first + ": " + p.second + "\r\n";
    w += "\r\n";
    w += body;
    return w;
}

// ===========================================================================
// موتور
// ===========================================================================
struct Cfg {
    std::string mode = "run", accounts = "accounts.json", base = "08:44:50", out = "engine_out", cafile, only;
    bool force = false, live = false, insecure = false, rt = false;
    int test_count = 5, send_threads = 4, connect_threads = 4, read_threads = 2, spin_us = 2000, probe_n = 10;
    double test_interval = 340, test_delay = 2.0;
    std::vector<int> send_cpus, conn_cpus, read_cpus;
};
static Cfg g_cfg;
static std::atomic<bool> g_stop{false};

static void pin_thread(int cpu) {
    cpu_set_t s;
    CPU_ZERO(&s);
    CPU_SET(cpu, &s);
    pthread_setaffinity_np(pthread_self(), sizeof s, &s);
}
static void set_rt_prio(int prio) {
    sched_param sp{};
    sp.sched_priority = prio;
    if (pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp) != 0) {
        static std::once_flag f;
        std::call_once(f, [] { fprintf(stderr, "⚠ SCHED_FIFO برقرار نشد (نیاز به root/CAP_SYS_NICE)\n"); });
    }
}
static void apply_thread_policy(const std::vector<int>& cpus, int idx, int rt_prio) {
    if (!cpus.empty()) pin_thread(cpus[idx % cpus.size()]);
    if (g_cfg.rt && rt_prio > 0) set_rt_prio(rt_prio);
}

// ---- لاگ‌های متنیِ زنده (خارج از مسیر داغ) ----
static std::mutex g_log_m;
static std::vector<std::string> g_log_lines;
static void log_line(const std::string& who, const std::string& msg) {
    std::string line = "[" + fmt_clock(now_ns()) + "] " + (who.empty() ? "" : who + ": ") + msg;
    std::lock_guard<std::mutex> g(g_log_m);
    g_log_lines.push_back(line);
    fputs((line + "\n").c_str(), stdout);
    fflush(stdout);
}

// ---- مدل داده ----
struct Settings {
    double interval_ms = 320;
    long long trigger_ms = 9800;
    int count = 5;
    std::string stop_on_text, volume_fields;
    bool stop_after_success = false, debug_volume = true, warm = true;
    int warm_lead_ms = 0;
};
struct Account;
struct Shot {
    Account* acc = nullptr;
    int k = 0;
    i64 planned_ns = 0, fire_ns = 0;
    std::string wire;
    long long vol = -1;
    std::atomic<int> cstate{0};          // 0 خالی 1 درحال‌باز‌شدن 2 آماده 3 برداشته‌شده 4 لغو 5 ناموفق
    Conn conn;
    double hs_ms = 0;
    i64 t_ready_ns = 0;
    // نتیجه
    bool fired = false, skipped = false;
    std::string net_error, conn_kind, ticket_sent, request_body;
    i64 sent_ns = 0, wrote_ns = 0, recv_ns = 0;
    int status = 0;
    std::string text;
    double age_ms = 0;
    long long gen_sent = -1;
    ojson ticket_recv;
    bool success_stop = false, text_stop = false;
    std::atomic<bool> done{false};
};
struct TicketHolder {
    std::mutex m;
    std::string ticket;
    std::atomic<long long> gen{0};
    bool update(const ojson& ot) {
        if (!ot.is_object()) return false;
        auto t = ot.find("ticket"), g = ot.find("generation");
        if (t == ot.end() || g == ot.end()) return false;
        std::string ts;
        if (t->is_string()) ts = t->get<std::string>(); else if (t->is_null()) return false; else ts = t->dump();
        if (ts.empty()) return false;
        long long gv;
        if (g->is_number()) gv = (long long)g->get<double>();
        else if (g->is_string()) { char* e; gv = strtoll(g->get<std::string>().c_str(), &e, 10); if (*e) return false; }
        else return false;
        std::lock_guard<std::mutex> lk(m);
        if (gv >= gen.load()) { ticket = ts; gen = gv; return true; }
        return false;
    }
    std::pair<std::string, long long> get() { std::lock_guard<std::mutex> lk(m); return {ticket, gen.load()}; }
};
struct Account {
    int row = 0;
    std::string id, name, broker;
    Settings st;
    ParsedReq req;
    HostInfo host;
    bool ticket_broker = false, uses_ticket = false;
    HostInfo ticket_host;
    std::string ticket_wire;
    i64 start_ns = 0, interval_ns = 0, trigger_ns = 0, last_ns = 0;
    int count = 0, before = 0;
    bool clamped = false, test_mode = false;
    std::deque<Shot> shots;
    std::atomic<bool> early_stop{false}, poll_stop{false}, done_flag{false};
    // دیباگ حجم
    VolumeVariants vv;
    bool dbg = false;
    std::string dbg_note;
    // اتصال گرم
    std::mutex hs_m;
    std::deque<double> hs;
    int stagger_ms = 0;
    std::atomic<int> cold{0}, warmn{0};
    TicketHolder holder;
    std::mutex tc_m;
    std::vector<Conn> tc_idle;
    std::atomic<bool> tk_fail_logged{false};
    std::atomic<int> tk_inflight{0};
    std::string label() const { return "#" + std::to_string(row) + " " + name; }
    int lead_ms() {
        if (st.warm_lead_ms) return std::max(WARM_MIN_LEAD_MS, st.warm_lead_ms);
        std::lock_guard<std::mutex> g(hs_m);
        if (hs.empty()) return WARM_AUTO_DEFAULT_MS;
        double mx = *std::max_element(hs.begin(), hs.end());
        return (int)std::min<double>(WARM_MAX_LEAD_MS, std::max<double>(WARM_MIN_LEAD_MS, 2 * mx + 50));
    }
    int m_ahead() {
        double is = std::max(interval_ns / 1e9, 0.001);
        return std::max(1, (int)std::ceil((lead_ms() / 1000.0) / is));
    }
    Summary summarize(int status, const std::string& text) const {
        return ticket_broker ? summarize_nahayatnegar(status, text) : summarize_generic(status, text);
    }
};
static std::vector<std::unique_ptr<Account>> g_accounts;
static std::atomic<long long> g_remaining{0};
static std::mutex g_done_m;
static std::condition_variable g_done_cv;

static void finalize_shot(Shot* s) {
    bool exp = false;
    if (!s->done.compare_exchange_strong(exp, true)) return;
    if (g_remaining.fetch_sub(1) <= 1) { std::lock_guard<std::mutex> g(g_done_m); g_done_cv.notify_all(); }
}
static void skip_shot(Shot* s) {
    s->skipped = true;
    finalize_shot(s);
}
static void complete_shot(Shot* s, int status, std::string text, const std::string& net_error) {
    Account& a = *s->acc;
    s->recv_ns = now_ns();
    s->status = status;
    if (!net_error.empty()) {
        s->net_error = net_error;
    } else {
        s->text = std::move(text);
        if (a.st.stop_after_success && a.summarize(status, s->text).success) { a.early_stop = true; s->success_stop = true; }
        if (a.uses_ticket) {
            ojson ot = extract_order_ticket(s->text);
            s->ticket_recv = ot;
            a.holder.update(ot);
        }
        if (!a.st.stop_on_text.empty() && contains(s->text, a.st.stop_on_text)) { a.early_stop = true; s->text_stop = true; }
    }
    if (g_cfg.live) {
        char b[256];
        snprintf(b, sizeof b, "#%d -> %s | +%.2fms", s->k + 1, net_error.empty() ? ("HTTP " + std::to_string(status)).c_str() : net_error.c_str(),
                 (s->recv_ns - s->sent_ns) / 1e6);
        log_line(a.label(), b);
    }
    finalize_shot(s);
}

// ---- خواندن پاسخ‌ها (epoll) ----
struct Flight {
    Shot* s;
    Conn c;
    RespParser p;
    i64 deadline_ns;
};
struct Reader {
    int ep = -1, evfd = -1;
    std::mutex m;
    std::vector<Flight*> incoming;
    std::unordered_map<int, Flight*> live;
    std::thread th;
};
static std::vector<std::unique_ptr<Reader>> g_readers;
static std::atomic<bool> g_quit{false};

static void reader_handoff(Shot* s, Conn& c) {
    Reader* r = g_readers[(size_t)s->k % g_readers.size()].get();
    Flight* f = new Flight{s, c, RespParser(), now_ns() + (i64)(IO_TIMEOUT_S * 1e9)};
    c = Conn();
    {
        std::lock_guard<std::mutex> g(r->m);
        r->incoming.push_back(f);
    }
    uint64_t one = 1;
    if (write(r->evfd, &one, sizeof one) < 0) {}
}
static void flight_finish(Reader* r, Flight* f, int status, const std::string& text, const std::string& err) {
    epoll_ctl(r->ep, EPOLL_CTL_DEL, f->c.fd, nullptr);
    r->live.erase(f->c.fd);
    conn_close(f->c);
    complete_shot(f->s, status, text, err);
    delete f;
}
static void flight_pump(Reader* r, Flight* f) {
    char buf[16384];
    while (true) {
        int st;
        ssize_t n = conn_read(f->c, buf, sizeof buf, st);
        if (n > 0) {
            f->p.feed(buf, (size_t)n);
            if (f->p.done()) { flight_finish(r, f, f->p.status, decode_body(f->p), ""); return; }
            if (f->p.failed()) { flight_finish(r, f, f->p.status, "", "خطای شبکه: " + f->p.err); return; }
            continue;
        }
        if (st == IO_WANT_READ || st == IO_WANT_WRITE) return;
        if (st == IO_EOF) {
            f->p.on_eof();
            if (f->p.done()) flight_finish(r, f, f->p.status, decode_body(f->p), "");
            else flight_finish(r, f, f->p.status, "", "خطای شبکه: " + (f->p.err.empty() ? std::string("اتصال بسته شد") : f->p.err));
            return;
        }
        flight_finish(r, f, 0, "", "خطای شبکه: خواندن پاسخ ناموفق بود");
        return;
    }
}
static void reader_main(Reader* r, int idx) {
    apply_thread_policy(g_cfg.read_cpus, idx, 0);
    epoll_event evs[64];
    while (true) {
        int n = epoll_wait(r->ep, evs, 64, 10);
        for (int i = 0; i < n; i++) {
            int fd = evs[i].data.fd;
            if (fd == r->evfd) {
                uint64_t v;
                if (read(r->evfd, &v, sizeof v) < 0) {}
                std::vector<Flight*> in;
                { std::lock_guard<std::mutex> g(r->m); in.swap(r->incoming); }
                for (Flight* f : in) {
                    epoll_event e{};
                    e.events = EPOLLIN | EPOLLRDHUP;
                    e.data.fd = f->c.fd;
                    r->live[f->c.fd] = f;
                    if (epoll_ctl(r->ep, EPOLL_CTL_ADD, f->c.fd, &e) != 0) { flight_finish(r, f, 0, "", "epoll_ctl ناموفق"); continue; }
                    flight_pump(r, f);   // اگر پاسخ همین حالا رسیده باشد
                }
            } else {
                auto it = r->live.find(fd);
                if (it != r->live.end()) flight_pump(r, it->second);
            }
        }
        i64 now = now_ns();
        std::vector<Flight*> late;
        for (auto& kv : r->live) if (now > kv.second->deadline_ns) late.push_back(kv.second);
        for (Flight* f : late) flight_finish(r, f, 0, "", "خطای شبکه: timeout (" + num_str(IO_TIMEOUT_S) + "s)");
        if (g_quit.load()) {
            std::lock_guard<std::mutex> g(r->m);
            if (r->live.empty() && r->incoming.empty()) break;
        }
    }
}

// ---- اتصال گرم ----
struct OpenTask {
    i64 at;
    Shot* s;
    bool operator>(const OpenTask& o) const { return at > o.at; }
};
static std::priority_queue<OpenTask, std::vector<OpenTask>, std::greater<OpenTask>> g_open_q;
static std::mutex g_open_m;
static std::condition_variable g_open_cv;

static void enqueue_open(Shot* s, i64 at) {
    std::lock_guard<std::mutex> g(g_open_m);
    g_open_q.push({at, s});
    g_open_cv.notify_one();
}
static void do_open(Shot* s) {
    Account& a = *s->acc;
    int exp = 0;
    if (!s->cstate.compare_exchange_strong(exp, 1)) return;
    Conn c;
    double tcp = 0, tls = 0;
    std::string err;
    if (!open_conn(a.host, c, &tcp, &tls, err)) {
        s->cstate = 5;
        if (!a.test_mode) log_line(a.label(), "⚠ باز کردن اتصال #" + std::to_string(s->k + 1) + " ناموفق: " + err);
        return;
    }
    set_nonblock(c.fd);
    s->hs_ms = tcp + tls;
    s->t_ready_ns = now_ns();
    s->conn = c;
    int e1 = 1;
    if (s->cstate.compare_exchange_strong(e1, 2)) {
        std::lock_guard<std::mutex> g(a.hs_m);
        a.hs.push_back(tcp + tls);
        if (a.hs.size() > 5) a.hs.pop_front();
    } else {
        Conn tmp = s->conn;
        s->conn = Conn();
        conn_close(tmp);
    }
}
static void connector_main(int idx) {
    apply_thread_policy(g_cfg.conn_cpus, idx, 0);
    std::unique_lock<std::mutex> lk(g_open_m);
    while (!g_quit.load()) {
        if (g_open_q.empty()) { g_open_cv.wait_for(lk, std::chrono::milliseconds(50)); continue; }
        OpenTask t = g_open_q.top();
        i64 now = now_ns();
        if (t.at > now) {
            g_open_cv.wait_for(lk, std::chrono::nanoseconds(std::min<i64>(t.at - now, 50000000)));
            continue;
        }
        g_open_q.pop();
        lk.unlock();
        if (!g_stop.load() && !t.s->acc->early_stop.load()) do_open(t.s);
        lk.lock();
    }
}
static void warm_ahead(Account& a, int k) {
    if (!a.st.warm || g_stop.load() || a.early_stop.load()) return;
    int m = a.m_ahead();
    for (int j = k + 1; j <= k + m && j < a.count; j++) enqueue_open(&a.shots[j], now_ns());
}

// ---- شلیک ----
static void cold_send(Shot* s, std::string wire) {
    Account& a = *s->acc;
    Conn c;
    double tcp = 0, tls = 0;
    std::string err;
    if (!open_conn(a.host, c, &tcp, &tls, err)) { s->sent_ns = now_ns(); complete_shot(s, 0, "", "خطای شبکه: " + err); return; }
    set_nonblock(c.fd);
    s->hs_ms = tcp + tls;
    s->conn_kind = a.st.warm ? "سرد" : "عادی";
    if (a.st.warm) a.cold++;
    s->sent_ns = now_ns();
    if (!conn_write_all(c, wire.data(), wire.size(), err)) { conn_close(c); complete_shot(s, 0, "", "خطای شبکه: " + err); return; }
    s->wrote_ns = now_ns();
    reader_handoff(s, c);
}
static void fire_shot(Shot* s, const std::string& wire) {
    Account& a = *s->acc;
    s->fired = true;
    Conn c;
    bool warm_ok = false;
    if (a.st.warm) {
        int exp = 2;
        if (s->cstate.compare_exchange_strong(exp, 3)) {
            c = s->conn;
            s->conn = Conn();
            if (conn_alive(c)) warm_ok = true; else conn_close(c);
        } else {
            int e1 = 1;
            s->cstate.compare_exchange_strong(e1, 4);   // اتصال هنوز در حال باز‌شدن است: لغو
        }
    }
    if (!warm_ok) {
        std::string w = wire;
        std::thread([s, w]() { cold_send(s, w); }).detach();
        return;
    }
    s->conn_kind = "گرم";
    a.warmn++;
    i64 t = now_ns();
    s->age_ms = (t - s->t_ready_ns) / 1e6;
    s->sent_ns = t;
    std::string err;
    if (!conn_write_all(c, wire.data(), wire.size(), err)) {
        conn_close(c);
        complete_shot(s, 0, "", "خطای شبکه: " + err);
        return;
    }
    s->wrote_ns = now_ns();
    reader_handoff(s, c);
}

// ---- انتظار دقیق تا لحظه‌ی شلیک ----
static void wait_until(i64 target) {
    const i64 spin = (i64)g_cfg.spin_us * 1000LL;
    while (true) {
        i64 now = now_ns();
        i64 rem = target - now;
        if (rem <= 0 || g_stop.load()) return;
        if (rem > spin + 100000) {
            i64 wake = std::min<i64>(target - spin, now + 200000000LL);
            timespec ts{(time_t)(wake / 1000000000LL), (long)(wake % 1000000000LL)};
            clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, &ts, nullptr);
        } else cpu_relax();
    }
}
static void sender_main(int idx, std::vector<Shot*> list) {
    apply_thread_policy(g_cfg.send_cpus, idx, 80);
    for (Shot* s : list) {
        wait_until(s->fire_ns);
        if (g_stop.load() || s->acc->early_stop.load()) { skip_shot(s); continue; }
        fire_shot(s, s->wire);
        warm_ahead(*s->acc, s->k);
    }
}

// ---- تیکت نهایت‌نگر ----
static bool ticket_get(Account& a, std::string& text, int& status, std::string& err) {
    Conn c;
    {
        std::lock_guard<std::mutex> g(a.tc_m);
        if (!a.tc_idle.empty()) { c = a.tc_idle.back(); a.tc_idle.pop_back(); }
    }
    bool reused = c.fd >= 0;
    for (int attempt = 0; attempt < 2; attempt++) {
        if (c.fd < 0) {
            double t1, t2;
            if (!open_conn(a.ticket_host, c, &t1, &t2, err)) return false;
            set_timeouts(c.fd, 3.0);
            reused = false;
        }
        if (!conn_write_all(c, a.ticket_wire.data(), a.ticket_wire.size(), err)) { conn_close(c); if (reused) continue; return false; }
        RespParser p;
        char buf[16384];
        bool bad = false;
        while (!p.done() && !p.failed()) {
            int st;
            ssize_t n = conn_read(c, buf, sizeof buf, st);
            if (n > 0) p.feed(buf, (size_t)n);
            else if (st == IO_EOF) { p.on_eof(); break; }
            else { err = "timeout/خطای خواندن تیکت"; bad = true; break; }
        }
        if (bad || !p.done()) { conn_close(c); if (reused && attempt == 0) { reused = false; continue; } if (err.empty()) err = p.err; return false; }
        status = p.status;
        text = decode_body(p);
        if (p.keep_alive) {
            std::lock_guard<std::mutex> g(a.tc_m);
            if (a.tc_idle.size() < 4) { a.tc_idle.push_back(c); c = Conn(); }
        }
        conn_close(c);
        return true;
    }
    return false;
}
static bool fetch_ticket(Account& a, bool announce) {
    std::string text, err;
    int status = 0;
    if (ticket_get(a, text, status, err)) {
        ojson ot = extract_order_ticket(text);
        if (!ot.is_null()) {
            a.holder.update(ot);
            if (announce) log_line(a.label(), "تیکت اولیه دریافت شد (generation=" + std::to_string(a.holder.gen.load()) + ")");
            return true;
        }
        if (!a.tk_fail_logged.exchange(true)) log_line(a.label(), "⚠ دریافت تیکت ناموفق: HTTP " + std::to_string(status));
    } else if (!a.tk_fail_logged.exchange(true)) log_line(a.label(), "⚠ خطای شبکه در دریافت تیکت: " + err);
    return false;
}
static void sleep_checked(double sec, const std::atomic<bool>* flag) {
    i64 end = now_ns() + (i64)(sec * 1e9);
    while (!g_stop.load() && !(flag && flag->load())) {
        i64 rem = end - now_ns();
        if (rem <= 0) return;
        timespec ts{0, (long)std::min<i64>(rem, 20000000)};
        nanosleep(&ts, nullptr);
    }
}
static void ticket_worker_main(Account* a, int idx) {
    apply_thread_policy(g_cfg.send_cpus, idx, 80);
    fetch_ticket(*a, true);
    std::thread poll([a] {
        double lead = (a->start_ns - now_ns()) / 1e9 - TICKET_LEAD_SECONDS;
        if (lead > 0) sleep_checked(lead, &a->poll_stop);
        while (!g_stop.load() && !a->done_flag.load() && !a->poll_stop.load()) {
            fetch_ticket(*a, false);
            sleep_checked(TICKET_POLL_SECONDS, &a->poll_stop);
        }
    });
    int n = 0;
    long long last_gen_used = -1;
    i64 last_sent = 0;
    Shot* last_shot = nullptr;
    const i64 extra = (i64)(TICKET_EXTRA_WAIT * 1e9);
    while (true) {
        if (g_stop.load() || a->early_stop.load() || n >= a->count) break;
        i64 earliest = n == 0 ? a->start_ns : last_sent + a->interval_ns;
        i64 deadline = earliest + extra;
        bool timed_out = false;
        for (;;) {
            i64 now = now_ns();
            long long g = a->holder.gen.load();
            bool fresh = n == 0 || g > last_gen_used;
            bool responded = !a->st.stop_after_success || !last_shot || last_shot->done.load();
            if (now >= earliest && responded && (fresh || now >= deadline)) { timed_out = !fresh; break; }
            if (now >= earliest + 6000000000LL) { timed_out = !fresh; break; }
            if (g_stop.load() || a->early_stop.load()) break;
            i64 gap = now < earliest ? earliest - now : 0;
            if (gap > 4000000) {
                i64 wake = std::min<i64>(now + gap - 2000000, now + 50000000LL);
                timespec ts{(time_t)(wake / 1000000000LL), (long)(wake % 1000000000LL)};
                clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, &ts, nullptr);
            } else cpu_relax();
        }
        if (g_stop.load() || a->early_stop.load()) break;
        Shot* s = &a->shots[n];
        auto snap = a->holder.get();
        if (timed_out) log_line(a->label(), "⚠ #" + std::to_string(n + 1) + ": تیکت تازه تا " + num_str(TICKET_EXTRA_WAIT * 1000) + "ms نرسید — با آخرین تیکت ارسال شد");
        ojson body = (a->dbg && n < (int)a->vv.bodies.size()) ? a->vv.bodies[n] : a->req.json_body;
        if (!snap.first.empty()) {
            body["orderTicket"] = snap.first;
            body["ticketGeneration"] = snap.second;
            s->ticket_sent = snap.first;
            s->gen_sent = snap.second;
        }
        std::string dumped = py_dumps_str(body);
        std::string wire = build_wire(a->req.method, a->host.u.target, a->req.headers, a->req.cookies, &dumped, nullptr, a->host.u.host_header);
        s->planned_ns = earliest;
        s->fire_ns = now_ns();
        s->request_body = dumped;
        last_gen_used = snap.second;
        last_sent = s->fire_ns;
        last_shot = s;
        fire_shot(s, wire);
        n++;
        warm_ahead(*a, n - 1);
        a->poll_stop = true;
        i64 until = last_sent + a->interval_ns + extra;
        std::thread([a, gen_used = snap.second, until] {
            sleep_checked(0.04, &a->done_flag);
            while (!g_stop.load() && !a->done_flag.load() && now_ns() < until) {
                if (a->holder.gen.load() > gen_used) return;
                if (a->tk_inflight.load() < 3) {
                    a->tk_inflight++;
                    std::thread([a] { fetch_ticket(*a, false); a->tk_inflight--; }).detach();
                }
                sleep_checked(TICKET_WATCH_INTERVAL, &a->done_flag);
            }
        }).detach();
    }
    for (int k = n; k < a->count; k++) skip_shot(&a->shots[k]);
    a->done_flag = true;
    poll.join();
}

// ===========================================================================
// آماده‌سازی حساب‌ها از accounts.json
// ===========================================================================
static double jnum(const ojson& o, const char* k, double def) {
    auto it = o.find(k);
    if (it == o.end()) return def;
    if (it->is_number()) return it->get<double>();
    if (it->is_string()) {
        std::string s = trim(it->get<std::string>());
        if (s.empty()) return def;
        char* e;
        double v = strtod(s.c_str(), &e);
        return *e ? def : v;
    }
    return def;
}
static bool jbool(const ojson& o, const char* k, bool def) {
    auto it = o.find(k);
    if (it == o.end()) return def;
    if (it->is_boolean()) return it->get<bool>();
    if (it->is_number()) return it->get<double>() != 0;
    return def;
}
static std::string jstr(const ojson& o, const char* k) {
    auto it = o.find(k);
    return (it != o.end() && it->is_string()) ? it->get<std::string>() : "";
}
static const std::set<std::string> KNOWN_BROKERS = {"saman", "pasargad", "karizma", "exir", "mofid", "nahayatnegar"};
static const std::vector<std::string> AUTO_VOLUME_KEYS = {"quantity", "qty", "volume", "ordervolume", "orderquantity", "orderqty", "order_volume", "order_quantity", "vol"};

static bool setup_account(Account& a, const ojson& jo, int row, i64 base_ns, bool test_mode, i64 test_start_ns, std::string& err) {
    a.row = row;
    a.id = jstr(jo, "id");
    a.name = jstr(jo, "name");
    a.broker = jstr(jo, "broker");
    a.test_mode = test_mode;
    if (!KNOWN_BROKERS.count(a.broker)) { err = "کارگزاری ناشناخته: " + a.broker; return false; }
    a.ticket_broker = a.broker == "nahayatnegar";
    ojson order = jo.contains("order") && jo["order"].is_object() ? jo["order"] : ojson::object();
    Settings& st = a.st;
    st.interval_ms = jnum(order, "interval_ms", 320);
    st.trigger_ms = (long long)jnum(order, "trigger", 9800);
    st.count = (int)jnum(order, "count", 5);
    st.stop_on_text = trim(jstr(order, "stop_on_text"));
    st.stop_after_success = jbool(order, "stop_after_success", false);
    st.debug_volume = jbool(order, "debug_volume", true);
    st.volume_fields = jstr(order, "volume_fields");
    st.warm = jbool(order, "warm_connections", true);
    st.warm_lead_ms = (int)jnum(order, "warm_lead_ms", 0);
    if (test_mode) { st.interval_ms = g_cfg.test_interval; st.count = g_cfg.test_count; }
    if (st.interval_ms <= 0 || st.trigger_ms < 0 || st.count < 1) { err = "وقفه/تریگر/تعداد معتبر نیست"; return false; }
    if (!parse_captured_curl(jstr(jo, "header"), a.req, err)) return false;
    if (!parse_url(a.req.url, a.host.u)) { err = "آدرس نامعتبر یا غیر HTTP(S): " + a.req.url; return false; }
    resolve_host(a.host);
    a.clamped = st.interval_ms < MIN_INTERVAL_MS;
    if (a.clamped) st.interval_ms = MIN_INTERVAL_MS;
    a.interval_ns = (i64)std::llround(st.interval_ms * 1e6);
    a.count = st.count;
    a.before = (a.count - 1) / 2;
    if (test_mode) { a.start_ns = test_start_ns; a.trigger_ns = a.start_ns; }
    else { a.trigger_ns = base_ns + st.trigger_ms * 1000000LL; a.start_ns = a.trigger_ns - (i64)a.before * a.interval_ns; }
    a.last_ns = a.start_ns + (i64)(a.count - 1) * a.interval_ns;
    a.stagger_ms = (int)(crc32(0, (const Bytef*)a.id.data(), (uInt)a.id.size()) % WARM_STAGGER_MAX_MS);

    a.uses_ticket = a.ticket_broker && a.req.has_json && a.req.json_body.is_object() && a.req.json_body.contains("orderTicket");
    if (a.uses_ticket) {
        std::string base = a.req.url.substr(0, a.req.url.find('?'));
        while (!base.empty() && base.back() == '/') base.pop_back();
        if (!parse_url(base + "/orderTicket", a.ticket_host.u)) { err = "آدرس تیکت نامعتبر"; return false; }
        resolve_host(a.ticket_host);
        KV th;
        for (auto& p : a.req.headers) { std::string lk = lower(p.first); if (lk != "content-type" && lk != "content-length") th.push_back(p); }
        a.ticket_wire = build_wire("get", a.ticket_host.u.target, th, a.req.cookies, nullptr, nullptr, a.ticket_host.u.host_header);
    }
    // دیباگ بای والیوم
    bool dbg = st.debug_volume;
    if (dbg) {
        std::set<std::string> wanted;
        std::string vf = st.volume_fields;
        size_t pos;
        while ((pos = vf.find("،")) != std::string::npos) vf.replace(pos, 2, ",");
        std::stringstream ss(vf);
        std::string tok;
        bool custom = false;
        while (std::getline(ss, tok, ',')) { tok = lower(trim(tok)); if (!tok.empty()) { wanted.insert(tok); custom = true; } }
        std::string total_key;
        if (!custom) {
            if (a.ticket_broker) { wanted = {"volume", "diffvolume"}; total_key = "grandTotal"; }
            else wanted.insert(AUTO_VOLUME_KEYS.begin(), AUTO_VOLUME_KEYS.end());
        }
        std::string reason;
        bool okv = false;
        if (a.req.has_json && (a.req.json_body.is_object() || a.req.json_body.is_array()))
            okv = build_volume_variants(a.req.json_body, wanted, total_key, a.count, a.vv, reason);
        else if (a.req.has_data)
            okv = build_volume_variants_form(a.req.data, wanted, a.count, a.vv, reason);
        else reason = "بدنه‌ی درخواست قابل تغییر نیست";
        if (okv) {
            a.dbg = true;
            std::string shown;
            for (size_t i = 0; i < a.vv.vols.size(); i++) { if (i) shown += ", "; shown += std::to_string(a.vv.vols[i]); }
            a.dbg_note = "دیباگ بای والیوم فعال — حجم پایه " + std::to_string(a.vv.base) + " | درخواست‌ها: " + shown;
        } else a.dbg_note = "⚠ دیباگ بای والیوم اعمال نشد: " + reason + " — بدنه عیناً ارسال می‌شود.";
    }
    for (int k = 0; k < a.count; k++) a.shots.emplace_back();
    for (int k = 0; k < a.count; k++) {
        Shot& s = a.shots[k];
        s.acc = &a;
        s.k = k;
        s.planned_ns = s.fire_ns = a.start_ns + (i64)k * a.interval_ns;
        if (a.dbg && k < (int)a.vv.vols.size()) s.vol = a.vv.vols[k];
        if (!a.uses_ticket) {
            bool use = a.dbg && k < (int)a.vv.vols.size();
            std::string dumped;
            if (a.req.has_json) {
                dumped = py_dumps_str(use && !a.vv.bodies.empty() ? a.vv.bodies[k] : a.req.json_body);
                s.wire = build_wire(a.req.method, a.host.u.target, a.req.headers, a.req.cookies, &dumped, nullptr, a.host.u.host_header);
                s.request_body = dumped;
            } else {
                std::string d = use && !a.vv.form_bodies.empty() ? a.vv.form_bodies[k] : a.req.data;
                s.wire = build_wire(a.req.method, a.host.u.target, a.req.headers, a.req.cookies, nullptr, a.req.has_data ? &d : nullptr, a.host.u.host_header);
                s.request_body = a.req.has_data ? d : "";
            }
        }
    }
    return true;
}

// ===========================================================================
// گزارش
// ===========================================================================
static std::string us_str(i64 ns) { char b[32]; snprintf(b, sizeof b, "%+.0fµs", ns / 1000.0); return b; }

static ojson shot_json(Account& a, Shot& s) {
    ojson j = ojson::object();
    j["account"] = a.name;
    j["row"] = a.row;
    j["n"] = s.k + 1;
    j["planned"] = fmt_clock(s.planned_ns, true);
    if (s.vol >= 0) j["volume"] = s.vol;
    if (s.skipped && !s.fired) { j["skipped"] = true; return j; }
    j["fired"] = fmt_clock(s.fire_ns, true);
    j["sent"] = fmt_clock(s.sent_ns, true);
    j["late_us"] = (s.sent_ns - s.fire_ns) / 1000;
    j["late_vs_planned_us"] = (s.sent_ns - s.planned_ns) / 1000;
    j["write_us"] = s.wrote_ns ? (s.wrote_ns - s.sent_ns) / 1000 : 0;
    j["received"] = fmt_clock(s.recv_ns, true);
    j["duration_ms"] = (s.recv_ns - s.sent_ns) / 1e6;
    j["conn"] = s.conn_kind;
    j["handshake_ms"] = s.hs_ms;
    if (s.conn_kind == "گرم") j["conn_age_ms"] = s.age_ms;
    if (s.gen_sent >= 0) j["gen_sent"] = s.gen_sent;
    if (!s.net_error.empty()) {
        j["net_error"] = s.net_error;
        j["result"] = "خطای شبکه";
    } else {
        Summary sm = a.summarize(s.status, s.text);
        j["status"] = s.status;
        j["result"] = sm.shown;
        j["message"] = sm.message;
        j["success"] = sm.success;
        j["response"] = s.text.substr(0, MAX_RESPONSE_CHARS);
    }
    if (!s.ticket_recv.is_null()) j["ticket_recv"] = s.ticket_recv;
    j["request_body"] = s.request_body.substr(0, MAX_RESPONSE_CHARS);
    if (s.success_stop) j["success_stop"] = true;
    if (s.text_stop) j["text_stop"] = true;
    return j;
}
static std::string jdump(const ojson& j, int indent = -1) { return j.dump(indent, ' ', false, ojson::error_handler_t::replace); }

static void print_plan(bool dry) {
    for (auto& ap : g_accounts) {
        Account& a = *ap;
        std::string line = "تریگر " + (a.test_mode ? std::string("(تست)") : fmt_clock(a.trigger_ns)) + " | " + std::to_string(a.count) + " درخواست، وقفه " +
                           num_str(a.st.interval_ms) + "ms" + (a.clamped ? " (⚠ به حداقل ۳۰۰ رسید)" : "") + " | اولین " + fmt_clock(a.start_ns) + "، آخرین " + fmt_clock(a.last_ns) +
                           " | " + a.host.u.host_header + " IP " + (a.host.resolved ? a.host.ip : "؟") +
                           (a.uses_ticket ? " | زنجیره‌ی تیکت" : "") + " | گرم " + (a.st.warm ? "روشن" : "خاموش");
        log_line(a.label(), line);
        if (!a.dbg_note.empty()) log_line(a.label(), a.dbg_note);
    }
    (void)dry;
}

static void write_reports(i64 run_start_ns) {
    mkdir(g_cfg.out.c_str(), 0755);
    std::string stamp = fmt_stamp(run_start_ns);
    std::string jpath = g_cfg.out + "/run_" + stamp + ".jsonl";
    std::string tpath = g_cfg.out + "/run_" + stamp + ".txt";
    std::ofstream jf(jpath), tf(tpath);
    tf << ENGINE_VERSION << "\nحالت: " << g_cfg.mode << " | ساعت پایه: " << g_cfg.base << " | thread ارسال: " << g_cfg.send_threads
       << " | spin: " << g_cfg.spin_us << "µs | RT: " << (g_cfg.rt ? "بله" : "خیر") << "\n\n";
    printf("\n================ خلاصه ================\n");
    for (auto& ap : g_accounts) {
        Account& a = *ap;
        int fired = 0, ok = 0, err = 0, skipped = 0;
        std::vector<i64> lates;
        i64 max_late = 0;
        for (auto& s : a.shots) {
            if (s.fired) {
                fired++;
                i64 late = s.sent_ns - s.fire_ns;
                lates.push_back(late);
                max_late = std::max(max_late, late);
                if (!s.net_error.empty()) err++;
                else if (a.summarize(s.status, s.text).success) ok++;
            } else skipped++;
            jf << jdump(shot_json(a, s)) << "\n";
        }
        std::sort(lates.begin(), lates.end());
        i64 med = lates.empty() ? 0 : lates[lates.size() / 2];
        char head[512];
        snprintf(head, sizeof head, "%s | شلیک %d از %d | موفق %d | خطای شبکه %d | لغو %d | گرم %d / سرد %d | تأخیر ارسال: میانه %.0fµs، حداکثر %.0fµs",
                 a.label().c_str(), fired, a.count, ok, err, skipped, a.warmn.load(), a.cold.load(), med / 1000.0, max_late / 1000.0);
        puts(head);
        tf << head << "\n";
        for (auto& s : a.shots) {
            ojson j = shot_json(a, s);
            char ln[640];
            if (j.contains("skipped")) { snprintf(ln, sizeof ln, "  #%d لغو شد (planned %s)", s.k + 1, fmt_clock(s.planned_ns, true).c_str()); }
            else {
                snprintf(ln, sizeof ln, "  #%d planned %s | sent %s (late %s) | recv +%.2fms | %s | %s%s%s",
                         s.k + 1, fmt_clock(s.planned_ns, true).c_str(), fmt_clock(s.sent_ns, true).c_str(), us_str(s.sent_ns - s.fire_ns).c_str(),
                         (s.recv_ns - s.sent_ns) / 1e6, s.net_error.empty() ? ("HTTP " + std::to_string(s.status)).c_str() : s.net_error.c_str(),
                         j.value("result", "").c_str(), s.vol >= 0 ? (" | حجم " + std::to_string(s.vol)).c_str() : "",
                         s.conn_kind == "سرد" ? " | ⚠ اتصال سرد" : "");
            }
            tf << ln << "\n";
        }
        tf << "\n";
    }
    tf << "\n--- لاگ زنده ---\n";
    for (auto& l : g_log_lines) tf << l << "\n";
    printf("\nگزارش: %s\n       %s\n", tpath.c_str(), jpath.c_str());
}

// ===========================================================================
// probe: سنجش تأخیر DNS/TCP/TLS تا هر میزبان (بدون ارسال هیچ درخواستی)
// ===========================================================================
static void run_probe(const std::vector<ojson>& accs) {
    std::map<std::string, HostInfo> hosts;
    for (auto& jo : accs) {
        ParsedReq pr;
        std::string err;
        if (!parse_captured_curl(jstr(jo, "header"), pr, err)) continue;
        HostInfo h;
        if (!parse_url(pr.url, h.u)) continue;
        hosts[h.u.host_header] = h;
    }
    for (auto& kv : hosts) {
        HostInfo h = kv.second;
        i64 t0 = now_ns();
        resolve_host(h);
        double dns = (now_ns() - t0) / 1e6;
        std::vector<double> tcp, tls;
        int fails = 0;
        for (int i = 0; i < g_cfg.probe_n; i++) {
            Conn c;
            double a = 0, b = 0;
            std::string err;
            if (open_conn(h, c, &a, &b, err)) { tcp.push_back(a); tls.push_back(b); conn_close(c); }
            else { fails++; if (fails == 1) printf("  خطا: %s\n", err.c_str()); }
            timespec ts{0, 50000000};
            nanosleep(&ts, nullptr);
        }
        auto stat = [](std::vector<double> v, const char* nm) {
            if (v.empty()) { printf("  %s: —\n", nm); return; }
            std::sort(v.begin(), v.end());
            printf("  %s: min %.2f | median %.2f | max %.2f ms\n", nm, v.front(), v[v.size() / 2], v.back());
        };
        printf("%s  IP %s  DNS %.2fms  (%d اتصال، %d خطا)\n", kv.first.c_str(), h.resolved ? h.ip.c_str() : "?", dns, g_cfg.probe_n, fails);
        stat(tcp, "TCP connect  ");
        if (h.u.https) stat(tls, "TLS handshake");
    }
}

// ===========================================================================
// main
// ===========================================================================
static std::vector<int> parse_cpu_list(const std::string& s) {
    std::vector<int> v;
    std::stringstream ss(s);
    std::string t;
    while (std::getline(ss, t, ',')) {
        size_t d = t.find('-');
        if (d != std::string::npos) { int a = atoi(t.substr(0, d).c_str()), b = atoi(t.substr(d + 1).c_str()); for (int i = a; i <= b; i++) v.push_back(i); }
        else if (!t.empty()) v.push_back(atoi(t.c_str()));
    }
    return v;
}
static void usage() {
    puts("استفاده:\n"
         "  sarkh_engine run   --accounts accounts.json --base 08:44:50 [--force]   ارسال زمان‌بندی‌شده (تریگری)\n"
         "  sarkh_engine test  --accounts accounts.json [--count 5] [--interval 340] [--delay 2]   ارسال فوری (مثل «ارسال تست»)\n"
         "  sarkh_engine dry   --accounts accounts.json --base 08:44:50              فقط برنامه‌ی زمانی را چاپ می‌کند\n"
         "  sarkh_engine probe --accounts accounts.json [--n 10]                      تأخیر DNS/TCP/TLS تا کارگزاران\n"
         "گزینه‌ها: --only name1,name2  --out DIR  --live  --insecure  --cafile FILE\n"
         "          --send-threads N  --send-cpus 2,3,4  --connect-threads N  --conn-cpus 5,6  --read-threads N  --read-cpus 7\n"
         "          --spin-us 2000  --rt  (SCHED_FIFO برای تردهای ارسال؛ نیاز به root)\n"
         "منطقه‌ی زمانی: ساعت پایه به وقت محلی سرور است؛ برای تهران:  TZ=Asia/Tehran ./sarkh_engine run ...");
}
static void on_signal(int) { g_stop = true; }

int main(int argc, char** argv) {
    if (argc < 2) { usage(); return 1; }
    g_cfg.mode = argv[1];
    for (int i = 2; i < argc; i++) {
        std::string k = argv[i];
        auto val = [&]() -> std::string { if (i + 1 >= argc) { fprintf(stderr, "مقدار %s وجود ندارد\n", k.c_str()); exit(1); } return argv[++i]; };
        if (k == "--accounts") g_cfg.accounts = val();
        else if (k == "--base") g_cfg.base = val();
        else if (k == "--out") g_cfg.out = val();
        else if (k == "--only") g_cfg.only = val();
        else if (k == "--cafile") g_cfg.cafile = val();
        else if (k == "--force") g_cfg.force = true;
        else if (k == "--live") g_cfg.live = true;
        else if (k == "--insecure") g_cfg.insecure = true;
        else if (k == "--rt") g_cfg.rt = true;
        else if (k == "--count") g_cfg.test_count = atoi(val().c_str());
        else if (k == "--interval") g_cfg.test_interval = atof(val().c_str());
        else if (k == "--delay") g_cfg.test_delay = atof(val().c_str());
        else if (k == "--n") g_cfg.probe_n = atoi(val().c_str());
        else if (k == "--send-threads") g_cfg.send_threads = std::max(1, atoi(val().c_str()));
        else if (k == "--connect-threads") g_cfg.connect_threads = std::max(1, atoi(val().c_str()));
        else if (k == "--read-threads") g_cfg.read_threads = std::max(1, atoi(val().c_str()));
        else if (k == "--spin-us") g_cfg.spin_us = std::max(0, atoi(val().c_str()));
        else if (k == "--send-cpus") g_cfg.send_cpus = parse_cpu_list(val());
        else if (k == "--conn-cpus") g_cfg.conn_cpus = parse_cpu_list(val());
        else if (k == "--read-cpus") g_cfg.read_cpus = parse_cpu_list(val());
        else { fprintf(stderr, "گزینه‌ی ناشناخته: %s\n", k.c_str()); usage(); return 1; }
    }
    if (!g_cfg.send_cpus.empty()) g_cfg.send_threads = (int)g_cfg.send_cpus.size();
    if (g_cfg.mode != "run" && g_cfg.mode != "test" && g_cfg.mode != "dry" && g_cfg.mode != "probe") { usage(); return 1; }
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    setvbuf(stdout, nullptr, _IOLBF, 0);

    std::ifstream f(g_cfg.accounts);
    if (!f) { fprintf(stderr, "فایل %s باز نشد\n", g_cfg.accounts.c_str()); return 1; }
    ojson all = ojson::parse(f, nullptr, false);
    if (all.is_discarded() || !all.is_array()) { fprintf(stderr, "accounts.json باید آرایه‌ی JSON باشد\n"); return 1; }
    std::set<std::string> only;
    if (!g_cfg.only.empty()) { std::stringstream ss(g_cfg.only); std::string t; while (std::getline(ss, t, ',')) only.insert(trim(t)); }
    std::vector<std::pair<int, ojson>> enabled;   // (ردیف ۱-پایه، حساب)
    for (size_t i = 0; i < all.size(); i++) {
        const ojson& jo = all[i];
        if (!jo.is_object() || !jbool(jo, "enabled", true)) continue;
        if (!only.empty() && !only.count(jstr(jo, "name"))) continue;
        enabled.emplace_back((int)i + 1, jo);
    }
    if (enabled.empty()) { fprintf(stderr, "هیچ حساب فعالی (طبق فیلتر) وجود ندارد\n"); return 1; }

    SSL_library_init();
    init_tls(g_cfg.insecure, g_cfg.cafile);
    if (g_cfg.mode == "probe") {
        std::vector<ojson> accs;
        for (auto& e : enabled) accs.push_back(e.second);
        run_probe(accs);
        return 0;
    }
    mlockall(MCL_CURRENT | MCL_FUTURE);

    i64 base_ns = 0, run_start = now_ns();
    bool test = g_cfg.mode == "test";
    if (!test) {
        std::string e;
        if (!parse_hms_today(g_cfg.base, base_ns, e)) { fprintf(stderr, "%s\n", e.c_str()); return 1; }
    }
    i64 test_start = run_start + (i64)(g_cfg.test_delay * 1e9);
    for (auto& e : enabled) {
        auto a = std::make_unique<Account>();
        std::string err;
        if (!setup_account(*a, e.second, e.first, base_ns, test, test_start, err)) {
            fprintf(stderr, "خطا — حساب «%s»: %s\n", jstr(e.second, "name").c_str(), err.c_str());
            return 2;
        }
        g_accounts.push_back(std::move(a));
    }
    printf("%s | حساب فعال: %zu | %s\n", ENGINE_VERSION, g_accounts.size(), test ? "حالت تست" : ("ساعت پایه " + g_cfg.base).c_str());
    print_plan(g_cfg.mode == "dry");
    if (g_cfg.mode == "dry") return 0;

    // بررسی گذشته‌بودن زمان‌ها
    if (!test) {
        i64 now = now_ns();
        std::vector<std::string> fully_past, started;
        for (auto& a : g_accounts) {
            if (a->last_ns <= now) fully_past.push_back(a->label());
            else if (a->start_ns <= now) started.push_back(a->label());
        }
        if (!fully_past.empty() && !g_cfg.force) {
            fprintf(stderr, "\nخطا: زمان همه‌ی درخواست‌های این حساب‌ها گذشته است (ساعت پایه/تریگر را اصلاح کنید یا از test استفاده کنید):\n");
            for (auto& s : fully_past) fprintf(stderr, "  %s\n", s.c_str());
            return 2;
        }
        if (!started.empty() && !g_cfg.force) {
            fprintf(stderr, "\nهشدار: زمان اولین درخواست این حساب‌ها گذشته؛ با --force ارسال همین الان شروع می‌شود:\n");
            for (auto& s : started) fprintf(stderr, "  %s\n", s.c_str());
            return 2;
        }
        if (!fully_past.empty()) {
            g_accounts.erase(std::remove_if(g_accounts.begin(), g_accounts.end(), [&](const std::unique_ptr<Account>& a) { return a->last_ns <= now; }), g_accounts.end());
            if (g_accounts.empty()) return 2;
        }
    }

    long long total = 0;
    for (auto& a : g_accounts) total += a->count;
    g_remaining = total;

    // خواننده‌ها
    for (int i = 0; i < g_cfg.read_threads; i++) {
        auto r = std::make_unique<Reader>();
        r->ep = epoll_create1(EPOLL_CLOEXEC);
        r->evfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        epoll_event e{};
        e.events = EPOLLIN;
        e.data.fd = r->evfd;
        epoll_ctl(r->ep, EPOLL_CTL_ADD, r->evfd, &e);
        g_readers.push_back(std::move(r));
    }
    for (int i = 0; i < g_cfg.read_threads; i++) g_readers[i]->th = std::thread(reader_main, g_readers[i].get(), i);
    std::vector<std::thread> connectors;
    for (int i = 0; i < g_cfg.connect_threads; i++) connectors.emplace_back(connector_main, i);

    // موج اول اتصال‌ها
    for (auto& ap : g_accounts) {
        Account& a = *ap;
        if (!a.st.warm) continue;
        int m0 = std::min(std::max(2, a.m_ahead()), a.count);
        i64 at = std::max<i64>(now_ns(), a.start_ns - (i64)(WARM_FIRST_LEAD_S * 1e9) + (i64)a.stagger_ms * 1000000LL);
        for (int j = 0; j < m0; j++) enqueue_open(&a.shots[j], at);
        char b[160];
        snprintf(b, sizeof b, "اتصال گرم: %d اتصال اولیه از %s | پیش‌گرمی %dms", m0, fmt_clock(at).c_str(), a.lead_ms());
        log_line(a.label(), b);
    }

    // ارسال‌کننده‌ها: شلیک‌های حساب‌های عادی، مرتب‌شده بر زمان و پخش‌شده بین ریسمان‌ها
    std::vector<Shot*> generic;
    for (auto& ap : g_accounts)
        if (!ap->uses_ticket) for (auto& s : ap->shots) generic.push_back(&s);
    std::sort(generic.begin(), generic.end(), [](Shot* x, Shot* y) { return x->fire_ns < y->fire_ns; });
    int S = std::max(1, std::min(g_cfg.send_threads, (int)std::max<size_t>(1, generic.size())));
    std::vector<std::vector<Shot*>> lists(S);
    for (size_t i = 0; i < generic.size(); i++) lists[i % S].push_back(generic[i]);
    std::vector<std::thread> senders;
    if (!generic.empty()) for (int i = 0; i < S; i++) senders.emplace_back(sender_main, i, lists[i]);
    std::vector<std::thread> tickets;
    int ti = 0;
    for (auto& ap : g_accounts) if (ap->uses_ticket) tickets.emplace_back(ticket_worker_main, ap.get(), ti++);
    log_line("", "آماده — منتظر زمان شلیک (Ctrl+C = توقف)");

    // انتظار برای پایان
    i64 last_shot = 0;
    for (auto& a : g_accounts) last_shot = std::max(last_shot, a->last_ns);
    {
        std::unique_lock<std::mutex> lk(g_done_m);
        while (g_remaining.load() > 0 && !g_stop.load()) {
            g_done_cv.wait_for(lk, std::chrono::milliseconds(100));
            if (now_ns() > last_shot + 30000000000LL) { log_line("", "⚠ بیش از ۳۰ثانیه بعد از آخرین شلیک — پایان اجباری"); break; }
        }
    }
    if (g_stop.load()) log_line("", "متوقف شد توسط کاربر.");
    for (auto& t : senders) t.join();
    for (auto& t : tickets) t.join();
    for (auto& ap : g_accounts) ap->done_flag = true;
    // پایان خواننده‌ها: شلیک‌های ناتمام timeout می‌شوند
    g_quit = true;
    g_open_cv.notify_all();
    for (auto& t : connectors) t.join();
    for (auto& r : g_readers) r->th.join();
    write_reports(run_start);
    fflush(stdout);
    _exit(0);   // ریسمان‌های کمکیِ جداشده را بدون مزاحمت رها می‌کند
}
