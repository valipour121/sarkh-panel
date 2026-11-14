import socket, ssl, threading, time, json, gzip, sys
HTTP_PORT, HTTPS_PORT = 18080, 18443
LOG = []; LOCK = threading.Lock()
GEN = {"n": 1}   # تیکت نهایت‌نگر
def ticket_obj(): return {"ticket": "T%d" % GEN["n"], "generation": GEN["n"]}

def handle(conn, tag):
    try:
        conn.settimeout(10)
        buf = b""
        first_t = None
        while b"\r\n\r\n" not in buf:
            d = conn.recv(65536)
            if not d: return
            if first_t is None: first_t = time.time()
            buf += d
        head, _, rest = buf.partition(b"\r\n\r\n")
        lines = head.decode("latin1").split("\r\n")
        method, path, _ = lines[0].split(" ", 2)
        hd = {l.split(":",1)[0].lower(): l.split(":",1)[1].strip() for l in lines[1:] if ":" in l}
        cl = int(hd.get("content-length", "0"))
        while len(rest) < cl:
            rest += conn.recv(65536)
        body = rest[:cl].decode("utf-8", "replace")
        status, payload, extra = 200, {"ok": 1}, {}
        enc = None; chunked = False
        if path.startswith("/ticket/orderTicket") and method == "GET":
            payload = {"data": {"orderTicket": ticket_obj()}}
        elif path.startswith("/ticket") and method == "POST":
            j = json.loads(body)
            with LOCK:
                cur = ticket_obj()
                if j.get("orderTicket") != cur["ticket"]:
                    status, payload = 400, {"detail": "Invalid parameter", "orderTicket": cur}
                else:
                    GEN["n"] += 1
                    payload = {"data": {"orderTicket": ticket_obj(), "id": 1}}
        elif path.startswith("/gz"):
            enc = "gzip"; chunked = True; payload = {"ok": 1, "msg": "سلام gz"}
        elif path.startswith("/err"):
            status, payload = 200, {"errorCode": 1032, "message": "کد خطا: 1032 بازار بسته است"}
        raw = json.dumps(payload, ensure_ascii=False).encode()
        if enc == "gzip": raw = gzip.compress(raw)
        h = "HTTP/1.1 %d X\r\nContent-Type: application/json; charset=utf-8\r\n" % status
        if enc: h += "Content-Encoding: gzip\r\n"
        if chunked:
            h += "Transfer-Encoding: chunked\r\n\r\n"
            out = h.encode() + (b"%x\r\n" % (len(raw)//2)) + raw[:len(raw)//2] + b"\r\n" + (b"%x\r\n" % (len(raw)-len(raw)//2)) + raw[len(raw)//2:] + b"\r\n0\r\n\r\n"
        else:
            h += "Content-Length: %d\r\n\r\n" % len(raw); out = h.encode() + raw
        conn.sendall(out)
        with LOCK: LOG.append({"t": first_t, "tag": tag, "m": method, "p": path, "b": body, "ua": hd.get("user-agent",""), "ck": hd.get("cookie","")})
    except Exception as e:
        pass
    finally:
        try: conn.close()
        except: pass

def serve(port, use_tls):
    s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port)); s.listen(512)
    ctx = None
    if use_tls:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); ctx.load_cert_chain("c.pem", "k.pem")
    while True:
        c, _ = s.accept()
        c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        def run(c=c):
            try:
                cc = ctx.wrap_socket(c, server_side=True) if ctx else c
            except Exception: c.close(); return
            handle(cc, "https" if use_tls else "http")
        threading.Thread(target=run, daemon=True).start()

def dump_loop():
    while True:
        time.sleep(0.3)
        with LOCK:
            json.dump({"log": LOG, "gen": GEN["n"]}, open("/tmp/mock_log.json", "w"))
threading.Thread(target=serve, args=(HTTP_PORT, False), daemon=True).start()
threading.Thread(target=serve, args=(HTTPS_PORT, True), daemon=True).start()
threading.Thread(target=dump_loop, daemon=True).start()
print("mock up", flush=True)
while True: time.sleep(1)
