#!/usr/bin/env python3
import socket
import sys


def build_resp(items):
    out = f"*{len(items)}\r\n".encode()
    for it in items:
        b = str(it).encode()
        out += b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"
    return out


def read_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed while reading")
        buf.extend(chunk)
    return bytes(buf)


def read_line(sock):
    data = bytearray()
    while True:
        ch = sock.recv(1)
        if not ch:
            raise ConnectionError("socket closed while reading line")
        data.extend(ch)
        if len(data) >= 2 and data[-2:] == b"\r\n":
            return bytes(data[:-2])


def read_resp(sock):
    t = sock.recv(1)
    if not t:
        raise ConnectionError("socket closed before reply type")
    t = t.decode()
    if t == '+':
        return ('simple_string', read_line(sock).decode())
    if t == '-':
        return ('error', read_line(sock).decode())
    if t == ':':
        return ('integer', int(read_line(sock)))
    if t == '$':
        ln = int(read_line(sock))
        if ln == -1:
            return ('bulk', None)
        s = read_exact(sock, ln)
        assert read_exact(sock, 2) == b"\r\n"
        return ('bulk', s.decode())
    if t == '*':
        n = int(read_line(sock))
        if n == -1:
            return ('array', None)
        arr = [read_resp(sock) for _ in range(n)]
        return ('array', arr)
    raise ValueError(f"unknown RESP type: {t}")


def main():
    if len(sys.argv) < 4:
        print("usage: resp_client.py HOST PORT MODE [ARGS..]", file=sys.stderr)
        sys.exit(2)
    host, port, mode, *args = sys.argv[1:]
    port = int(port)
    with socket.create_connection((host, port)) as s:
        s.settimeout(8.0)
        if mode == 'blpop':
            key, timeout = args
            payload = build_resp(['BLPOP', key, timeout])
        elif mode == 'rpush':
            key, *values = args
            payload = build_resp(['RPUSH', key, *values])
        else:
            raise SystemExit(f'unknown mode: {mode}')
        s.sendall(payload)
        reply = read_resp(s)
        print(reply)


if __name__ == '__main__':
    main()


