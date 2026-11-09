#!/usr/bin/env python3
"""Independent, standard-library client and wire codec for Atlas protocol v1."""
from __future__ import annotations

import argparse
import json
import socket
import struct
from dataclasses import dataclass

PUT, GET, DELETE, ADD, HASH, STATS = range(1, 7)
OK, INVALID, NOT_FOUND, CONFLICT, OVERLOADED, TIMEOUT, INTERNAL, SHUTTING_DOWN = range(8)
STATUS_NAMES = ["ok", "invalid", "not_found", "conflict", "overloaded", "timeout", "internal", "shutting_down"]
MAX_RESPONSE = 1 << 20


def key_payload(key: str | bytes) -> bytes:
    key = key.encode() if isinstance(key, str) else key
    if not 1 <= len(key) <= 128:
        raise ValueError("key must contain 1..128 UTF-8 bytes")
    return struct.pack("!H", len(key)) + key


def put_payload(key: str | bytes, value: str | bytes, expected_version: int = 0) -> bytes:
    value = value.encode() if isinstance(value, str) else value
    return key_payload(key) + struct.pack("!Q", expected_version) + value


def add_payload(key: str | bytes, delta: int) -> bytes:
    return key_payload(key) + struct.pack("!q", delta)


def hash_payload(iterations: int, seed: bytes = b"atlas") -> bytes:
    return struct.pack("!I", iterations) + seed


def encode(kind: int, request_id: int, payload: bytes = b"", version: int = 1) -> bytes:
    body = struct.pack("!BBQ", version, kind, request_id) + payload
    return struct.pack("!I", len(body)) + body


@dataclass(frozen=True)
class Response:
    kind: int
    request_id: int
    status: int
    payload: bytes

    def versioned(self) -> tuple[int, bytes]:
        if self.status != OK or len(self.payload) < 8:
            raise ValueError("not a successful versioned response")
        return struct.unpack("!Q", self.payload[:8])[0], self.payload[8:]


def decode_response(body: bytes) -> Response:
    if len(body) < 12:
        raise ValueError("response shorter than header and status")
    version, kind, request_id, status = struct.unpack("!BBQH", body[:12])
    if version != 1 or not kind & 0x80:
        raise ValueError("invalid response header")
    return Response(kind & 0x7F, request_id, status, body[12:])


def recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise EOFError("peer closed connection")
        chunks.extend(chunk)
    return bytes(chunks)


def receive(sock: socket.socket) -> Response:
    length = struct.unpack("!I", recv_exact(sock, 4))[0]
    if not 12 <= length <= MAX_RESPONSE:
        raise ValueError(f"invalid response length: {length}")
    return decode_response(recv_exact(sock, length))


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 7777, timeout: float = 5):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.next_id = 1

    def close(self):
        self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def call(self, kind: int, payload: bytes = b"") -> Response:
        request_id = self.next_id
        self.next_id += 1
        self.sock.sendall(encode(kind, request_id, payload))
        response = receive(self.sock)
        if (response.kind, response.request_id) != (kind, request_id):
            raise ValueError("response does not match request")
        return response

    def stats(self) -> dict:
        response = self.call(STATS)
        if response.status != OK:
            raise RuntimeError(response)
        return json.loads(response.payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7777)
    sub = parser.add_subparsers(dest="operation", required=True)
    p = sub.add_parser("put")
    p.add_argument("key")
    p.add_argument("value")
    p.add_argument("--version", type=int, default=0)
    for operation in ("get", "delete"):
        sub.add_parser(operation).add_argument("key")
    p = sub.add_parser("add")
    p.add_argument("key")
    p.add_argument("delta", type=int)
    p = sub.add_parser("hash")
    p.add_argument("iterations", type=int)
    p.add_argument("seed")
    sub.add_parser("stats")
    args = parser.parse_args()
    kind = {"put": PUT, "get": GET, "delete": DELETE, "add": ADD, "hash": HASH, "stats": STATS}[args.operation]
    if kind == PUT:
        payload = put_payload(args.key, args.value, args.version)
    elif kind in (GET, DELETE):
        payload = key_payload(args.key)
    elif kind == ADD:
        payload = add_payload(args.key, args.delta)
    elif kind == HASH:
        payload = hash_payload(args.iterations, args.seed.encode())
    else:
        payload = b""
    with Client(args.host, args.port) as client:
        response = client.call(kind, payload)
    result = {"request_id": response.request_id, "status": STATUS_NAMES[response.status] if response.status < len(STATUS_NAMES) else response.status}
    if response.status == OK and kind in (PUT, GET, ADD):
        version, value = response.versioned()
        result.update(version=version, value=value.decode(errors="replace"), value_hex=value.hex())
    elif response.status == OK and kind == STATS:
        result["metrics"] = json.loads(response.payload)
    elif response.status == OK and kind == HASH:
        result["digest_hex"] = response.payload.hex()
    else:
        result["message"] = response.payload.decode(errors="replace")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if response.status == OK else 1)


if __name__ == "__main__":
    main()
