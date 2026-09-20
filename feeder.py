#!/usr/bin/env python3
"""
meshcore-wdgwars-feeder
------------------------
Sibling of adsb-wdgw. Same HMAC-SHA256 envelope, same WDGoWars upload
endpoint, `meshcore_nodes` payload slot instead of `aircraft`.

Unlike adsb-wdgw (which polls an HTTP JSON endpoint), this feeder holds a
long-lived, read-only TCP connection to a MeshCore companion node and
uploads adverts as they're heard over the air. It is designed to run
through a duplexer/proxy that lets a second client connect alongside an
existing one (e.g. Home Assistant's meshcore-ha integration) without
taking over the node's single direct connection slot. This feeder never
sends contacts, adverts, or messages of its own -- it only listens.

Node identity, schema, and envelope all follow the confirmed-working
wire contract for the same WDGoWars mesh slot:
  - node_id is the first 16 lowercase hex chars (8 bytes) of the node's
    64-hex public key -- the canonical form WDGoWars expects.
  - `type` is the constant envelope marker "MESHCORE"; `network` is the
    constant "meshcore"; the node's own role goes in `node_type`.
  - Records with no GPS fix or an unrecognised role are dropped rather
    than uploaded with an invented value.

Dependencies: meshcore (meshcore_py), Python 3.10+.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import signal
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

from meshcore import MeshCore
from meshcore.events import Event, EventType

# ── Config from environment ───────────────────────────────────────────────


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip().strip('"').strip("'")


WDGWARS_API_KEY = _env("WDGWARS_API_KEY")
UPLOAD_URL = _env("WDGWARS_UPLOAD_URL", "https://wdgwars.pl/api/upload/")
BATCH_SIZE = int(_env("WDGWARS_BATCH_SIZE", "1000"))
FLUSH_INTERVAL = int(_env("WDGWARS_FLUSH_INTERVAL", "30"))
LOG_LEVEL = _env("WDGWARS_LOG_LEVEL", "INFO").upper()

# Duplexer/proxy address this feeder connects to as its second, passive
# TCP client. Point this at the duplexer, not directly at the MeshCore
# node, unless the node itself accepts more than one connection.
MESHCORE_HOST = _env("MESHCORE_HOST", "127.0.0.1")
MESHCORE_PORT = int(_env("MESHCORE_PORT", "5000"))

# Firmware allows a handful of reconnect attempts per drop before giving
# up; this feeder wraps that with its own outer retry loop so a duplexer
# hiccup doesn't require a container restart.
RECONNECT_DELAY = int(_env("WDGWARS_RECONNECT_DELAY", "10"))

# ── Logging ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("feeder")
logging.getLogger("meshcore").setLevel(logging.WARNING if LOG_LEVEL != "DEBUG" else logging.DEBUG)

# ── MeshCore advert -> WDGWars record ─────────────────────────────────────

# adv_type is the low nibble of the advert's flags byte. Confirmed against
# meshcore_py's own CONTACT_TYPENAMES and independently against the
# MeshCore app's SQLite schema -- both agree on this mapping. 0
# ("NONE"/unset) is left unmapped and dropped: it isn't a
# role any real node advertises, so guessing a default would misrepresent it.
ADV_TYPE_NAMES = {
    1: "COMPANION",
    2: "REPEATER",
    3: "ROOM_SERVER",
    4: "SENSOR",
}

MESHCORE_ENVELOPE_TYPE = "MESHCORE"

# RX_LOG_DATA's payload_type is a 4-bit field parsed from the packet header;
# 4 is PAYLOAD_TYPENAMES[4] == "ADVERT" in meshcore_py. Subscribing with this
# attribute filter means only advert receptions reach the handler at all --
# text messages, acks, and routed traffic the node forwards are never
# inspected or uploaded.
ADVERT_PAYLOAD_TYPE = 4


def _format_first_seen(epoch_ts: Any) -> str:
    try:
        return datetime.fromtimestamp(int(epoch_ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def build_record(log_data: dict[str, Any]) -> dict[str, Any] | None:
    """Convert one RX_LOG_DATA(ADVERT) payload into a WDGWars meshcore_nodes
    record, or None if the sighting can't be sent honestly.

    Dropped rather than guessed:
      * no public key (adv_key) or a key that isn't 64 hex -- there is no
        way to derive a server-legal node_id without it.
      * no GPS fix -- wdgwars.pl rejects lat/lon-less records as no_gps.
      * an adv_type this feeder has not confirmed a role name for.
    """
    adv_key = str(log_data.get("adv_key") or "").strip().lower()
    short_id = adv_key[:16] if adv_key else "?"
    log.debug(
        "Advert heard: key=%s name=%r adv_type=%s rssi=%s",
        short_id, log_data.get("adv_name"), log_data.get("adv_type"), log_data.get("rssi"),
    )

    if len(adv_key) != 64 or not all(c in "0123456789abcdef" for c in adv_key):
        log.debug("Dropped %s: no usable public key (adv_key=%r)", short_id, log_data.get("adv_key"))
        return None

    lat = log_data.get("adv_lat")
    lon = log_data.get("adv_lon")
    if lat is None or lon is None:
        log.debug("Dropped %s: no GPS fix (advert carries no adv_lat/adv_lon)", short_id)
        return None
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        log.debug("Dropped %s: non-numeric GPS fix (adv_lat=%r adv_lon=%r)", short_id, log_data.get("adv_lat"), log_data.get("adv_lon"))
        return None
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        log.debug("Dropped %s: GPS fix out of range (lat=%s lon=%s)", short_id, lat, lon)
        return None

    node_type = ADV_TYPE_NAMES.get(log_data.get("adv_type"))
    if node_type is None:
        log.debug("Dropped %s: unrecognized adv_type=%s", short_id, log_data.get("adv_type"))
        return None

    node_id = adv_key[:16]
    name = str(log_data.get("adv_name") or "").strip() or node_id

    return {
        "node_id": node_id,
        "node_type": node_type,
        "name": name,
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "rssi": log_data.get("rssi"),
        "first_seen": _format_first_seen(log_data.get("recv_time")),
        "type": MESHCORE_ENVELOPE_TYPE,
        "network": "meshcore",
        "public_key": adv_key,
        "path_hops": int(log_data.get("path_len") or 0),
    }


# ── Upload envelope + POST (byte-identical to adsb-wdgw) ──────────────────


def build_envelope(nodes: list[dict[str, Any]], api_key: str) -> bytes:
    payload = {"networks": [], "aircraft": [], "meshcore_nodes": nodes}
    data_b64 = base64.b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()
    nonce = secrets.token_hex(8)
    sig = hmac.new(api_key.encode(), (nonce + data_b64).encode(), hashlib.sha256).hexdigest()
    return json.dumps({"data": data_b64, "nonce": nonce, "sig": sig}).encode()


def upload_batch(nodes: list[dict[str, Any]], api_key: str, url: str) -> bool:
    body = build_envelope(nodes, api_key)
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "X-API-Key": api_key,
            "Content-Type": "application/json",
            "User-Agent": "meshcore-wdgwars-feeder/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode())
            log.debug("Upload response: %s", result)
            return True
    except urllib.error.HTTPError as e:
        log.warning("Upload HTTP %s: %s", e.code, e.read().decode(errors="replace")[:300])
    except urllib.error.URLError as e:
        log.warning("Upload network error: %s", e.reason)
    except Exception as e:
        log.warning("Upload error: %s", e)
    return False


def upload_records(nodes: list[dict[str, Any]], api_key: str, url: str) -> None:
    if not nodes:
        return
    total = len(nodes)
    uploaded = 0
    for i in range(0, total, BATCH_SIZE):
        chunk = nodes[i : i + BATCH_SIZE]
        if upload_batch(chunk, api_key, url):
            uploaded += len(chunk)
    if uploaded == total:
        log.info("Uploaded %d meshcore node sighting(s)", uploaded)
    else:
        log.warning("Uploaded %d/%d meshcore node sighting(s)", uploaded, total)


# ── Live capture ───────────────────────────────────────────────────────────


class NodeBuffer:
    """Coalesces sightings between flushes, keeping the freshest per node_id.

    RX_LOG_DATA handlers run synchronously inline (see meshcore_py's
    EventDispatcher), so plain dict writes here need no locking.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, dict[str, Any]] = {}

    def add(self, record: dict[str, Any]) -> None:
        self._nodes[record["node_id"]] = record

    def drain(self) -> list[dict[str, Any]]:
        nodes = list(self._nodes.values())
        self._nodes.clear()
        return nodes


async def flush_loop(buffer: NodeBuffer, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=FLUSH_INTERVAL)
        except asyncio.TimeoutError:
            pass
        nodes = buffer.drain()
        if nodes:
            await asyncio.to_thread(upload_records, nodes, WDGWARS_API_KEY, UPLOAD_URL)


async def run_session(buffer: NodeBuffer, stop_event: asyncio.Event) -> None:
    """One connect-listen-until-disconnected cycle. Raises on failure to
    connect so the caller's outer loop can back off and retry.

    Returns as soon as either the node disconnects or `stop_event` is set
    (e.g. SIGTERM from `docker stop`) -- without racing stop_event here,
    a clean shutdown request would sit blocked on the mesh connection
    until Docker's stop grace period expires and it gets SIGKILLed
    instead, skipping the final buffer flush and clean disconnect.
    """

    def handle_rx_log_data(event: Event) -> None:
        record = build_record(event.payload)
        if record is not None:
            buffer.add(record)
            log.debug(
                "Buffered %s (%s) rssi=%s hops=%d",
                record["node_id"], record["name"], record["rssi"], record["path_hops"],
            )

    disconnected = asyncio.Event()

    def handle_disconnected(event: Event) -> None:
        log.warning("Disconnected from %s:%d (%s)", MESHCORE_HOST, MESHCORE_PORT, event.payload)
        disconnected.set()

    log.info("Connecting to MeshCore duplexer at %s:%d", MESHCORE_HOST, MESHCORE_PORT)
    mc = await MeshCore.create_tcp(MESHCORE_HOST, MESHCORE_PORT, auto_reconnect=True)

    mc.subscribe(
        EventType.RX_LOG_DATA,
        handle_rx_log_data,
        attribute_filters={"payload_type": ADVERT_PAYLOAD_TYPE},
    )
    mc.subscribe(EventType.DISCONNECTED, handle_disconnected)

    log.info("Connected. Listening for node adverts (read-only, sends nothing to the mesh)")
    try:
        disconnected_waiter = asyncio.ensure_future(disconnected.wait())
        stop_waiter = asyncio.ensure_future(stop_event.wait())
        try:
            await asyncio.wait(
                {disconnected_waiter, stop_waiter}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for waiter in (disconnected_waiter, stop_waiter):
                if not waiter.done():
                    waiter.cancel()
    finally:
        await mc.disconnect()


async def main() -> None:
    log.info("meshcore-wdgwars-feeder starting")
    log.info("  MeshCore endpoint : %s:%d", MESHCORE_HOST, MESHCORE_PORT)
    log.info("  Upload URL        : %s", UPLOAD_URL)
    log.info("  Flush interval    : %ds", FLUSH_INTERVAL)
    log.info("  Batch size        : %d", BATCH_SIZE)
    log.info("  Log level         : %s", LOG_LEVEL)

    buffer = NodeBuffer()
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            loop.add_signal_handler(getattr(signal, sig_name), stop_event.set)

    flush_task = asyncio.create_task(flush_loop(buffer, stop_event))

    try:
        while not stop_event.is_set():
            try:
                await run_session(buffer, stop_event)
            except Exception as e:
                log.warning("MeshCore connection error: %s", e)

            if stop_event.is_set():
                break

            log.info("Reconnecting in %ds", RECONNECT_DELAY)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=RECONNECT_DELAY)
            except asyncio.TimeoutError:
                pass
    finally:
        stop_event.set()
        # Upload whatever the buffer still holds before exiting.
        nodes = buffer.drain()
        if nodes:
            await asyncio.to_thread(upload_records, nodes, WDGWARS_API_KEY, UPLOAD_URL)
        await flush_task


if __name__ == "__main__":
    if not WDGWARS_API_KEY:
        log.error("WDGWARS_API_KEY environment variable is required")
        sys.exit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped")
