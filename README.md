# meshcore-wdgw

A Docker sidecar that listens to a [MeshCore](https://meshcore.co.uk/) LoRa mesh node and uploads observed node adverts (position, signal strength, role) to [WDGoWars](https://wdgwars.pl) live, as they're heard over the air.

Sibling of [adsb-wdgw](https://github.com/jaas666/adsb-wdgw); same HMAC-SHA256 signed envelope, same upload endpoint, `meshcore_nodes` payload slot instead of `aircraft`. Standalone repo and compose stack -- it does not depend on or extend the ADS-B feeder.

## Why this exists, and why it's not a Home Assistant add-on

A MeshCore companion node only accepts **one** direct WiFi/TCP client at a time. If [`meshcore-ha`](https://github.com/meshcore-dev/meshcore-ha) already holds that slot, nothing else can connect directly -- including this feeder.

This project assumes you have a **duplexer/proxy** in front of the node that accepts multiple downstream clients and forwards the node's traffic to all of them (fanning out both replies *and* unsolicited push notifications, not just request/response pairs -- see [Duplexer requirements](#duplexer-requirements)). This feeder is a second, **read-only** client through that duplexer: it never sends adverts, contacts, or messages of its own, and never touches Home Assistant's connection.

## How it works

Unlike adsb-wdgw (which polls an HTTP JSON snapshot), this feeder holds a long-lived TCP connection and reacts to events as MeshCore's firmware pushes them:

1. Connects to the duplexer via [`meshcore_py`](https://github.com/meshcore-dev/meshcore_py) (`pip install meshcore`), the same library `meshcore-ha` is built on.
2. Subscribes to `EventType.RX_LOG_DATA` filtered to `payload_type == 4` (`ADVERT`) -- the push notification the companion firmware sends for every demodulated RF frame, narrowed to just the ones that are node adverts.
3. Each matching event's payload already carries everything needed for one sighting: the advertising node's full 64-hex public key (`adv_key`), its self-reported position (`adv_lat`/`adv_lon`), name (`adv_name`), role (`adv_type`), hop count (`path_len`), and this specific reception's `rssi`/`snr`.
4. Sightings are buffered (freshest per node wins) and flushed to WDGoWars every `WDGWARS_FLUSH_INTERVAL` seconds as an HMAC-signed envelope, batched at up to `WDGWARS_BATCH_SIZE` records per request.

No CSV files, no cron, no MeshCore app export step -- the node is heard directly.

### Resolved: which meshcore_py event carries per-node RSSI/SNR

This was the main open question going in. `EventType.ADVERTISEMENT` (the "a contact changed, refresh your list" notification) only carries a bare public key, no signal data. The signal data lives one layer down, in `EventType.RX_LOG_DATA` -- the raw-RF-reception push notification, which meshcore_py's own parser (`meshcore_parser.py`) further decodes when the received frame is itself an advert. That decode is what supplies `adv_key`/`adv_lat`/`adv_lon`/`adv_name`/`adv_type` alongside the reception's own `rssi`/`snr` -- one event, all the fields this feeder needs, no correlation between separate event streams required.

### Node ID derivation

Matches the convention WDGoWars expects: `node_id` is the first 16 lowercase hex characters (8 bytes) of the node's 64-hex public key. Since `RX_LOG_DATA`'s advert decode always hands over the *full* key, this feeder never has to fall back to a short on-air ID the way a CSV-based tool does.

### Record schema

Follows the same wire contract confirmed against the real server:

| Field | Source |
|---|---|
| `node_id` | first 16 hex chars of `adv_key` |
| `node_type` | `adv_type` mapped to `COMPANION` / `REPEATER` / `ROOM_SERVER` / `SENSOR` |
| `name` | `adv_name`, falling back to `node_id` |
| `lat` / `lon` | `adv_lat` / `adv_lon` |
| `rssi` | this reception's `rssi` |
| `first_seen` | this reception's timestamp |
| `type` | constant `"MESHCORE"` (envelope marker) |
| `network` | constant `"meshcore"` |
| `public_key` | full 64-hex `adv_key` |
| `path_hops` | `path_len` (0 = heard direct) |

Records with no GPS fix, no usable public key, or an `adv_type` this feeder hasn't confirmed a role name for are dropped rather than uploaded with a guessed value.

## Duplexer requirements

`RX_LOG_DATA` is an **unsolicited** push notification -- the node emits it whenever it hears something, not in response to a command this feeder sent. For this feeder to see it, your duplexer needs to forward the node's outbound traffic to every connected downstream client, not just proxy one client's requests to matching responses. If your duplexer only relays 1:1 request/response pairs, this feeder will connect and send `CMD_APP_START` successfully but never receive advert data.

Confirm this before relying on it: point `--debug` logging at the connection and check that `RX_LOG_DATA` events actually arrive while Home Assistant's connection stays up.

## Quick start

```bash
cp .env.example .env
# edit .env: set WDGWARS_API_KEY, MESHCORE_HOST, MESHCORE_PORT
docker compose up -d
docker compose logs -f meshcore-wdgwars
```

Your WDGoWars API key is available in your profile at [wdgwars.pl](https://wdgwars.pl).

## Configuration

All variables are optional except `WDGWARS_API_KEY`, `MESHCORE_HOST`, and `MESHCORE_PORT`.

| Variable | Default | Description |
|---|---|---|
| `WDGWARS_API_KEY` | *(required)* | Your WDGoWars API key |
| `MESHCORE_HOST` | `127.0.0.1` | Host of the duplexer's second connection slot |
| `MESHCORE_PORT` | `5000` | Port of the duplexer's second connection slot |
| `WDGWARS_UPLOAD_URL` | `https://wdgwars.pl/api/upload/` | WDGoWars upload endpoint |
| `WDGWARS_FLUSH_INTERVAL` | `30` | Seconds between uploads of buffered sightings |
| `WDGWARS_BATCH_SIZE` | `1000` | Node records per upload request |
| `WDGWARS_RECONNECT_DELAY` | `10` | Seconds to wait before retrying after a dropped connection |
| `WDGWARS_LOG_LEVEL` | `INFO` | Log verbosity (`DEBUG`, `INFO`, `WARNING`) |

## Running locally without Docker

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export WDGWARS_API_KEY=your_key
export MESHCORE_HOST=127.0.0.1
export MESHCORE_PORT=5000
python3 feeder.py
```

## Contributing

The entire feeder lives in [`feeder.py`](feeder.py). Issues and pull requests are open at [github.com/jaas666/meshcore-wdgw](https://github.com/jaas666/meshcore-wdgw).
