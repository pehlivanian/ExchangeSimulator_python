# Exchange Simulator

A multi-threaded exchange simulator with price-time priority matching, supporting live trading, LOBSTER historical data replay, and NASDAQ ITCH 5.0 historical data replay.

## Quick Start

### Live Trading Mode

**Terminal 1:** Start exchange server
```bash
python exchange_server.py -v
```

**Terminal 2:** Start order client
```bash
python order_client_with_fsm.py
```

Or use the client programmatically:
```python
from order_client_with_fsm import OrderClientWithFSM

client = OrderClientWithFSM()
client.connect()
order = client.create_order('limit', 100, 5000000, 'B')  # Buy 100 @ $500
client.submit_order_sync(order)
print(f'Order {order.exchange_order_id}: {order.state_name}')
client.disconnect()
```

### Historical Replay Mode (LOBSTER — built into the server)

**Terminal 1:** Start server with LOBSTER data
```bash
python exchange_server.py \
    --historical AMZN_2012-06-21_34200000_57600000_message_1.csv \
    --orderbook AMZN_2012-06-21_34200000_57600000_orderbook_1.csv \
    --wait-ready --throttle 10000
```

**Terminal 2:** Start market data client to build order book and plot
```bash
python udp_book_builder.py --save-plot bid_ask.pdf --plot-after 5000
```

Then press Enter in Terminal 1 to start replay.

**Note:** Use `--wait-ready` or `--wait-subscribers N` so clients can connect before replay starts.


### LOBSTER Client Replay Mode

In this mode, a client replays LOBSTER data as orders to a live exchange server. This simulates a liquidity provider feeding orders into the market.

**Terminal 1:** Start exchange server
```bash
python exchange_server.py -v
```
**Terminal 2 (optional):** Observe the order book
```bash
python udp_book_builder.py --levels 5
```

**Terminal 3:** Start LOBSTER client to replay data
```bash
python historical_LOBSTER_client.py AMZN_2012-06-21_34200000_57600000_message_1.csv \
    --throttle 10000
```

-- Will there be any trading activity in this case? Fire up the stp_client.py and find out.


### ITCH 5.0 Client Replay Mode

`historical_ITCH_client.py` reads a NASDAQ TotalView-ITCH 5.0 HDF5 file (produced by `create_ITCH_HDF5_store.py`) and replays every order-book event for a single stock through the live exchange server. The exchange book mirrors the real ITCH book tick-by-tick, so a strategy running in parallel can interact with genuine NASDAQ order flow.

#### Basic replay — all messages for one stock

**Terminal 1:** Start exchange server with market data
```bash
python exchange_server.py --market-data-port 10002
```

**Terminal 2 (optional):** Watch the order book
```bash
python udp_book_builder.py --levels 10
```

**Terminal 3:** Replay AAPL for the full day
```bash
python historical_ITCH_client.py \
    --hdf5 data/itch.h5 \
    --ticker AAPL
```

#### Market-hours only with progress output

```bash
python historical_ITCH_client.py \
    --hdf5 data/itch.h5\
    --ticker AAPL \
    --start-time 09:30:00 \
    --end-time   16:00:00 \
    --print-every 25000
```

#### Throttled replay (100 µs between messages)

Useful when running alongside a strategy that needs time to react:
```bash
python historical_ITCH_client.py \
    --hdf5 data/itch.h5 \
    --ticker AAPL \
    --start-time 09:30:00 \
    --end-time   16:00:00 \
    --throttle 100
```

#### Replay with book diagnostics

**Terminal 1:**
```bash
python exchange_server.py --market-data-port 10002
```

**Terminal 2:**
```bash
python udp_book_builder.py --levels 20 --update-every 500 \
    --diagnostics aapl_report.json --plot-book-shape aapl_shape.pdf
```

**Terminal 3:**
```bash
python historical_ITCH_client.py \
    --hdf5 data/itch.h5 \
    --ticker AAPL \
    --start-time 09:30:00 \
    --end-time   16:00:00 \
    --print-every 50000
```

After the replay finishes, Ctrl+C the book builder to write the report and plot.

#### Running alongside the Avellaneda-Stoikov market maker

**Terminal 1:**
```bash
python exchange_server.py --market-data-port 10002
```

**Terminal 2:** Book builder for observation
```bash
python udp_book_builder.py --levels 10 --update-every 100
```

**Terminal 3:** ITCH replay drives the order book
```bash
python historical_ITCH_client.py \
    --hdf5 data/itch.h5 \
    --ticker AAPL \
    --start-time 09:30:00 \
    --end-time   16:00:00 \
    --throttle 500
```

**Terminal 4:** AS market maker quotes against real NASDAQ flow
```bash
python avellaneda_stoikov.py --gamma 0.1 --order-size 100 -v
```

#### Verbose per-message output (debugging)

```bash
python historical_ITCH_client.py \
    --hdf5 data/itch.h5 \
    --ticker AAPL \
    --start-time 09:30:00 \
    --max-messages 5000 \
    --verbose \
    --verbose-errors
```

#### Select stock by NASDAQ locator integer instead of ticker

```bash
python historical_ITCH_client.py \
    --hdf5 data/itch.h5 \
    --locate 7421
```


### Stylized-Facts Liquidity Provider with Order Book Diagnostics

Generate synthetic order flow and collect order book diagnostics (spread statistics, average book shape by level, message type breakdown).

**Terminal 1:** Start exchange server
```bash
python exchange_server.py
```

**Terminal 2:** Start liquidity provider
```bash
python liquidity_provider.py --mid-price 100.00 --throttle 50000 --log-file orders.csv
```

**Terminal 3:** Build order book with diagnostics and book shape plot
```bash
python udp_book_builder.py --levels 20 --update-every 10 \
    --diagnostics report.json --plot-book-shape lob_shape.pdf
```

Let it run for a while, then Ctrl+C all three. On exit the book builder writes:
- `report.json` — diagnostics (spread statistics, average book shape by level, message type breakdown)
- `lob_shape.pdf` — horizontal bar chart of average bid/ask depth by level
- A human-readable summary printed to stdout

The same workflow works with `historical_LOBSTER_client.py` in place of the liquidity provider:

**Terminal 1:** Start exchange server
```bash
python exchange_server.py
```

**Terminal 2:** Build order book with diagnostics
```bash
python udp_book_builder.py --levels 20 --update-every 10 \
    --diagnostics report.json --plot-book-shape lob_shape.pdf
```

**Terminal 3:** Replay LOBSTER data as live orders
```bash
python historical_LOBSTER_client.py AMZN_2012-06-21_34200000_57600000_message_1.csv \
    --throttle 10000
```

After replay completes, Ctrl+C the book builder to generate the report and plot.


### Avellaneda-Stoikov Market Maker

Run an optimal market maker (Avellaneda & Stoikov, 2008) that quotes bid/ask prices adjusted for inventory risk against the liquidity provider's synthetic order flow.

**Terminal 1:** Start exchange server
```bash
python exchange_server.py
```

**Terminal 2 (optional):** Observe the order book
```bash
python udp_book_builder.py --levels 5 --update-every 10
```

**Terminal 3:** Start liquidity provider
```bash
python liquidity_provider.py --mid-price 100.00 --throttle 50000
```

**Terminal 4:** Start AS market maker
```bash
python avellaneda_stoikov.py --gamma 0.1 --order-size 100 -v
```


The LP derives its mid price from the live BBO via UDP market data — there is no artificial random walk. Price movement emerges from market order imbalance: autocorrelated runs of buys sweep the ask side (pushing mid up), then runs of sells sweep the bid side (pulling it back). Orders use exponential TTLs (short-lived at the inside for fast churn, long-lived deep in the book).

Three CLI knobs control volatility:

| Flag | Default | Effect |
|------|---------|--------|
| `--frac-d` | `0.2` | ARFIMA fractional differencing parameter (0 = iid, 0.2 = standard decay, < 0.5) |
| `--market-size-div` | `5` | Market order size divisor (lower = bigger orders = more levels consumed per trade) |
| `--market-weight` | `0.40` | Fraction of ticks that produce a market order (limit weights auto-adjust) |

For higher volatility:
```bash
python liquidity_provider.py --mid-price 100.00 --throttle 50000 \
    --frac-d 0.35 --market-size-div 3
```

The market maker continuously estimates volatility from BBO updates, computes a reservation price shifted by inventory risk, and derives an optimal spread. Quotes widen with larger inventory and higher volatility.

## Components

### exchange_server.py

The central order book and matching engine. Accepts orders and broadcasts market data.

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Show order book after each order |
| `--historical FILE` | LOBSTER message file for replay mode |
| `--orderbook FILE` | LOBSTER orderbook file (seeds book, enables validation) |
| `--throttle MICROSECONDS` | Delay between messages |
| `--max-messages N` | Stop after N messages |
| `--no-validate` | Disable orderbook validation |
| `--wait-subscribers N` | Wait for N market data subscribers before starting |

### historical_ITCH_client.py

Replays NASDAQ TotalView-ITCH 5.0 data from an HDF5 file as live orders to the exchange server. Each ITCH message type is translated to the appropriate exchange operation:

| ITCH type | Action |
|-----------|--------|
| `/A`, `/F` | Submit limit order; track in order map |
| `/D` | Cancel resting order |
| `/X` | Partial cancel: cancel + resubmit with reduced size |
| `/E`, printable `/C` | Send market order (aggressor side) so the tape event is broadcast; reconcile resting order size |
| non-printable `/C` | Silently shrink resting order (no tape print) |
| `/U` | Cancel original order; submit replacement at new price/size (side preserved) |
| `/P` | Synthetic limit + market pair for hidden-order tape prints |

| Option | Description |
|--------|-------------|
| `--hdf5 FILE` | Path to ITCH HDF5 file (required) |
| `--ticker SYMBOL` | Stock ticker, e.g. `AAPL` (mutually exclusive with `--locate`) |
| `--locate N` | NASDAQ stock locator integer (alternative to `--ticker`) |
| `--host HOST` | Exchange host (default: localhost) |
| `--port PORT` | Exchange TCP order port (default: 10000) |
| `--start-time HH:MM:SS` | Skip messages before this time |
| `--end-time HH:MM:SS` | Stop after this time |
| `--throttle MICROSECONDS` | Sleep between messages in µs (default: 0) |
| `--max-messages N` | Stop after N messages |
| `-v, --verbose` | Print each message as processed |
| `--verbose-errors` | Print full error details |

### historical_LOBSTER_client.py

Replays LOBSTER data as live orders to the exchange server.

| Option | Description |
|--------|-------------|
| `message_file` | LOBSTER message file (required) |
| `--host HOST` | Exchange host (default: localhost) |
| `--throttle MICROSECONDS` | Delay between messages |
| `--max-messages N` | Stop after N messages |
| `-v, --verbose` | Print each message |

### udp_book_builder.py

Subscribes to market data, builds order book, plots bid/ask.

| Option | Description |
|--------|-------------|
| `--levels N` | Book levels to display (default: 10) |
| `--save-plot FILE` | Save bid/ask plot to PDF |
| `--plot-after N` | Save plot after N messages |
| `--no-clear` | Don't clear screen between updates |
| `--diagnostics FILE` | Collect microstructure diagnostics; write JSON report to FILE on exit |
| `--plot-book-shape FILE` | Save average book shape plot to FILE (requires `--diagnostics`) |

### liquidity_provider.py

Generates synthetic order flow following empirical microstructure stylized facts.

| Option | Description |
|--------|-------------|
| `--mid-price PRICE` | Bootstrap mid price in dollars before BBO exists (default: 100.00) |
| `--md-port PORT` | UDP market data port (default: 10002) |
| `--throttle MICROSECONDS` | Microseconds between orders (default: 50000) |
| `--frac-d FLOAT` | ARFIMA fractional differencing parameter (default: 0.2) |
| `--market-size-div INT` | Market order size divisor (default: 5) |
| `--market-weight FLOAT` | Market order probability per tick (default: 0.40) |
| `--log-file FILE` | Save order log to CSV |
| `--user USER` | User identifier (default: lp) |
| `--seed INT` | Random seed for reproducible output |
| `-v, --verbose` | Print each order |

### avellaneda_stoikov.py

Avellaneda-Stoikov optimal market maker. Quotes bid/ask adjusted for inventory risk.

| Option | Description |
|--------|-------------|
| `--host HOST` | Exchange host (default: localhost) |
| `--order-port PORT` | Order TCP port (default: 10000) |
| `--md-port PORT` | Market data UDP port (default: 10002) |
| `--gamma FLOAT` | Risk aversion parameter (default: 0.01) |
| `--k FLOAT` | Order arrival intensity (default: 20.0) |
| `--horizon FLOAT` | Time horizon in seconds (default: 300) |
| `--sigma-window N` | Mid-price observations for volatility (default: 100) |
| `--max-inventory N` | Maximum absolute position (default: 500) |
| `--order-size N` | Quote size per side (default: 100) |
| `--min-requote FLOAT` | Minimum seconds between re-quotes (default: 0.1) |
| `--warmup FLOAT` | Seconds to wait before quoting (default: 5.0) |
| `--k-auto` | Dynamically estimate k from observed trade rate |
| `-v, --verbose` | Print quote updates and fills |
| `-o, --output FILE` | Output plot filename (default: as_market_maker.pdf) |
| `--signal {ofi}` | Enable alpha signal (default: none) |
| `--ofi-top-params A B` | Power-law coefficients for top quintile (default: 17.42 0.468) |
| `--ofi-bot-params A B` | Power-law coefficients for bottom quintile (default: -35.64 0.457) |
| `--ofi-top-edge FLOAT` | Imbalance threshold for bullish regime (default: 0.6) |
| `--ofi-bot-edge FLOAT` | Imbalance threshold for bearish regime (default: -0.333) |
| `--ofi-max-delta FLOAT` | Max delta in ms for extrapolation cap (default: 1.0) |

### order_client_with_fsm.py

Python client library with order state tracking.

```python
from order_client_with_fsm import OrderClientWithFSM

client = OrderClientWithFSM()
client.connect()
client.start_async_receive()  # Enable async fill notifications

# Create and submit orders
order = client.create_order('limit', 100, 5000000, 'B')  # limit buy
client.submit_order_sync(order)

# Cancel
client.cancel_order_sync(order.exchange_order_id)

client.disconnect()
```

### stp_client.py

Subscribes to the trade feed and prints executed trades.

```bash
python stp_client.py
```

| Option | Description |
|--------|-------------|
| `--host HOST` | Exchange host (default: localhost) |

## Order Format

```
limit,size,price,side,user[,ttl]   # Limit order
market,size,0,side,user            # Market order
cancel,order_id,user               # Cancel order
modify,order_id,size,price,user    # Modify order (cancel-replace)
```

- **Price**: price * 10000 (e.g., $500.00 = 5000000)
- **Side**: B=Buy, S=Sell
- **TTL**: Time-to-live in seconds (default: 3600)

### Order Validation

The exchange rejects orders that fail any of the following checks:

| Rule | Threshold | Reason |
|------|-----------|--------|
| Minimum price | price >= 20000 ($2.00) | Reject sub-penny/sub-dollar stocks |
| Maximum size | size <= 999999 | Reject unreasonably large orders |
| Fat-finger detection | size <= price | If size > price, the fields are likely swapped (LOBSTER prices are always much larger than share quantities) |

Validation applies to new limit orders and modify orders. Market orders are exempt (they carry price=0 by design). Rejected orders receive a `REJECT` message with a descriptive reason.

## Data Sources

### LOBSTER Data

Download sample data from [LOBSTER](https://lobsterdata.com/). Files needed:
- `*_message_*.csv` — Order events (time, type, id, size, price, direction)
- `*_orderbook_*.csv` — Book snapshots (ask1, size1, bid1, size1, ...)

### NASDAQ ITCH 5.0 Data

ITCH data is stored in HDF5 format with one table per message type (`/A`, `/F`, `/D`, `/E`, `/C`, `/X`, `/U`, `/P`), indexed by `stock_locate`. Use `create_ITCH_HDF5_store.py` (or the accompanying notebook) to convert a raw ITCH binary file to the HDF5 store. The file is passed to `historical_ITCH_client.py` via `--hdf5`.

## Tests

Note: The AMZN_*_message_* and AMZN_*_orderbook_* files must be present or
      accessible (symlinked) from the unittests directory for some of the tests.

```bash
pytest tests/                           # All tests
pytest tests/unittests/                 # Unit tests only
pytest tests/integrationtests/          # Integration tests only
```
