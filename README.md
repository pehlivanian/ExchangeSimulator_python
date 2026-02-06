# Exchange Simulator

A multi-threaded exchange simulator with price-time priority matching, supporting live trading and LOBSTER historical data replay.

## Quick Start

### Live Trading Mode

```bash
# Terminal 1: Start exchange server
python exchange_server.py

# Terminal 2: Connect with order client
python -c "
from order_client_with_fsm import OrderClientWithFSM
client = OrderClientWithFSM('localhost', 10000)
client.connect()
order = client.create_order('limit', 100, 5000000, 'B')  # Buy 100 @ $500
client.submit_order_sync(order)
print(f'Order {order.exchange_order_id}: {order.state_name}')
"
```

### Historical Replay Mode

```bash
# Terminal 1: Start server with LOBSTER data
python exchange_server.py \
    --historical AMZN_2012-06-21_34200000_57600000_message_1.csv \
    --orderbook AMZN_2012-06-21_34200000_57600000_orderbook_1.csv \
    --throttle 1000

# Terminal 2: Build order book and plot bid/ask
python udp_book_builder.py \
    --orderbook AMZN_2012-06-21_34200000_57600000_orderbook_1.csv \
    --save-plot bid_ask.pdf --plot-after 5000
```

## Components

### exchange_server.py

The matching engine. Accepts TCP orders, broadcasts UDP market data.

| Option | Description |
|--------|-------------|
| `--order-port PORT` | TCP order port (default: 10000) |
| `--feed-port PORT` | STP trade feed port (default: 10001) |
| `--market-data-port PORT` | UDP market data port (default: 10002, 0=disable) |
| `-v, --verbose` | Show order book after each order |
| `--historical FILE` | LOBSTER message file for replay mode |
| `--orderbook FILE` | LOBSTER orderbook file (seeds book, enables validation) |
| `--throttle MICROSECONDS` | Delay between messages |
| `--max-messages N` | Stop after N messages |
| `--no-validate` | Disable orderbook validation |
| `--wait-subscribers N` | Wait for N UDP subscribers before starting |

### historical_order_client.py

Replays LOBSTER data as live orders via TCP.

| Option | Description |
|--------|-------------|
| `message_file` | LOBSTER message file (required) |
| `--host HOST` | Exchange host (default: localhost) |
| `--port PORT` | Exchange port (default: 10000) |
| `--throttle MICROSECONDS` | Delay between messages |
| `--max-messages N` | Stop after N messages |
| `-v, --verbose` | Print each message |

### udp_book_builder.py

Subscribes to UDP market data, builds order book, plots bid/ask.

| Option | Description |
|--------|-------------|
| `--port PORT` | UDP port (default: 10002) |
| `--orderbook FILE` | LOBSTER orderbook file to seed initial state |
| `--levels N` | Book levels to display (default: 10) |
| `--save-plot FILE` | Save bid/ask plot to PDF |
| `--plot-after N` | Save plot after N messages |
| `--no-clear` | Don't clear screen between updates |

### order_client_with_fsm.py

Python client library with order state tracking.

```python
from order_client_with_fsm import OrderClientWithFSM

client = OrderClientWithFSM('localhost', 10000)
client.connect()
client.start_async_receive()  # Enable async fill notifications

# Create and submit orders
order = client.create_order('limit', 100, 5000000, 'B')  # limit buy
client.submit_order_sync(order)

# Cancel
client.cancel_order_sync(order.exchange_order_id)

client.disconnect()
```

## Order Format

```
limit,size,price,side,user[,ttl]   # Limit order
market,size,0,side,user            # Market order
cancel,order_id,user               # Cancel order
```

- **Price**: price * 10000 (e.g., $500.00 = 5000000)
- **Side**: B=Buy, S=Sell
- **TTL**: Time-to-live in seconds (default: 3600)

## LOBSTER Data

Download sample data from [LOBSTER](https://lobsterdata.com/). Files needed:
- `*_message_*.csv` - Order events (time, type, id, size, price, direction)
- `*_orderbook_*.csv` - Book snapshots (ask1, size1, bid1, size1, ...)

## Tests

```bash
pytest tests/                           # All tests
pytest tests/unittests/                 # Unit tests only
pytest tests/integrationtests/          # Integration tests only
```
