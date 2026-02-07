# Exchange Simulator

A multi-threaded exchange simulator with price-time priority matching, supporting live trading and LOBSTER historical data replay.

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

### Historical Replay Mode

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


### Liquidity Provider Replay Mode

In this mode, a client replays LOBSTER data as orders to a live exchange server. This simulates a liquidity provider feeding orders into the market.

**Terminal 1:** Start exchange server
```bash
python exchange_server.py -v
```
**Terminal 2 (optional):** Observe the order book
```bash
python udp_bo--	ok_builder.py --levels 5
```

**Terminal 3:** Start historical order client to replay LOBSTER data
```bash
python historical_order_client.py AMZN_2012-06-21_34200000_57600000_message_1.csv \
    --throttle 10000
```

-- Will there be any trading activity in this case? Fire up the stp_client.py and find out.



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

### historical_order_client.py

Replays LOBSTER data as live orders.

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
```

- **Price**: price * 10000 (e.g., $500.00 = 5000000)
- **Side**: B=Buy, S=Sell
- **TTL**: Time-to-live in seconds (default: 3600)

## LOBSTER Data

Download sample data from [LOBSTER](https://lobsterdata.com/). Files needed:
- `*_message_*.csv` - Order events (time, type, id, size, price, direction)
- `*_orderbook_*.csv` - Book snapshots (ask1, size1, bid1, size1, ...)

## Tests

Note: The AMZN_*_message_* and AMZN_*_orderbook_* files must be present or 
      accessible (symlinked) from the unittests directory for some of the tests.

```bash
pytest tests/                           # All tests
pytest tests/unittests/                 # Unit tests only
pytest tests/integrationtests/          # Integration tests only
```
