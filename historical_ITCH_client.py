#!/usr/bin/env python3
"""
Historical ITCH Client - Replays NASDAQ ITCH 5.0 data from HDF5 to the exchange.

Reads ITCH messages from an HDF5 file produced by the ITCH parser notebook and
converts each message to the appropriate exchange operation:

  /A  Add Order              -> limit order (buy_sell_indicator determines side)
  /F  Add Order w/ MPID      -> same as /A
  /D  Delete Order           -> cancel
  /X  Order Cancel (partial) -> cancel + resubmit with reduced size
  /E  Order Executed         -> market order on aggressor side + reconcile resting
  /C  Executed with Price    -> if printable: same as /E; if non-printable: silently
                                shrink the resting order without a tape print
  /U  Order Replace          -> cancel old + new limit order (same side)
  /P  Non-Cross Trade        -> inject synthetic limit+market pair to produce an
                                EXECUTE tape event; neither party is in the visible
                                book (both were hidden orders), so no tracked order
                                is affected

/P buy_sell_indicator is always 1 after 2014 (garbage); direction is inferred from
context (synthetic SELL limit + market BUY).

Usage:
    python historical_ITCH_client.py --hdf5 data/itch.h5 --ticker AAPL
    python historical_ITCH_client.py --hdf5 data/itch.h5 --ticker AAPL \\
        --start-time 09:30:00 --end-time 16:00:00 --throttle 100
"""

import argparse
import socket
import sys
import time
from typing import Dict, Iterator, Optional, Tuple

import pandas as pd


# All orders placed under this username
_USER = "itch_replay"

# Long TTL so orders don't expire during a fast replay
_ORDER_TTL = 86400  # 24 hours


# ---------------------------------------------------------------------------
# ITCHReader — loads and merges ITCH message tables for one stock
# ---------------------------------------------------------------------------

class ITCHReader:
    """
    Loads ITCH message tables from an HDF5 file for a single stock and yields
    them in timestamp order.

    HDF5 tables used (all keyed by stock_locate):
        /A  order_reference_number, buy_sell_indicator, shares, price
        /F  order_reference_number, buy_sell_indicator, shares, price
        /D  order_reference_number
        /E  order_reference_number, executed_shares
        /C  order_reference_number, executed_shares, execution_price, printable
        /X  order_reference_number, cancelled_shares
        /U  original_order_reference_number, new_order_reference_number, shares, price
        /P  shares, price   (order_reference_number is 0 for hidden orders)

    Each yielded row is a named tuple with fields:
        timestamp_s  float   seconds from midnight
        msg_type     str     one of A F D E C X U P
        order_ref    int     primary order reference number (0 for /P)
        new_ref      int     /U new ref (-1 otherwise)
        side         int     +1=buy -1=sell  0=not applicable
        shares       int     shares for this event
        price        int     price in LOBSTER format (price * 10000); 0 if N/A
        printable    int     1=tape print  0=non-printable (meaningful for /C only)
    """

    def __init__(self, hdf5_path: str,
                 ticker: Optional[str] = None,
                 stock_locate: Optional[int] = None):
        if ticker is None and stock_locate is None:
            raise ValueError("Specify either --ticker or --locate")
        self.hdf5_path = hdf5_path
        self.ticker = ticker
        self._locate: Optional[int] = stock_locate
        self._df: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------
    def load(self) -> int:
        """Load and merge all message types. Returns total row count."""
        if self._locate is None:
            self._locate = self._lookup_locate(self.ticker)

        frames = []
        loc = self._locate

        with pd.HDFStore(self.hdf5_path, 'r') as store:
            keys = set(store.keys())

            # /A  Add Order
            if '/A' in keys:
                df = store.select('A', where=f'stock_locate == {loc}',
                                  columns=['timestamp', 'order_reference_number',
                                           'buy_sell_indicator', 'shares', 'price'])
                if len(df):
                    df = df.rename(columns={'order_reference_number': 'order_ref'})
                    df['msg_type'] = 'A'
                    df['new_ref'] = -1
                    df['side'] = df['buy_sell_indicator']
                    frames.append(df[['timestamp', 'msg_type', 'order_ref',
                                      'new_ref', 'side', 'shares', 'price']])

            # /F  Add Order with MPID  (same fields as /A for our purposes)
            if '/F' in keys:
                df = store.select('F', where=f'stock_locate == {loc}',
                                  columns=['timestamp', 'order_reference_number',
                                           'buy_sell_indicator', 'shares', 'price'])
                if len(df):
                    df = df.rename(columns={'order_reference_number': 'order_ref'})
                    df['msg_type'] = 'F'
                    df['new_ref'] = -1
                    df['side'] = df['buy_sell_indicator']
                    frames.append(df[['timestamp', 'msg_type', 'order_ref',
                                      'new_ref', 'side', 'shares', 'price']])

            # /D  Delete Order
            if '/D' in keys:
                df = store.select('D', where=f'stock_locate == {loc}',
                                  columns=['timestamp', 'order_reference_number'])
                if len(df):
                    df = df.rename(columns={'order_reference_number': 'order_ref'})
                    df['msg_type'] = 'D'
                    df['new_ref'] = -1
                    df['side'] = 0
                    df['shares'] = 0
                    df['price'] = 0
                    frames.append(df[['timestamp', 'msg_type', 'order_ref',
                                      'new_ref', 'side', 'shares', 'price']])

            # /E  Order Executed
            if '/E' in keys:
                df = store.select('E', where=f'stock_locate == {loc}',
                                  columns=['timestamp', 'order_reference_number',
                                           'executed_shares'])
                if len(df):
                    df = df.rename(columns={'order_reference_number': 'order_ref',
                                           'executed_shares': 'shares'})
                    df['msg_type'] = 'E'
                    df['new_ref'] = -1
                    df['side'] = 0
                    df['price'] = 0
                    frames.append(df[['timestamp', 'msg_type', 'order_ref',
                                      'new_ref', 'side', 'shares', 'price']])

            # /C  Executed with Price  (may or may not print to tape)
            if '/C' in keys:
                df = store.select('C', where=f'stock_locate == {loc}',
                                  columns=['timestamp', 'order_reference_number',
                                           'executed_shares', 'execution_price',
                                           'printable'])
                if len(df):
                    df = df.rename(columns={'order_reference_number': 'order_ref',
                                           'executed_shares': 'shares',
                                           'execution_price': 'price'})
                    df['msg_type'] = 'C'
                    df['new_ref'] = -1
                    df['side'] = 0
                    frames.append(df[['timestamp', 'msg_type', 'order_ref',
                                      'new_ref', 'side', 'shares', 'price',
                                      'printable']])

            # /X  Order Cancel (partial)
            if '/X' in keys:
                df = store.select('X', where=f'stock_locate == {loc}',
                                  columns=['timestamp', 'order_reference_number',
                                           'cancelled_shares'])
                if len(df):
                    df = df.rename(columns={'order_reference_number': 'order_ref',
                                           'cancelled_shares': 'shares'})
                    df['msg_type'] = 'X'
                    df['new_ref'] = -1
                    df['side'] = 0
                    df['price'] = 0
                    frames.append(df[['timestamp', 'msg_type', 'order_ref',
                                      'new_ref', 'side', 'shares', 'price']])

            # /U  Order Replace
            if '/U' in keys:
                df = store.select('U', where=f'stock_locate == {loc}',
                                  columns=['timestamp',
                                           'original_order_reference_number',
                                           'new_order_reference_number',
                                           'shares', 'price'])
                if len(df):
                    df = df.rename(
                        columns={'original_order_reference_number': 'order_ref',
                                 'new_order_reference_number': 'new_ref'})
                    df['msg_type'] = 'U'
                    df['side'] = 0
                    frames.append(df[['timestamp', 'msg_type', 'order_ref',
                                      'new_ref', 'side', 'shares', 'price']])

            # /P  Non-Cross Trade — hidden-order executions that print to tape.
            # buy_sell_indicator is always 1 after 2014 (useless); we use price
            # and size only.  order_reference_number is 0 for hidden orders.
            if '/P' in keys:
                df = store.select('P', where=f'stock_locate == {loc}',
                                  columns=['timestamp', 'shares', 'price'])
                if len(df):
                    df['msg_type'] = 'P'
                    df['order_ref'] = 0
                    df['new_ref'] = -1
                    df['side'] = 0
                    frames.append(df[['timestamp', 'msg_type', 'order_ref',
                                      'new_ref', 'side', 'shares', 'price']])

        if not frames:
            self._df = pd.DataFrame()
            return 0

        combined = pd.concat(frames, ignore_index=True)
        combined.sort_values('timestamp', inplace=True)
        combined['timestamp_s'] = combined['timestamp'].dt.total_seconds()
        # printable is only set for /C rows; fill 1 everywhere else
        if 'printable' not in combined.columns:
            combined['printable'] = 1
        else:
            combined['printable'] = combined['printable'].fillna(1).astype(int)
        combined.reset_index(drop=True, inplace=True)
        self._df = combined
        return len(combined)

    # ------------------------------------------------------------------
    def iter_messages(self, start_time_s: float = 0.0,
                      end_time_s: float = float('inf')) -> Iterator:
        """Yield rows as named tuples in timestamp order."""
        if self._df is None:
            self.load()
        for row in self._df.itertuples(index=False):
            ts = row.timestamp_s
            if ts < start_time_s:
                continue
            if ts > end_time_s:
                break
            yield row

    # ------------------------------------------------------------------
    def _lookup_locate(self, ticker: str) -> int:
        with pd.HDFStore(self.hdf5_path, 'r') as store:
            if '/R' not in store.keys():
                raise ValueError("HDF5 file has no /R (Stock Directory) table")
            r = store.select('R', columns=['stock_locate', 'stock'])
        matches = r[r['stock'] == ticker]
        if matches.empty:
            raise ValueError(f"Ticker '{ticker}' not found in stock directory")
        return int(matches.iloc[0]['stock_locate'])

    @property
    def stock_locate(self) -> Optional[int]:
        return self._locate


# ---------------------------------------------------------------------------
# HistoricalITCHClient — sends ITCH events to the exchange server over TCP
# ---------------------------------------------------------------------------

class HistoricalITCHClient:
    """
    Replays ITCH messages to the exchange simulator.

    Maintains a map  itch_ref -> (exchange_order_id, remaining_shares, price, side)
    so that delete / execute / cancel / replace can reference the correct
    exchange order.
    """

    def __init__(self, host: str, port: int, verbose_errors: bool = False):
        self.host = host
        self.port = port
        self._socket: Optional[socket.socket] = None
        self._buffer = ""
        self._verbose_errors = verbose_errors

        # itch_ref -> (exchange_id, remaining_shares, price, side)
        self._order_map: Dict[int, Tuple[int, int, int, str]] = {}

        # Statistics
        self.orders_sent = 0
        self.cancels_sent = 0
        self.market_orders_sent = 0
        self.immediate_fills = 0
        self.noncross_trades = 0
        self.errors = 0
        self.skipped = 0
        self.add_errors = 0
        self.cancel_errors = 0
        self.execute_errors = 0

    # ------------------------------------------------------------------
    def connect(self) -> bool:
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self.host, self.port))
            self._socket.settimeout(5.0)
            return True
        except Exception as e:
            print(f"Failed to connect: {e}", file=sys.stderr)
            return False

    def disconnect(self) -> None:
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

    # ------------------------------------------------------------------
    def _send_and_receive(self, message: str) -> str:
        if not self._socket:
            return "ERROR,Not connected"
        try:
            # Discard any stale async messages that accumulated since the last
            # command (e.g. passive FILL/PARTIAL_FILL from a market order that
            # hit one of our own resting orders).  If these are left in the
            # socket they will be read as the response to the *next* command,
            # causing _parse_ack to return the wrong exchange order ID.
            self._buffer = ""
            self._socket.setblocking(False)
            try:
                while True:
                    stale = self._socket.recv(4096)
                    if not stale:
                        break
            except (BlockingIOError, OSError):
                pass
            finally:
                self._socket.setblocking(True)

            if not message.endswith('\n'):
                message += '\n'
            self._socket.sendall(message.encode('utf-8'))

            self._socket.settimeout(5.0)
            data = self._socket.recv(4096)
            self._buffer += data.decode('utf-8')

            # Drain any additional data (e.g. ACK + FILL on immediate match)
            self._socket.setblocking(False)
            try:
                while True:
                    more = self._socket.recv(4096)
                    if not more:
                        break
                    self._buffer += more.decode('utf-8')
            except BlockingIOError:
                pass
            finally:
                self._socket.setblocking(True)

            lines = []
            while '\n' in self._buffer:
                line, self._buffer = self._buffer.split('\n', 1)
                line = line.strip()
                if line:
                    lines.append(line)
            return '\n'.join(lines) if lines else ""

        except socket.timeout:
            return "ERROR,Timeout"
        except Exception as e:
            return f"ERROR,{e}"

    def _parse_ack(self, response: str) -> Tuple[Optional[int], bool]:
        """Return (exchange_order_id, was_immediately_filled)."""
        for line in response.strip().split('\n'):
            parts = line.split(',')
            if len(parts) < 2:
                continue
            t = parts[0]
            try:
                if t == 'ACK':
                    return int(parts[1]), False
                if t == 'FILL':
                    return int(parts[1]), True
                if t == 'PARTIAL_FILL':
                    return int(parts[1]), False   # resting remainder still lives
            except (ValueError, IndexError):
                pass
        return None, False

    # ------------------------------------------------------------------
    # Per-message-type handlers
    # ------------------------------------------------------------------

    def process_add(self, row) -> bool:
        """Handle /A and /F  — submit a limit order and track it."""
        side = 'B' if row.side == 1 else 'S'
        order_str = f"limit,{row.shares},{row.price},{side},{_USER},{_ORDER_TTL}"
        response = self._send_and_receive(order_str)

        exchange_id, was_fill = self._parse_ack(response)
        if exchange_id is not None:
            self.orders_sent += 1
            if was_fill:
                self.immediate_fills += 1
            else:
                self._order_map[row.order_ref] = (exchange_id, row.shares,
                                                  row.price, side)
            return True
        else:
            self.errors += 1
            self.add_errors += 1
            if self._verbose_errors:
                print(f"  ADD error: {order_str} -> {response}")
            return False

    def process_delete(self, row) -> bool:
        """Handle /D  — cancel the resting order."""
        if row.order_ref not in self._order_map:
            self.skipped += 1
            return False

        exchange_id, _, _, _ = self._order_map[row.order_ref]
        cancel_str = f"cancel,{exchange_id},{_USER}"
        response = self._send_and_receive(cancel_str)

        del self._order_map[row.order_ref]
        if response.startswith('CANCEL_ACK'):
            self.cancels_sent += 1
            return True
        else:
            self.errors += 1
            self.cancel_errors += 1
            if self._verbose_errors:
                print(f"  DELETE error: {cancel_str} -> {response}")
            return False

    def process_execute(self, row) -> bool:
        """
        Handle /E and printable /C — a resting order was hit.

        Send a market order on the aggressor side (opposite of the resting
        order's side) so the exchange generates a proper execution event and
        broadcasts it over UDP.  Then reconcile the resting order's size.

        For non-printable /C (printable=0), skip the market order — the
        execution does not appear on the tape — and only shrink the resting order.
        """
        if row.order_ref not in self._order_map:
            self.skipped += 1
            return False

        exchange_id, old_size, price, side = self._order_map[row.order_ref]

        if row.printable:
            aggressor_side = 'S' if side == 'B' else 'B'
            market_str = f"market,{row.shares},0,{aggressor_side},{_USER}"
            response = self._send_and_receive(market_str)

            if 'REJECT' in response:
                if self._verbose_errors:
                    print(f"  EXECUTE rejected: {market_str} -> {response}")
                self.errors += 1
                self.execute_errors += 1
                self._shrink_or_remove(row.order_ref, exchange_id, old_size,
                                       price, side, row.shares)
                return False

            self.market_orders_sent += 1

        self._shrink_or_remove(row.order_ref, exchange_id, old_size,
                               price, side, row.shares)
        return True

    def process_noncross_trade(self, row) -> bool:
        """
        Handle /P Non-Cross Trade — hidden-order tape print.

        Both parties were non-displayed (hidden) orders, so no entry in
        _order_map is affected.  We inject a synthetic SELL limit at the trade
        price followed immediately by a market BUY so the exchange broadcasts
        an EXECUTE event at the correct price and size.

        If the synthetic SELL immediately crosses an existing bid it will fill
        against that bid (still producing a tape print at the right price).
        Any unfilled remainder of the synthetic SELL is cancelled so the
        visible book is left unchanged on net.
        """
        # Place a synthetic SELL at the trade price
        sell_str = f"limit,{row.shares},{row.price},S,{_USER},2"  # TTL=2s
        sell_resp = self._send_and_receive(sell_str)
        sell_id, sell_filled = self._parse_ack(sell_resp)

        if sell_id is None:
            self.errors += 1
            if self._verbose_errors:
                print(f"  NONCROSS SELL failed: {sell_str} -> {sell_resp}")
            return False

        self.orders_sent += 1

        if sell_filled:
            # Immediately crossed an existing bid — tape print already generated
            self.market_orders_sent += 1
            return True

        # SELL is resting — consume it with a market BUY
        buy_str = f"market,{row.shares},0,B,{_USER}"
        buy_resp = self._send_and_receive(buy_str)

        if 'REJECT' not in buy_resp:
            self.market_orders_sent += 1

        # Cancel any unfilled remainder of the synthetic SELL
        cancel_str = f"cancel,{sell_id},{_USER}"
        cancel_resp = self._send_and_receive(cancel_str)
        if cancel_resp.startswith('CANCEL_ACK'):
            self.cancels_sent += 1

        return 'REJECT' not in buy_resp

    def _shrink_or_remove(self, order_ref: int, exchange_id: int,
                          old_size: int, price: int, side: str,
                          executed: int) -> None:
        """Cancel the resting order; resubmit if there is a remaining quantity."""
        new_size = old_size - executed
        cancel_str = f"cancel,{exchange_id},{_USER}"
        resp = self._send_and_receive(cancel_str)

        if 'CANCEL_ACK' not in resp:
            # Cancel rejected — order was already consumed by the matching
            # engine (e.g. our market order fully filled it).  Nothing to
            # resubmit; just clean up the map.
            self._order_map.pop(order_ref, None)
            return

        self.cancels_sent += 1

        if new_size > 0:
            order_str = f"limit,{new_size},{price},{side},{_USER},{_ORDER_TTL}"
            resp2 = self._send_and_receive(order_str)
            new_id, was_fill = self._parse_ack(resp2)
            if new_id is not None:
                self.orders_sent += 1
                if was_fill:
                    self.immediate_fills += 1
                    self._order_map.pop(order_ref, None)
                else:
                    self._order_map[order_ref] = (new_id, new_size, price, side)
            else:
                self._order_map.pop(order_ref, None)
                self.errors += 1
        else:
            self._order_map.pop(order_ref, None)

    def process_cancel(self, row) -> bool:
        """
        Handle /X  — partial cancellation.

        Cancel the existing exchange order; resubmit with the reduced size.
        """
        if row.order_ref not in self._order_map:
            self.skipped += 1
            return False

        exchange_id, old_size, price, side = self._order_map[row.order_ref]
        cancel_str = f"cancel,{exchange_id},{_USER}"
        response = self._send_and_receive(cancel_str)

        if not response.startswith('CANCEL_ACK'):
            self.errors += 1
            self.cancel_errors += 1
            if self._verbose_errors:
                print(f"  CANCEL error: {cancel_str} -> {response}")
            del self._order_map[row.order_ref]
            return False

        self.cancels_sent += 1
        new_size = old_size - row.shares

        if new_size > 0:
            order_str = f"limit,{new_size},{price},{side},{_USER},{_ORDER_TTL}"
            response = self._send_and_receive(order_str)
            new_id, was_fill = self._parse_ack(response)
            if new_id is not None:
                self.orders_sent += 1
                if was_fill:
                    self.immediate_fills += 1
                    del self._order_map[row.order_ref]
                else:
                    self._order_map[row.order_ref] = (new_id, new_size, price, side)
            else:
                del self._order_map[row.order_ref]
                self.errors += 1
        else:
            del self._order_map[row.order_ref]

        return True

    def process_replace(self, row) -> bool:
        """
        Handle /U  — cancel original, submit replacement with same side.

        The ITCH spec guarantees that a replace keeps the same side; only
        price and size change.  We inherit side from tracking.
        """
        if row.order_ref not in self._order_map:
            self.skipped += 1
            return False

        exchange_id, _, _, side = self._order_map[row.order_ref]

        cancel_str = f"cancel,{exchange_id},{_USER}"
        response = self._send_and_receive(cancel_str)
        del self._order_map[row.order_ref]

        if not response.startswith('CANCEL_ACK'):
            self.errors += 1
            self.cancel_errors += 1
            if self._verbose_errors:
                print(f"  REPLACE cancel error: {cancel_str} -> {response}")
            return False

        self.cancels_sent += 1

        order_str = f"limit,{row.shares},{row.price},{side},{_USER},{_ORDER_TTL}"
        response = self._send_and_receive(order_str)
        new_id, was_fill = self._parse_ack(response)

        if new_id is not None:
            self.orders_sent += 1
            if was_fill:
                self.immediate_fills += 1
            else:
                self._order_map[row.new_ref] = (new_id, row.shares, row.price, side)
            return True
        else:
            self.errors += 1
            self.add_errors += 1
            if self._verbose_errors:
                print(f"  REPLACE submit error: {order_str} -> {response}")
            return False

    # ------------------------------------------------------------------
    def process_message(self, row) -> bool:
        t = row.msg_type
        if t in ('A', 'F'):
            return self.process_add(row)
        elif t == 'D':
            return self.process_delete(row)
        elif t in ('E', 'C'):
            return self.process_execute(row)
        elif t == 'X':
            return self.process_cancel(row)
        elif t == 'U':
            return self.process_replace(row)
        elif t == 'P':
            result = self.process_noncross_trade(row)
            if result:
                self.noncross_trades += 1
            return result
        else:
            self.skipped += 1
            return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_hhmm(s: str) -> float:
    """Convert HH:MM:SS (or HH:MM:SS.fff) to seconds from midnight."""
    parts = s.split(':')
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            f"Invalid time '{s}'. Expected HH:MM:SS")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])


def _fmt_time(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Historical ITCH Client — replay NASDAQ ITCH 5.0 HDF5 data '
                    'as orders to the exchange simulator.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Replay all AAPL messages
  python historical_ITCH_client.py --hdf5 data/itch.h5 --ticker AAPL

  # Replay regular market hours only, progress every 10k messages
  python historical_ITCH_client.py --hdf5 data/itch.h5 --ticker AAPL \\
      --start-time 09:30:00 --end-time 16:00:00 --print-every 10000

  # Slow replay (100 µs between messages) with verbose output
  python historical_ITCH_client.py --hdf5 data/itch.h5 --ticker AAPL \\
      --throttle 100 --verbose

  # Check with book builder (run in a separate terminal first):
  #   python exchange_server.py --market-data-port 10002
  #   python udp_book_builder.py --port 10002
"""
    )

    parser.add_argument('--hdf5', required=True, metavar='FILE',
                        help='Path to the ITCH HDF5 file (e.g., data/itch.h5)')

    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument('--ticker', metavar='SYMBOL',
                     help='Stock ticker symbol (e.g., AAPL)')
    grp.add_argument('--locate', type=int, metavar='N',
                     help='NASDAQ stock locator integer (alternative to --ticker)')

    parser.add_argument('--host', default='localhost',
                        help='Exchange host (default: localhost)')
    parser.add_argument('--port', type=int, default=10000,
                        help='Exchange TCP order port (default: 10000)')
    parser.add_argument('--throttle', type=int, default=0, metavar='MICROSECONDS',
                        help='Sleep between messages in µs (0 = no throttle)')
    parser.add_argument('--max-messages', type=int, default=0, metavar='N',
                        help='Stop after N messages (0 = all)')
    parser.add_argument('--start-time', metavar='HH:MM:SS', default=None,
                        help='Skip messages before this time (e.g., 09:30:00)')
    parser.add_argument('--end-time', metavar='HH:MM:SS', default=None,
                        help='Stop after this time (e.g., 16:00:00)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Print each message as processed')
    parser.add_argument('--verbose-errors', action='store_true',
                        help='Print full error details')

    args = parser.parse_args()

    start_time_s = _parse_hhmm(args.start_time) if args.start_time else 0.0
    end_time_s   = _parse_hhmm(args.end_time)   if args.end_time   else float('inf')
    throttle_s   = args.throttle / 1_000_000.0  if args.throttle   else 0.0

    # ------------------------------------------------------------------
    reader = ITCHReader(
        hdf5_path=args.hdf5,
        ticker=args.ticker,
        stock_locate=args.locate,
    )

    print("=" * 70)
    print("  HISTORICAL ITCH CLIENT")
    print("=" * 70)
    print(f"  HDF5 file:    {args.hdf5}")
    print("  Loading messages...", end='', flush=True)

    try:
        total = reader.load()
    except (ValueError, FileNotFoundError) as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)

    label = args.ticker if args.ticker else f"locate={args.locate}"
    print(f" {total:,} rows  (stock_locate={reader.stock_locate})")

    print(f"  Ticker:       {label}")
    print(f"  Exchange:     {args.host}:{args.port}")
    if args.start_time:
        print(f"  Start time:   {args.start_time}")
    if args.end_time:
        print(f"  End time:     {args.end_time}")
    if args.throttle:
        print(f"  Throttle:     {args.throttle} µs")
    if args.max_messages:
        print(f"  Max messages: {args.max_messages:,}")
    print("=" * 70)
    print()

    client = HistoricalITCHClient(
        args.host, args.port, verbose_errors=args.verbose_errors)
    if not client.connect():
        sys.exit(1)

    TYPE_NAMES = {
        'A': 'ADD', 'F': 'ADD_MPID', 'D': 'DELETE',
        'E': 'EXECUTE', 'C': 'EXEC_PX', 'X': 'CANCEL', 'U': 'REPLACE',
        'P': 'NONCROSS',
    }

    count = 0
    wall_start = time.time()

    try:
        for row in reader.iter_messages(start_time_s, end_time_s):
            count += 1
            if args.max_messages and count > args.max_messages:
                break

            if args.verbose:
                side_str = ('BUY ' if row.side == 1 else
                            'SELL' if row.side == -1 else '    ')
                px = row.price / 10000.0 if row.price else 0.0
                print(f"[{count:7d}] {_fmt_time(row.timestamp_s)} "
                      f"{TYPE_NAMES.get(row.msg_type,'?'):8s} "
                      f"ref={row.order_ref:15d} "
                      f"shares={row.shares:7d} price={px:10.4f} {side_str}")

            client.process_message(row)

            if count % 10_000 == 0:
                elapsed = time.time() - wall_start
                rate = count / elapsed if elapsed else 0
                print(f"  [{count:>10,}/{total:,}]  {_fmt_time(row.timestamp_s)}"
                      f"  {rate:,.0f} msg/s"
                      f"  orders={client.orders_sent:,}"
                      f"  errors={client.errors:,}"
                      f"  active={len(client._order_map):,}")

            if throttle_s:
                time.sleep(throttle_s)

    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        client.disconnect()

    elapsed = time.time() - wall_start
    rate = count / elapsed if elapsed else 0

    print()
    print("=" * 70)
    print("  REPLAY COMPLETE")
    print("=" * 70)
    print(f"  Ticker:             {label}")
    print(f"  Messages processed: {count:,}")
    print(f"  Orders sent:        {client.orders_sent:,}")
    print(f"  Market orders sent: {client.market_orders_sent:,}")
    print(f"  Immediate fills:    {client.immediate_fills:,}")
    print(f"  Non-cross trades:   {client.noncross_trades:,}")
    print(f"  Cancels sent:       {client.cancels_sent:,}")
    print(f"  Errors:             {client.errors:,}")
    if client.errors:
        print(f"    - Add errors:     {client.add_errors:,}")
        print(f"    - Cancel errors:  {client.cancel_errors:,}")
        print(f"    - Execute errors: {client.execute_errors:,}")
    print(f"  Skipped:            {client.skipped:,}")
    print(f"  Active orders:      {len(client._order_map):,}")
    print(f"  Elapsed time:       {elapsed:.2f}s ({rate:.0f} msg/s)")
    print("=" * 70)


if __name__ == '__main__':
    main()
