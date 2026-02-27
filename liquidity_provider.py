#!/usr/bin/env python3
"""
Stylized-Facts Liquidity Provider with Emergent Mid Price

Generates limit orders and occasional market orders following empirical
microstructure stylized facts from Bouchaud, Mezard & Potters (2002) et al.

The mid price emerges naturally from order book dynamics: market orders
consume liquidity on one side, shifting the BBO, and the LP reads the
live BBO via UDP market data to derive mid. This removes artificial
volatility and makes price dynamics emergent rather than imposed.

Stylized facts implemented:
  1. Power-law limit order placement:  P(delta) ~ (delta + delta1)^{-mu}, mu ~ 0.6
  2. Mixed order-size distribution:    ~75 % round lots (100-share multiples, Pareto on lot
                                       count); ~25 % odd lots (lognormal, median ≈ 40 shares)
  3. Long memory in order flow:        sign autocorrelation C(tau) ~ tau^{-beta}, beta ~ 0.6
  4. Distance-dependent cancellation:  exponential TTL ~ base * exp(growth * delta)

Each iteration picks ONE random action (bid, ask, market order) with
independently sampled side, price, and size.

Usage:
    python3 exchange_server.py
    python3 liquidity_provider.py --mid-price 100.00 --throttle 50000
    python3 udp_book_builder.py --levels 20 --update-every 10
"""

import argparse
import math
import random
import select
import socket
import sys
import threading
import time
from collections import deque

from udp_book_builder import BookBuilder


# ---------------------------------------------------------------------------
# Stylized-fact parameter defaults
# ---------------------------------------------------------------------------
DEFAULT_MID_PRICE = 1000000      # $100.00 in LOBSTER (x10000)
TICK_SIZE = 100                  # $0.01 in LOBSTER

# Fact 1: Power-law placement  P(delta) ~ (delta + delta1)^{-mu}
MU = 0.6
DELTA1 = 1          # offset in ticks so delta=0 is valid
MAX_DELTA_TICKS = 200

# Fact 2: Order size — mixture of round lots and odd lots
#   Round lots (multiples of 100):  number-of-lots ~ Pareto(ALPHA_SIZE)
#   Odd lots (1–99):                size ~ Lognormal(ODD_LOT_MU, ODD_LOT_SIGMA)
ROUND_LOT_PROB = 0.75       # fraction of orders that are round lots
ALPHA_SIZE = 1.2             # Pareto exponent for round-lot count (lower → heavier tail)
MIN_LOTS = 5                 # minimum number of round lots (= 500 shares)
MAX_LOTS = 500               # maximum number of round lots (= 50,000 shares)
ODD_LOT_MU = 3.7             # lognormal μ  →  median ≈ 40 shares
ODD_LOT_SIGMA = 0.6          # lognormal σ  →  most odd lots land in 20–90 range

# Fact 3: Long memory in order flow via ARFIMA(0,d,0)
FRAC_D = 0.2          # fractional differencing parameter (decay ~ k^{2d-1})
MEMORY_LENGTH = 500   # MA truncation length
MARKET_SIZE_DIV = 5   # market size = sample_order_size() // this

# Fact 4: Exponential TTL — short inside (fast churn at BBO), long outside
TTL_BASE = 3.0        # ~3 seconds at the inside
TTL_GROWTH = 0.05     # exponential growth rate per tick
TTL_MAX = 120         # cap at 2 minutes

# Action weights
WEIGHT_BID = 0.45
WEIGHT_ASK = 0.45
WEIGHT_MARKET = 0.10


class FractionalOrderFlow:
    """
    Generates buy/sell signs (+1/-1) with long-range autocorrelation
    using an ARFIMA(0,d,0) process.

    The latent process X_t = sum_{k=0}^{M} pi_k * eps_{t-k} where
    eps_t ~ N(0,1) iid and pi_k are the fractional MA coefficients.
    sign_t = +1 if X_t > 0 else -1.

    Autocorrelation decays as C(k) ~ k^{2d-1}.  For d=0.2 this gives
    the standard stylized-fact decay exponent of 0.6.
    """

    def __init__(self, d: float = FRAC_D, memory: int = MEMORY_LENGTH):
        self.d = d
        self.memory = memory
        # Precompute MA coefficients: pi_0=1, pi_k = pi_{k-1} * (k-1+d)/k
        self.pi = [0.0] * memory
        self.pi[0] = 1.0
        for k in range(1, memory):
            self.pi[k] = self.pi[k - 1] * (k - 1 + d) / k
        # Ring buffer of past innovations
        self.eps: deque = deque(maxlen=memory)

    def next_sign(self) -> int:
        """Return +1 (buy) or -1 (sell) with long memory."""
        e = random.gauss(0.0, 1.0)
        self.eps.appendleft(e)
        x = 0.0
        for k, eps_k in enumerate(self.eps):
            x += self.pi[k] * eps_k
        return 1 if x > 0 else -1


def sample_delta_ticks() -> int:
    """
    Sample distance from best quote in ticks using power-law placement.
    P(delta) ~ (delta + delta1)^{-mu}

    Inverse CDF of the truncated power law on [0, MAX_DELTA_TICKS]:
      F(d) = (  (d+d1)^{1-mu} - d1^{1-mu}  )
             / ( (dmax+d1)^{1-mu} - d1^{1-mu} )

      d = ( d1^{1-mu} + U * ((dmax+d1)^{1-mu} - d1^{1-mu}) )^{1/(1-mu)} - d1
    """
    u = random.random()
    one_minus_mu = 1.0 - MU
    inv = 1.0 / one_minus_mu
    lo = DELTA1 ** one_minus_mu
    hi = (MAX_DELTA_TICKS + DELTA1) ** one_minus_mu
    d = (lo + u * (hi - lo)) ** inv - DELTA1
    return max(0, int(round(d)))


def sample_order_size() -> int:
    """
    Sample order volume from a two-component mixture:

    Round lot (prob = ROUND_LOT_PROB):
        Draw number-of-lots ~ Pareto(ALPHA_SIZE, xmin=MIN_LOTS) via inverse CDF,
        clamp to [MIN_LOTS, MAX_LOTS], then multiply by 100.
        Expected lot count ≈ ALPHA_SIZE/(ALPHA_SIZE-1) * MIN_LOTS = 30 lots = 3,000 shares.

    Odd lot (prob = 1 - ROUND_LOT_PROB):
        Draw size ~ Lognormal(ODD_LOT_MU, ODD_LOT_SIGMA), clamp to [1, 99].
        Median ≈ exp(ODD_LOT_MU) ≈ 40 shares; suppresses unrealistically tiny
        orders (2, 3, …) while keeping some small orders in the mix.
    """
    if random.random() < ROUND_LOT_PROB:
        u = random.random()
        if u == 0:
            u = 1e-10
        n_lots = MIN_LOTS * u ** (-1.0 / ALPHA_SIZE)
        return min(int(round(n_lots)), MAX_LOTS) * 100
    else:
        v = random.lognormvariate(ODD_LOT_MU, ODD_LOT_SIGMA)
        return max(1, min(int(round(v)), 99))


def order_ttl(delta_ticks: int) -> int:
    """
    Exponential TTL for an order at `delta_ticks` from the best quote.
    Short-lived at the inside (fast churn), long-lived deep in the book.
    """
    t = TTL_BASE * math.exp(TTL_GROWTH * delta_ticks)
    return max(1, min(int(round(t)), TTL_MAX))


class LiquidityProvider:
    """
    Sends limit orders and occasional market orders to the exchange,
    following empirical microstructure stylized facts.

    The mid price is derived from the live BBO via UDP market data.
    Each iteration picks ONE random action — a bid, an ask, or a market
    order — with independently sampled parameters.
    """

    def __init__(self, host: str, port: int, user: str = "lp",
                 mid_price: int = DEFAULT_MID_PRICE,
                 md_port: int = 10002,
                 order_interval: float = 0.05,
                 frac_d: float = FRAC_D,
                 market_size_div: int = MARKET_SIZE_DIV,
                 market_weight: float = WEIGHT_MARKET,
                 verbose: bool = False,
                 log_file: str | None = None):
        self.host = host
        self.port = port
        self.user = user
        self._bootstrap_mid = mid_price
        self.md_port = md_port
        self.order_interval = order_interval
        self.market_size_div = market_size_div
        self.market_weight = market_weight
        self.verbose = verbose

        # Recompute limit weights to fill remaining probability
        self.weight_bid = (1.0 - self.market_weight) / 2.0
        self.weight_ask = (1.0 - self.market_weight) / 2.0

        self.sock: socket.socket | None = None
        self.flow = FractionalOrderFlow(d=frac_d)

        # UDP market data book builder
        self._book_builder = BookBuilder()
        self._md_thread: threading.Thread | None = None

        # Order log
        self._log_file = None
        if log_file:
            self._log_file = open(log_file, 'w')
            self._log_file.write(
                "seq,timestamp,type,side,price,size,delta_ticks,ttl,mid_price\n"
            )

        # Stats
        self.orders_sent = 0
        self.trades_sent = 0
        self._seq = 0

    def _start_md_feed(self):
        """Subscribe to UDP market data and process in a daemon thread."""
        md_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        md_sock.settimeout(2.0)
        md_sock.sendto(b"subscribe", ('127.0.0.1', self.md_port))
        print(f"Subscribed to market data on UDP port {self.md_port}")

        def _reader():
            while True:
                try:
                    data, _ = md_sock.recvfrom(1024)
                    self._book_builder.process_message(data.decode('utf-8').strip())
                except socket.timeout:
                    continue
                except OSError:
                    break

        self._md_thread = threading.Thread(target=_reader, daemon=True)
        self._md_thread.start()

    def _get_mid(self) -> int:
        """Return mid price from live BBO, or bootstrap mid if no BBO yet."""
        bid = self._book_builder.book.get_best_bid_price()
        ask = self._book_builder.book.get_best_ask_price()
        if bid and ask:
            raw = (bid + ask + TICK_SIZE) // 2   # ceiling → symmetric for odd-tick spreads
            return (raw // TICK_SIZE) * TICK_SIZE
        return self._bootstrap_mid

    def connect(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(5.0)
            self.sock.connect((self.host, self.port))
            self.sock.setblocking(False)
            print(f"Connected to exchange at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def _send(self, msg: str):
        """Fire-and-forget send."""
        try:
            self.sock.sendall((msg + "\n").encode('utf-8'))
        except Exception as e:
            if self.verbose:
                print(f"  Send error: {e}")

    def _drain(self):
        """Drain queued responses so the socket buffer doesn't fill up."""
        while True:
            ready, _, _ = select.select([self.sock], [], [], 0)
            if not ready:
                break
            try:
                data = self.sock.recv(65536)
                if not data:
                    break
            except (BlockingIOError, OSError):
                break

    def _log(self, otype: str, side: str, price: int | None,
             size: int, delta: int | None, ttl: int | None):
        """Write one row to the log file."""
        if not self._log_file:
            return
        self._seq += 1
        mid = self._get_mid()
        ts = time.strftime("%H:%M:%S", time.localtime())
        p = f"{price/10000:.2f}" if price else ""
        d = str(delta) if delta is not None else ""
        t = str(ttl) if ttl is not None else ""
        self._log_file.write(
            f"{self._seq},{ts},{otype},{side},{p},{size},{d},{t},"
            f"{mid/10000:.2f}\n"
        )

    def _place_limit(self, side: str):
        """Place one limit order with independently sampled delta and size."""
        delta_ticks = sample_delta_ticks()
        size = sample_order_size()
        delta_price = delta_ticks * TICK_SIZE
        ttl = order_ttl(delta_ticks)
        mid = self._get_mid()

        if side == 'B':
            price = mid - delta_price
        else:
            price = mid + delta_price

        if price <= 0:
            return

        self._send(f"limit,{size},{price},{side},{self.user},{ttl}")
        self.orders_sent += 1

        if self.verbose:
            tag = "BID" if side == 'B' else "ASK"
            print(f"  {tag} {size}@${price/10000:.2f} (delta={delta_ticks}, ttl={ttl}s)")

        self._log("LIMIT", "BID" if side == 'B' else "ASK",
                  price, size, delta_ticks, ttl)

    def _place_market(self):
        """Place a market order; side chosen by long-memory process."""
        sign = self.flow.next_sign()
        side = 'B' if sign == 1 else 'S'
        size = max(1, sample_order_size() // self.market_size_div)

        self._send(f"market,{size},0,{side},{self.user}")
        self.trades_sent += 1

        if self.verbose:
            print(f"  MARKET {'BUY' if side == 'B' else 'SELL'} {size}")

        self._log("MARKET", "BUY" if side == 'B' else "SELL",
                  None, size, None, None)

    # ------------------------------------------------------------------
    def run(self):
        """Main loop: each iteration picks ONE random action."""
        self._start_md_feed()

        mid = self._get_mid()
        print(f"Starting liquidity provider (bootstrap mid=${mid/10000:.2f})")
        print(f"  Order interval: {self.order_interval}s")
        print(f"  Placement exponent mu={MU}, Size: {ROUND_LOT_PROB:.0%} round lots "
              f"(Pareto α={ALPHA_SIZE}), {1-ROUND_LOT_PROB:.0%} odd lots "
              f"(LogNorm μ={ODD_LOT_MU} σ={ODD_LOT_SIGMA})")
        print(f"  ARFIMA(0,d,0) d={self.flow.d}, memory={self.flow.memory}")
        print(f"  Exponential TTL: base={TTL_BASE}s, growth={TTL_GROWTH}, "
              f"max={TTL_MAX}s")
        print(f"  Market size div: {self.market_size_div}, "
              f"Market weight: {self.market_weight}")
        print(f"Press Ctrl+C to stop.\n")

        try:
            iteration = 0
            while True:
                self._drain()

                # Pick ONE action
                r = random.random()
                if r < self.weight_bid:
                    self._place_limit('B')
                elif r < self.weight_bid + self.weight_ask:
                    self._place_limit('S')
                elif r < self.weight_bid + self.weight_ask + self.market_weight:
                    self._place_market()
                # else: no order this tick (natural rate variation)

                time.sleep(random.expovariate(1.0 / self.order_interval))

                iteration += 1
                if iteration % 200 == 0:
                    mid = self._get_mid()
                    print(f"[{iteration}] Orders: {self.orders_sent}  "
                          f"Trades: {self.trades_sent}  "
                          f"Mid: ${mid/10000:.2f}")
                    if self._log_file:
                        self._log_file.flush()

        except KeyboardInterrupt:
            print(f"\nStopped. Total orders: {self.orders_sent}, "
                  f"trades: {self.trades_sent}")
        finally:
            if self._log_file:
                self._log_file.close()
                print(f"Order log saved to: {self._log_file.name}")
            if self.sock:
                self.sock.close()


def main():
    parser = argparse.ArgumentParser(
        description='Stylized-facts liquidity provider'
    )
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=10000)
    parser.add_argument('--mid-price', type=float, default=100.0,
                        help='Bootstrap mid price in dollars (before BBO exists)')
    parser.add_argument('--md-port', type=int, default=10002,
                        help='UDP market data port (default: 10002)')
    parser.add_argument('--throttle', type=int, default=50000, metavar='MICROSECONDS',
                        help='Microseconds between orders (default: 50000 = 50ms)')
    parser.add_argument('--frac-d', type=float, default=FRAC_D,
                        help='ARFIMA fractional differencing parameter '
                             '(0=iid, 0.2=standard, <0.5; default: 0.2)')
    parser.add_argument('--market-size-div', type=int, default=MARKET_SIZE_DIV,
                        help='Market order size divisor — smaller = bigger '
                             'market orders = more volatility (default: 5)')
    parser.add_argument('--market-weight', type=float, default=WEIGHT_MARKET,
                        help='Probability of market order each tick '
                             '(default: 0.40)')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--user', default='lp')
    parser.add_argument('--log-file', type=str, default=None, metavar='FILE',
                        help='Save order log to CSV file')
    parser.add_argument('--seed', type=int, default=None,
                        help='Random seed for reproducible output')
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    mid_lobster = round(args.mid_price * 10000 / TICK_SIZE) * TICK_SIZE

    lp = LiquidityProvider(
        host=args.host,
        port=args.port,
        user=args.user,
        mid_price=mid_lobster,
        md_port=args.md_port,
        order_interval=args.throttle / 1_000_000.0,
        frac_d=args.frac_d,
        market_size_div=args.market_size_div,
        market_weight=args.market_weight,
        verbose=args.verbose,
        log_file=args.log_file,
    )

    if not lp.connect():
        sys.exit(1)

    lp.run()


if __name__ == '__main__':
    main()
