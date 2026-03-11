"""
logger.py
---------
Structured logging: console + rotating file + CSV trade log.
"""

import csv
import logging
import logging.handlers
import os
import time
from pathlib import Path
from typing import Optional


def setup_logging(log_dir: str = "logs", level: str = "INFO") -> logging.Logger:
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  %(name)-16s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # Rotating file handler
    fh = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "bot.log"),
        maxBytes=10 * 1024 * 1024,   # 10MB
        backupCount=5,
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    return root


class TradeCSVLogger:
    """
    Appends every trade (entry + exit) to a CSV file.
    Useful for backtesting validation and performance analysis.
    """
    FIELDS = [
        "timestamp", "event", "order_id", "market_title", "condition_id",
        "side", "entry_price", "exit_price", "fair_value_at_entry",
        "edge_at_entry", "size_usdc", "shares", "hold_seconds",
        "pnl_usdc", "cumulative_pnl", "reason",
    ]

    def __init__(self, path: str = "logs/trades.csv"):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._cumulative_pnl = 0.0

        # Write header if new file
        if not os.path.exists(path):
            with open(path, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=self.FIELDS).writeheader()

    def log_entry(self, order) -> None:
        self._write({
            "timestamp":          time.strftime("%Y-%m-%d %H:%M:%S"),
            "event":              "ENTRY",
            "order_id":           order.id,
            "market_title":       order.market_title,
            "condition_id":       order.condition_id,
            "side":               order.side,
            "entry_price":        f"{order.entry_price:.4f}",
            "exit_price":         "",
            "fair_value_at_entry": f"{order.fair_value:.4f}",
            "edge_at_entry":      f"{order.edge_at_entry:+.4f}",
            "size_usdc":          f"{order.size_usdc:.2f}",
            "shares":             f"{order.shares:.4f}",
            "hold_seconds":       "",
            "pnl_usdc":           "",
            "cumulative_pnl":     f"{self._cumulative_pnl:.2f}",
            "reason":             "",
        })

    def log_exit(self, order, reason: str = "") -> None:
        self._cumulative_pnl += order.pnl_usdc or 0
        self._write({
            "timestamp":          time.strftime("%Y-%m-%d %H:%M:%S"),
            "event":              "EXIT",
            "order_id":           order.id,
            "market_title":       order.market_title,
            "condition_id":       order.condition_id,
            "side":               order.side,
            "entry_price":        f"{order.entry_price:.4f}",
            "exit_price":         f"{order.exit_price:.4f}" if order.exit_price else "",
            "fair_value_at_entry": f"{order.fair_value:.4f}",
            "edge_at_entry":      f"{order.edge_at_entry:+.4f}",
            "size_usdc":          f"{order.size_usdc:.2f}",
            "shares":             f"{order.shares:.4f}",
            "hold_seconds":       f"{order.hold_seconds:.1f}",
            "pnl_usdc":           f"{order.pnl_usdc:+.2f}" if order.pnl_usdc is not None else "",
            "cumulative_pnl":     f"{self._cumulative_pnl:.2f}",
            "reason":             reason,
        })

    def _write(self, row: dict) -> None:
        with open(self._path, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=self.FIELDS).writerow(row)
