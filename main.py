import os
import time
import json
import secrets
import logging
from dataclasses import dataclass
from threading import RLock
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Optional

from fastapi import FastAPI, HTTPException, Query, Request

# ============================================================
# CONFIG
# ============================================================

SECRET = os.getenv("APP_SECRET", "12345")
CLAIM_TTL_SECONDS = int(os.getenv("CLAIM_TTL_SECONDS", "15"))
ACK_RETENTION_SECONDS = int(os.getenv("ACK_RETENTION_SECONDS", "600"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("tv-mt5-signal-server")

app = FastAPI(title="TradingView -> MT5 Signal Server")


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class SignalItem:
    payload: Dict[str, Any]
    state: str = "pending"  # pending | claimed | acked
    claim_token: Optional[str] = None
    claimed_at_monotonic: float = 0.0
    claim_expires_at_monotonic: float = 0.0
    acked_at_monotonic: float = 0.0


class SignalStore:
    def __init__(self) -> None:
        self._lock = RLock()
        self._by_id: Dict[str, SignalItem] = {}
        self._queues: Dict[str, Deque[str]] = defaultdict(deque)
        self._last_signal: Optional[Dict[str, Any]] = None

    # ----------------------------
    # cleanup
    # ----------------------------
    def _cleanup_acked_locked(self) -> None:
        now = time.monotonic()
        to_remove = []

        for signal_id, item in self._by_id.items():
            if item.state == "acked" and item.acked_at_monotonic > 0:
                if now - item.acked_at_monotonic >= ACK_RETENTION_SECONDS:
                    to_remove.append(signal_id)

        for signal_id in to_remove:
            item = self._by_id.pop(signal_id, None)
            if item is None:
                continue

            symbol = item.payload["symbol"]
            queue = self._queues.get(symbol)
            if queue is not None:
                try:
                    queue.remove(signal_id)
                except ValueError:
                    pass

                if not queue:
                    self._queues.pop(symbol, None)

        if to_remove:
            logger.info("Cleaned acked signals: %s", len(to_remove))

    def _expire_claims_for_symbol_locked(self, symbol: str) -> None:
        now = time.monotonic()
        queue = self._queues.get(symbol)
        if not queue:
            return

        for signal_id in list(queue):
            item = self._by_id.get(signal_id)
            if item is None:
                continue

            if item.state == "claimed" and item.claim_expires_at_monotonic > 0:
                if now >= item.claim_expires_at_monotonic:
                    logger.warning(
                        "Claim expired. signal_id=%s symbol=%s",
                        signal_id,
                        symbol,
                    )
                    item.state = "pending"
                    item.claim_token = None
                    item.claimed_at_monotonic = 0.0
                    item.claim_expires_at_monotonic = 0.0

    # ----------------------------
    # webhook
    # ----------------------------
    def enqueue(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        signal_id = payload["signal_id"]
        symbol = payload["symbol"]

        with self._lock:
            self._cleanup_acked_locked()
            self._expire_claims_for_symbol_locked(symbol)

            existing = self._by_id.get(signal_id)
            if existing is not None:
                existing_symbol = existing.payload["symbol"]
                if existing_symbol != symbol:
                    raise HTTPException(
                        status_code=409,
                        detail=f"signal_id already exists with another symbol: {signal_id}",
                    )

                return {
                    "status": "duplicate",
                    "signal_id": signal_id,
                    "symbol": symbol,
                    "state": existing.state,
                    "queue_size": self.total_queue_size_locked(),
                }

            self._by_id[signal_id] = SignalItem(payload=payload)
            self._queues[symbol].append(signal_id)
            self._last_signal = dict(payload)

            logger.info(
                "Queued signal. signal_id=%s symbol=%s action=%s side=%s",
                payload["signal_id"],
                payload["symbol"],
                payload["action"],
                payload.get("side"),
            )

            return {
                "status": "queued",
                "signal_id": signal_id,
                "symbol": symbol,
                "queue_size": self.total_queue_size_locked(),
            }

    # ----------------------------
    # next-signal
    # ----------------------------
    def get_next_for_symbol(self, symbol: str) -> Dict[str, Any]:
        with self._lock:
            self._cleanup_acked_locked()
            self._expire_claims_for_symbol_locked(symbol)

            queue = self._queues.get(symbol)
            if not queue:
                return {"status": "empty"}

            while queue:
                head_signal_id = queue[0]
                item = self._by_id.get(head_signal_id)

                if item is None:
                    queue.popleft()
                    continue

                if item.state == "acked":
                    queue.popleft()
                    continue

                # Строгий порядок по symbol:
                # если первый pending -> claim и отдать
                # если первый claimed и ещё не истёк -> empty
                # дальше очередь не трогаем, чтобы не ломать порядок
                if item.state == "claimed":
                    return {"status": "empty"}

                if item.state == "pending":
                    now = time.monotonic()
                    claim_token = secrets.token_urlsafe(32)

                    item.state = "claimed"
                    item.claim_token = claim_token
                    item.claimed_at_monotonic = now
                    item.claim_expires_at_monotonic = now + CLAIM_TTL_SECONDS

                    response = {
                        "status": "ok",
                        "claim_token": claim_token,
                        "claim_ttl_seconds": CLAIM_TTL_SECONDS,
                    }
                    response.update(item.payload)

                    logger.info(
                        "Claimed signal. signal_id=%s symbol=%s ttl=%ss",
                        head_signal_id,
                        symbol,
                        CLAIM_TTL_SECONDS,
                    )
                    return response

            self._queues.pop(symbol, None)
            return {"status": "empty"}

    # ----------------------------
    # ack
    # ----------------------------
    def ack(self, signal_id: str, symbol: str, claim_token: str) -> Dict[str, Any]:
        with self._lock:
            self._cleanup_acked_locked()

            item = self._by_id.get(signal_id)
            if item is None:
                return {"status": "not_found", "signal_id": signal_id, "symbol": symbol}

            real_symbol = item.payload["symbol"]
            if real_symbol != symbol:
                logger.warning(
                    "Ack rejected: symbol mismatch. signal_id=%s got=%s expected=%s",
                    signal_id,
                    symbol,
                    real_symbol,
                )
                return {
                    "status": "symbol_mismatch",
                    "signal_id": signal_id,
                    "symbol": symbol,
                }

            if item.state == "acked":
                return {
                    "status": "already_acked",
                    "signal_id": signal_id,
                    "symbol": symbol,
                }

            if item.state != "claimed":
                return {
                    "status": "not_claimed",
                    "signal_id": signal_id,
                    "symbol": symbol,
                }

            now = time.monotonic()

            if item.claim_expires_at_monotonic <= 0 or now >= item.claim_expires_at_monotonic:
                item.state = "pending"
                item.claim_token = None
                item.claimed_at_monotonic = 0.0
                item.claim_expires_at_monotonic = 0.0

                logger.warning(
                    "Ack rejected: claim expired. signal_id=%s symbol=%s",
                    signal_id,
                    symbol,
                )
                return {
                    "status": "claim_expired",
                    "signal_id": signal_id,
                    "symbol": symbol,
                }

            if item.claim_token != claim_token:
                logger.warning(
                    "Ack rejected: invalid claim token. signal_id=%s symbol=%s",
                    signal_id,
                    symbol,
                )
                return {
                    "status": "invalid_claim",
                    "signal_id": signal_id,
                    "symbol": symbol,
                }

            item.state = "acked"
            item.claim_token = None
            item.claimed_at_monotonic = 0.0
            item.claim_expires_at_monotonic = 0.0
            item.acked_at_monotonic = now

            queue = self._queues.get(symbol)
            if queue is not None:
                if queue and queue[0] == signal_id:
                    queue.popleft()
                else:
                    try:
                        queue.remove(signal_id)
                    except ValueError:
                        pass

                if not queue:
                    self._queues.pop(symbol, None)

            logger.info(
                "Ack success. signal_id=%s symbol=%s",
                signal_id,
                symbol,
            )

            return {
                "status": "acked",
                "signal_id": signal_id,
                "symbol": symbol,
                "queue_size": self.total_queue_size_locked(),
            }

    # ----------------------------
    # stats / helpers
    # ----------------------------
    def total_queue_size_locked(self) -> int:
        total = 0
        for queue in self._queues.values():
            total += len(queue)
        return total

    def set_last_signal(self, payload: Dict[str, Any]) -> None:
        with self._lock:
            self._last_signal = dict(payload)

    def get_last_signal(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if self._last_signal is None:
                return None
            return dict(self._last_signal)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            self._cleanup_acked_locked()

            pending_total = 0
            claimed_total = 0
            acked_total = 0

            per_symbol: Dict[str, Dict[str, int]] = {}

            for symbol, queue in self._queues.items():
                pending_count = 0
                claimed_count = 0
                acked_count = 0

                for signal_id in queue:
                    item = self._by_id.get(signal_id)
                    if item is None:
                        continue

                    if item.state == "pending":
                        pending_count += 1
                    elif item.state == "claimed":
                        claimed_count += 1
                    elif item.state == "acked":
                        acked_count += 1

                per_symbol[symbol] = {
                    "queue_size": len(queue),
                    "pending": pending_count,
                    "claimed": claimed_count,
                    "acked": acked_count,
                }

            for item in self._by_id.values():
                if item.state == "pending":
                    pending_total += 1
                elif item.state == "claimed":
                    claimed_total += 1
                elif item.state == "acked":
                    acked_total += 1

            return {
                "status": "ok",
                "queue_size": self.total_queue_size_locked(),
                "signals_total": len(self._by_id),
                "pending_total": pending_total,
                "claimed_total": claimed_total,
                "acked_total": acked_total,
                "claim_ttl_seconds": CLAIM_TTL_SECONDS,
                "ack_retention_seconds": ACK_RETENTION_SECONDS,
                "per_symbol": per_symbol,
            }


store = SignalStore()


# ============================================================
# VALIDATION HELPERS
# ============================================================

def require_secret(value: Any) -> None:
    if str(value) != SECRET:
        raise HTTPException(status_code=403, detail="bad secret")


def require_str_field(data: Dict[str, Any], field: str) -> str:
    value = data.get(field)
    if value is None:
        raise HTTPException(status_code=400, detail=f"missing fields: {field}")

    text = str(value).strip()
    if text == "":
        raise HTTPException(status_code=400, detail=f"missing fields: {field}")

    return text


def optional_str_field(data: Dict[str, Any], field: str) -> Optional[str]:
    value = data.get(field)
    if value is None:
        return None

    text = str(value).strip()
    if text == "":
        return None

    return text


def require_float_field(data: Dict[str, Any], field: str) -> float:
    value = data.get(field)
    if value is None or value == "":
        raise HTTPException(status_code=400, detail=f"missing fields: {field}")

    try:
        return float(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"field must be numeric: {field}")


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def normalize_action(action: str) -> str:
    return action.strip().lower()


def normalize_side(side: Optional[str]) -> Optional[str]:
    if side is None:
        return None
    return side.strip().lower()


def build_signal_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    require_secret(data.get("secret"))

    signal_id = require_str_field(data, "signal_id")
    symbol = normalize_symbol(require_str_field(data, "symbol"))
    action = normalize_action(require_str_field(data, "action"))

    if action not in ("open", "close"):
        raise HTTPException(status_code=400, detail="action must be 'open' or 'close'")

    timeframe = optional_str_field(data, "timeframe")
    time_text = optional_str_field(data, "time")

    payload: Dict[str, Any] = {
        "signal_id": signal_id,
        "symbol": symbol,
        "action": action,
    }

    if timeframe is not None:
        payload["timeframe"] = timeframe
    if time_text is not None:
        payload["time"] = time_text

    if action == "open":
        side = normalize_side(require_str_field(data, "side"))
        if side not in ("buy", "sell"):
            raise HTTPException(status_code=400, detail="side must be 'buy' or 'sell'")

        payload["side"] = side
        payload["entry"] = require_float_field(data, "entry")
        payload["sl"] = require_float_field(data, "sl")
        payload["tp1"] = require_float_field(data, "tp1")
        payload["tp2"] = require_float_field(data, "tp2")
        payload["tp3"] = require_float_field(data, "tp3")
    else:
        side = normalize_side(optional_str_field(data, "side"))
        if side is not None:
            payload["side"] = side

    return payload


async def parse_json_body(request: Request) -> Dict[str, Any]:
    try:
        data = await request.json()
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid json")
    except Exception:
        raise HTTPException(status_code=400, detail="invalid request body")

    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="json body must be an object")

    return data


# ============================================================
# ROUTES
# ============================================================

@app.get("/")
def root():
    return store.stats()


@app.get("/health")
def health():
    return store.stats()


@app.get("/last-signal")
def get_last_signal():
    last_signal = store.get_last_signal()
    if last_signal is None:
        return {"status": "empty"}
    return last_signal


@app.post("/webhook")
async def webhook(request: Request):
    data = await parse_json_body(request)
    payload = build_signal_payload(data)
    return store.enqueue(payload)


@app.get("/next-signal")
def next_signal(
    secret: str = Query(...),
    symbol: str = Query(...),
):
    require_secret(secret)

    normalized_symbol = normalize_symbol(symbol)
    if normalized_symbol == "":
        raise HTTPException(status_code=400, detail="symbol is required")

    return store.get_next_for_symbol(normalized_symbol)


@app.post("/ack-signal")
async def ack_signal(request: Request):
    data = await parse_json_body(request)

    require_secret(data.get("secret"))

    signal_id = require_str_field(data, "signal_id")
    symbol = normalize_symbol(require_str_field(data, "symbol"))
    claim_token = require_str_field(data, "claim_token")

    return store.ack(
        signal_id=signal_id,
        symbol=symbol,
        claim_token=claim_token,
    )


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
        workers=1,
    )
