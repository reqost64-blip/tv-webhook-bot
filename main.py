
from collections import deque
from threading import Lock
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

SECRET = "12345"

queue_lock = Lock()
signal_queue = deque()
processed_ids = set()
processed_order = deque(maxlen=5000)
last_signal = None


def require_fields(data: dict, fields: list[str]) -> None:
    missing = [field for field in fields if data.get(field) in (None, "")]
    if missing:
        raise HTTPException(status_code=400, detail=f"missing fields: {', '.join(missing)}")


def add_processed(signal_id: str) -> None:
    if signal_id in processed_ids:
        return

    if len(processed_order) == processed_order.maxlen:
        old_id = processed_order.popleft()
        processed_ids.discard(old_id)

    processed_order.append(signal_id)
    processed_ids.add(signal_id)


@app.get("/")
def root():
    return {
        "status": "ok",
        "queue_size": len(signal_queue),
        "processed_count": len(processed_ids),
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "queue_size": len(signal_queue),
        "processed_count": len(processed_ids),
    }


@app.get("/last-signal")
def get_last_signal():
    if last_signal is None:
        return {"status": "empty"}
    return last_signal


@app.post("/webhook")
async def webhook(request: Request):
    global last_signal

    data = await request.json()

    if data.get("secret") != SECRET:
        raise HTTPException(status_code=403, detail="bad secret")

    require_fields(data, ["signal_id", "symbol", "action"])

    action = str(data["action"]).lower()
    if action == "open":
        require_fields(data, ["side", "entry", "sl", "tp1", "tp2", "tp3"])
    elif action != "close":
        raise HTTPException(status_code=400, detail="action must be 'open' or 'close'")

    signal_id = str(data["signal_id"])
    data["signal_id"] = signal_id
    last_signal = data

    with queue_lock:
        already_in_queue = any(item["signal_id"] == signal_id for item in signal_queue)
        if signal_id in processed_ids or already_in_queue:
            return {
                "status": "duplicate",
                "signal_id": signal_id,
                "queue_size": len(signal_queue),
            }

        signal_queue.append(data)

        return {
            "status": "queued",
            "signal_id": signal_id,
            "queue_size": len(signal_queue),
        }


@app.get("/next-signal")
def next_signal(secret: str):
    if secret != SECRET:
        raise HTTPException(status_code=403, detail="bad secret")

    with queue_lock:
        if not signal_queue:
            return {"status": "empty", "queue_size": 0}

        return {
            "status": "ok",
            "queue_size": len(signal_queue),
            "signal": signal_queue[0],
        }


@app.post("/ack-signal")
async def ack_signal(request: Request):
    data = await request.json()

    if data.get("secret") != SECRET:
        raise HTTPException(status_code=403, detail="bad secret")

    require_fields(data, ["signal_id"])
    signal_id = str(data["signal_id"])

    with queue_lock:
        if signal_queue and signal_queue[0]["signal_id"] == signal_id:
            signal_queue.popleft()
            add_processed(signal_id)
            return {
                "status": "acked",
                "signal_id": signal_id,
                "queue_size": len(signal_queue),
            }

        if signal_id in processed_ids:
            return {
                "status": "already_acked",
                "signal_id": signal_id,
                "queue_size": len(signal_queue),
            }

        return {
            "status": "not_found",
            "signal_id": signal_id,
            "queue_size": len(signal_queue),
        }
