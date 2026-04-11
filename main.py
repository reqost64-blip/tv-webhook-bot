from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

signals = []
TEST_SECRET = "12345"

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/last-signal")
def last_signal():
    if not signals:
        return {"status": "empty"}
    return signals[-1]

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()

    if data.get("secret") != TEST_SECRET:
        raise HTTPException(status_code=403, detail="bad secret")

    signal_id = data.get("signal_id")
    if signal_id and any(x.get("signal_id") == signal_id for x in signals):
        return {"status": "duplicate"}

    signals.append(data)
    return {"status": "received", "data": data}
