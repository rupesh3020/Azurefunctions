from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from uuid import uuid4

from .fake_autogpt import FakeAutoGPT

app = FastAPI(title="Trading API")
autogpt = FakeAutoGPT()

class Order(BaseModel):
    symbol: str
    side: str
    quantity: int

orders: dict[str, Order] = {}

@app.post("/orders")
def create_order(order: Order):
    order_id = str(uuid4())
    orders[order_id] = order
    return {"order_id": order_id, **order.model_dump()}

@app.get("/orders/{order_id}")
def get_order(order_id: str):
    order = orders.get(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

@app.get("/signal/{symbol}")
def get_signal(symbol: str):
    action = autogpt.generate_signal(symbol)
    return {"symbol": symbol, "suggested_action": action}
