# Azurefunctions
codes for azure functions

## Trading API

A minimal trading application built with FastAPI. It uses a simple `FakeAutoGPT` to generate random trading signals.

### Running locally

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Start the API server:
   ```bash
   uvicorn trading_app.main:app --reload
   ```
3. Query a trading signal:
   ```bash
   curl http://localhost:8000/signal/TSLA
   ```

### Azure Function

The FastAPI trading bot is exposed as an Azure Function using `AsgiMiddleware`.
To run it locally with the Azure Functions Core Tools:

```bash
func start
```

Requests to `/api/trading/*` will be served by the FastAPI app. For example:

```bash
curl http://localhost:7071/api/trading/signal/TSLA
```
