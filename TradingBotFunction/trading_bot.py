import azure.functions as func
from azure.functions import AsgiMiddleware

from trading_app.main import app as fastapi_app

async def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    """Azure Function entry point wrapping the FastAPI trading app."""
    return await AsgiMiddleware(fastapi_app).handle_async(req, context)

