from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.routers.kv import router as kv_router
from app.schemas.kv import KV_FAILURE_EXAMPLES


app = FastAPI(title="mini_redis", version="0.1.0")
app.include_router(kv_router)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_request, _exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=400, content=KV_FAILURE_EXAMPLES["invalid_input"])


@app.get("/v1/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
