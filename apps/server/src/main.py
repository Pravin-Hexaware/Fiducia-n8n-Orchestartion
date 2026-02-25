import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.parsing_sourcing_routes import router as parsing_router
from api.fundMandate import router as mandate_router
from api.risk_api import router as risk_router
from api.report_api import router as report_router
from api.dashboard import router as dashboard_router

from database.db import init_db, close_db

@asynccontextmanager
async def lifespan(app: FastAPI):

    await init_db()
    yield

    await close_db()


app = FastAPI(
    title="FundAgent API",
    description="API for Compass Master application",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(parsing_router)
app.include_router(mandate_router)
app.include_router(risk_router)
app.include_router(report_router)
app.include_router(dashboard_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host='0.0.0.0',
        port=8000,
        reload=False,
    )



