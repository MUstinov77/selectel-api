import logging
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from app.api.v1 import api_router
from app.core.logging import setup_logging
from app.db.session import async_session_maker
from app.services.parser import parse_and_store
from app.services.scheduler import create_scheduler

logger = logging.getLogger(__name__)

app = FastAPI(title="Selectel Vacancies API")
app.include_router(api_router)

setup_logging()

_scheduler: Optional[AsyncIOScheduler] = None


async def _run_parse_job() -> None:
    try:
        async with async_session_maker() as session:
            await parse_and_store(session)
    except Exception as exc:
        logger.exception("Ошибка фонового парсинга: %s", exc)


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("Запуск приложения")
    await _run_parse_job()
    global _scheduler
    _scheduler = create_scheduler(_run_parse_job)
    _scheduler.start()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("Остановка приложения")
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
