from sqlalchemy import text
from .database import engine

async def run_startup_migrations():
    async with engine.begin() as conn:
        # Добавляем недостающие колонки в message_logs
        await conn.execute(text("ALTER TABLE message_logs ADD COLUMN IF NOT EXISTS peer_id BIGINT"))
        await conn.execute(text("ALTER TABLE message_logs ADD COLUMN IF NOT EXISTS access_hash BIGINT"))
