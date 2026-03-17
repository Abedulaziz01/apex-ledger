import asyncpg
import pytest_asyncio
import os
from dotenv import load_dotenv
from src.ledger.core.event_store import EventStore

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


@pytest_asyncio.fixture()
async def store():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    s = EventStore(pool)
    yield s
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE events, event_streams, outbox RESTART IDENTITY CASCADE"
        )
    await pool.close()