from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "postgresql+asyncpg://manyara:toormaster@172.29.98.161:5432/aviation_db"

_engine = None
_SessionLocal = None


def get_engine_and_session():
    """
    Lazily create the engine & sessionmaker inside the correct event loop.
    Prevents asyncpg cross-loop corruption during tests.
    """
    global _engine, _SessionLocal

    if _engine is None:
        _engine = create_async_engine(
            DATABASE_URL,
            echo=False,
            future=True,
        )

        _SessionLocal = sessionmaker(
            bind=_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    return _engine, _SessionLocal


async def get_db():
    """
    FastAPI dependency that always uses the engine/session
    created inside the ACTIVE event loop.
    """
    _, SessionLocal = get_engine_and_session()

    async with SessionLocal() as session:
        yield session
