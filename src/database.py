from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncEngine
from typing import Callable
from config import DATABASE_URI, DATABASE_URI_SWAP_BSC

# 創建主數據庫引擎
def get_main_engine() -> AsyncEngine:
    return create_async_engine(DATABASE_URI, echo=False)

# 創建主數據庫會話工廠
def get_main_session_factory() -> Callable[..., AsyncSession]:
    engine = get_main_engine()
    return sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False
    )

# 創建 swap 數據庫會話工廠
def get_swap_session_factory() -> Callable[..., AsyncSession]:
    engine = create_async_engine(DATABASE_URI_SWAP_BSC, echo=False)
    return sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False
    )

# 創建默認會話工廠
SessionLocal = get_main_session_factory() 