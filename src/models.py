import os
import re
import asyncio
import logging
from typing import List
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, select, update, text, or_, func, distinct, case
from datetime import datetime, timedelta, timezone
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from config import *

load_dotenv()
# 初始化資料庫
Base = declarative_base()
DATABASES = {
    "BSC": DATABASE_URI_SWAP_BSC
}

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

# 为每条链初始化 engine 和 sessionmaker
engines = {
    chain: create_async_engine(db_uri, echo=False, future=True)
    for chain, db_uri in DATABASES.items()
}

sessions = {
    chain: sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    for chain, engine in engines.items()
}

TZ_UTC8 = timezone(timedelta(hours=8))

def get_utc8_time():
    """获取 UTC+8 当前时间"""
    return datetime.now(TZ_UTC8).replace(tzinfo=None)

def make_naive_time(dt):
    """将时间转换为无时区的格式"""
    if isinstance(dt, datetime) and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

@as_declarative()
class Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    # 添加一个动态 schema 属性
    __table_args__ = {}
    
    @classmethod
    def with_schema(cls, schema: str):
        cls.__table_args__ = {"schema": schema}
        for table in Base.metadata.tables.values():
            if table.name == cls.__tablename__:
                table.schema = schema
        return cls

class WalletSummary(Base):
    """
    整合的錢包數據表
    """
    __tablename__ = 'wallet'
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    id = Column(Integer, primary_key=True, comment='ID')
    address = Column(String(100), nullable=False, unique=True, comment='錢包地址')
    balance = Column(Float, nullable=True, comment='錢包餘額')
    balance_USD = Column(Float, nullable=True, comment='錢包餘額 (USD)')
    chain = Column(String(50), nullable=False, comment='區塊鏈類型')
    tag = Column(String(50), nullable=True, comment='標籤')
    twitter_name = Column(String(50), nullable=True, comment='X名稱')
    twitter_username = Column(String(50), nullable=True, comment='X用戶名')
    is_smart_wallet = Column(Boolean, nullable=True, comment='是否為聰明錢包')
    wallet_type = Column(Integer, nullable=True, comment='0:一般聰明錢，1:pump聰明錢，2:moonshot聰明錢')
    asset_multiple = Column(Float, nullable=True, comment='資產翻倍數(到小數第1位)')
    token_list = Column(String(512), nullable=True, comment='用户最近交易的三种代币信息')

    # 交易數據
    avg_cost_30d = Column(Float, nullable=True, comment='30日平均成本')
    avg_cost_7d = Column(Float, nullable=True, comment='7日平均成本')
    avg_cost_1d = Column(Float, nullable=True, comment='1日平均成本')
    total_transaction_num_30d = Column(Integer, nullable=True, comment='30日總交易次數')
    total_transaction_num_7d = Column(Integer, nullable=True, comment='7日總交易次數')
    total_transaction_num_1d = Column(Integer, nullable=True, comment='1日總交易次數')
    buy_num_30d = Column(Integer, nullable=True, comment='30日買入次數')
    buy_num_7d = Column(Integer, nullable=True, comment='7日買入次數')
    buy_num_1d = Column(Integer, nullable=True, comment='1日買入次數')
    sell_num_30d = Column(Integer, nullable=True, comment='30日賣出次數')
    sell_num_7d = Column(Integer, nullable=True, comment='7日賣出次數')
    sell_num_1d = Column(Integer, nullable=True, comment='1日賣出次數')
    win_rate_30d = Column(Float, nullable=True, comment='30日勝率')
    win_rate_7d = Column(Float, nullable=True, comment='7日勝率')
    win_rate_1d = Column(Float, nullable=True, comment='1日勝率')

    # 盈虧數據
    pnl_30d = Column(Float, nullable=True, comment='30日盈虧')
    pnl_7d = Column(Float, nullable=True, comment='7日盈虧')
    pnl_1d = Column(Float, nullable=True, comment='1日盈虧')
    pnl_percentage_30d = Column(Float, nullable=True, comment='30日盈虧百分比')
    pnl_percentage_7d = Column(Float, nullable=True, comment='7日盈虧百分比')
    pnl_percentage_1d = Column(Float, nullable=True, comment='1日盈虧百分比')
    pnl_pic_30d = Column(String(1024), nullable=True, comment='30日每日盈虧圖')
    pnl_pic_7d = Column(String(1024), nullable=True, comment='7日每日盈虧圖')
    pnl_pic_1d = Column(String(1024), nullable=True, comment='1日每日盈虧圖')
    unrealized_profit_30d = Column(Float, nullable=True, comment='30日未實現利潤')
    unrealized_profit_7d = Column(Float, nullable=True, comment='7日未實現利潤')
    unrealized_profit_1d = Column(Float, nullable=True, comment='1日未實現利潤')
    total_cost_30d = Column(Float, nullable=True, comment='30日總成本')
    total_cost_7d = Column(Float, nullable=True, comment='7日總成本')
    total_cost_1d = Column(Float, nullable=True, comment='1日總成本')
    avg_realized_profit_30d = Column(Float, nullable=True, comment='30日平均已實現利潤')
    avg_realized_profit_7d = Column(Float, nullable=True, comment='7日平均已實現利潤')
    avg_realized_profit_1d = Column(Float, nullable=True, comment='1日平均已實現利潤')

    # 收益分布數據
    distribution_gt500_30d = Column(Integer, nullable=True, comment='30日收益分布 >500% 的次數')
    distribution_200to500_30d = Column(Integer, nullable=True, comment='30日收益分布 200%-500% 的次數')
    distribution_0to200_30d = Column(Integer, nullable=True, comment='30日收益分布 0%-200% 的次數')
    distribution_0to50_30d = Column(Integer, nullable=True, comment='30日收益分布 0%-50% 的次數')
    distribution_lt50_30d = Column(Integer, nullable=True, comment='30日收益分布 <50% 的次數')
    distribution_gt500_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 >500% 的比例')
    distribution_200to500_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 200%-500% 的比例')
    distribution_0to200_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 0%-200% 的比例')
    distribution_0to50_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 0%-50% 的比例')
    distribution_lt50_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 <50% 的比例')

    distribution_gt500_7d = Column(Integer, nullable=True, comment='7日收益分布 >500% 的次數')
    distribution_200to500_7d = Column(Integer, nullable=True, comment='7日收益分布 200%-500% 的次數')
    distribution_0to200_7d = Column(Integer, nullable=True, comment='7日收益分布 0%-200% 的次數')
    distribution_0to50_7d = Column(Integer, nullable=True, comment='7日收益分布 0%-50% 的次數')
    distribution_lt50_7d = Column(Integer, nullable=True, comment='7日收益分布 <50% 的次數')
    distribution_gt500_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 >500% 的比例')
    distribution_200to500_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 200%-500% 的比例')
    distribution_0to200_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 0%-200% 的比例')
    distribution_0to50_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 0%-50% 的比例')
    distribution_lt50_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 <50% 的比例')

    # 更新時間和最後交易時間
    update_time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')
    last_transaction_time = Column(Integer, nullable=True, comment='最後活躍時間')
    is_active = Column(Boolean, nullable=True, comment='是否還是聰明錢')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
class ErrorLog(Base):
    """
    錯誤訊息記錄表
    """
    __tablename__ = 'error_logs'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='ID')
    timestamp = Column(DateTime, nullable=False, default=get_utc8_time, comment='時間')
    module_name = Column(String(100), nullable=True, comment='檔案名稱')
    function_name = Column(String(100), nullable=True, comment='函數名稱')
    error_message = Column(Text, nullable=False, comment='錯誤訊息')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
# -------------------------------------------------------------------------------------------------------------------

async def write_wallet_data_to_db(session, wallet_data, chain):
    """
    将钱包数据写入或更新 WalletSummary 表
    """
    schema = chain.lower()
    WalletSummary.with_schema(schema)
    try:
        # 查詢是否已經存在相同的 wallet_address
        existing_wallet = await session.execute(
            select(WalletSummary).filter(WalletSummary.address == wallet_data["wallet_address"])
        )
        existing_wallet = existing_wallet.scalars().first()
        if existing_wallet:
            # 如果已經存在，就更新資料
            existing_wallet.balance = wallet_data.get("balance", 0)
            existing_wallet.balance_USD = wallet_data.get("balance_USD", 0)
            existing_wallet.chain = wallet_data.get("chain", "BSC")
            existing_wallet.wallet_type = wallet_data.get("wallet_type", 0)
            existing_wallet.asset_multiple = float(wallet_data.get("asset_multiple", 0) or 0)
            existing_wallet.token_list=wallet_data.get("token_list", False)
            existing_wallet.avg_cost_30d = wallet_data["stats_30d"].get("avg_cost", 0)
            existing_wallet.avg_cost_7d = wallet_data["stats_7d"].get("avg_cost", 0)
            existing_wallet.avg_cost_1d = wallet_data["stats_1d"].get("avg_cost", 0)
            existing_wallet.total_transaction_num_30d = wallet_data["stats_30d"].get("total_transaction_num", 0)
            existing_wallet.total_transaction_num_7d = wallet_data["stats_7d"].get("total_transaction_num", 0)
            existing_wallet.total_transaction_num_1d = wallet_data["stats_1d"].get("total_transaction_num", 0)
            existing_wallet.buy_num_30d = wallet_data["stats_30d"].get("buy_num", 0)
            existing_wallet.buy_num_7d = wallet_data["stats_7d"].get("buy_num", 0)
            existing_wallet.buy_num_1d = wallet_data["stats_1d"].get("buy_num", 0)
            existing_wallet.sell_num_30d = wallet_data["stats_30d"].get("sell_num", 0)
            existing_wallet.sell_num_7d = wallet_data["stats_7d"].get("sell_num", 0)
            existing_wallet.sell_num_1d = wallet_data["stats_1d"].get("sell_num", 0)
            existing_wallet.win_rate_30d = wallet_data["stats_30d"].get("win_rate", 0)
            existing_wallet.win_rate_7d = wallet_data["stats_7d"].get("win_rate", 0)
            existing_wallet.win_rate_1d = wallet_data["stats_1d"].get("win_rate", 0)
            existing_wallet.pnl_30d = wallet_data["stats_30d"].get("pnl", 0)
            existing_wallet.pnl_7d = wallet_data["stats_7d"].get("pnl", 0)
            existing_wallet.pnl_1d = wallet_data["stats_1d"].get("pnl", 0)
            existing_wallet.pnl_percentage_30d = wallet_data["stats_30d"].get("pnl_percentage", 0)
            existing_wallet.pnl_percentage_7d = wallet_data["stats_7d"].get("pnl_percentage", 0)
            existing_wallet.pnl_percentage_1d = wallet_data["stats_1d"].get("pnl_percentage", 0)
            existing_wallet.pnl_pic_30d = wallet_data.get("pnl_pic_30d", False)
            existing_wallet.pnl_pic_7d = wallet_data.get("pnl_pic_7d", False)
            existing_wallet.pnl_pic_1d = wallet_data.get("pnl_pic_1d", False)
            existing_wallet.unrealized_profit_30d = wallet_data["stats_30d"].get("unrealized_profit", 0)
            existing_wallet.unrealized_profit_7d = wallet_data["stats_7d"].get("unrealized_profit", 0)
            existing_wallet.unrealized_profit_1d = wallet_data["stats_1d"].get("unrealized_profit", 0)
            existing_wallet.total_cost_30d = wallet_data["stats_30d"].get("total_cost", 0)
            existing_wallet.total_cost_7d = wallet_data["stats_7d"].get("total_cost", 0)
            existing_wallet.total_cost_1d = wallet_data["stats_1d"].get("total_cost", 0)
            existing_wallet.avg_realized_profit_30d = wallet_data["stats_30d"].get("avg_realized_profit", 0)
            existing_wallet.avg_realized_profit_7d = wallet_data["stats_7d"].get("avg_realized_profit", 0)
            existing_wallet.avg_realized_profit_1d = wallet_data["stats_1d"].get("avg_realized_profit", 0)
            existing_wallet.distribution_gt500_30d = wallet_data["distribution_30d"].get("gt500", 0)
            existing_wallet.distribution_200to500_30d = wallet_data["distribution_30d"].get("200to500", 0)
            existing_wallet.distribution_0to200_30d = wallet_data["distribution_30d"].get("0to200", 0)
            existing_wallet.distribution_0to50_30d = wallet_data["distribution_30d"].get("0to50", 0)
            existing_wallet.distribution_lt50_30d = wallet_data["distribution_30d"].get("lt50", 0)
            existing_wallet.distribution_gt500_percentage_30d = wallet_data["distribution_percentage_30d"].get("gt500", 0.0)
            existing_wallet.distribution_200to500_percentage_30d = wallet_data["distribution_percentage_30d"].get("200to500", 0.0)
            existing_wallet.distribution_0to200_percentage_30d = wallet_data["distribution_percentage_30d"].get("0to200", 0.0)
            existing_wallet.distribution_0to50_percentage_30d = wallet_data["distribution_percentage_30d"].get("0to50", 0.0)
            existing_wallet.distribution_lt50_percentage_30d = wallet_data["distribution_percentage_30d"].get("lt50", 0.0)
            existing_wallet.distribution_gt500_7d = wallet_data["distribution_7d"].get("gt500", 0)
            existing_wallet.distribution_200to500_7d = wallet_data["distribution_7d"].get("200to500", 0)
            existing_wallet.distribution_0to200_7d = wallet_data["distribution_7d"].get("0to200", 0)
            existing_wallet.distribution_0to50_7d = wallet_data["distribution_7d"].get("0to50", 0)
            existing_wallet.distribution_lt50_7d = wallet_data["distribution_7d"].get("lt50", 0)
            existing_wallet.distribution_gt500_percentage_7d = wallet_data["distribution_percentage_7d"].get("gt500", 0.0)
            existing_wallet.distribution_200to500_percentage_7d = wallet_data["distribution_percentage_7d"].get("200to500", 0.0)
            existing_wallet.distribution_0to200_percentage_7d = wallet_data["distribution_percentage_7d"].get("0to200", 0.0)
            existing_wallet.distribution_0to50_percentage_7d = wallet_data["distribution_percentage_7d"].get("0to50", 0.0)
            existing_wallet.distribution_lt50_percentage_7d = wallet_data["distribution_percentage_7d"].get("lt50", 0.0)
            existing_wallet.update_time = get_utc8_time()
            existing_wallet.last_transaction_time = wallet_data.get("last_transaction_time", int(datetime.now(timezone.utc).timestamp()))

            print(f"Successfully updated wallet: {wallet_data['wallet_address']}")
        else:
            # 如果不存在，就創建新記錄，直接複用現有更新邏輯
            wallet_summary = WalletSummary(
                address=wallet_data["wallet_address"],
                balance=wallet_data.get("balance", 0),
                balance_USD=wallet_data.get("balance_USD", 0),
                chain=wallet_data.get("chain", "BSC"),
                wallet_type=wallet_data.get("wallet_type", 0),
                asset_multiple=wallet_data.get("asset_multiple", 0),
                token_list=wallet_data.get("token_list", ""),
                avg_cost_30d=wallet_data["stats_30d"].get("avg_cost", 0),
                avg_cost_7d=wallet_data["stats_7d"].get("avg_cost", 0),
                avg_cost_1d=wallet_data["stats_1d"].get("avg_cost", 0),
                total_transaction_num_30d=wallet_data["stats_30d"].get("total_transaction_num", 0),
                total_transaction_num_7d=wallet_data["stats_7d"].get("total_transaction_num", 0),
                total_transaction_num_1d=wallet_data["stats_1d"].get("total_transaction_num", 0),
                buy_num_30d=wallet_data["stats_30d"].get("buy_num", 0),
                buy_num_7d=wallet_data["stats_7d"].get("buy_num", 0),
                buy_num_1d=wallet_data["stats_1d"].get("buy_num", 0),
                sell_num_30d=wallet_data["stats_30d"].get("sell_num", 0),
                sell_num_7d=wallet_data["stats_7d"].get("sell_num", 0),
                sell_num_1d=wallet_data["stats_1d"].get("sell_num", 0),
                win_rate_30d=wallet_data["stats_30d"].get("win_rate", 0),
                win_rate_7d=wallet_data["stats_7d"].get("win_rate", 0),
                win_rate_1d=wallet_data["stats_1d"].get("win_rate", 0),
                pnl_30d=wallet_data["stats_30d"].get("pnl", 0),
                pnl_7d=wallet_data["stats_7d"].get("pnl", 0),
                pnl_1d=wallet_data["stats_1d"].get("pnl", 0),
                pnl_percentage_30d=wallet_data["stats_30d"].get("pnl_percentage", 0),
                pnl_percentage_7d=wallet_data["stats_7d"].get("pnl_percentage", 0),
                pnl_percentage_1d=wallet_data["stats_1d"].get("pnl_percentage", 0),
                pnl_pic_30d=wallet_data.get("pnl_pic_30d", ""),
                pnl_pic_7d=wallet_data.get("pnl_pic_7d", ""),
                pnl_pic_1d=wallet_data.get("pnl_pic_1d", ""),
                unrealized_profit_30d=wallet_data["stats_30d"].get("unrealized_profit", 0),
                unrealized_profit_7d=wallet_data["stats_7d"].get("unrealized_profit", 0),
                unrealized_profit_1d=wallet_data["stats_1d"].get("unrealized_profit", 0),
                total_cost_30d = wallet_data["stats_30d"].get("total_cost", 0),
                total_cost_7d = wallet_data["stats_7d"].get("total_cost", 0),
                total_cost_1d = wallet_data["stats_1d"].get("total_cost", 0),
                avg_realized_profit_30d = wallet_data["stats_30d"].get("avg_realized_profit", 0),
                avg_realized_profit_7d = wallet_data["stats_7d"].get("avg_realized_profit", 0),
                avg_realized_profit_1d = wallet_data["stats_1d"].get("avg_realized_profit", 0),
                distribution_gt500_30d = wallet_data["distribution_30d"].get("gt500", 0),
                distribution_200to500_30d = wallet_data["distribution_30d"].get("200to500", 0),
                distribution_0to200_30d = wallet_data["distribution_30d"].get("0to200", 0),
                distribution_0to50_30d = wallet_data["distribution_30d"].get("0to50", 0),
                distribution_lt50_30d = wallet_data["distribution_30d"].get("lt50", 0),
                distribution_gt500_percentage_30d = wallet_data["distribution_percentage_30d"].get("gt500", 0.0),
                distribution_200to500_percentage_30d = wallet_data["distribution_percentage_30d"].get("200to500", 0.0),
                distribution_0to200_percentage_30d = wallet_data["distribution_percentage_30d"].get("0to200", 0.0),
                distribution_0to50_percentage_30d = wallet_data["distribution_percentage_30d"].get("0to50", 0.0),
                distribution_lt50_percentage_30d = wallet_data["distribution_percentage_30d"].get("lt50", 0.0),
                distribution_gt500_7d = wallet_data["distribution_7d"].get("gt500", 0),
                distribution_200to500_7d = wallet_data["distribution_7d"].get("200to500", 0),
                distribution_0to200_7d = wallet_data["distribution_7d"].get("0to200", 0),
                distribution_0to50_7d = wallet_data["distribution_7d"].get("0to50", 0),
                distribution_lt50_7d = wallet_data["distribution_7d"].get("lt50", 0),
                distribution_gt500_percentage_7d = wallet_data["distribution_percentage_7d"].get("gt500", 0.0),
                distribution_200to500_percentage_7d = wallet_data["distribution_percentage_7d"].get("200to500", 0.0),
                distribution_0to200_percentage_7d = wallet_data["distribution_percentage_7d"].get("0to200", 0.0),
                distribution_0to50_percentage_7d = wallet_data["distribution_percentage_7d"].get("0to50", 0.0),
                distribution_lt50_percentage_7d = wallet_data["distribution_percentage_7d"].get("lt50", 0.0),
                update_time=get_utc8_time(),
                last_transaction_time=wallet_data.get("last_transaction_time", int(datetime.now(timezone.utc).timestamp()))
            )
            wallet_summary = WalletSummary(...)
            session.add(wallet_summary)
            print(f"Successfully added wallet: {wallet_data['wallet_address']}")
        return True
    except Exception as e:
        print(f"Error saving wallet: {wallet_data['wallet_address']} - {str(e)}")
        return False
    
async def log_error(session, error_message: str, module_name: str, function_name: str = None, additional_info: str = None):
    """記錄錯誤訊息到資料庫"""
    schema = "BSC".lower()
    WalletSummary.with_schema(schema)
    try:
        error_log = ErrorLog(
            error_message=error_message,
            module_name=module_name,
            function_name=function_name,
        )
        session.add(error_log)  # 使用異步添加操作
        await session.commit()  # 提交變更
    except Exception as e:
        try:
            await session.rollback()  # 在錯誤發生時進行回滾
        except Exception as rollback_error:
            print(f"回滾錯誤: {rollback_error}")
        print(f"無法記錄錯誤訊息: {e}")
    finally:
        try:
            await session.close()  # 確保 session 被正確關閉
        except Exception as close_error:
            print(f"關閉 session 時錯誤: {close_error}")