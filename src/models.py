import os
import re
import time
import traceback
import logging
import aiohttp
import sqlalchemy
from typing import List, Union, Optional
from dotenv import load_dotenv
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, select, update, Index, text, distinct, case, delete, BIGINT, UniqueConstraint
from datetime import datetime, timedelta, timezone
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from config import *
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from sqlalchemy.dialects.postgresql import insert
from event_processor import EventProcessor

load_dotenv()

# 設置日誌
logger = logging.getLogger(__name__)
# logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

# 初始化資料庫
Base = declarative_base()
DATABASES = {
    "BSC": DATABASE_URI_SWAP_BSC
}

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

class WalletTokenState(Base):
    __tablename__ = 'wallet_token_state'
    __table_args__ = (
        UniqueConstraint('wallet_address', 'token_address', 'chain', name='uq_wallet_token_chain'),
        {'schema': 'dex_query_v1'}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet_address = Column(String(100), nullable=False, comment="钱包地址")
    token_address = Column(String(100), nullable=False, comment="代币地址")
    chain = Column(String(50), nullable=False, comment="链名称 (例如 'BSC', 'SOLANA')")
    chain_id = Column(Integer, nullable=False, comment="链 ID")
    
    # 当前持仓状态
    current_amount = Column(Float, nullable=False, default=0.0, comment="当前持仓数量")
    current_total_cost = Column(Float, nullable=False, default=0.0, comment="当前持仓的总成本")
    current_avg_buy_price = Column(Float, nullable=False, default=0.0, comment="当前持仓的平均买入价")
    position_opened_at = Column(BIGINT, nullable=True, comment="当前仓位的首次建立时间 (Unix timestamp)")
    
    # 历史累计状态
    historical_buy_amount = Column(Float, nullable=False, default=0.0, comment="历史累计总买入数量")
    historical_sell_amount = Column(Float, nullable=False, default=0.0, comment="历史累计总卖出数量")
    historical_buy_cost = Column(Float, nullable=False, default=0.0, comment="历史累计总买入成本")
    historical_sell_value = Column(Float, nullable=False, default=0.0, comment="历史累计总卖出价值")
    historical_realized_pnl = Column(Float, nullable=False, default=0.0, comment="历史累计已实现盈亏")
    historical_buy_count = Column(Integer, nullable=False, default=0, comment="历史累计买入次数")
    historical_sell_count = Column(Integer, nullable=False, default=0, comment="历史累计卖出次数")
    
    # 元数据
    last_transaction_time = Column(BIGINT, nullable=True, comment="最后一次交易的时间 (Unix timestamp)")
    updated_at = Column(DateTime, nullable=False, default=get_utc8_time, comment="本条记录的最后更新时间")

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class WalletSummary(Base):
    __tablename__ = 'wallet'
    __table_args__ = {'schema': 'dex_query_v1'}

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    id = Column(Integer, primary_key=True, comment='ID')
    wallet_address = Column(String(512), nullable=False, unique=True, comment='錢包地址')
    balance = Column(Float, nullable=True, comment='錢包餘額')
    balance_usd = Column(Float, nullable=True, comment='錢包餘額 (USD)')
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
    last_transaction_time = Column(BIGINT, nullable=True, comment='最後活躍時間')
    is_active = Column(Boolean, nullable=True, comment='是否還是聰明錢')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Holding(Base):
    __tablename__ = 'wallet_holding'
    __table_args__ = {'schema': 'dex_query_v1'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet_address = Column(String(255), nullable=False)  # 添加长度限制
    token_address = Column(String(255), nullable=False)  # 添加长度限制
    token_icon = Column(String(255), nullable=True)  # 添加长度限制
    token_name = Column(String(255), nullable=True)  # 添加长度限制
    chain = Column(String(50), nullable=False, default='Unknown')  # 添加长度限制
    amount = Column(Float, nullable=False, default=0.0)
    value = Column(Float, nullable=False, default=0.0)
    value_USDT = Column(Float, nullable=False, default=0.0)
    unrealized_profits = Column(Float, nullable=False, default=0.0)
    pnl = Column(Float, nullable=False, default=0.0)
    pnl_percentage = Column(Float, nullable=False, default=0.0)
    avg_price = Column(Float, nullable=False, default=0.0)
    marketcap = Column(Float, nullable=False, default=0.0)
    is_cleared = Column(Boolean, nullable=False, default=False)
    cumulative_cost = Column(Float, nullable=False, default=0.0)
    cumulative_profit = Column(Float, nullable=False, default=0.0)
    last_transaction_time = Column(BIGINT, nullable=True, comment='最後活躍時間')
    time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Transaction(Base):
    __tablename__ = 'wallet_transaction'
    __table_args__ = (
        UniqueConstraint('wallet_address', 'token_address', 'transaction_time', 'signature', name='wallet_transaction_pkey'),
        {'schema': 'dex_query_v1'}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet_address = Column(String(100), nullable=False, comment="聰明錢錢包地址")
    wallet_balance = Column(Float, nullable=True, comment="錢包餘額")
    token_address = Column(String(100), nullable=False, comment="代幣地址")
    token_icon = Column(Text, nullable=True, comment="代幣圖片網址")
    token_name = Column(String(100), nullable=True, comment="代幣名稱")
    price = Column(Float, nullable=True, comment="價格")
    amount = Column(Float, nullable=False, comment="數量")
    marketcap = Column(Float, nullable=True, comment="市值")
    value = Column(Float, nullable=True, comment="價值")
    holding_percentage = Column(Float, nullable=True, comment="倉位百分比")
    chain = Column(String(50), nullable=False, comment="區塊鏈")
    chain_id = Column(Integer, nullable=False, comment="區塊鏈ID")
    realized_profit = Column(Float, nullable=True, comment="已實現利潤")
    realized_profit_percentage = Column(Float, nullable=True, comment="已實現利潤百分比")
    transaction_type = Column(String(10), nullable=False, comment="事件 (buy, sell)")
    transaction_time = Column(BIGINT, nullable=False, comment="交易時間")
    time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')
    signature = Column(String(100), nullable=False, comment="交易簽名")
    
    # 新增欄位
    from_token_address = Column(String(100), nullable=True, comment="來源代幣地址")
    from_token_symbol = Column(String(100), nullable=True, comment="來源代幣符號")
    from_token_amount = Column(Float, nullable=True, comment="來源代幣數量")
    dest_token_address = Column(String(100), nullable=True, comment="目標代幣地址")
    dest_token_symbol = Column(String(100), nullable=True, comment="目標代幣符號")
    dest_token_amount = Column(Float, nullable=True, comment="目標代幣數量")

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class TokenBuyData(Base):
    __tablename__ = 'wallet_buy_data'
    __table_args__ = {'schema': 'dex_query_v1'}

    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(100), nullable=False, comment="錢包地址")
    chain = Column(String(50), nullable=False, comment="區塊鏈")
    chain_id = Column(Integer, nullable=False, comment="區塊鏈ID")
    token_address = Column(String(100), nullable=False, comment="代幣地址")
    total_amount = Column(Float, nullable=False, default=0.0, comment="當前持有代幣數量")
    total_cost = Column(Float, nullable=False, default=0.0, comment="當前持倉總成本")
    avg_buy_price = Column(Float, nullable=False, default=0.0, comment="當前持倉平均買入價格")
    position_opened_at = Column(BIGINT, nullable=True, comment="當前倉位開始時間")
    historical_total_buy_amount = Column(Float, nullable=False, default=0.0, comment="歷史總買入數量")
    historical_total_buy_cost = Column(Float, nullable=False, default=0.0, comment="歷史總買入成本")
    historical_total_sell_amount = Column(Float, nullable=False, default=0.0, comment="歷史總賣出數量")
    historical_total_sell_value = Column(Float, nullable=False, default=0.0, comment="歷史總賣出價值")
    historical_avg_buy_price = Column(Float, nullable=False, default=0.0, comment="歷史平均買入價格")
    historical_avg_sell_price = Column(Float, nullable=False, default=0.0, comment="歷史平均賣出價格")
    last_active_position_closed_at = Column(BIGINT, nullable=True, comment="上一個活躍倉位關閉時間")
    last_transaction_time = Column(Integer, nullable=False, default=0.0, comment="最後活躍時間")
    realized_profit = Column(Float, default=0.0, comment="已實現利潤")
    realized_profit_percentage = Column(Float, default=0.0, comment="已實現利潤百分比")
    total_buy_count = Column(Integer, default=0, comment="總買入次數")
    total_sell_count = Column(Integer, default=0, comment="總賣出次數")
    total_holding_seconds = Column(Integer, default=0, comment="總持倉秒數")
    date = Column(DateTime, nullable=False, comment="日期")
    updated_at = Column(DateTime, nullable=False, default=get_utc8_time, comment="最後更新時間")

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class FollowWallet(Base):
    __tablename__ = 'wallet_follow'
    __table_args__ = {'schema': 'dex_query_v1'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet_address = Column(String(100), nullable=False, comment="錢包地址")
    is_active = Column(Boolean, nullable=False, default=False)
    updated_at = Column(DateTime, nullable=False, default=get_utc8_time, comment="最後更新時間")

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class ErrorLog(Base):
    """
    錯誤訊息記錄表
    """
    __tablename__ = 'error_logs'
    __table_args__ = {'schema': 'dex_query_v1'}

    id = Column(Integer, primary_key=True, autoincrement=True, comment='ID')
    timestamp = Column(DateTime, nullable=False, default=get_utc8_time, comment='時間')
    module_name = Column(String(100), nullable=True, comment='檔案名稱')
    function_name = Column(String(100), nullable=True, comment='函數名稱')
    error_message = Column(Text, nullable=False, comment='錯誤訊息')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
# -------------------------------------------------------------------------------------------------------------------
def convert_to_decimal(value: Union[float, str, None]) -> Union[Decimal, None]:
    """
    將數值轉換為 Decimal 類型，確保不會出現科學符號
    
    Args:
        value: 要轉換的數值，可以是浮點數、字符串或 None
        
    Returns:
        轉換後的 Decimal 數值，如果輸入為 None 則返回 None
    """
    if value is None:
        return None
    try:
        # 如果是字符串且包含科學符號，先轉換為浮點數
        if isinstance(value, str) and ('e' in value.lower() or 'E' in value):
            value = float(value)
        # 轉換為 Decimal 並使用字符串形式以保持精確度
        return Decimal(str(value))
    except (ValueError, TypeError, InvalidOperation):
        return Decimal('0')

async def write_wallet_data_to_db(session, wallet_data, chain, twitter_name: Optional[str] = None, twitter_username: Optional[str] = None):
    try:
        # 確保 update_time 是 naive datetime
        if isinstance(wallet_data.get('update_time'), datetime):
            wallet_data['update_time'] = wallet_data['update_time'].replace(tzinfo=None)
            
        # 自動補齊last_transaction_time與chain_id
        if 'last_transaction_time' not in wallet_data:
            wallet_data['last_transaction_time'] = 0
        if 'chain_id' not in wallet_data or wallet_data['chain_id'] is None:
            chain_map = {'BSC': 9006, 'SOLANA': 501}
            wallet_data['chain_id'] = chain_map.get(wallet_data.get('chain', 'BSC').upper(), 9006)

        # 檢查錢包是否已存在
        existing_wallet = await session.execute(
            select(WalletSummary).where(WalletSummary.wallet_address == wallet_data['wallet_address'])
        )
        existing_wallet = existing_wallet.scalar_one_or_none()

        if existing_wallet:
            # 更新已存在的錢包
            logger.info(f"更新已存在的錢包: {wallet_data['wallet_address']}")
            for key, value in wallet_data.items():
                if hasattr(existing_wallet, key):
                    setattr(existing_wallet, key, value)
            
            # 更新 Twitter 信息
            if twitter_name is not None:
                existing_wallet.twitter_name = twitter_name
            if twitter_username is not None:
                existing_wallet.twitter_username = twitter_username
                
            await session.commit()
            return existing_wallet
        else:
            # 創建新錢包
            logger.info(f"創建新錢包: {wallet_data['wallet_address']}")
            new_wallet = WalletSummary(**wallet_data)
            if twitter_name is not None:
                new_wallet.twitter_name = twitter_name
            if twitter_username is not None:
                new_wallet.twitter_username = twitter_username
            session.add(new_wallet)
            await session.commit()
            return new_wallet
    except Exception as e:
        logger.error(f"提交錢包數據時發生錯誤: {e}")
        await session.rollback()
        raise

async def save_holding(tx_data_list: list, wallet_address: str, session: AsyncSession, chain: str):
    """Save transaction record to the database, and delete tokens no longer held in bulk"""
    try:
        schema = 'dex_query_v1'
        await session.execute(text("SET search_path TO dex_query_v1;"))
        Holding.with_schema(schema)

        # 查询数据库中钱包的所有持仓
        existing_holdings = await session.execute(
            select(Holding).filter(Holding.wallet_address == wallet_address)
        )
        existing_holdings = existing_holdings.scalars().all()

        # 提取数据库中现有的 token_address 集合
        existing_token_addresses = {holding.token_address for holding in existing_holdings}

        # 提取 tx_data_list 中当前持有的 token_address 集合
        current_token_addresses = {token.get("token_address") for token in tx_data_list}

        # 计算需要删除的 tokens
        tokens_to_delete = existing_token_addresses - current_token_addresses

        # 删除不再持有的代币记录
        if tokens_to_delete:
            await session.execute(
                delete(Holding).filter(
                    Holding.wallet_address == wallet_address,
                    Holding.token_address.in_(tokens_to_delete)
                )
            )

        # 更新或新增持仓
        for token_data in tx_data_list:
            token_address = token_data.get("token_address")
            if not token_address:
                print(f"Invalid token data: {token_data}")
                continue

            existing_holding = next((h for h in existing_holdings if h.token_address == token_address), None)

            holding_data = {
                "wallet_address": wallet_address,
                "token_address": token_address.lower(),
                "token_icon": token_data.get('token_icon', ''),
                "token_name": token_data.get('token_name', ''),
                "chain": token_data.get('chain', 'BSC'),
                "amount": token_data.get('amount', 0),
                "value": token_data.get('value', 0),
                "value_USDT": token_data.get('value_USDT', 0),
                "unrealized_profits": token_data.get('unrealized_profits', 0),
                "pnl": token_data.get('pnl', 0),
                "pnl_percentage": token_data.get('pnl_percentage', 0),
                "avg_price": token_data.get('avg_price', 0),
                "marketcap": token_data.get('marketcap', 0),
                "is_cleared": token_data.get('is_cleared', 0),
                "cumulative_cost": token_data.get('cumulative_cost', 0),
                "cumulative_profit": token_data.get('cumulative_profit', 0),
                "last_transaction_time": make_naive_time(token_data.get('last_transaction_time', datetime.now())),
                "time": make_naive_time(token_data.get('time', datetime.now())),
            }

            if existing_holding:
                # 更新现有记录
                for key, value in holding_data.items():
                    setattr(existing_holding, key, value)
            else:
                # 新增记录
                holding = Holding(**holding_data)
                session.add(holding)

        # 提交数据库变更
        await session.commit()

    except Exception as e:
        # 错误处理
        await session.rollback()
        print(f"Error while saving holding for wallet {wallet_address}: {str(e)}")

async def save_holding2(tx_data_list: list, wallet_address: str, session: AsyncSession, chain: str):
    """Save transaction record to the database, and delete tokens no longer held in bulk"""
    try:
        schema = 'dex_query_v1'
        await session.execute(text("SET search_path TO dex_query_v1;"))
        Holding.with_schema(schema)
        # print(f"開始保存錢包 {wallet_address} 的持倉數據，共 {len(tx_data_list)} 筆")
        # 查询数据库中钱包的所有持仓
        existing_holdings = await session.execute(
            select(Holding).filter(Holding.wallet_address == wallet_address)
        )
        existing_holdings = existing_holdings.scalars().all()
        # print(f"現有持倉記錄數: {len(existing_holdings)}")

        # 提取数据库中现有的 token_address 集合
        existing_token_addresses = {holding.token_address for holding in existing_holdings}

        # 提取 tx_data_list 中当前持有的 token_address 集合
        current_token_addresses = {token.get("token_address") for token in tx_data_list}

        # 计算需要删除的 tokens
        tokens_to_delete = existing_token_addresses - current_token_addresses

        # 删除不再持有的代币记录
        if tokens_to_delete:
            # print(f"刪除不再持有的代幣: {tokens_to_delete}")
            await session.execute(
                delete(Holding).filter(
                    Holding.wallet_address == wallet_address,
                    Holding.token_address.in_(tokens_to_delete)
                )
            )
        updated_count = 0
        added_count = 0
        # 更新或新增持仓
        for token_data in tx_data_list:
            token_address = token_data.get("token_address")
            if not token_address:
                print(f"Invalid token data: {token_data}")
                continue

            existing_holding = next((h for h in existing_holdings if h.token_address == token_address), None)

            holding_data = {
                "wallet_address": wallet_address,
                "token_address": token_address,
                "token_icon": token_data.get('token_icon', ''),
                "token_name": token_data.get('token_name', ''),
                "chain": token_data.get('chain', 'Unknown'),
                "amount": token_data.get('amount', 0),
                "value": token_data.get('value', 0),
                "value_USDT": token_data.get('value_USDT', 0),
                "unrealized_profits": token_data.get('unrealized_profit', 0),
                "pnl": token_data.get('pnl', 0),
                "pnl_percentage": token_data.get('pnl_percentage', 0),
                "avg_price": convert_to_decimal(token_data.get('avg_price', 0)),
                "marketcap": token_data.get('marketcap', 0),
                "is_cleared": token_data.get('sell_amount', 0) >= token_data.get('buy_amount', 0),
                "cumulative_cost": token_data.get('cost', 0),
                "cumulative_profit": token_data.get('profit', 0),
                "last_transaction_time": make_naive_time(token_data.get('last_transaction_time', datetime.now())),
                "time": make_naive_time(token_data.get('time', datetime.now())),
            }

            if existing_holding:
                # 更新现有记录
                for key, value in holding_data.items():
                    setattr(existing_holding, key, value)
                updated_count += 1
            else:
                # 新增记录
                holding = Holding(**holding_data)
                session.add(holding)
                added_count += 1

        # 提交数据库变更
        await session.commit()
        # print(f"錢包 {wallet_address} 持倉保存完成：更新 {updated_count} 筆，新增 {added_count} 筆")
        return True

    except Exception as e:
        # 错误处理
        await session.rollback()
        print(f"保存持倉數據時出錯，錢包 {wallet_address}: {str(e)}")
        print(traceback.format_exc())  # 打印完整錯誤堆疊
        return False

def remove_emoji(text):
    if text is None:
        return ''  # 如果 text 是 None，直接返回空字符串
    
    # 確保是字符串，將非字符串轉換為字符串
    if not isinstance(text, str):
        text = str(text)
        
    emoji_pattern = re.compile(
        "[" 
        "\U0001F600-\U0001F64F"  # 表情符号
        "\U0001F300-\U0001F5FF"  # 符号和图片字符
        "\U0001F680-\U0001F6FF"  # 运输和地图符号
        "\U0001F1E0-\U0001F1FF"  # 国旗
        "]+",
        flags=re.UNICODE,
    )
    
    return emoji_pattern.sub(r"", text)

async def save_wallet_buy_data(wallet_address, tx_data, token_address, session, chain, auto_commit=False):
    """
    保存钱包购买代币的数据，包括平均买入价格、总量和总成本，並確保 chain、chain_id、date 欄位正確。
    只有当交易时间大于现有记录的 last_transaction_time 时才进行累加更新。
    """
    try:
        schema = 'dex_query_v1'
        await session.execute(text("SET search_path TO dex_query_v1;"))
        TokenBuyData.with_schema(schema)
        token_address = token_address.lower()

        if not session.is_active:
            print(f"警告: 传入的 session 不是活动状态")
            return False

        # 決定 chain_id
        chain_id = 9006 if chain.upper() == "BSC" else 0

        # 取得 last_transaction_time，並計算 date（UTC+8 當天 00:00:00）
        last_transaction_time = tx_data.get("last_transaction_time")
        if last_transaction_time is None:
            last_transaction_time = int(time.time())
        if isinstance(last_transaction_time, datetime):
            ts = int(last_transaction_time.timestamp())
        else:
            ts = int(last_transaction_time)
            if ts > 9999999999:  # 如果是毫秒時間戳
                ts = ts // 1000  # 轉換為秒級時間戳
        
        # 轉為 UTC+8 當天 00:00:00
        dt_utc8 = datetime.utcfromtimestamp(ts) + timedelta(hours=8)
        date = dt_utc8.date()  # 只取 date
        # 若 tx_data 有 date 參數則優先用
        date = tx_data.get("date", date)

        # --- 新增防呆：確保 last_active_position_closed_at 一定是 int 或 None ---
        last_active_position_closed_at = tx_data.get("last_active_position_closed_at")
        if isinstance(last_active_position_closed_at, datetime):
            last_active_position_closed_at = int(last_active_position_closed_at.timestamp())
        elif last_active_position_closed_at is not None:
            try:
                last_active_position_closed_at = int(last_active_position_closed_at)
            except Exception:
                last_active_position_closed_at = None
        # -------------------------------------------------------------

        async def do_save():
            # 设置 schema 搜索路径
            await session.execute(text("SET search_path TO dex_query_v1;"))
            
            # 查詢現有紀錄（wallet_address, token_address, chain, date 為唯一 key）
            check_query = text("""
                SELECT id, last_transaction_time, historical_total_buy_amount, historical_total_buy_cost, 
                       historical_total_sell_amount, historical_total_sell_value,
                       total_amount, total_cost, avg_buy_price, total_buy_count, total_sell_count
                FROM wallet_buy_data 
                WHERE wallet_address = :wallet AND LOWER(token_address) = :token AND chain = :chain AND date = :date
            """)
            result = await session.execute(check_query, {"wallet": wallet_address, "token": token_address, "chain": chain, "date": date})
            existing_record = result.fetchone()

            # 从传入数据中获取值
            new_avg_buy_price = float(tx_data.get("avg_buy_price", 0))
            new_total_amount = float(tx_data.get("total_amount", 0))
            new_total_cost = float(tx_data.get("total_cost", 0))
            new_historical_total_buy_amount = float(tx_data.get("historical_total_buy_amount", 0))
            new_historical_total_buy_cost = float(tx_data.get("historical_total_buy_cost", 0))
            new_historical_total_sell_amount = float(tx_data.get("historical_total_sell_amount", 0))
            new_historical_total_sell_value = float(tx_data.get("historical_total_sell_value", 0))
            new_total_buy_count = int(tx_data.get("total_buy_count", 0))
            new_total_sell_count = int(tx_data.get("total_sell_count", 0))
            
            if existing_record:
                existing_id = existing_record[0]
                existing_last_transaction_time = existing_record[1] or 0
                
                # 关键逻辑：只有当新交易时间大于现有记录的 last_transaction_time 时才进行更新
                if ts <= existing_last_transaction_time:
                    print(f"[SKIP] 交易已处理过: wallet={wallet_address}, token={token_address}, "
                          f"new_time={ts}, existing_time={existing_last_transaction_time}")
                    return True  # 跳过已处理的交易
                
                # print(f"[UPDATE] 处理新交易: wallet={wallet_address}, token={token_address}, "
                #       f"new_time={ts}, existing_time={existing_last_transaction_time}")
                
                # 累加历史数据（新交易的增量）
                historical_total_buy_amount = float(existing_record[2] or 0) + new_historical_total_buy_amount
                historical_total_buy_cost = float(existing_record[3] or 0) + new_historical_total_buy_cost
                historical_total_sell_amount = float(existing_record[4] or 0) + new_historical_total_sell_amount
                historical_total_sell_value = float(existing_record[5] or 0) + new_historical_total_sell_value
                
                # 当前持仓数据直接使用新值（因为这是最新的持仓状态）
                total_amount = new_total_amount
                total_cost = new_total_cost
                avg_buy_price = new_avg_buy_price
                
                # 累加交易次数
                total_buy_count = int(existing_record[9] or 0) + new_total_buy_count
                total_sell_count = int(existing_record[10] or 0) + new_total_sell_count
                
            else:
                existing_id = None
                print(f"[INSERT] 新增记录: wallet={wallet_address}, token={token_address}, time={ts}")
                
                # 新记录直接使用传入的所有值
                historical_total_buy_amount = new_historical_total_buy_amount
                historical_total_buy_cost = new_historical_total_buy_cost
                historical_total_sell_amount = new_historical_total_sell_amount
                historical_total_sell_value = new_historical_total_sell_value
                total_amount = new_total_amount
                total_cost = new_total_cost
                avg_buy_price = new_avg_buy_price
                total_buy_count = new_total_buy_count
                total_sell_count = new_total_sell_count
            
            # 计算历史平均价格
            historical_avg_buy_price = historical_total_buy_cost / historical_total_buy_amount if historical_total_buy_amount > 0 else 0
            historical_avg_sell_price = historical_total_sell_value / historical_total_sell_amount if historical_total_sell_amount > 0 else 0

            if existing_id:
                update_query = text("""
                    UPDATE wallet_buy_data 
                    SET avg_buy_price = :avg_price, 
                        total_amount = :amount, 
                        total_cost = :cost,
                        historical_total_buy_amount = :historical_total_buy_amount,
                        historical_total_buy_cost = :historical_total_buy_cost,
                        historical_avg_buy_price = :historical_avg_buy_price,
                        historical_total_sell_amount = :historical_total_sell_amount,
                        historical_total_sell_value = :historical_total_sell_value,
                        historical_avg_sell_price = :historical_avg_sell_price,
                        last_active_position_closed_at = :last_active_position_closed_at,
                        last_transaction_time = :last_transaction_time,
                        total_buy_count = :total_buy_count,
                        total_sell_count = :total_sell_count,
                        updated_at = :update_time
                    WHERE id = :id
                """)
                await session.execute(update_query, {
                    "avg_price": avg_buy_price,
                    "amount": total_amount,
                    "cost": total_cost,
                    "historical_total_buy_amount": historical_total_buy_amount,
                    "historical_total_buy_cost": historical_total_buy_cost,
                    "historical_avg_buy_price": historical_avg_buy_price,
                    "historical_total_sell_amount": historical_total_sell_amount,
                    "historical_total_sell_value": historical_total_sell_value,
                    "historical_avg_sell_price": historical_avg_sell_price,
                    "last_active_position_closed_at": last_active_position_closed_at,
                    "last_transaction_time": ts,  # 更新为新的交易时间
                    "total_buy_count": total_buy_count,
                    "total_sell_count": total_sell_count,
                    "update_time": get_utc8_time(),
                    "id": existing_id
                })
            else:
                insert_query = text("""
                    INSERT INTO wallet_buy_data 
                        (wallet_address, token_address, chain, chain_id, date, avg_buy_price, total_amount, total_cost, updated_at, 
                         historical_total_buy_amount, historical_total_buy_cost, historical_avg_buy_price, 
                         historical_total_sell_amount, historical_total_sell_value, historical_avg_sell_price,
                         last_active_position_closed_at, last_transaction_time, total_buy_count, total_sell_count)
                    VALUES 
                        (:wallet, :token, :chain, :chain_id, :date, :avg_price, :amount, :cost, :update_time, 
                         :historical_total_buy_amount, :historical_total_buy_cost, :historical_avg_buy_price,
                         :historical_total_sell_amount, :historical_total_sell_value, :historical_avg_sell_price,
                         :last_active_position_closed_at, :last_transaction_time, :total_buy_count, :total_sell_count)
                    RETURNING id
                """)
                try:
                    result = await session.execute(insert_query, {
                        "wallet": wallet_address,
                        "token": token_address,
                        "chain": chain,
                        "chain_id": chain_id,
                        "date": date,
                        "avg_price": avg_buy_price,
                        "amount": total_amount,
                        "cost": total_cost,
                        "update_time": get_utc8_time(),
                        "historical_total_buy_amount": historical_total_buy_amount,
                        "historical_total_buy_cost": historical_total_buy_cost,
                        "historical_avg_buy_price": historical_avg_buy_price,
                        "historical_total_sell_amount": historical_total_sell_amount,
                        "historical_total_sell_value": historical_total_sell_value,
                        "historical_avg_sell_price": historical_avg_sell_price,
                        "last_active_position_closed_at": last_active_position_closed_at,
                        "last_transaction_time": ts,
                        "total_buy_count": total_buy_count,
                        "total_sell_count": total_sell_count
                    })
                    # 獲取新插入記錄的 ID，如果失敗則返回 False
                    try:
                        new_id = result.scalar_one()
                        if not new_id:
                            print(f"警告: 插入記錄成功但未返回 ID (wallet={wallet_address}, token={token_address})")
                    except Exception as e:
                        print(f"警告: 獲取插入記錄 ID 失敗 (wallet={wallet_address}, token={token_address}): {str(e)}")
                        return False
                except Exception as e:
                    print(f"錯誤: 插入記錄失敗 (wallet={wallet_address}, token={token_address}): {str(e)}")
                    return False

        # 決定是否需要自己開 transaction
        if session.in_transaction():
            await do_save()
        elif auto_commit:
            async with session.begin():
                await do_save()
        else:
            await do_save()

        return True

    except Exception as e:
        print(f"保存钱包买入数据时发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

async def save_past_transaction(async_session, tx_data, wallet_address, signature, chain, auto_commit=False):
    """
    保存或更新交易記錄到數據庫，使用 UPSERT（on conflict do update）
    """
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # 設置 schema
        await async_session.execute(text("SET search_path TO dex_query_v1;"))
        Transaction.with_schema('dex_query_v1')
        
        # 準備交易數據
        tx_data_copy = tx_data.copy()
        tx_data_copy.pop('id', None)
        
        # 設置默認值和數據處理
        if 'wallet_balance' not in tx_data_copy or tx_data_copy['wallet_balance'] is None:
            tx_data_copy['wallet_balance'] = 0.0
            
        # 處理時間
        if isinstance(tx_data_copy.get('time'), str):
            tx_data_copy['time'] = datetime.fromisoformat(tx_data_copy['time'])
            
        # 將毫秒級時間戳轉換為秒級
        if 'transaction_time' in tx_data_copy and tx_data_copy['transaction_time'] > 9999999999:
            tx_data_copy['transaction_time'] = int(tx_data_copy['transaction_time'] / 1000)
            # logger.info(f"轉換時間戳從毫秒到秒: {tx_data_copy['transaction_time']}")
        
        # 使用正確的唯一約束進行 upsert
        stmt = text("""
            INSERT INTO dex_query_v1.wallet_transaction (
                wallet_address, wallet_balance, token_address, token_icon, token_name,
                price, amount, marketcap, value, holding_percentage, chain, chain_id,
                realized_profit, realized_profit_percentage, transaction_type,
                transaction_time, time, signature, from_token_address, dest_token_address,
                from_token_symbol, dest_token_symbol, from_token_amount, dest_token_amount
            )
            VALUES (
                :wallet_address, :wallet_balance, :token_address, :token_icon, :token_name,
                :price, :amount, :marketcap, :value, :holding_percentage, :chain, :chain_id,
                :realized_profit, :realized_profit_percentage, :transaction_type,
                :transaction_time, :time, :signature, :from_token_address, :dest_token_address,
                :from_token_symbol, :dest_token_symbol, :from_token_amount, :dest_token_amount
            )
            ON CONFLICT (wallet_address, token_address, transaction_time, signature) DO UPDATE SET
                wallet_balance = EXCLUDED.wallet_balance,
                token_icon = EXCLUDED.token_icon,
                token_name = EXCLUDED.token_name,
                price = EXCLUDED.price,
                amount = EXCLUDED.amount,
                marketcap = EXCLUDED.marketcap,
                value = EXCLUDED.value,
                holding_percentage = EXCLUDED.holding_percentage,
                realized_profit = EXCLUDED.realized_profit,
                realized_profit_percentage = EXCLUDED.realized_profit_percentage,
                transaction_type = EXCLUDED.transaction_type,
                time = EXCLUDED.time,
                from_token_address = EXCLUDED.from_token_address,
                dest_token_address = EXCLUDED.dest_token_address,
                from_token_symbol = EXCLUDED.from_token_symbol,
                dest_token_symbol = EXCLUDED.dest_token_symbol,
                from_token_amount = EXCLUDED.from_token_amount,
                dest_token_amount = EXCLUDED.dest_token_amount
        """)
        
        # 執行插入或更新
        await async_session.execute(stmt, tx_data_copy)
        
        if auto_commit:
            await async_session.commit()
        
        return True
        
    except Exception as e:
        logger.error(f"保存交易記錄失敗: {str(e)}")
        logger.error(f"失敗的交易數據: {tx_data}")
        logger.error("錯誤堆疊:", exc_info=True)
        
        try:
            await async_session.rollback()
            logger.info("已回滾失敗的事務")
        except Exception as rollback_error:
            logger.error(f"回滾事務失敗: {str(rollback_error)}")
        
        raise

async def get_token_buy_data_with_new_session(wallet_address: str, token_address: str, chain):
    """
    當原始會話不可用時，使用新的會話獲取代幣數據
    """
    print(f"創建新會話查詢代幣數據: wallet: {wallet_address}, token: {token_address}")
    engine = create_async_engine(
        DATABASES.get(chain, DATABASE_URI_SWAP_BSC), 
        echo=False
    )
    async_session = sessionmaker(
        bind=engine, 
        class_=AsyncSession, 
        expire_on_commit=False
    )
    
    async with async_session() as new_session:
        async with new_session.begin():
            schema = 'dex_query_v1'
            TokenBuyData.with_schema(schema)
            
            result = await new_session.execute(
                select(TokenBuyData).filter(
                    TokenBuyData.wallet_address == wallet_address,
                    TokenBuyData.token_address == token_address
                )
            )
            token_data = result.scalars().first()

            if token_data:
                return {
                    "token_address": token_data.token_address,
                    "avg_buy_price": token_data.avg_buy_price,
                    "total_amount": token_data.total_amount,
                    "total_cost": token_data.total_cost
                }
            else:
                return {
                    "token_address": token_address,
                    "avg_buy_price": 0,
                    "total_amount": 0,
                    "total_cost": 0
                }

async def get_token_buy_data(wallet_address: str, token_address: str, session: AsyncSession, chain):
    """
    查詢 TokenBuyData 數據表，獲取指定錢包地址和代幣地址的持倉數據。
    優化事務管理，能夠適應不同的事務狀態。
    """
    try:
        schema = 'dex_query_v1'
        TokenBuyData.with_schema(schema)

        # 檢查會話狀態
        if not session.is_active:
            print(f"警告: 傳入的 session 不是活動狀態。wallet: {wallet_address}, token: {token_address}")
            # 如果會話不活動，創建一個新的會話並返回數據
            # 這是一個應急措施，理想情況下應該避免這種情況
            return await get_token_buy_data_with_new_session(wallet_address, token_address, chain)
            
        # 檢查事務狀態
        has_transaction = session.in_transaction()

        async def _execute_query():
            # 執行查詢
            result = await session.execute(
                select(TokenBuyData).filter(
                    TokenBuyData.wallet_address == wallet_address,
                    TokenBuyData.token_address == token_address
                )
            )
            token_data = result.scalars().first()

            if token_data:
                return {
                    "token_address": token_data.token_address,
                    "avg_buy_price": token_data.avg_buy_price,
                    "total_amount": token_data.total_amount,
                    "total_cost": token_data.total_cost
                }
            else:
                # 返回空數據結構而不是 None，避免後續處理出錯
                return {
                    "token_address": token_address,
                    "avg_buy_price": 0,
                    "total_amount": 0,
                    "total_cost": 0
                }

        # 根據事務狀態決定執行方式
        if has_transaction:
            # 已在事務中，直接執行查詢
            return await _execute_query()
        else:
            # 不在事務中，創建一個只讀事務
            async with session.begin():
                return await _execute_query()

    except SQLAlchemyError as e:
        print(f"Database error while querying TokenBuyData: {str(e)}")
        import traceback
        traceback.print_exc()
        # 返回空數據結構而不是 None，避免後續處理出錯
        return {
            "token_address": token_address,
            "avg_buy_price": 0,
            "total_amount": 0,
            "total_cost": 0
        }
    except Exception as e:
        print(f"Unexpected error while querying TokenBuyData: {str(e)}")
        import traceback
        traceback.print_exc()
        # 返回空數據結構而不是 None，避免後續處理出錯
        return {
            "token_address": token_address,
            "avg_buy_price": 0,
            "total_amount": 0,
            "total_cost": 0
        }
    
async def get_wallet_token_holdings(wallet_address, session, chain):
    """
    獲取錢包中所有有持倉的代幣記錄
    
    Args:
        wallet_address (str): 錢包地址
        session (AsyncSession): 資料庫會話
        chain (str): 區塊鏈名稱
        
    Returns:
        list: TokenBuyData記錄列表
    """
    try:
        schema = 'dex_query_v1'
        TokenBuyData.with_schema(schema)
        
        # 查詢此錢包所有代幣的TokenBuyData記錄
        token_buy_data_query = select(TokenBuyData).filter(
            TokenBuyData.wallet_address == wallet_address,
            TokenBuyData.total_amount > 0  # 只獲取仍有持倉的代幣
        )
        result = await session.execute(token_buy_data_query)
        token_buy_data_records = result.scalars().all()
        
        # print(f"錢包 {wallet_address} 找到 {len(token_buy_data_records)} 個有持倉的代幣")
        
        return token_buy_data_records
    
    except Exception as e:
        print(f"查詢錢包 {wallet_address} 的代幣持倉記錄時出錯: {e}")
        import traceback
        traceback.print_exc()
        return []
    
async def clear_all_holdings(wallet_address: str, session: AsyncSession, chain: str):
    """清除錢包的所有持倉記錄"""
    try:
        schema = 'dex_query_v1'
        await session.execute(text("SET search_path TO dex_query_v1;"))
        Holding.with_schema(schema)

        await session.execute(
            delete(Holding).filter(Holding.wallet_address == wallet_address)
        )
        await session.commit()
        # print(f"Cleared all holdings for wallet {wallet_address}.")
    except Exception as e:
        await session.rollback()
        print(f"Error while clearing holdings for wallet {wallet_address}: {e}")

async def deactivate_wallets(session, addresses):
    """
    根據提供的地址列表，將 WalletSummary 中符合的錢包地址的 is_active 欄位設置為 False。
    
    :param session: 資料庫會話
    :param addresses: 一個包含要更新的地址的列表
    :return: 更新成功的錢包數量
    """
    try:
        await session.execute(text("SET search_path TO dex_query_v1;"))
        result = await session.execute(
            update(WalletSummary)
            .where(WalletSummary.wallet_address.in_(addresses))  # 篩選出符合的錢包地址
            .values(is_active=False)  # 將 is_active 設為 False
        )
        await session.commit()  # 提交交易
        
        return result.rowcount  # 返回更新的行數，即更新成功的錢包數量
    except Exception as e:
        # 日誌記錄錯誤
        await log_error(
            session,
            str(e),
            "models",
            "deactivate_wallets",
            f"更新錢包 is_active 欄位為 False 失敗，原因 {e}"
        )
        return 0  # 若更新失敗，返回 0
    
async def activate_wallets(session, addresses):
    try:
        await session.execute(text("SET search_path TO dex_query_v1;"))
        result = await session.execute(
            update(WalletSummary)
            .where(WalletSummary.wallet_address.in_(addresses))
            .values(is_active=True)
        )
        await session.commit()  # 提交交易
        
        return result.rowcount  # 返回更新的行數，即更新成功的錢包數量
    except Exception as e:
        # 日誌記錄錯誤
        await log_error(
            session,
            str(e),
            "models",
            "activate_wallets",
            f"更新錢包 is_active 欄位為 False 失敗，原因 {e}"
        )
        return 0  # 若更新失敗，返回 0

# 寫入資料庫的函數
async def add_wallets_to_db(chain, session: AsyncSession, addresses: list):
    schema = 'dex_query_v1'
    FollowWallet.__table__.schema = schema  # 根據傳入的 chain 動態修改 schema
    
    async with session.begin():  # 使用 async with 保證 session 正確管理
        for wallet_address in addresses:
            # 檢查該地址是否已經存在
            result = await session.execute(select(FollowWallet).filter(FollowWallet.wallet_address == wallet_address))
            existing_wallet = result.scalars().first()  # 如果該地址已經存在，則取出該錢包

            if existing_wallet:
                if not existing_wallet.is_active:
                    # 如果地址已存在且 is_active 為 False，則將其更新為 True 並更新 updated_at
                    existing_wallet.is_active = True
                    existing_wallet.updated_at = get_utc8_time()
            else:
                # 如果地址不存在，則創建新錢包
                new_wallet = FollowWallet(wallet_address=wallet_address, is_active=True, updated_at=get_utc8_time())
                session.add(new_wallet)  # 添加新錢包資料

    await session.commit()

# 刪除資料庫地址的函數
async def remove_wallets_from_db(chain, session: AsyncSession, addresses: list):
    schema = 'dex_query_v1'
    FollowWallet.with_schema(schema)

    async with session.begin():
        result = await session.execute(select(FollowWallet).filter(FollowWallet.wallet_address.in_(addresses)))
        wallets_to_deactivate = result.scalars().all()

        for wallet in wallets_to_deactivate:
            wallet.is_active = False
            wallet.updated_at = get_utc8_time()

    await session.commit()

async def get_active_wallets(chain, session: AsyncSession):
    schema = 'dex_query_v1'
    WalletSummary.with_schema(schema)

    async with session.begin():
        query = select(WalletSummary.wallet_address).where(
            WalletSummary.is_active == True
        )
        result = await session.execute(query)
        wallets = result.scalars().all()
    return list(wallets)

async def log_error(session, error_message: str, module_name: str, function_name: str = None, additional_info: str = None):
    """記錄錯誤訊息到資料庫"""
    schema = "dex_query_v1"
    await session.execute(text("SET search_path TO dex_query_v1;"))
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

WALLET_SYNC_API_ENDPOINT = os.getenv("WALLET_SYNC_API_ENDPOINT", "http://moonx.backend:4200/internal/sync_kol_wallets")

def wallet_to_api_dict(wallet) -> dict:
    """將錢包數據轉換為 API 格式"""
    try:
        # 檢查是否有任何交易記錄
        has_transactions = (
            (wallet.total_transaction_num_30d or 0) > 0 or
            (wallet.total_transaction_num_7d or 0) > 0 or
            (wallet.total_transaction_num_1d or 0) > 0
        )
        
        if not has_transactions:
            logger.info(f"錢包 {wallet.wallet_address} 沒有交易記錄，跳過 API 推送")
            return None

        logger.info(f"開始轉換錢包數據: {wallet.wallet_address}")
        logger.debug(f"交易統計 - 30d: {wallet.total_transaction_num_30d}, 7d: {wallet.total_transaction_num_7d}, 1d: {wallet.total_transaction_num_1d}")

        # 創建新的字典，首先添加必填項
        api_data = {
            "address": wallet.wallet_address,  # 改為 address
            "chain": wallet.chain.upper() if wallet.chain else "BSC",
            "last_transaction_time": wallet.last_transaction_time,
            "isActive": wallet.is_active if wallet.is_active is not None else True,
            "walletType": wallet.wallet_type if wallet.wallet_type is not None else 0
        }
        
        # 添加所有可能的字段
        fields_mapping = {
            "balance": "balance",
            "balance_usd": "balanceUsd",
            "tag": "tag",
            "twitter_name": "twitterName",
            "twitter_username": "twitterUsername",
            "is_smart_wallet": "isSmartWallet",
            "asset_multiple": "assetMultiple",
            "token_list": "tokenList",
            "avg_cost_30d": "avgCost30d",
            "avg_cost_7d": "avgCost7d",
            "avg_cost_1d": "avgCost1d",
            "total_transaction_num_30d": "totalTransactionNum30d",
            "total_transaction_num_7d": "totalTransactionNum7d",
            "total_transaction_num_1d": "totalTransactionNum1d",
            "buy_num_30d": "buyNum30d",
            "buy_num_7d": "buyNum7d",
            "buy_num_1d": "buyNum1d",
            "sell_num_30d": "sellNum30d",
            "sell_num_7d": "sellNum7d",
            "sell_num_1d": "sellNum1d",
            "win_rate_30d": "winRate30d",
            "win_rate_7d": "winRate7d",
            "win_rate_1d": "winRate1d",
            "pnl_30d": "pnl30d",
            "pnl_7d": "pnl7d",
            "pnl_1d": "pnl1d",
            "pnl_percentage_30d": "pnlPercentage30d",
            "pnl_percentage_7d": "pnlPercentage7d",
            "pnl_percentage_1d": "pnlPercentage1d",
            "pnl_pic_30d": "pnlPic30d",
            "pnl_pic_7d": "pnlPic7d",
            "pnl_pic_1d": "pnlPic1d",
            "unrealized_profit_30d": "unrealizedProfit30d",
            "unrealized_profit_7d": "unrealizedProfit7d",
            "unrealized_profit_1d": "unrealizedProfit1d",
            "total_cost_30d": "totalCost30d",
            "total_cost_7d": "totalCost7d",
            "total_cost_1d": "totalCost1d",
            "avg_realized_profit_30d": "avgRealizedProfit30d",
            "avg_realized_profit_7d": "avgRealizedProfit7d",
            "avg_realized_profit_1d": "avgRealizedProfit1d"
        }
        
        # 定義需要限制小數位數的數值字段（移除字符串字段）
        decimal_fields = {
            "balance", "balance_usd", "asset_multiple",
            "avg_cost_30d", "avg_cost_7d", "avg_cost_1d",
            "total_transaction_num_30d", "total_transaction_num_7d", "total_transaction_num_1d",
            "buy_num_30d", "buy_num_7d", "buy_num_1d",
            "sell_num_30d", "sell_num_7d", "sell_num_1d",
            "win_rate_30d", "win_rate_7d", "win_rate_1d",
            "pnl_30d", "pnl_7d", "pnl_1d",
            "pnl_percentage_30d", "pnl_percentage_7d", "pnl_percentage_1d",
            "unrealized_profit_30d", "unrealized_profit_7d", "unrealized_profit_1d",
            "total_cost_30d", "total_cost_7d", "total_cost_1d",
            "avg_realized_profit_30d", "avg_realized_profit_7d", "avg_realized_profit_1d"
        }

        # 定義字符串字段
        string_fields = {
            "tag", "twitter_name", "twitter_username", "token_list", 
            "pnl_pic_30d", "pnl_pic_7d", "pnl_pic_1d"
        }

        from decimal import Decimal

        # 遍歷所有字段映射
        for db_field, api_field in fields_mapping.items():
            if hasattr(wallet, db_field):
                value = getattr(wallet, db_field)
                if value is not None:
                    # 如果是 Decimal 類型，轉換為 float
                    if isinstance(value, Decimal):
                        value = float(value)
                    
                    # 處理字符串字段
                    if db_field in string_fields:
                        if isinstance(value, str) and value.strip():  # 確保字符串不為空
                            api_data[api_field] = value
                            logger.debug(f"添加字符串字段 {db_field}: {value}")
                        elif value is not None:  # 非字符串但非空值也添加
                            api_data[api_field] = str(value)
                            logger.debug(f"轉換並添加字段 {db_field}: {value}")
                    
                    # 處理數值字段
                    elif db_field in decimal_fields and isinstance(value, (int, float)):
                        # 限制小數位數為10位
                        value = round(float(value), 10)
                        api_data[api_field] = value
                        logger.debug(f"添加數值字段 {db_field}: {value}")
                    
                    # 處理布爾字段
                    elif db_field == "is_smart_wallet":
                        api_data[api_field] = bool(value)
                        logger.debug(f"添加布爾字段 {db_field}: {value}")
                    
                    # 處理其他字段（非數值且超過1e8的跳過）
                    elif isinstance(value, (int, float)) and abs(value) > 1e8:
                        logger.debug(f"跳過超大數值字段 {db_field}: {value}")
                        continue
                    else:
                        api_data[api_field] = value
                        logger.debug(f"添加其他字段 {db_field}: {value}")

        logger.info(f"成功轉換錢包數據: {wallet.wallet_address}")
        logger.debug(f"轉換後的API數據: {api_data}")
        return api_data
    except Exception as e:
        logger.error(f"轉換錢包數據時發生錯誤: {str(e)}")
        logger.error(f"錢包地址: {getattr(wallet, 'wallet_address', 'unknown')}")
        logger.error(traceback.format_exc())
        return None

async def push_wallet_to_api(api_data: dict) -> bool:
    """推送錢包數據到 API"""
    try:
        if not api_data:
            logger.warning("API 數據為空，跳過推送")
            return False
            
        logger.info(f"準備推送錢包數據到 API: {api_data.get('address')}")  # 改為 address
        logger.debug(f"完整 API 數據: {api_data}")
            
        async with aiohttp.ClientSession() as session:
            # 將單個錢包數據包裝成列表
            data_to_send = [api_data]
            async with session.post(WALLET_SYNC_API_ENDPOINT, json=data_to_send) as resp:
                if resp.status == 200:
                    logger.info(f"成功推送錢包數據到 API: {api_data.get('address')}")  # 改為 address
                    return True
                else:
                    text = await resp.text()
                    logger.error(f"API 推送失敗: {resp.status}, {text}")
                    logger.error(f"失敗的錢包地址: {api_data.get('address')}")  # 改為 address
                    return False
    except Exception as e:
        logger.error(f"推送到 API 時發生錯誤: {str(e)}")
        logger.error(f"錯誤的錢包地址: {api_data.get('address')}")  # 改為 address
        return False
    
async def get_token_info_from_db(token_address: str, chain_id: int, session: AsyncSession):
    stmt = text("""
        SELECT 
            address,               -- 作為 token_address
            symbol,                  -- 作為 token_name
            logo,                  -- 作為 token_icon
            chain_id,
            supply,
            decimals,
            price_usd,
            fdv_usd,
            symbol
        FROM dex_query_v1.tokens
        WHERE LOWER(address) = :token_address AND chain_id = :chain_id
        LIMIT 1
    """)
    result = await session.execute(stmt, {
        "token_address": token_address.lower(),
        "chain_id": chain_id
    })
    row = result.fetchone()
    if row:
        return {
            "token_address": row.address,
            "name": row.symbol,
            "logo": row.logo,
            "chain_id": row.chain_id,
            "supply": row.supply,
            "decimals": row.decimals,
            "price_usd": row.price_usd,
            "marketcap": row.fdv_usd,
            "symbol": row.symbol
        }
    return None