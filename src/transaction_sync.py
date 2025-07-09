import os
import time
import asyncio
import logging
import psutil
import traceback
import json
from typing import Dict, List, Any, Set, Optional, Union
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ConversionSyntax
import decimal  # 添加 decimal 模塊導入
from sqlalchemy import select, text, and_, delete, update, insert, case
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from models import *
from token_info import TokenInfoFetcher, redis_client
from balance import fetch_wallet_balances, get_price
from eth_utils import to_checksum_address as eth_to_checksum_address
import hashlib
import web3
from web3 import Web3
from functools import lru_cache
from aiokafka import AIOKafkaConsumer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, RPC_URL
from sqlalchemy.dialects.postgresql import insert
from collections import defaultdict
from token_cache import token_cache  # 导入 Redis 缓存实例
from database import get_main_session_factory, get_swap_session_factory, get_main_engine, SessionLocal
from event_processor import EventProcessor
from models import WalletTokenState, Base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import inspect
from functools import wraps

logger = logging.getLogger(__name__)
now_utc_plus_8 = datetime.utcnow() + timedelta(hours=8)

# 初始化 Web3
w3 = Web3(Web3.HTTPProvider(RPC_URL))

# 常見代幣地址對應符號，先行返回避免查詢
COMMON_TOKEN_SYMBOLS = {
    "0x0000000000000000000000000000000000000000": "BNB",
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": "WBNB",
    "0x55d398326f99059ff775485246999027b3197955": "USDT",
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": "USDC",
}

class KafkaConsumer:
    def __init__(self, main_session_factory, swap_session_factory, main_engine):
        self.main_session_factory = main_session_factory
        self.swap_session_factory = swap_session_factory
        self.main_engine = main_engine
        self.consumer = None
        self.wallet_address_set = set()
        self.running = True
        self.session = None  # 初始化 session 属性为 None

        # 初始化 EventProcessor
        self.event_processor = EventProcessor()

    async def refresh_wallet_address_set(self):
        """刷新钱包地址集合"""
        try:
            async with self.swap_session_factory() as session:
                addresses = await get_active_wallets("BSC", session)
                self.wallet_address_set = set(addresses)
                logger.info(f"[CACHE] 已刷新 wallet_address_set, 共 {len(self.wallet_address_set)} 筆")
        except Exception as e:
            logger.error(f"刷新钱包地址集合时出错: {str(e)}")

    async def start(self):
        """启动 Kafka 消费者"""
        try:
            logger.info("Starting Kafka consumer...")
            
            # 在这里启动 EventProcessor，因为它需要在事件循环中启动
            self.event_processor.start()

            # 创建数据库会话
            self.session = self.swap_session_factory()
            
            # 生成唯一的 group_id
            unique_group_id = f"smart_money_group_{int(time.time())}"
            logger.info(f"Using unique consumer group ID: {unique_group_id}")
            
            # 初始化消费者
            self.consumer = AIOKafkaConsumer(
                'web3_trade_sm_events',
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=unique_group_id,
                auto_offset_reset='latest',  # 從最早的消息開始消費
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,  # 每5秒自動提交一次
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                session_timeout_ms=45000,  # 45秒超時
                heartbeat_interval_ms=15000,  # 15秒心跳
                max_poll_interval_ms=300000,  # 5分鐘最大輪詢間隔
                max_poll_records=500  # 每次最多拉取500條消息
            )

            logger.info(f"Connecting to Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
            await self.consumer.start()
            logger.info("Kafka consumer started successfully, listening to topic: web3_trade_sm_events")

            # 刷新钱包地址集合
            await self.refresh_wallet_address_set()

            logger.info("Starting to consume messages...")
            try:
                async for msg in self.consumer:
                    if not self.running:
                        break
                    try:
                        logger.debug(f"Received message: partition={msg.partition}, offset={msg.offset}")
                        await self.process_message(msg.value)
                    except Exception as e:
                        logger.error(f"处理消息时出错: {str(e)}")
            finally:
                await self.stop()

        except Exception as e:
            logger.error(f"Kafka consumer error: {str(e)}")
            if self.consumer:
                await self.consumer.stop()
            if self.session:
                await self.session.close()
            self.session = None

    async def stop(self):
        """停止 Kafka 消费者"""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
        
        # 停止 EventProcessor
        if self.event_processor:
            self.event_processor.stop()

        if self.session:
            await self.session.close()
            self.session = None

    async def process_message(self, message: Dict):
        """處理 Kafka 消息"""
        try:
            # 檢查消息類型
            if message.get('type') != 'com.zeroex.web3.core.event.data.TradeEvent':
                return

            event = message.get('event', {})
            if not event or event.get('network') != 'BSC':
                return

            tx_hash = event.get('hash')
            wallet_address = event.get('address')
            token_address = event.get('tokenAddress')
            
            # 只处理在 wallet_address_set 中的钱包
            if wallet_address not in self.wallet_address_set:
                return

            # 1. 统一数据类型
            side = event.get('side', '').lower()
            if side == 'buy':
                amount = Decimal(str(event.get('toTokenAmount', '0')))
            else:
                amount = Decimal(str(event.get('fromTokenAmount', '0')))
            value_usd = Decimal(str(event.get('volumeUsd', '0')))
            timestamp = int(event.get('timestamp', 0))
            price = value_usd / amount if amount > 0 else Decimal('0')

            # 2. 获取所需信息
            token_info = await TokenInfoFetcher.get_token_info(token_address)
            if not token_info:
                logger.warning(f"无法获取代币信息: {token_address}")
                return

            # 从缓存/DB获取交易前状态
            state_before = await token_cache.get_token_data(wallet_address, token_address, self.session)
            
            # 3. 核心计算
            transaction_type = token_cache.determine_transaction_type(side, amount, state_before['total_amount'])

            realized_profit_this_trade = Decimal('0')
            if transaction_type in ['sell', 'clean']:
                if state_before['avg_buy_price'] > 0:
                    realized_profit_this_trade = (price - state_before['avg_buy_price']) * amount

            # 更新状态
            new_state = await update_wallet_token_state(self.session, wallet_address, token_address, "BSC", 9006, side, amount, value_usd, timestamp, realized_profit_this_trade)

            # 4. 準備交易記錄數據
            wallet_balance = Decimal('0')
            try:
                wallet_balance = await get_wallet_balance(wallet_address)
            except Exception as e:
                logger.warning(f"获取钱包余额失败: {str(e)}")

            holding_percentage = Decimal('0')
            if side == 'buy':
                if wallet_balance > 0:
                    holding_percentage = min((value_usd / wallet_balance) * 100, Decimal('100'))
            else:
                if state_before['total_amount'] > 0:
                    holding_percentage = min((amount / state_before['total_amount']) * 100, Decimal('100'))

            if side == 'buy':
                from_token_address, dest_token_address = event.get('quoteMint', ''), event.get('baseMint', '')
                from_token_symbol, dest_token_symbol = event.get('quoteSymbol', ''), event.get('baseSymbol', '')
                from_token_amount, dest_token_amount = Decimal(str(event.get('fromTokenAmount', '0'))), Decimal(str(event.get('toTokenAmount', '0')))
            else:
                from_token_address, dest_token_address = event.get('baseMint', ''), event.get('quoteMint', '')
                from_token_symbol, dest_token_symbol = event.get('baseSymbol', ''), event.get('quoteSymbol', '')
                from_token_amount, dest_token_amount = Decimal(str(event.get('fromTokenAmount', '0'))), Decimal(str(event.get('toTokenAmount', '0')))
            
            if not from_token_symbol:
                # 先檢查常見映射
                from_token_symbol = COMMON_TOKEN_SYMBOLS.get(from_token_address.lower(), '')

            if not from_token_symbol:
                try:
                    info = await TokenInfoFetcher.get_token_info(from_token_address.lower())
                    if info and from_token_address.lower() in info:
                        token_meta = info[from_token_address.lower()]
                        from_token_symbol = token_meta.get('symbol', '') or token_meta.get('name', '') or ''
                except Exception:
                    from_token_symbol = ''

            if not dest_token_symbol:
                dest_token_symbol = COMMON_TOKEN_SYMBOLS.get(dest_token_address.lower(), '')

            if not dest_token_symbol:
                try:
                    info = await TokenInfoFetcher.get_token_info(dest_token_address.lower())
                    if info and dest_token_address.lower() in info:
                        token_meta = info[dest_token_address.lower()]
                        dest_token_symbol = token_meta.get('symbol', '') or token_meta.get('name', '') or ''
                except Exception:
                    dest_token_symbol = ''

            try:
                supply = Decimal(str(token_info.get('supply', '0')))
            except decimal.InvalidOperation:
                # logger.warning(f"代币 {token_address} 的 supply 值无效: {token_info.get('supply')}")
                supply = Decimal('0')
            
            marketcap = price * supply
            
            # 檢查 marketcap 是否超過異常值，如果超過則設置為 0
            if marketcap > Decimal('10000000000'):
                # logger.warning(f"代币 {token_address} 的 marketcap 值異常: {marketcap}，設置為 0")
                marketcap = Decimal('0')

            tx_data = {
                "wallet_address": wallet_address, "wallet_balance": float(wallet_balance),
                "token_address": token_address, "token_icon": token_info.get('logo', ''), "token_name": token_info.get('name', ''),
                "price": float(price), "amount": float(amount), "marketcap": float(marketcap), "value": float(value_usd),
                "holding_percentage": float(holding_percentage), "chain": "BSC", "chain_id": 9006,
                "realized_profit": float(realized_profit_this_trade),
                "realized_profit_percentage": float((price / state_before['avg_buy_price'] - 1) * 100 if state_before['avg_buy_price'] > 0 and side != 'buy' else 0),
                "transaction_type": transaction_type, "transaction_time": timestamp, "time": get_utc8_time(),
                "from_token_address": from_token_address, "dest_token_address": dest_token_address,
                "from_token_symbol": from_token_symbol, "dest_token_symbol": dest_token_symbol,
                "from_token_amount": float(from_token_amount), "dest_token_amount": float(dest_token_amount),
                "signature": tx_hash
            }

            # 5. 保存交易记录并推送事件
            success = await save_past_transaction(self.session, tx_data, wallet_address, tx_hash, "BSC", auto_commit=True)
            if success:
                logger.info(f"成功保存交易記錄: {tx_hash}")
                await self.event_processor.add_event(tx_data, wallet_address, token_address)
            
            # 更新Redis缓存
            if new_state:
                token_cache.redis.set(token_cache._get_key(wallet_address, token_address), json.dumps(token_cache._decimal_to_str({
                    'total_amount': new_state['current_amount'],
                    'total_cost': new_state['current_total_cost'],
                    'avg_buy_price': new_state['current_avg_buy_price'],
                    'realized_profit': new_state['historical_realized_pnl'],
                    'last_transaction_time': new_state['last_transaction_time']
                })))
            else:
                logger.error(f"更新錢包代幣狀態失敗，無法更新 Redis 緩存: {wallet_address}, {token_address}")

            return True

        except Exception as e:
            logger.error(f"处理消息时出错: {str(e)}", exc_info=True)
            return False

async def update_wallet_token_state(session, wallet_address, token_address, chain, chain_id, side, amount, value, timestamp, realized_profit_this_trade):
    """
    使用 UPSERT 更新 wallet_token_state 表
    
    Args:
        session: 數據庫會話
        wallet_address: 錢包地址
        token_address: 代幣地址
        chain: 鏈名稱
        chain_id: 鏈 ID
        side: 交易方向 (buy/sell)
        amount: 交易數量
        value: 交易價值
        timestamp: 交易時間戳
        realized_profit_this_trade: 本次交易已實現盈虧
        
    Returns:
        dict: 包含更新後狀態的字典，如果失敗則返回 None
    """
    try:
        # 準備基礎數據
        base_values = {
            'wallet_address': wallet_address,
            'token_address': token_address,
            'chain': chain,
            'chain_id': chain_id,
            'last_transaction_time': timestamp,
            'updated_at': get_utc8_time()
        }

        # 根據交易方向計算更新值
        if side == 'buy':
            update_values = {
                'current_amount': WalletTokenState.current_amount + amount,
                'current_total_cost': WalletTokenState.current_total_cost + value,
                'historical_buy_amount': WalletTokenState.historical_buy_amount + amount,
                'historical_buy_cost': WalletTokenState.historical_buy_cost + value,
                'historical_buy_count': WalletTokenState.historical_buy_count + 1
            }
            # 如果當前持倉為0,則更新開倉時間
            if base_values.get('current_amount', 0) == 0:
                update_values['position_opened_at'] = timestamp
        else:  # sell
            update_values = {
                'current_amount': WalletTokenState.current_amount - amount,
                'current_total_cost': case(
                    (WalletTokenState.current_amount > 0,
                     WalletTokenState.current_total_cost * (1 - amount / WalletTokenState.current_amount)),
                    else_=0
                ),
                'historical_sell_amount': WalletTokenState.historical_sell_amount + amount,
                'historical_sell_value': WalletTokenState.historical_sell_value + value,
                'historical_sell_count': WalletTokenState.historical_sell_count + 1,
                'historical_realized_pnl': WalletTokenState.historical_realized_pnl + realized_profit_this_trade
            }

        # 合併基礎數據和更新值
        insert_values = {**base_values}
        if side == 'buy':
            insert_values.update({
                'current_amount': amount,
                'current_total_cost': value,
                'current_avg_buy_price': value / amount if amount > 0 else 0,
                'position_opened_at': timestamp,
                'historical_buy_amount': amount,
                'historical_buy_cost': value,
                'historical_sell_amount': 0,
                'historical_sell_value': 0,
                'historical_realized_pnl': 0,
                'historical_buy_count': 1,
                'historical_sell_count': 0
            })
        else:
            insert_values.update({
                'current_amount': 0,
                'current_total_cost': 0,
                'current_avg_buy_price': 0,
                'position_opened_at': None,
                'historical_buy_amount': 0,
                'historical_buy_cost': 0,
                'historical_sell_amount': amount,
                'historical_sell_value': value,
                'historical_realized_pnl': realized_profit_this_trade,
                'historical_buy_count': 0,
                'historical_sell_count': 1
            })

        # 構建 UPSERT 語句
        stmt = insert(WalletTokenState).values(**insert_values)
        
        # 在衝突時更新
        stmt = stmt.on_conflict_do_update(
            constraint='uq_wallet_token_chain',
            set_=update_values
        )
        
        # 執行語句
        await session.execute(stmt)
        
        # 如果是買入且金額大於0,更新平均買入價格
        if side == 'buy' and amount > 0:
            await session.execute(
                update(WalletTokenState)
                .where(
                    WalletTokenState.wallet_address == wallet_address,
                    WalletTokenState.token_address == token_address,
                    WalletTokenState.chain == chain,
                    WalletTokenState.current_amount > 0
                )
                .values(
                    current_avg_buy_price=WalletTokenState.current_total_cost / WalletTokenState.current_amount
                )
            )
        
        # 查詢更新後的數據
        result = await session.execute(
            select(WalletTokenState).where(
                WalletTokenState.wallet_address == wallet_address,
                WalletTokenState.token_address == token_address,
                WalletTokenState.chain == chain
            )
        )
        updated_state = result.scalar_one_or_none()
        
        if updated_state:
            return {
                'current_amount': updated_state.current_amount,
                'current_total_cost': updated_state.current_total_cost,
                'current_avg_buy_price': updated_state.current_avg_buy_price,
                'historical_realized_pnl': updated_state.historical_realized_pnl,
                'last_transaction_time': updated_state.last_transaction_time
            }
        else:
            logger.error(f"無法查詢更新後的狀態: {wallet_address}, {token_address}")
            return None

    except Exception as e:
        logger.error(f"更新錢包代幣狀態時出錯: {str(e)}", exc_info=True)
        return None

async def with_timeout(coro, timeout=60, description="操作"):
    try:
        start_time = time.time()
        result = await asyncio.wait_for(coro, timeout)
        end_time = time.time()
        duration = end_time - start_time
        if duration > 5:  # 只記錄執行時間超過5秒的操作
            logger.info(f"{description}完成，耗時: {duration:.2f}秒")
        return result
    except asyncio.TimeoutError:
        logger.error(f"{description}超時（{timeout}秒）")
        raise
        
# 簡單的資源使用監控
def log_resource_usage(operation=""):
    process = psutil.Process()
    mem_info = process.memory_info()
    cpu_percent = process.cpu_percent(interval=0.1)
    logger.info(f"資源使用 [{operation}]: CPU {cpu_percent}%, 記憶體 {mem_info.rss / (1024 * 1024):.1f} MB")

def get_update_time():
    # 回傳 UTC+8 的 naive datetime
    return (datetime.utcnow() + timedelta(hours=8)).replace(tzinfo=None)

async def fetch_token_info_for_wallets(remaining_tokens, batch_size=30):
    """
    批量獲取代幣信息，分批处理以避免超时
    """
    all_results = {}
    tokens_list = list(remaining_tokens)
    
    # 分批处理
    for i in range(0, len(tokens_list), batch_size):
        batch = tokens_list[i:i+batch_size]
        tasks = [TokenInfoFetcher.get_token_info(token) for token in batch]
        batch_results = await asyncio.gather(*tasks)
        batch_dict = dict(zip(batch, batch_results))
        all_results.update(batch_dict)
        
        # 添加短暂暂停，避免过快请求
        await asyncio.sleep(0.1)
    
    return all_results

def to_checksum_address(address):
    """將地址轉換為 checksum 格式"""
    if not address:
        return None
    try:
        return web3.Web3.to_checksum_address(address)
    except Exception as e:
        logger.error(f"Error converting address to checksum: {str(e)}")
        return None

# 添加緩存裝飾器
@lru_cache(maxsize=1000)
def get_cached_token_info(address: str) -> Optional[Dict]:
    """緩存單個代幣信息"""
    return None

async def get_token_info_from_Ian(token_addresses: List[str], engine) -> Dict:
    """从数据库批量获取代币信息，使用緩存和優化查詢"""    
    try:
        async_session = sessionmaker(
            bind=engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )
        async with async_session() as token_session:
            if isinstance(token_addresses, str):
                token_addresses = [token_addresses]
            
            # 轉換為 checksum 地址
            checksum_addresses = [to_checksum_address(addr) for addr in token_addresses]
            
            # 檢查緩存
            cached_results = {}
            addresses_to_fetch = []
            
            for addr in checksum_addresses:
                cached_data = get_cached_token_info(addr)
                if cached_data is not None:
                    cached_results[addr] = cached_data
                else:
                    addresses_to_fetch.append(addr)
            
            if not addresses_to_fetch:
                return cached_results
            
            # 修正 IN 子句的語法
            query = """
            SELECT address, symbol, name, decimals, logo, supply
            FROM dex_query_v1.tokens 
            WHERE address = ANY(:token_addresses)
            """
            
            # 使用參數化查詢
            result = await token_session.execute(
                text(query), 
                {'token_addresses': addresses_to_fetch}
            )
            
            # 處理查詢結果
            token_data_dict = cached_results.copy()
            for row in result:
                token_dict = dict(row._mapping)
                addr = token_dict['address']
                token_data_dict[addr] = token_dict
                # 更新緩存
                get_cached_token_info.cache_clear()  # 清除舊緩存
                get_cached_token_info(addr)  # 添加新緩存
            
            return token_data_dict
            
    except Exception as e:
        logger.error(f"Error getting token info batch: {str(e)}")
        logger.error(traceback.format_exc())
        return {}

def _process_buy_token_data(token_data, amount, value):
    """處理買入時的代幣數據，確保所有數值操作使用 Decimal 類型"""
    total_amount = Decimal(str(token_data["total_amount"]))
    total_cost = Decimal(str(token_data["total_cost"]))
    amount = Decimal(str(amount)) if not isinstance(amount, Decimal) else amount
    value = Decimal(str(value)) if not isinstance(value, Decimal) else value
    
    # 更新總量和總成本
    new_total_amount = total_amount + amount
    new_total_cost = total_cost + value
    
    # 計算平均買入價格
    avg_buy_price = new_total_cost / new_total_amount if new_total_amount > Decimal('0') else Decimal('0')
    
    # 更新代幣數據
    token_data.update({
        "total_amount": new_total_amount,
        "total_cost": new_total_cost,
        "avg_buy_price": avg_buy_price
    })
    
    return token_data

def _process_sell_token_data(token_data, amount, value):
    """處理賣出時的代幣數據，確保所有數值操作使用 Decimal 類型"""
    # 確保所有數值都是 Decimal 類型
    total_amount = Decimal(str(token_data["total_amount"]))
    amount = Decimal(str(amount)) if not isinstance(amount, Decimal) else amount
    value = Decimal(str(value)) if not isinstance(value, Decimal) else value
    total_cost = Decimal(str(token_data["total_cost"]))
    
    if total_amount <= Decimal('0'):
        token_data.update({
            "total_amount": Decimal('0'),
            "total_cost": Decimal('0'),
            "pnl": value,
            "pnl_percentage": Decimal('100'),
            "sell_percentage": Decimal('100')
        })
        return token_data
        
    # 計算賣出百分比
    sell_percentage = min((amount / (amount + total_amount)) * Decimal('100'), Decimal('100'))
    
    # 計算平均買入價格
    avg_buy_price = total_cost / total_amount if total_amount > Decimal('0') else Decimal('0')
    
    # 計算賣出價格
    sell_price = value / amount if amount > Decimal('0') else Decimal('0')
    
    # 計算新的總量
    new_total_amount = max(Decimal('0'), total_amount - amount)
    
    # 計算利潤
    pnl = (sell_price - avg_buy_price) * amount
    
    # 計算利潤百分比
    pnl_percentage = ((sell_price / avg_buy_price) - Decimal('1')) * Decimal('100') if avg_buy_price > Decimal('0') else Decimal('0')
    
    # 更新代幣數據
    token_data.update({
        "pnl": pnl,
        "pnl_percentage": pnl_percentage,
        "sell_percentage": sell_percentage,
        "total_amount": new_total_amount,
        "total_cost": Decimal('0') if new_total_amount <= amount else total_cost
    })
    
    return token_data

async def get_wallet_addresses_from_summary(session):
    """從 WalletSummary 表獲取所有錢包地址"""
    try:
        await session.execute(text("SET search_path TO dex_query_v1;"))
        WalletSummary.__table__.schema = "dex_query_v1"  # 根據您的實際 schema 設置
        
        # 查詢所有錢包地址
        query = select(WalletSummary.wallet_address).distinct()
        result = await session.execute(query)
        wallet_addresses = result.scalars().all()

        return list(wallet_addresses)
    except Exception as e:
        logger.error(f"獲取錢包地址時發生錯誤: {str(e)}")
        return []

async def get_existing_signatures(session, wallet_addresses):
    """獲取指定錢包的現有交易簽名"""
    await session.execute(text("SET search_path TO dex_query_v1;"))
    Transaction.__table__.schema = "dex_query_v1"  # 根據您的實際 schema 設置
    
    query = select(Transaction.signature).where(
        Transaction.wallet_address.in_(wallet_addresses)
    )
    result = await session.execute(query)
    signatures = set(result.scalars().all())
    
    return signatures

async def get_wallet_trades_batch(session, wallet_addresses: List[str]):
    """批量獲取錢包交易記錄"""
    try:
        # 設置 schema
        await session.execute(text("SET search_path TO dex_query_v1;"))
        
        # 將地址轉換為 checksum 格式
        checksum_addresses = [to_checksum_address(addr) for addr in wallet_addresses]
        
        # 構建查詢
        query = text("""
            SELECT 
                id, chain_id, token_in, token_out, amount_in, amount_out,
                decimals_in, decimals_out, base_amount, quote_amount,
                base_decimals, quote_decimals, base_balance, quote_balance,
                price_usd, price, side, dex, pair_address, created_at,
                signer, tx_hash, block_number, block_timestamp, timestamp,
                log_index, tx_index, ins_index, inner_ins_index, payload
            FROM trades 
            WHERE chain_id = 9006 
            AND signer = ANY(:wallet_addresses)
            ORDER BY timestamp DESC
        """)
        
        # 執行查詢
        result = await session.execute(query, {"wallet_addresses": checksum_addresses})
        trades = result.mappings().all()
        
        # 將結果轉換為字典列表
        trades_list = []
        for trade in trades:
            trade_dict = dict(trade)
            # 確保數值類型正確
            for key in ['amount_in', 'amount_out', 'base_amount', 'quote_amount', 
                       'base_balance', 'quote_balance', 'price_usd', 'price']:
                if key in trade_dict and trade_dict[key] is not None:
                    trade_dict[key] = Decimal(str(trade_dict[key]))
            trades_list.append(trade_dict)
        
        return trades_list
        
    except Exception as e:
        logger.error(f"獲取錢包交易記錄時發生錯誤: {str(e)}")
        raise

async def get_wallet_token_buy_data(wallet_address, session):
    """獲取指定錢包的所有代幣購買數據"""
    try:
        TokenBuyData.__table__.schema = "dex_query_v1"
        
        query = select(TokenBuyData).where(
            TokenBuyData.wallet_address == wallet_address
        )
        result = await session.execute(query)
        token_buy_data_list = result.scalars().all()
        
        token_buy_data_dict = {}
        for data in token_buy_data_list:
            token_buy_data_dict[data.token_address] = {
                "token_address": data.token_address,
                "avg_buy_price": Decimal(str(data.avg_buy_price)) if data.avg_buy_price else Decimal('0'),
                "total_amount": Decimal(str(data.total_amount)) if data.total_amount else Decimal('0'),
                "total_cost": Decimal(str(data.total_cost)) if data.total_cost else Decimal('0'),
                "historical_total_buy_amount": Decimal(str(data.historical_total_buy_amount)) if data.historical_total_buy_amount else Decimal('0'),
                "historical_total_buy_cost": Decimal(str(data.historical_total_buy_cost)) if data.historical_total_buy_cost else Decimal('0'),
                "historical_avg_buy_price": Decimal(str(data.historical_avg_buy_price)) if data.historical_avg_buy_price else Decimal('0'),
                "historical_total_sell_amount": Decimal(str(data.historical_total_sell_amount)) if data.historical_total_sell_amount else Decimal('0'),
                "historical_total_sell_value": Decimal(str(data.historical_total_sell_value)) if data.historical_total_sell_value else Decimal('0'),
                "historical_avg_sell_price": Decimal(str(data.historical_avg_sell_price)) if data.historical_avg_sell_price else Decimal('0'),
                "total_buy_count": data.total_buy_count if data.total_buy_count else 0,
                "total_sell_count": data.total_sell_count if data.total_sell_count else 0,
                "last_transaction_time": data.last_transaction_time if data.last_transaction_time else 0
            }
        
        return token_buy_data_dict
    
    except Exception as e:
        logger.error(f"獲取錢包 {wallet_address} 的代幣購買數據時發生錯誤: {str(e)}")
        return {}

async def process_trades_and_update_wallets(wallet, trades, session, bnb_price, main_engine, logger, balance_usd):
    """處理錢包交易並更新錢包數據"""
    try:
        # 初始化或獲取現有的代幣買入數據
        token_buy_data = {}
        processed_signatures = set()
        transactions_to_save = []

        # 收集所有代幣地址
        token_addresses = set()
        for trade in trades:
            token_addresses.add(trade['token_in'])
            token_addresses.add(trade['token_out'])
        
                # 獲取代幣信息
        token_info_map = await get_token_info_from_Ian(list(token_addresses), main_engine)
                
        # 按時間戳排序交易
        sorted_trades = sorted(trades, key=lambda x: x['timestamp'])
        
        # 持倉追蹤map：{token_address: {'amount': Decimal, 'cost': Decimal}}
        token_holding_map = {}
        
        for trade in sorted_trades:
            try:
                if trade['tx_hash'] in processed_signatures:
                    continue
                
                # 根據 side 決定 amount 與 decimals
                if trade['side'] == 1:  # buy
                    amount = Decimal(str(trade['amount_in'])) / Decimal(10 ** trade['decimals_in'])
                    decimals = trade['decimals_in']
                else:  # sell
                    amount = Decimal(str(trade['amount_out'])) / Decimal(10 ** trade['decimals_out'])
                    decimals = trade['decimals_out']
                
                token_address = trade['token_in'] if trade['side'] == 1 else trade['token_out']
                price_usd = Decimal(str(trade['price_usd']))
                value = amount * price_usd

                # 取得處理前的持倉與成本
                prev_holding = token_holding_map.get(token_address, {'amount': Decimal('0'), 'cost': Decimal('0')})
                prev_amount = prev_holding['amount']
                prev_cost = prev_holding['cost']
                avg_buy_price = prev_cost / prev_amount if prev_amount > 0 else Decimal('0')

                # 預設值為 0
                realized_profit = Decimal('0')
                realized_profit_percentage = Decimal('0')
                holding_percentage = Decimal('0')
                
                # 判斷交易類型與計算持倉
                if trade['side'] == 1:  # buy
                    if prev_amount == 0:
                        transaction_type = 'build'
                    else:
                        transaction_type = 'buy'
                    # 更新持倉
                    new_amount = prev_amount + amount
                    new_cost = prev_cost + value
                    # 買入時，realized_profit 和 percentage 都應為 0
                    realized_profit = Decimal('0')
                    realized_profit_percentage = Decimal('0')
                else:  # sell
                    if prev_amount > 0:
                        holding_percentage = min((amount / prev_amount) * Decimal('100'), Decimal('100'))
                    after_amount = prev_amount - amount
                    if prev_amount > 0:
                        # 賣出時計算 realized_profit 和 percentage
                        realized_profit = (price_usd - avg_buy_price) * amount
                        if avg_buy_price > 0:
                            realized_profit_percentage = ((price_usd / avg_buy_price - 1) * 100)
                            realized_profit_percentage = max(realized_profit_percentage, Decimal('-100'))
                        else:
                            realized_profit_percentage = Decimal('0')
                    else:
                        realized_profit = Decimal('0')
                        realized_profit_percentage = Decimal('0')

                    if after_amount <= 0:
                        transaction_type = 'clean'
                        new_amount = Decimal('0')
                        new_cost = Decimal('0')
                    else:
                        transaction_type = 'sell'
                        new_amount = after_amount
                        new_cost = prev_cost * (new_amount / prev_amount) if prev_amount > 0 else Decimal('0')
                # 更新持倉map
                token_holding_map[token_address] = {'amount': new_amount, 'cost': new_cost}

                # 補齊 from/dest token 欄位
                if trade['side'] == 1:  # buy
                    from_token_address = trade.get('quoteMint', '') or ''
                    dest_token_address = trade.get('baseMint', '') or ''
                    from_token_symbol = trade.get('quoteSymbol', '') or ''
                    dest_token_symbol = trade.get('baseSymbol', '') or ''
                    from_token_amount = trade.get('fromTokenAmount', 0) or 0
                    dest_token_amount = trade.get('toTokenAmount', 0) or 0
                else:  # sell
                    from_token_address = trade.get('baseMint', '') or ''
                    dest_token_address = trade.get('quoteMint', '') or ''
                    from_token_symbol = trade.get('baseSymbol', '') or ''
                    dest_token_symbol = trade.get('quoteSymbol', '') or ''
                    from_token_amount = trade.get('fromTokenAmount', 0) or 0
                    dest_token_amount = trade.get('toTokenAmount', 0) or 0

                if not from_token_symbol:
                    try:
                        info = await TokenInfoFetcher.get_token_info(from_token_address.lower())
                        if info and from_token_address.lower() in info:
                            token_meta = info[from_token_address.lower()]
                            from_token_symbol = token_meta.get('symbol', '') or token_meta.get('name', '') or ''
                    except Exception:
                        from_token_symbol = ''

                if not dest_token_symbol:
                    try:
                        info = await TokenInfoFetcher.get_token_info(dest_token_address.lower())
                        if info and dest_token_address.lower() in info:
                            token_meta = info[dest_token_address.lower()]
                            dest_token_symbol = token_meta.get('symbol', '') or token_meta.get('name', '') or ''
                    except Exception:
                        dest_token_symbol = ''

                transaction = {
                    'wallet_address': wallet.wallet_address,
                    'wallet_balance': float(balance_usd),
                    'token_address': token_address,
                    'amount': float(amount),
                    'value': float(value),
                    'price': float(price_usd),
                    'transaction_type': transaction_type,
                    'transaction_time': trade['timestamp'],
                    'signature': trade['tx_hash'],
                    'chain': 'BSC',
                    'chain_id': 9006,
                    'chain_id': trade['chain_id'],
                    'from_token_address': from_token_address,
                    'from_token_symbol': from_token_symbol,
                    'from_token_amount': float(from_token_amount),
                    'dest_token_address': dest_token_address,
                    'dest_token_symbol': dest_token_symbol,
                    'dest_token_amount': float(dest_token_amount),
                    'realized_profit': float(realized_profit),
                    'realized_profit_percentage': float(realized_profit_percentage),
                    'holding_percentage': float(holding_percentage),
                    'time': get_utc8_time(),
                }
                
                # 添加代幣信息與計算 marketcap
                if trade['token_in'] in token_info_map:
                    token_info = token_info_map[trade['token_in']]
                    try:
                        supply = Decimal(str(token_info.get('supply', 0)))
                    except decimal.InvalidOperation:
                        # logger.warning(f"代币 {trade['token_in']} 的 supply 值无效: {token_info.get('supply')}")
                        supply = Decimal('0')
                    
                    marketcap = price_usd * supply if supply > 0 else Decimal('0')
                    if marketcap > 10000000000:
                        marketcap = Decimal('0')
                    transaction.update({
                        'token_name': token_info.get('name'),
                        'token_icon': token_info.get('logo'),
                        'marketcap': float(marketcap)  # 使用計算出的 marketcap
                    })
                else:
                    transaction.update({
                        'token_name': None,
                        'token_icon': None,
                        'marketcap': 0.0
                    })
                
                transactions_to_save.append(transaction)
                processed_signatures.add(trade['tx_hash'])
                
            except Exception as e:
                logger.error(f"處理交易時發生錯誤: {str(e)}")
                logger.info(f"處理交易數據: {trade}")
                continue

        # 批量保存交易記錄
        if transactions_to_save:
            try:
                for tx in transactions_to_save:
                    await save_past_transaction(session, tx, tx['wallet_address'], tx['signature'], tx['chain'])
                await session.commit()  # 顯式提交事務
                logger.info(f"成功保存 {len(transactions_to_save)} 筆交易記錄")
            except Exception as e:
                logger.error(f"保存交易記錄時發生錯誤: {str(e)}")
                await session.rollback() # 保存失敗時回滾
                raise
        
        return transactions_to_save

    except Exception as e:
        logger.error(f"處理錢包交易時發生錯誤: {str(e)}")
        raise

async def analyze_wallet_transactions(transactions):
    """分析錢包交易歷史，返回統計數據"""
    wallet_stats = {
        'buy': [],
        'sell': [],
        'remaining_tokens': {},
        'total_buy_value': Decimal(0),
        'total_sell_value': Decimal(0),
        'total_buy_count': 0,
        'total_sell_count': 0,
        'total_pnl': Decimal(0),
        'total_pnl_percentage': Decimal(0),
        'win_count': 0,
        'loss_count': 0,
        'total_win_pnl': Decimal(0),
        'total_loss_pnl': Decimal(0),
        'avg_win_pnl': Decimal(0),
        'avg_loss_pnl': Decimal(0),
        'max_win_pnl': Decimal(0),
        'max_loss_pnl': Decimal(0),
        'max_win_pnl_percentage': Decimal(0),
        'max_loss_pnl_percentage': Decimal(0),
        'total_holding_seconds': 0,
        'last_transaction_time': 0
    }
    
    # 按時間排序交易
    sorted_transactions = sorted(transactions, key=lambda x: x.transaction_time)
    
    for tx in sorted_transactions:
        action = tx.transaction_type
        token = tx.token_address
        amount = Decimal(str(tx.amount))
        value = Decimal(str(tx.value))
        
        # 統一時間戳為秒
        timestamp = tx.transaction_time
        if timestamp > 9999999999: # 檢查是否為毫秒
            timestamp = timestamp / 1000
        
        if action in ['buy', 'build']:
            wallet_stats['buy'].append({
                'token': token,
                'amount': amount,
                'value': value,
                'timestamp': timestamp
            })
            wallet_stats['total_buy_value'] += value
            wallet_stats['total_buy_count'] += 1
            
            wallet_stats['remaining_tokens'].setdefault(token, {
                'amount': Decimal(0), 
                'cost': Decimal(0),
                'first_buy_time': timestamp
            })
            wallet_stats['remaining_tokens'][token]['amount'] += amount
            wallet_stats['remaining_tokens'][token]['cost'] += value
            
        elif action in ['sell', 'clean']:
            if token in wallet_stats['remaining_tokens']:
                wallet_stats['remaining_tokens'][token]['amount'] -= amount
                
                # 計算已實現盈虧
                if wallet_stats['remaining_tokens'][token]['amount'] <= 0:
                    # 清空該代幣的持倉
                    wallet_stats['remaining_tokens'].pop(token)
                
            wallet_stats['sell'].append({
                'token': token,
                'amount': amount,
                'value': value,
                'timestamp': timestamp
            })
            wallet_stats['total_sell_value'] += value
            wallet_stats['total_sell_count'] += 1
            
        # 更新最後交易時間
        if timestamp > wallet_stats['last_transaction_time']:
            wallet_stats['last_transaction_time'] = timestamp
    
    return wallet_stats

async def analyze_wallets_data(wallet_stats, bnb_price):
    # 防呆：檢查 wallet_stats 是否為 {address: dict} 結構
    if not isinstance(wallet_stats, dict) or not all(isinstance(v, dict) for v in wallet_stats.values()):
        logger.error(f'analyze_wallets_data 收到的 wallet_stats 格式異常: {wallet_stats}')
        return {}
    wallet_analysis = {}
    current_time = int(time.time())  # 当前时间戳（秒）
    total_wallets = len(wallet_stats)
    processed_wallets = 0

    async def process_wallet(wallet, stats):
        nonlocal processed_wallets
        logger.info(f"開始分析錢包 {wallet} ({processed_wallets+1}/{total_wallets})的數據")
        wallet_start_time = time.time()

        total_buy = sum(Decimal(str(tx['value'])) for tx in stats['buy'])
        total_sell = sum(Decimal(str(tx['value'])) for tx in stats['sell'])
        pnl = stats.get('pnl', Decimal('0'))
        num_buy = len(stats['buy'])
        num_sell = len(stats['sell'])
        total_transactions = num_buy + num_sell

        # 盈利代币数量和胜率
        all_tokens = set([tx['token'] for tx in stats['buy']] + [tx['token'] for tx in stats['sell']])
        profitable_tokens = 0
        for token in all_tokens:
            token_buy = sum(Decimal(str(tx['value'])) for tx in stats['buy'] if tx['token'] == token)
            token_sell = sum(Decimal(str(tx['value'])) for tx in stats['sell'] if tx['token'] == token)
            if token_sell - token_buy > 0:
                profitable_tokens += 1
        total_tokens = len(all_tokens)
        win_rate = (profitable_tokens / total_tokens) * 100 if total_tokens > 0 else 0

        # 時間範圍
        one_day_ago = current_time - 24 * 60 * 60
        seven_days_ago = current_time - 7 * 24 * 60 * 60
        thirty_days_ago = current_time - 30 * 24 * 60 * 60

        def calculate_metrics(buy, sell, start_time):
            buy_filtered = [tx for tx in buy if tx['timestamp'] >= start_time]
            sell_filtered = [tx for tx in sell if tx['timestamp'] >= start_time]
            # 重新計算 win_rate for 這個區間
            tokens_in_range = set([tx['token'] for tx in buy_filtered] + [tx['token'] for tx in sell_filtered])
            profitable_tokens_in_range = 0
            for token in tokens_in_range:
                token_buy = sum(Decimal(str(tx['value'])) for tx in buy_filtered if tx['token'] == token)
                token_sell = sum(Decimal(str(tx['value'])) for tx in sell_filtered if tx['token'] == token)
                if token_sell - token_buy > 0:
                    profitable_tokens_in_range += 1
            total_tokens_in_range = len(tokens_in_range)
            win_rate_in_range = (profitable_tokens_in_range / total_tokens_in_range) * 100 if total_tokens_in_range > 0 else 0
            if not buy_filtered and not sell_filtered:
                return {
                    "win_rate": float(win_rate_in_range),
                    "total_cost": 0,
                    "avg_cost": 0,
                    "total_transaction_num": 0,
                    "buy_num": 0,
                    "sell_num": 0,
                    "pnl": 0,
                    "pnl_percentage": 0,
                    "avg_realized_profit": 0,
                    "unrealized_profit": 0
                }
            total_buy_value = sum(Decimal(str(tx['value'])) for tx in buy_filtered)
            total_sell_value = sum(Decimal(str(tx['value'])) for tx in sell_filtered)
            num_buy = len(buy_filtered)
            num_sell = len(sell_filtered)
            pnl_value = total_sell_value - total_buy_value
            pnl_percentage = Decimal('0')
            if total_buy_value > Decimal('0'):
                pnl_percentage = max((pnl_value / total_buy_value) * Decimal('100'), Decimal('-100'))
            avg_cost = Decimal('0')
            if num_buy > 0:
                avg_cost = total_buy_value / Decimal(num_buy)
            avg_realized_profit = Decimal('0')
            if num_sell > 0:
                avg_realized_profit = pnl_value / Decimal(num_sell)
            total_transaction_num = num_buy + num_sell
            return {
                "win_rate": float(win_rate_in_range),
                "total_cost": float(total_buy_value),
                "avg_cost": float(avg_cost),
                "total_transaction_num": total_transaction_num,
                "buy_num": num_buy,
                "sell_num": num_sell,
                "pnl": float(pnl_value),
                "pnl_percentage": float(pnl_percentage),
                "avg_realized_profit": float(avg_realized_profit),
                "unrealized_profit": 0
            }
        # ...其餘原有代碼保持不變...

        metrics_1d = calculate_metrics(stats['buy'], stats['sell'], one_day_ago)
        metrics_7d = calculate_metrics(stats['buy'], stats['sell'], seven_days_ago)
        metrics_30d = calculate_metrics(stats['buy'], stats['sell'], thirty_days_ago)
        # print(metrics_1d)
        # print(metrics_7d)
        # print(metrics_30d)

        # 型別防呆：確保 stats_1d/7d/30d 一定是 dict
        for k, v in zip(['stats_1d', 'stats_7d', 'stats_30d'], [metrics_1d, metrics_7d, metrics_30d]):
            if isinstance(v, list):
                logger.error(f"{k} 應為 dict，卻是 list，內容: {v}")
                v = {}

        asset_multiple = metrics_30d['pnl_percentage'] / 100 if metrics_30d['pnl_percentage'] != 0 else 0
        
        # 计算最后活跃的前三个代币
        all_tokens = stats['buy'] + stats['sell']
        sorted_tokens = sorted(all_tokens, key=lambda tx: tx['timestamp'], reverse=True)
        token_list = ",".join(
            list(dict.fromkeys(tx['token'] for tx in sorted_tokens))[:3]
        )

        # 计算 pnl_pic
        def calculate_daily_pnl(buy, sell, days, start_days_ago=None):
            daily_pnl = []
            start_days_ago = start_days_ago or days
            for day in range(start_days_ago):
                start_time = current_time - (day + 1) * 24 * 60 * 60
                end_time = current_time - day * 24 * 60 * 60
                daily_buy = sum(Decimal(str(tx['value'])) for tx in buy if start_time <= tx['timestamp'] < end_time)
                daily_sell = sum(Decimal(str(tx['value'])) for tx in sell if start_time <= tx['timestamp'] < end_time)
                daily_pnl.append(float(daily_sell - daily_buy))
            return ",".join(map(str, daily_pnl[::-1]))

        pnl_pic_1d = calculate_daily_pnl(stats['buy'], stats['sell'], 1)
        pnl_pic_7d = calculate_daily_pnl(stats['buy'], stats['sell'], 7)
        pnl_pic_30d = calculate_daily_pnl(stats['buy'], stats['sell'], 30)

        def calculate_distribution(sell, start_time):
            sell_filtered = [tx for tx in sell if tx['timestamp'] >= start_time]
            distribution = {"lt50": 0, "0to50": 0, "0to200": 0, "200to500": 0, "gt500": 0}
            total_buy_dec = total_buy if total_buy != Decimal('0') else Decimal('1')
            for tx in sell_filtered:
                tx_value = Decimal(str(tx['value']))
                if num_buy > 0:
                    pnl_percentage = ((tx_value - total_buy_dec) / total_buy_dec) * Decimal('100')
                else:
                    pnl_percentage = Decimal('0')
                if pnl_percentage < Decimal('-50'):
                    distribution["lt50"] += 1
                elif Decimal('-50') <= pnl_percentage < Decimal('0'):
                    distribution["0to50"] += 1
                elif Decimal('0') <= pnl_percentage <= Decimal('200'):
                    distribution["0to200"] += 1
                elif Decimal('200') < pnl_percentage <= Decimal('500'):
                    distribution["200to500"] += 1
                else:
                    distribution["gt500"] += 1
            return distribution

        def calculate_distribution_percentage(distribution, total_tokens):
            total_distribution = sum(distribution.values())
            return {key: float((Decimal(value) / Decimal(total_distribution) * Decimal('100'))) \
                    if total_distribution > 0 else 0 \
                    for key, value in distribution.items()}

        distribution_7d = calculate_distribution(stats['sell'], seven_days_ago)
        distribution_30d = calculate_distribution(stats['sell'], thirty_days_ago)
        distribution_percentage_7d = calculate_distribution_percentage(distribution_7d, total_tokens)
        distribution_percentage_30d = calculate_distribution_percentage(distribution_30d, total_tokens)

        # 获取钱包余额
        balances = await with_timeout(
            fetch_wallet_balances([wallet], bnb_price),
            timeout=30,
            description=f"獲取錢包 {wallet} 的餘額"
        )

        remaining_tokens = stats['remaining_tokens']
        tokens_count = len(remaining_tokens)
        if tokens_count > 10:
            logger.info(f"獲取錢包 {wallet} 的 {tokens_count} 個代幣信息")
        token_info_results = await with_timeout(
            fetch_token_info_for_wallets(remaining_tokens),
            timeout=60,
            description=f"獲取錢包 {wallet} 的代幣信息"
        )
        tx_data_list = []
        for token, data in remaining_tokens.items():
            token_info = token_info_results.get(token, {})
            supply = Decimal('0')
            if token_info and token_info.get('supply') is not None and token_info.get('decimals') is not None:
                supply = Decimal(str(token_info['supply']))
            token_info_Ian = await TokenInfoFetcher.get_token_info(token.lower())
            amount = Decimal(str(data.get("amount", 0)))
            profit = Decimal(str(data.get("profit", 0)))
            cost = Decimal(str(data.get("cost", 0)))
            last_transaction_time = max(
                tx['timestamp'] for tx in (stats['buy'] + stats['sell']) if tx['token'] == token
            )
            buy_transactions = [
                {
                    'amount': Decimal(str(remaining_tokens[token]['amount'])),
                    'cost': Decimal(str(remaining_tokens[token]['cost']))
                } 
                for token in remaining_tokens
            ]
            total_buy_value = sum(tx['cost'] for tx in buy_transactions)
            total_buy_amount = sum(tx['amount'] for tx in buy_transactions if tx['amount'] > Decimal('0'))
            avg_price = Decimal('0')
            if total_buy_amount > Decimal('0'):
                avg_price = total_buy_value / total_buy_amount
            token_info_price_native = Decimal(token_info.get("priceNative", "0"))
            token_info_price_usd = Decimal(token_info.get("priceUsd", "0"))
            pnl_value = profit - cost
            pnl_percentage = Decimal('0')
            if cost > Decimal('0'):
                pnl_percentage = (profit - cost) / cost * Decimal('100')
            
            marketcap = avg_price * supply
            if marketcap > 10000000000:
                marketcap = Decimal('0')
                
            remaining_tokens_summary = {
                "token_address": token,
                "token_name": token_info_Ian.get("symbol", None),
                "token_icon": token_info_Ian.get("logo", None),
                "chain": "BSC",
                "chain_id": 9006,
                "amount": float(amount),
                "value": float(amount * token_info_price_native),
                "value_USDT": float(amount * token_info_price_usd),
                "unrealized_profits": float(amount * token_info_price_usd),
                "pnl": float(pnl_value),
                "pnl_percentage": float(pnl_percentage),
                "marketcap": float(marketcap),
                "is_cleared": 0,
                "cumulative_cost": float(cost),
                "cumulative_profit": float(profit),
                "last_transaction_time": last_transaction_time,
                "time": get_update_time(),
                "avg_price": float(avg_price),
            }
            tx_data_list.append(remaining_tokens_summary)
        balance_usd = Decimal(str(balances[wallet]['balance_usd']))
        processed_wallets += 1
        wallet_end_time = time.time()
        wallet_duration = wallet_end_time - wallet_start_time
        logger.info(f"完成錢包 {wallet} 的數據分析，耗時: {wallet_duration:.2f}秒")
        return wallet, {
            'wallet_address': wallet,
            'balance': float(balances[wallet]['balance']),
            'balance_usd': float(balance_usd),
            'chain': "BSC",
            'chain_id': 9006,
            'tag': None,
            'twitter_name': None,
            'twitter_username': None,
            'is_smart_wallet': True,
            'wallet_type': 0,
            'asset_multiple': asset_multiple,
            'token_list': token_list,
            'avg_cost_30d': metrics_30d.get('avg_cost', 0),
            'avg_cost_7d': metrics_7d.get('avg_cost', 0),
            'avg_cost_1d': metrics_1d.get('avg_cost', 0),
            'total_transaction_num_30d': metrics_30d.get('total_transaction_num', 0),
            'total_transaction_num_7d': metrics_7d.get('total_transaction_num', 0),
            'total_transaction_num_1d': metrics_1d.get('total_transaction_num', 0),
            'buy_num_30d': metrics_30d.get('buy_num', 0),
            'buy_num_7d': metrics_7d.get('buy_num', 0),
            'buy_num_1d': metrics_1d.get('buy_num', 0),
            'sell_num_30d': metrics_30d.get('sell_num', 0),
            'sell_num_7d': metrics_7d.get('sell_num', 0),
            'sell_num_1d': metrics_1d.get('sell_num', 0),
            'win_rate_30d': metrics_30d.get('win_rate', 0),
            'win_rate_7d': metrics_7d.get('win_rate', 0),
            'win_rate_1d': metrics_1d.get('win_rate', 0),
            'pnl_30d': metrics_30d.get('pnl', 0),
            'pnl_7d': metrics_7d.get('pnl', 0),
            'pnl_1d': metrics_1d.get('pnl', 0),
            'pnl_percentage_30d': metrics_30d.get('pnl_percentage', 0),
            'pnl_percentage_7d': metrics_7d.get('pnl_percentage', 0),
            'pnl_percentage_1d': metrics_1d.get('pnl_percentage', 0),
            'pnl_pic_30d': pnl_pic_30d,
            'pnl_pic_7d': pnl_pic_7d,
            'pnl_pic_1d': pnl_pic_1d,
            'unrealized_profit_30d': metrics_30d.get('unrealized_profit', 0),
            'unrealized_profit_7d': metrics_7d.get('unrealized_profit', 0),
            'unrealized_profit_1d': metrics_1d.get('unrealized_profit', 0),
            'total_cost_30d': metrics_30d.get('total_cost', 0),
            'total_cost_7d': metrics_7d.get('total_cost', 0),
            'total_cost_1d': metrics_1d.get('total_cost', 0),
            'avg_realized_profit_30d': metrics_30d.get('avg_realized_profit', 0),
            'avg_realized_profit_7d': metrics_7d.get('avg_realized_profit', 0),
            'avg_realized_profit_1d': metrics_1d.get('avg_realized_profit', 0),
            'distribution_gt500_30d': distribution_30d.get('gt500', 0),
            'distribution_200to500_30d': distribution_30d.get('200to500', 0),
            'distribution_0to200_30d': distribution_30d.get('0to200', 0),
            'distribution_0to50_30d': distribution_30d.get('0to50', 0),
            'distribution_lt50_30d': distribution_30d.get('lt50', 0),
            'distribution_gt500_percentage_30d': distribution_percentage_30d.get('gt500', 0),
            'distribution_200to500_percentage_30d': distribution_percentage_30d.get('200to500', 0),
            'distribution_0to200_percentage_30d': distribution_percentage_30d.get('0to200', 0),
            'distribution_0to50_percentage_30d': distribution_percentage_30d.get('0to50', 0),
            'distribution_lt50_percentage_30d': distribution_percentage_30d.get('lt50', 0),
            'distribution_gt500_7d': distribution_7d.get('gt500', 0),
            'distribution_200to500_7d': distribution_7d.get('200to500', 0),
            'distribution_0to200_7d': distribution_7d.get('0to200', 0),
            'distribution_0to50_7d': distribution_7d.get('0to50', 0),
            'distribution_lt50_7d': distribution_7d.get('lt50', 0),
            'distribution_gt500_percentage_7d': distribution_percentage_7d.get('gt500', 0),
            'distribution_200to500_percentage_7d': distribution_percentage_7d.get('200to500', 0),
            'distribution_0to200_percentage_7d': distribution_percentage_7d.get('0to200', 0),
            'distribution_0to50_percentage_7d': distribution_percentage_7d.get('0to50', 0),
            'distribution_lt50_percentage_7d': distribution_percentage_7d.get('lt50', 0),
            'update_time': get_update_time(),
            'last_transaction_time': stats['last_transaction_time'] if 'last_transaction_time' in stats else stats.get('last_transaction', 0),
            'is_active': True,
            'remaining_tokens': tx_data_list,
        }
    tasks = [process_wallet(wallet, stats) for wallet, stats in wallet_stats.items()]
    results = await asyncio.gather(*tasks)
    wallet_analysis = {wallet: analysis for wallet, analysis in results}
    return wallet_analysis

async def update_wallet_summary(wallet, session):
    """更新錢包摘要數據"""
    try:
        wallet_address = wallet.wallet_address
        logger.info(f"準備更新錢包 {wallet_address} 的數據")
        
        # 準備錢包數據
        wallet_data = {
            'wallet_address': wallet_address,
            'balance': wallet.balance,
            'balance_usd': wallet.balance_usd,
            'chain': wallet.chain,
            'chain_id': wallet.chain_id,
            'tag': wallet.tag,
            'twitter_name': wallet.twitter_name,
            'twitter_username': wallet.twitter_username,
            'is_smart_wallet': wallet.is_smart_wallet,
            'wallet_type': wallet.wallet_type,
            'asset_multiple': wallet.asset_multiple,
            'token_list': wallet.token_list,
            'stats_30d': wallet.stats_30d,
            'stats_7d': wallet.stats_7d,
            'stats_1d': wallet.stats_1d,
            'pnl_pic_30d': wallet.pnl_pic_30d,
            'pnl_pic_7d': wallet.pnl_pic_7d,
            'pnl_pic_1d': wallet.pnl_pic_1d,
            'distribution_gt500_30d': wallet.distribution_gt500_30d,
            'distribution_200to500_30d': wallet.distribution_200to500_30d,
            'distribution_0to200_30d': wallet.distribution_0to200_30d,
            'distribution_0to50_30d': wallet.distribution_0to50_30d,
            'distribution_lt50_30d': wallet.distribution_lt50_30d,
            'distribution_gt500_percentage_30d': wallet.distribution_gt500_percentage_30d,
            'distribution_200to500_percentage_30d': wallet.distribution_200to500_percentage_30d,
            'distribution_0to200_percentage_30d': wallet.distribution_0to200_percentage_30d,
            'distribution_0to50_percentage_30d': wallet.distribution_0to50_percentage_30d,
            'distribution_lt50_percentage_30d': wallet.distribution_lt50_percentage_30d,
            'distribution_gt500_7d': wallet.distribution_gt500_7d,
            'distribution_200to500_7d': wallet.distribution_200to500_7d,
            'distribution_0to200_7d': wallet.distribution_0to200_7d,
            'distribution_0to50_7d': wallet.distribution_0to50_7d,
            'distribution_lt50_7d': wallet.distribution_lt50_7d,
            'distribution_gt500_percentage_7d': wallet.distribution_gt500_percentage_7d,
            'distribution_200to500_percentage_7d': wallet.distribution_200to500_percentage_7d,
            'distribution_0to200_percentage_7d': wallet.distribution_0to200_percentage_7d,
            'distribution_0to50_percentage_7d': wallet.distribution_0to50_percentage_7d,
            'distribution_lt50_percentage_7d': wallet.distribution_lt50_percentage_7d,
            'update_time': wallet.update_time,
            'last_transaction': wallet.last_transaction,
            'is_active': wallet.is_active
        }
        
        logger.info(f"Twitter 信息: name={wallet_data['twitter_name']}, username={wallet_data['twitter_username']}")
        logger.info("錢包數據摘要:")
        logger.info(f"- balance: {wallet_data['balance']}")
        logger.info(f"- balance_usd: {wallet_data['balance_usd']}")
        logger.info(f"- stats_30d: {wallet_data['stats_30d']}")
        logger.info(f"- stats_7d: {wallet_data['stats_7d']}")
        logger.info(f"- stats_1d: {wallet_data['stats_1d']}")
        
        # 準備更新語句
        stmt = insert(WalletSummary).values(
            wallet_address=wallet_data['wallet_address'],
            balance=wallet_data['balance'],
            balance_usd=wallet_data['balance_usd'],
            chain=wallet_data['chain'],
            chain_id=wallet_data['chain_id'],
            tag=wallet_data['tag'],
            twitter_name=wallet_data['twitter_name'],
            twitter_username=wallet_data['twitter_username'],
            is_smart_wallet=wallet_data['is_smart_wallet'],
            wallet_type=wallet_data['wallet_type'],
            asset_multiple=wallet_data['asset_multiple'],
            token_list=wallet_data['token_list'],
            avg_cost_30d=wallet_data['stats_30d']['avg_cost'],
            avg_cost_7d=wallet_data['stats_7d']['avg_cost'],
            avg_cost_1d=wallet_data['stats_1d']['avg_cost'],
            total_transaction_num_30d=wallet_data['stats_30d']['total_transaction_num'],
            total_transaction_num_7d=wallet_data['stats_7d']['total_transaction_num'],
            total_transaction_num_1d=wallet_data['stats_1d']['total_transaction_num'],
            buy_num_30d=wallet_data['stats_30d']['buy_num'],
            buy_num_7d=wallet_data['stats_7d']['buy_num'],
            buy_num_1d=wallet_data['stats_1d']['buy_num'],
            sell_num_30d=wallet_data['stats_30d']['sell_num'],
            sell_num_7d=wallet_data['stats_7d']['sell_num'],
            sell_num_1d=wallet_data['stats_1d']['sell_num'],
            win_rate_30d=wallet_data['stats_30d']['win_rate'],
            win_rate_7d=wallet_data['stats_7d']['win_rate'],
            win_rate_1d=wallet_data['stats_1d']['win_rate'],
            pnl_30d=wallet_data['stats_30d']['pnl'],
            pnl_7d=wallet_data['stats_7d']['pnl'],
            pnl_1d=wallet_data['stats_1d']['pnl'],
            pnl_percentage_30d=wallet_data['stats_30d']['pnl_percentage'],
            pnl_percentage_7d=wallet_data['stats_7d']['pnl_percentage'],
            pnl_percentage_1d=wallet_data['stats_1d']['pnl_percentage'],
            pnl_pic_30d=wallet_data['pnl_pic_30d'],
            pnl_pic_7d=wallet_data['pnl_pic_7d'],
            pnl_pic_1d=wallet_data['pnl_pic_1d'],
            unrealized_profit_30d=wallet_data['stats_30d']['unrealized_profit'],
            unrealized_profit_7d=wallet_data['stats_7d']['unrealized_profit'],
            unrealized_profit_1d=wallet_data['stats_1d']['unrealized_profit'],
            total_cost_30d=wallet_data['stats_30d']['total_cost'],
            total_cost_7d=wallet_data['stats_7d']['total_cost'],
            total_cost_1d=wallet_data['stats_1d']['total_cost'],
            avg_realized_profit_30d=wallet_data['stats_30d']['avg_realized_profit'],
            avg_realized_profit_7d=wallet_data['stats_7d']['avg_realized_profit'],
            avg_realized_profit_1d=wallet_data['stats_1d']['avg_realized_profit'],
            distribution_gt500_30d=wallet_data['distribution_gt500_30d'],
            distribution_200to500_30d=wallet_data['distribution_200to500_30d'],
            distribution_0to200_30d=wallet_data['distribution_0to200_30d'],
            distribution_0to50_30d=wallet_data['distribution_0to50_30d'],
            distribution_lt50_30d=wallet_data['distribution_lt50_30d'],
            distribution_gt500_percentage_30d=wallet_data['distribution_gt500_percentage_30d'],
            distribution_200to500_percentage_30d=wallet_data['distribution_200to500_percentage_30d'],
            distribution_0to200_percentage_30d=wallet_data['distribution_0to200_percentage_30d'],
            distribution_0to50_percentage_30d=wallet_data['distribution_0to50_percentage_30d'],
            distribution_lt50_percentage_30d=wallet_data['distribution_lt50_percentage_30d'],
            update_time=wallet_data['update_time'],
            last_transaction_time=wallet_data['last_transaction'],
            is_active=wallet_data['is_active']
        )
        
        # 添加 ON CONFLICT 子句
        stmt = stmt.on_conflict_do_update(
            constraint='wallet_wallet_address_key',
            set_=dict(stmt.values)
        )
        
        logger.info("SQL 語句準備完成，開始執行更新")
        
        try:
            await session.execute(stmt)
            await session.commit()
            logger.info(f"成功更新錢包 {wallet_address} 的數據")
            return True
        except Exception as e:
            logger.error(f"更新錢包 {wallet_address} 摘要時發生錯誤: {str(e)}")
            logger.error(f"錯誤詳情: {type(e).__name__}")
            logger.error(f"錯誤堆疊: {traceback.format_exc()}")
            await session.rollback()
            raise
        
    except Exception as e:
        logger.error(f"更新錢包摘要時發生錯誤: {str(e)}")
        raise

async def process_wallet_batch(wallet_addresses, main_session_factory, swap_session_factory, main_engine, logger, twitter_names: Optional[List[str]] = None, twitter_usernames: Optional[List[str]] = None):
    """處理一批錢包的數據"""
    try:
        from web3 import Web3
        # 統一所有地址為 checksum 格式
        wallet_addresses = [Web3.to_checksum_address(addr) for addr in wallet_addresses]
        batch_start_time = time.time()
        logger.info(f"開始處理 {len(wallet_addresses)} 個錢包的批次")
        
        # 批次處理前，先獲取 BNB 價格和所有錢包的當前餘額
        try:
            bnb_price = await get_price()
            balances = await fetch_wallet_balances(wallet_addresses, bnb_price)
            logger.info(f"成功獲取 {len(balances)} 個錢包的當前餘額")
        except Exception as e:
            logger.error(f"獲取錢包餘額或BNB價格失敗: {e}, 將使用默認值 0")
            bnb_price = Decimal('0')
            balances = defaultdict(lambda: {'balance_usd': Decimal('0'), 'balance': Decimal('0')})
        
        # 獲取錢包交易數據（使用 main_session_factory 因為是查詢 trades 表）
        async with main_session_factory() as session:
            trades = await get_wallet_trades_batch(session, wallet_addresses)
            logger.info(f"獲取到 {len(trades)} 筆交易記錄")
            
        # 按錢包地址分組交易
        trades_by_wallet = defaultdict(list)
        for trade in trades:
            if isinstance(trade, dict) and 'signer' in trade:
                # 分組時也轉為 checksum 格式
                signer = Web3.to_checksum_address(trade['signer'])
                trades_by_wallet[signer].append(trade)
            else:
                logger.warning(f"跳過無效的交易數據: {trade}")
        
        logger.info(f"按錢包分組後，共有 {len(trades_by_wallet)} 個錢包有交易記錄")
            
        # 處理每個錢包的交易
        processed_trades_by_wallet = {}
        for i, wallet_address in enumerate(wallet_addresses):
            try:
                # 獲取對應的 Twitter 信息
                twitter_name = twitter_names[i] if twitter_names and i < len(twitter_names) else None
                twitter_username = twitter_usernames[i] if twitter_usernames and i < len(twitter_usernames) else None
                
                logger.info(f"處理錢包 {wallet_address}，Twitter信息: name={twitter_name}, username={twitter_username}")
                
                # 創建錢包對象
                wallet = WalletSummary(
                    wallet_address=wallet_address,
                    chain='BSC',
                    twitter_name=twitter_name,
                    twitter_username=twitter_username,
                    is_active=True,
                    update_time=datetime.now(timezone.utc)
                )
                
                # 獲取當前錢包的餘額
                wallet_balance_data = balances.get(wallet_address, {'balance_usd': Decimal('0'), 'balance': Decimal('0')})
                current_balance_usd = wallet_balance_data.get('balance_usd', Decimal('0'))
                
                # 處理所有交易
                async with swap_session_factory() as session:
                    await session.execute(text("SET search_path TO dex_query_v1;"))
                    
                    # 處理交易並更新錢包數據
                    processed_trades = await process_trades_and_update_wallets(
                            wallet, 
                        trades_by_wallet.get(wallet_address, []),
                        session,
                        bnb_price,
                            main_engine, 
                            logger,
                        current_balance_usd
                        )
                    
                    if processed_trades:
                        processed_trades_by_wallet[wallet] = processed_trades
                        logger.info(f"錢包 {wallet_address} 處理了 {len(processed_trades)} 筆交易")
                    else:
                        logger.info(f"錢包 {wallet_address} 沒有交易記錄")
                    
            except Exception as e:
                logger.error(f"處理錢包 {wallet_address} 時發生錯誤: {str(e)}")
                continue
        
        # 分析錢包數據並更新到數據庫
        updated_wallets = 0
        # bnb_price 已在批次處理前獲取
        # bnb_price = await get_price()
        for wallet, trades in processed_trades_by_wallet.items():
            try:
                logger.info(f"開始分析錢包 {wallet.wallet_address} 的交易數據")

                # 將 dict 轉為 Transaction 物件
                transaction_objects = []
                for tx in trades:
                    transaction_objects.append(
                        Transaction(
                            wallet_address=tx['wallet_address'],
                            token_address=tx['token_address'],
                            amount=tx['amount'],
                            value=tx['value'],
                            price=tx['price'],
                            transaction_type=tx['transaction_type'],
                            transaction_time=tx['transaction_time'],
                            signature=tx['signature'],
                            chain=tx['chain'],
                            chain_id=tx['chain_id'],
                            from_token_address=tx.get('from_token_address'),
                            from_token_symbol=tx.get('from_token_symbol'),
                            from_token_amount=tx.get('from_token_amount'),
                            dest_token_address=tx.get('dest_token_address'),
                            dest_token_symbol=tx.get('dest_token_symbol'),
                            dest_token_amount=tx.get('dest_token_amount'),
                        )
                    )

                # 分析錢包交易數據
                wallet_stats = await analyze_wallet_transactions(transaction_objects)
                wallet_analysis = await analyze_wallets_data({wallet.wallet_address: wallet_stats}, bnb_price)
                # logger.info(f"錢包 {wallet.wallet_address} 數據分析完成: {wallet_analysis}")

                # 判斷 tag
                tag = None
                if wallet.twitter_name and wallet.twitter_username:
                    tag = 'kol'
                    
                if wallet_analysis and wallet.wallet_address in wallet_analysis:
                    wallet_analysis[wallet.wallet_address]['tag'] = tag
                    async with swap_session_factory() as session:
                        success = await write_wallet_data_to_db(
                            session,
                            wallet_analysis[wallet.wallet_address],
                            "BSC", 
                            wallet.twitter_name,
                            wallet.twitter_username
                        )
                        if success:
                            updated_wallets += 1
                            logger.info(f"成功更新錢包 {wallet.wallet_address} 的數據到數據庫")
                            # 查詢剛寫入的 WalletSummary 並 push
                            async with swap_session_factory() as session2:
                                result = await session2.execute(
                                    select(WalletSummary).where(WalletSummary.wallet_address == wallet.wallet_address)
                                )
                                db_wallet = result.scalar_one_or_none()
                                if db_wallet:
                                    api_data = wallet_to_api_dict(db_wallet)
                                    if api_data:
                                        await push_wallet_to_api(api_data)
                        else:
                            logger.error(f"更新錢包 {wallet.wallet_address} 數據失敗")
                else:
                    logger.warning(f"錢包 {wallet.wallet_address} 沒有分析數據")
            except Exception as e:
                logger.error(f"更新錢包 {wallet.wallet_address} 數據時發生錯誤: {str(e)}")
                continue
        
        batch_end_time = time.time()
        batch_duration = batch_end_time - batch_start_time
        logger.info(f"完成處理 {len(wallet_addresses)} 個錢包的批次，耗時: {batch_duration:.2f}秒，成功更新 {updated_wallets} 個錢包")
        
        return {
            "processed_wallets": len(wallet_addresses),
            "updated_wallets": updated_wallets,
            "duration": batch_duration
        }
        
    except Exception as e:
        logger.error(f"處理錢包批次時發生錯誤: {str(e)}")
        raise

def setup_tasks(scheduler, main_session_factory, swap_session_factory, main_engine):
    """設置定時任務"""
    
    # 創建 Kafka 消費者實例
    kafka_consumer = KafkaConsumer(main_session_factory, swap_session_factory, main_engine)
    
    # 啟動 Kafka 消費者
    scheduler.add_job(
        kafka_consumer.start,
        'date',  # 立即執行一次
        id='kafka_consumer',
        replace_existing=True
    )
    logger.info("已設置 Kafka 消費者任務")

async def get_wallet_balance(wallet_address: str) -> Decimal:
    """使用 eth_getBalance 獲取錢包 BNB 餘額"""
    try:
        # 確保地址是 checksum 格式
        checksum_address = eth_to_checksum_address(wallet_address)
        # 獲取餘額（以 wei 為單位）
        balance_wei = w3.eth.get_balance(checksum_address)
        # 轉換為 BNB（除以 10^18）
        balance_bnb = Decimal(balance_wei) / Decimal(10**18)
        return balance_bnb
    except Exception as e:
        logger.error(f"獲取錢包 {wallet_address} 餘額時發生錯誤: {str(e)}")
        return Decimal('0')

class TokenBuyDataCache:
    def __init__(self):
        # {wallet_address: {token_address: {date: token_data}}}
        self._cache = {}
        self._last_flush_time = time.time()
        self._flush_interval = 300  # 5分鐘同步一次到資料庫
        self._dirty = set()  # 記錄需要更新的 (wallet_address, token_address, date)

    def get(self, wallet_address: str, token_address: str, date: datetime.date) -> dict:
        """獲取指定日期的代幣數據，如果不存在則返回默認值"""
        wallet_cache = self._cache.setdefault(wallet_address, {})
        token_cache = wallet_cache.setdefault(token_address, {})
        return token_cache.get(date, {
            "token_address": token_address,
            "date": date,
            "total_amount": Decimal('0'),
            "total_cost": Decimal('0'),
            "avg_buy_price": Decimal('0'),
            "position_opened_at": None,
            "historical_total_buy_amount": Decimal('0'),
            "historical_total_buy_cost": Decimal('0'),
            "historical_total_sell_amount": Decimal('0'),
            "historical_total_sell_value": Decimal('0'),
            "historical_avg_buy_price": Decimal('0'),
            "historical_avg_sell_price": Decimal('0'),
            "last_transaction_time": None,
            "realized_profit": Decimal('0'),
            "realized_profit_percentage": Decimal('0'),
            "total_buy_count": 0,
            "total_sell_count": 0,
            "total_holding_seconds": 0,
            "updated_at": get_utc8_time()
        })

    def update(self, wallet_address: str, token_address: str, date: datetime.date, action: str, amount: Decimal, value: Decimal, transaction_time: int):
        """更新緩存中的代幣數據"""
        token_data = self.get(wallet_address, token_address, date)
        current_time = transaction_time

        if action == 'buy':
            # 更新當前持倉數據
            token_data['total_amount'] += amount
            token_data['total_cost'] += value
            token_data['avg_buy_price'] = token_data['total_cost'] / token_data['total_amount'] if token_data['total_amount'] > 0 else Decimal('0')
            
            # 更新歷史買入數據
            token_data['historical_total_buy_amount'] += amount
            token_data['historical_total_buy_cost'] += value
            token_data['historical_avg_buy_price'] = (
                token_data['historical_total_buy_cost'] / token_data['historical_total_buy_amount']
                if token_data['historical_total_buy_amount'] > 0 else Decimal('0')
            )
            
            # 更新買入次數
            token_data['total_buy_count'] += 1
            
            # 更新開倉時間
            if token_data['position_opened_at'] is None:
                token_data['position_opened_at'] = current_time

        elif action == 'sell':
            # 計算賣出部分的成本和利潤
            if token_data['total_amount'] > 0:
                sell_ratio = min(amount / token_data['total_amount'], Decimal('1'))
                cost_basis = token_data['total_cost'] * sell_ratio
                profit = value - cost_basis
                profit_percentage = ((value / cost_basis) - Decimal('1')) * Decimal('100') if cost_basis > 0 else Decimal('0')
                
                # 更新當前持倉數據
                token_data['total_amount'] -= amount
                if token_data['total_amount'] <= 0:
                    token_data['total_cost'] = Decimal('0')
                    token_data['avg_buy_price'] = Decimal('0')
                    token_data['position_opened_at'] = None
                else:
                    token_data['total_cost'] *= (1 - sell_ratio)
                    token_data['avg_buy_price'] = token_data['total_cost'] / token_data['total_amount']
            else:
                # 超賣情況
                token_data['total_amount'] -= amount
                profit = Decimal('0')
                profit_percentage = Decimal('0')
            
            # 更新歷史賣出數據
            token_data['historical_total_sell_amount'] += amount
            token_data['historical_total_sell_value'] += value
            token_data['historical_avg_sell_price'] = (
                token_data['historical_total_sell_value'] / token_data['historical_total_sell_amount']
                if token_data['historical_total_sell_amount'] > 0 else Decimal('0')
            )
            
            # 更新已實現利潤
            token_data['realized_profit'] += profit
            token_data['realized_profit_percentage'] = max(profit_percentage, Decimal('-100'))
            
            # 更新賣出次數
            token_data['total_sell_count'] += 1

        # 更新最後交易時間
        token_data['last_transaction_time'] = current_time
        token_data['updated_at'] = get_utc8_time()

        # 更新緩存
        wallet_cache = self._cache.setdefault(wallet_address, {})
        token_cache = wallet_cache.setdefault(token_address, {})
        token_cache[date] = token_data
        self._dirty.add((wallet_address, token_address, date))

    async def maybe_flush(self, session):
        """如果距離上次同步超過間隔時間，則同步到資料庫"""
        current_time = time.time()
        if current_time - self._last_flush_time >= self._flush_interval and self._dirty:
            await self.flush(session)

    async def flush(self, session):
        """強制同步所有更改到資料庫"""
        try:
            for wallet_address, token_address, date in self._dirty:
                token_data = self._cache[wallet_address][token_address][date]
                await save_wallet_buy_data(
                    wallet_address=wallet_address,
                    token_address=token_address,
                    tx_data={**token_data, "date": date},
                    session=session,
                    chain="BSC",
                    auto_commit=True
                )
            self._dirty.clear()
            self._last_flush_time = time.time()
        except Exception as e:
            print(f"Error flushing cache to database: {e}")
            import traceback
            traceback.print_exc()

    def clear(self):
        """清空緩存"""
        self._cache.clear()
        self._dirty.clear()

# 創建全局緩存實例
token_buy_data_cache = TokenBuyDataCache()

if __name__ == "__main__":
    from config import DATABASE_URI, DATABASE_URI_SWAP_BSC
    import asyncio
    
    # 設置日誌格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger.info("Starting transaction sync service...")
    
    # 創建數據庫引擎和會話工廠
    main_engine = create_async_engine(DATABASE_URI, echo=False)
    swap_engine = create_async_engine(DATABASE_URI_SWAP_BSC, echo=False)
    
    main_session_factory = sessionmaker(
        bind=main_engine, 
        class_=AsyncSession, 
        expire_on_commit=False
    )
    
    swap_session_factory = sessionmaker(
        bind=swap_engine, 
        class_=AsyncSession, 
        expire_on_commit=False
    )
    
    logger.info("Database connections established")
    
    # 創建調度器
    scheduler = AsyncIOScheduler()
    setup_tasks(scheduler, main_session_factory, swap_session_factory, main_engine)
    
    # 定義主要的異步函數
    async def main():
        # 檢查並創建數據庫表
        async with main_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, checkfirst=True)
            logger.info("数据库表检查完成，不存在的表已创建。")

        # 啟動調度器
        scheduler.start()
        logger.info("Scheduler started")
        
        # 保持程序運行，直到按下 Ctrl+C
        try:
            while True:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            # 關閉調度器
            logger.info("Received shutdown signal, stopping scheduler...")
            scheduler.shutdown()
            logger.info("Scheduler stopped")
    
    # 運行主函數
    logger.info("Starting main loop...")
    asyncio.run(main())